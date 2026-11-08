import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from wyoming.event import Event

from wyoming_bridge.connections.processor import ProcessorConnection
from wyoming_bridge.processors.types import ProcessorId

@pytest.fixture
def processor_connection():
    return ProcessorConnection(uri="mock://processor", processor_id=ProcessorId("test_processor"))

async def cleanup_listener_task(processor_connection):
    if processor_connection._listen_task:
        processor_connection._listen_task.cancel()
        try:
            await processor_connection._listen_task
        except asyncio.CancelledError:
            pass

@pytest.mark.asyncio
async def test_connect(processor_connection):
    # Create a mock client that we can control
    mock_client = MagicMock()
    mock_client.connect = AsyncMock()
    mock_client.disconnect = AsyncMock()
    mock_client.read_event = AsyncMock(return_value=None)  # Ensure read_event task exits
    
    with patch("wyoming.client.AsyncClient.from_uri", return_value=mock_client):
        await processor_connection._connect()
        assert processor_connection._is_connected
        mock_client.connect.assert_called_once()
        
    await cleanup_listener_task(processor_connection)

@pytest.mark.asyncio
async def test_disconnect(processor_connection):
    # Create a mock client that we can control
    mock_client = MagicMock()
    mock_client.connect = AsyncMock()
    mock_client.disconnect = AsyncMock()
    mock_client.read_event = AsyncMock(return_value=None)  # Ensure read_event task exits
    
    with patch("wyoming.client.AsyncClient.from_uri", return_value=mock_client):
        await processor_connection._connect()
        await processor_connection._disconnect()
        assert not processor_connection._is_connected
        mock_client.disconnect.assert_called_once()
    await cleanup_listener_task(processor_connection)

@pytest.mark.asyncio
async def test_write_event_without_response(processor_connection):
    # Create a mock client that we can control
    mock_client = MagicMock()
    mock_client.connect = AsyncMock()
    mock_client.disconnect = AsyncMock()
    mock_client.write_event = AsyncMock()
    mock_client.read_event = AsyncMock(return_value=None)  # Ensure read_event task exits
    
    event = Event(type="test_event")
    with patch("wyoming.client.AsyncClient.from_uri", return_value=mock_client):
        await processor_connection._connect()
        await processor_connection.write_event(event, wait_for_response=False)
        mock_client.write_event.assert_called_once_with(event)
        
    await cleanup_listener_task(processor_connection)

@pytest.mark.asyncio
async def test_write_event_with_response(processor_connection):
    event = Event(type="test_event")
    response_event = Event(type="response_event")
    
    # Create a controlled mock client
    mock_client = MagicMock()
    mock_client.connect = AsyncMock()
    mock_client.disconnect = AsyncMock()
    mock_client.read_event = AsyncMock(return_value=None)  # Default return to stop background task
    
    with patch("wyoming.client.AsyncClient.from_uri", return_value=mock_client):
        await processor_connection._connect()
        
        # Mock the event handling to simulate a response
        async def mock_write_event_with_request_id(event_to_write):
            # Check for request_id in the event data (which write_event should add)
            assert 'data' in event_to_write.__dict__, "Event should have data attribute"
            assert 'request_id' in event_to_write.data, "Event should have request_id in data"
            request_id = event_to_write.data["request_id"]
            
            # Simulate responding to the request by calling the set_result on the waiting future
            response_event.data = {"request_id": request_id}
            if request_id in processor_connection._pending_responses:
                processor_connection._pending_responses[request_id].set_result(response_event)
            return None
            
        # Track if our mock was called
        mock_was_called = False
        
        # Wrap our function to track calls
        async def tracking_mock(event_to_write):
            nonlocal mock_was_called
            mock_was_called = True
            return await mock_write_event_with_request_id(event_to_write)
        
        # Replace the client's write_event method with our tracking mock
        mock_client.write_event = tracking_mock
        
        # Call the actual write_event method with wait_for_response
        response = await processor_connection.write_event(event, wait_for_response=True, timeout=1.0)
        
        assert response == response_event
        
        assert mock_was_called, "write_event was not called"
        
    await cleanup_listener_task(processor_connection)

@pytest.mark.asyncio
async def test_write_event_timeout(processor_connection):
    event = Event(type="test_event")
    
    # Create a controlled mock client
    mock_client = MagicMock()
    mock_client.connect = AsyncMock()
    mock_client.disconnect = AsyncMock()
    mock_client.write_event = AsyncMock()
    mock_client.read_event = AsyncMock(return_value=None)  # Default return to stop background task
    
    with patch("wyoming.client.AsyncClient.from_uri", return_value=mock_client):
        await processor_connection._connect()
        response = await processor_connection.write_event(event, wait_for_response=True, timeout=0.1)
        assert response is None
        
    await cleanup_listener_task(processor_connection)

@pytest.mark.asyncio
async def test_connect_disconnect_sequence():
    """Test that multiple connect/disconnect cycles work properly"""
    # Create a fresh instance for this test
    processor_connection = ProcessorConnection(
        uri="mock://processor", 
        processor_id=ProcessorId("test_processor")
    )
    
    # Create a mock client that we can control
    mock_client = MagicMock()
    mock_client.connect = AsyncMock()
    mock_client.disconnect = AsyncMock()
    mock_client.read_event = AsyncMock(return_value=None)  # This will cause the listen task to exit
    
    # Patch the client creation function to return our mock
    with patch("wyoming.client.AsyncClient.from_uri", return_value=mock_client):
        # First connect/disconnect cycle
        await processor_connection._connect()
        assert processor_connection._is_connected
        
        await processor_connection._disconnect()
        assert not processor_connection._is_connected
        
        # Second connect/disconnect cycle
        await processor_connection._connect()
        assert processor_connection._is_connected
        
        await processor_connection._disconnect()
        assert not processor_connection._is_connected
        
        # Verify connect and disconnect were each called twice
        assert mock_client.connect.call_count == 2
        assert mock_client.disconnect.call_count == 2

    await cleanup_listener_task(processor_connection)


@pytest.mark.asyncio
async def test_eof_marks_disconnected_and_releases_waiters(processor_connection):
    """If the processor closes the connection, blocked callers return at once
    instead of waiting out their timeout, and the next write reconnects."""
    read_gate = asyncio.Event()

    async def read_event():
        await read_gate.wait()
        return None  # clean EOF

    mock_client = MagicMock()
    mock_client.connect = AsyncMock()
    mock_client.disconnect = AsyncMock()
    mock_client.write_event = AsyncMock()
    mock_client.read_event = AsyncMock(side_effect=read_event)

    with patch("wyoming.client.AsyncClient.from_uri", return_value=mock_client):
        pending = asyncio.create_task(
            processor_connection.write_event(Event(type="transcript", data={}), wait_for_response=True, timeout=30)
        )
        await asyncio.sleep(0.01)
        read_gate.set()
        response = await asyncio.wait_for(pending, timeout=1)

    assert response is None
    assert not processor_connection._is_connected
    assert processor_connection._client is None
    assert processor_connection._listen_task is None
    mock_client.disconnect.assert_awaited_once()


@pytest.mark.asyncio
async def test_read_error_disconnects_without_self_cancel(processor_connection):
    """A read error inside the listener must fully clean up (it calls
    _disconnect() from within its own task)."""
    mock_client = MagicMock()
    mock_client.connect = AsyncMock()
    mock_client.disconnect = AsyncMock()
    mock_client.read_event = AsyncMock(side_effect=RuntimeError("socket broke"))

    with patch("wyoming.client.AsyncClient.from_uri", return_value=mock_client):
        await processor_connection._connect()
        await asyncio.sleep(0.01)

    assert not processor_connection._is_connected
    assert processor_connection._client is None
    mock_client.disconnect.assert_awaited_once()


@pytest.mark.asyncio
async def test_stale_listener_does_not_touch_new_client(processor_connection):
    """A listener for a replaced client exits without disconnecting the new one."""
    old_client = MagicMock()
    old_client.read_event = AsyncMock(return_value=None)
    old_client.disconnect = AsyncMock()
    new_client = MagicMock()
    new_client.disconnect = AsyncMock()

    processor_connection._client = new_client
    processor_connection._is_connected = True

    await processor_connection._listen_to_events(old_client)

    assert processor_connection._client is new_client
    assert processor_connection._is_connected
    new_client.disconnect.assert_not_called()
