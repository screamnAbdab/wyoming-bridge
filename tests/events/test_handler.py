import pytest
from unittest.mock import AsyncMock, MagicMock

from wyoming.event import Event
from wyoming.info import Describe

from wyoming_bridge.events.handler import WyomingEventHandler


def make_handler(bridge):
    handler = WyomingEventHandler(bridge, MagicMock(), MagicMock())
    handler.writer = MagicMock()
    return handler


@pytest.mark.asyncio
async def test_describe_does_not_take_over_server_binding():
    """A Describe-only connection must not steal the live session's writer."""
    bridge = MagicMock()
    bridge.event_handler_id = "live-session"
    bridge.bind_server_handler = AsyncMock()
    bridge.handle_describe = AsyncMock()
    bridge.on_server_event = AsyncMock()

    handler = make_handler(bridge)
    assert await handler.handle_event(Describe().event())

    bridge.handle_describe.assert_awaited_once_with(handler.writer)
    bridge.bind_server_handler.assert_not_called()
    bridge.on_server_event.assert_not_called()


@pytest.mark.asyncio
async def test_session_event_binds_server_and_forwards():
    bridge = MagicMock()
    bridge.event_handler_id = None
    bridge.bind_server_handler = AsyncMock()
    bridge.handle_describe = AsyncMock()
    bridge.on_server_event = AsyncMock()

    handler = make_handler(bridge)
    event = Event(type="audio-start", data={"rate": 16000, "width": 2, "channels": 1})
    assert await handler.handle_event(event)

    bridge.bind_server_handler.assert_awaited_once_with(handler.event_handler_id, handler.writer)
    bridge.on_server_event.assert_awaited_once_with(event)
    bridge.handle_describe.assert_not_called()
