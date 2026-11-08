import asyncio
import logging
from typing import Optional, Dict, Tuple

from wyoming.audio import AudioChunk
from wyoming.client import AsyncClient
from wyoming.event import Event, async_write_event
from wyoming.info import Describe, Info

from wyoming_bridge.connections.processor import ProcessorConnection
from wyoming_bridge.connections.target import WyomingTargetConnection
from wyoming_bridge.connections.server import ServerConnection
from wyoming_bridge.core.info import wrap_wyoming_info, read_service_type
from wyoming_bridge.integrations.homeassistant import Homeassistant
from wyoming_bridge.processors.types import Processors, ProcessorId, SubscriptionMode, SubscriptionStage
from wyoming_bridge.events.pipeline import Pipeline, ProcessorEntry, Subscriptions

_LOGGER = logging.getLogger("bridge")

# How long to wait for the target to answer a Describe.
DESCRIBE_TIMEOUT = 10.0
# Backoff bounds for the startup handshake retry loop.
INIT_RETRY_MIN = 1.0
INIT_RETRY_MAX = 30.0

class WyomingBridge:
    def __init__(self, target_uri: str, processors: Processors, hass: Homeassistant) -> None:
        _LOGGER.debug("Initializing WyomingBridge")
        self._target_uri = target_uri
        self._processors = processors
        self._hass = hass
        self._running = False

        self._server = ServerConnection()
        self._target: Optional[WyomingTargetConnection] = None

        self._subscriptions: Dict[Tuple[SubscriptionStage, SubscriptionMode], Subscriptions] = {}
        self._build_subscriptions_index()
        
        self._downstream: Optional[Pipeline] = None
        self._upstream: Optional[Pipeline] = None

        # Last wrapped Info from the target, served to Describe requests.
        self._info_event: Optional[Event] = None
        self._target_lock = asyncio.Lock()
        self._init_task: Optional[asyncio.Task] = None

    @property
    def event_handler_id(self) -> Optional[str]:
        return self._server.event_handler_id

    def _build_subscriptions_index(self) -> None:
        self._subscriptions.clear()

        for processor in self._processors:
            for sub in processor["subscriptions"]:

                key = (sub["stage"], sub["mode"])

                if key not in self._subscriptions:
                    self._subscriptions[key] = {}
                
                if sub["event"] not in self._subscriptions[key]:
                    self._subscriptions[key][sub["event"]] = []

                entry = ProcessorEntry(
                    processor_id=ProcessorId(processor["id"]),
                    connection=ProcessorConnection(
                        uri=processor["uri"],
                        processor_id=ProcessorId(processor["id"]),
                    )
                )
                self._subscriptions[key][sub["event"]].append(entry)

    def _build_event_flows(self) -> None:
        if not self._target:
            _LOGGER.warning("No target connection available")
            self._downstream = None
            return
        
        if not self._server:
            _LOGGER.warning("No server connection available")
            self._upstream = None
            return

        self._downstream = Pipeline(
            destination=self._target,
            blocking_subscriptions=self._subscriptions.get((SubscriptionStage.PRE_TARGET, SubscriptionMode.BLOCKING), {}),
            non_blocking_subscriptions=self._subscriptions.get((SubscriptionStage.PRE_TARGET, SubscriptionMode.NON_BLOCKING), {}),
            on_blocking_flow_complete=self._hass.store_extensions
        )

        self._upstream = Pipeline(
            destination=self._server,
            blocking_subscriptions=self._subscriptions.get((SubscriptionStage.POST_TARGET, SubscriptionMode.BLOCKING), {}),
            non_blocking_subscriptions=self._subscriptions.get((SubscriptionStage.POST_TARGET, SubscriptionMode.NON_BLOCKING), {}),
            on_blocking_flow_complete=self._hass.store_extensions
        )

    async def start(self) -> None:
        if self._running:
            return
        
        self._running = True

        # Connect to the target up front instead of waiting for a client's
        # Describe, so the first voice command after a restart works.
        self._init_task = asyncio.create_task(
            self._initialize_target_with_retry(), name="bridge target init"
        )
        _LOGGER.info("Wyoming bridge started successfully")

    async def stop(self) -> None:
        if not self._running:
            return

        self._running = False

        if self._init_task and not self._init_task.done():
            self._init_task.cancel()
            try:
                await self._init_task
            except asyncio.CancelledError:
                pass
        self._init_task = None

        # Signal the run loop to stop
        if hasattr(self, '_stop_event'):
            self._stop_event.set()

        await self._server.unbind_event_handler()
        _LOGGER.info("Wyoming bridge stopped")

    async def run(self) -> None:
        """Run main bridge loop."""
        await self.start()
        
        try:
            # Wait for stop signal - all work is event-driven
            self._stop_event = asyncio.Event()
            await self._stop_event.wait()
        except asyncio.CancelledError:
            _LOGGER.info("Bridge loop cancelled")
            raise
        finally:
            await self.stop()

    # Server bindings
    async def bind_server_handler(self, event_handler_id: str, writer: asyncio.StreamWriter) -> None:
        """Set current server event writer."""
        await self._server.bind_event_handler(event_handler_id, writer)

    async def unbind_server_handler(self) -> None:
        """Remove server event writer."""
        await self._server.unbind_event_handler()

    # Event handling
    async def on_server_event(self, event: Event) -> None:
        if Describe.is_type(event.type):
            # Normally answered per-connection via handle_describe(); this
            # path only exists for callers without their own writer.
            info_event = await self._get_info_event()
            if info_event is not None:
                await self._server.write_event(info_event)
            return

        if not AudioChunk.is_type(event.type):
            # Log all events except AudioChunk to avoid excessive logging
            _LOGGER.debug("Event received from server: %s", event.type)

        if not self._downstream:
            # Target wasn't reachable at startup; try once more now.
            await self._ensure_target()

        if not self._downstream:
            _LOGGER.warning("No downstream flow found")
            return

        await self._downstream.process_event(event)

    async def on_target_event(self, event: Event) -> None:
        if not self._upstream:
            _LOGGER.warning("No server connection available for event: %s", event.type)
            return

        await self._upstream.process_event(event)

    async def handle_describe(self, writer: asyncio.StreamWriter) -> None:
        """Answer a Describe on the requesting connection only.

        Never touches the bound server writer, the target connection, or the
        pipelines, so a Describe (e.g. a Home Assistant health check) arriving
        while a voice session is in progress can't disturb that session.
        """
        info_event = await self._get_info_event()
        if info_event is None:
            return

        try:
            await async_write_event(info_event, writer)
        except Exception:
            _LOGGER.exception("Failed to send Info to client")

    async def _get_info_event(self) -> Optional[Event]:
        """Fresh Info from the target, falling back to the cached copy."""
        info_event = await self._fetch_target_info()
        if info_event is None:
            if self._info_event is not None:
                _LOGGER.warning("Target unreachable; answering Describe with cached Info")
            return self._info_event

        self._info_event = info_event
        await self._ensure_target(info_event)
        return info_event

    async def _fetch_target_info(self) -> Optional[Event]:
        """Describe the target on a throwaway connection; return wrapped Info."""
        try:
            async with AsyncClient.from_uri(self._target_uri) as client:
                await client.connect()
                _LOGGER.debug("Sending 'describe' event to target")
                await client.write_event(Describe().event())

                async def read_info() -> Optional[Event]:
                    while True:
                        received_event = await client.read_event()
                        if received_event is None:
                            _LOGGER.debug("Connection lost with target")
                            return None

                        _LOGGER.debug("Received '%s' event from target", received_event.type)
                        if Info.is_type(received_event.type):
                            return wrap_wyoming_info(received_event)

                return await asyncio.wait_for(read_info(), timeout=DESCRIBE_TIMEOUT)

        except ConnectionError as error:
            _LOGGER.error("Failed to connect to target: %s", error)
        except asyncio.TimeoutError:
            _LOGGER.error("Timeout describing target")
        except Exception:
            _LOGGER.exception("Unexpected error describing target")
        return None

    async def _ensure_target(self, info_event: Optional[Event] = None) -> bool:
        """Create the target connection and pipelines once, if not done yet.

        Once built they are never replaced, so a later Describe can't swap
        them out from under an in-progress session.
        """
        async with self._target_lock:
            if self._target is not None:
                if info_event is not None:
                    target_type = read_service_type(info_event)
                    if target_type != self._target.TARGET_TYPE:
                        _LOGGER.warning(
                            "Target now reports service type '%s' but bridge was set up for '%s'; restart the bridge to switch",
                            target_type, self._target.TARGET_TYPE,
                        )
                return True

            if info_event is None:
                info_event = await self._fetch_target_info()
                if info_event is None:
                    return False
                self._info_event = info_event

            target_type = read_service_type(info_event)
            try:
                TargetConnection = WyomingTargetConnection.from_service_type(target_type)
            except ValueError:
                TargetConnection = None
            if TargetConnection is None:
                _LOGGER.error("No target connection found for service type '%s'", target_type)
                return False

            self._target = TargetConnection(
                uri=self._target_uri,
                on_target_event=self.on_target_event
            )
            self._build_event_flows()
            _LOGGER.info("Target ready: service type '%s'", target_type)
            return True

    async def _initialize_target_with_retry(self) -> None:
        delay = INIT_RETRY_MIN
        while self._running:
            if await self._ensure_target():
                return
            _LOGGER.warning("Target not ready; retrying in %.0fs", delay)
            await asyncio.sleep(delay)
            delay = min(delay * 2, INIT_RETRY_MAX)
