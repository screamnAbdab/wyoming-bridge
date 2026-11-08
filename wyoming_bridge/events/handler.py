import logging
import time

from wyoming.event import Event
from wyoming.info import Describe
from wyoming.server import AsyncEventHandler

from wyoming_bridge.core.bridge import WyomingBridge

_LOGGER = logging.getLogger("main")

class WyomingEventHandler(AsyncEventHandler):
    def __init__(self, bridge: WyomingBridge, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        self.bridge = bridge
        self.event_handler_id = str(time.monotonic_ns())

    async def handle_event(self, event: Event) -> bool:
        """Forward all events from the server to the bridge."""

        # Describe is answered on this connection only. It must not take over
        # the bridge's server binding: a Describe-only connection (e.g. a Home
        # Assistant health check) arriving mid-session would otherwise steal
        # the live session's transcript.
        if Describe.is_type(event.type):
            await self.bridge.handle_describe(self.writer)
            return True

        # Establish/re-establish upstream connection if needed
        if self.bridge.event_handler_id is None or \
           self.bridge.event_handler_id != self.event_handler_id:
            _LOGGER.debug(
                "New event handler taking over or first connection. Old ID: %s, New ID: %s",
                self.bridge.event_handler_id,
                self.event_handler_id
            )
            await self.bridge.bind_server_handler(self.event_handler_id, self.writer)

        await self.bridge.on_server_event(event)
        return True
