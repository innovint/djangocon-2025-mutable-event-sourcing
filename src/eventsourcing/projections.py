import re

from eventsourcing.domain_events import AggregateEvent
from eventsourcing.notification_bus import Notification


class Projector:
    _fn_name_pattern = re.compile(r"(?<!^)(?=[A-Z])")

    def handle(self, event: Notification):
        self.apply(event)

    def apply(self, event: AggregateEvent):
        name = self._fn_name_pattern.sub("_", event.__class__.__name__).lower()
        fn_name = f"apply_{name}"

        if not hasattr(self, fn_name):
            raise NotImplementedError(f"Method {fn_name} not implemented")

        getattr(self, fn_name)(event)
