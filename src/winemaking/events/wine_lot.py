from decimal import Decimal
from enum import StrEnum

from eventsourcing.domain_events import ActionSequenced
from eventsourcing.domain_events import AggregateEvent
from eventsourcing.domain_events import Timestamped
from eventsourcing.domain_events import ValueChange
from winemaking.types import ComponentAmount
from winemaking.types import Composition
from winemaking.types import LotComponent


class WineLotEventType(StrEnum):
    CREATED = "WINE_LOT_CREATED"
    UPDATED = "WINE_LOT_UPDATED"
    DELETED = "WINE_LOT_DELETED"
    VOLUME_BLENDED = "VOLUME_BLENDED"
    VOLUME_RECEIVED = "VOLUME_RECEIVED"
    VOLUME_REMEASURED = "VOLUME_REMEASURED"
    VOLUME_BOTTLED = "VOLUME_BOTTLED"
    VOLUME_MOVED = "VOLUME_MOVED"


class WineLotCreated(AggregateEvent, Timestamped):
    event_type = WineLotEventType.CREATED
    code: str
    components: list[ComponentAmount]

    @property
    def composition(self) -> Composition:
        """Convenience property to access the composition of the wine lot."""
        return Composition(
            components={
                LotComponent(
                    variety=c.component.variety, appellation=c.component.appellation, vintage=c.component.vintage
                ): c.percent
                for c in self.components
            }
        )


class WineLotUpdated(AggregateEvent):
    event_type = WineLotEventType.UPDATED
    code: ValueChange[str]


class WineLotDeleted(Timestamped, AggregateEvent):
    event_type = WineLotEventType.DELETED


class VolumeBlended(ActionSequenced, Timestamped, AggregateEvent):
    event_type = WineLotEventType.VOLUME_BLENDED
    volumes: dict[str, Decimal]  # Mapping of wine lot IDs to their contributing amounts
    volume_received: Decimal


class VolumeReceived(ActionSequenced, Timestamped, AggregateEvent):
    event_type = WineLotEventType.VOLUME_RECEIVED
    volume: Decimal


class VolumeRemeasured(ActionSequenced, Timestamped, AggregateEvent):
    event_type = WineLotEventType.VOLUME_REMEASURED
    volume: Decimal


class VolumeBottled(ActionSequenced, Timestamped, AggregateEvent):
    event_type = WineLotEventType.VOLUME_BOTTLED
    volume: Decimal


class VolumeMoved(ActionSequenced, Timestamped, AggregateEvent):
    event_type = WineLotEventType.VOLUME_MOVED
    volume: Decimal
    to_wine_lot_id: str
