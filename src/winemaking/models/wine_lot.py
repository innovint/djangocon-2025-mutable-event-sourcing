import re
import uuid
from datetime import datetime
from decimal import Decimal
from typing import Self

from django.db import models
from django.utils import timezone
from django_pydantic_field.v2 import SchemaField

from eventsourcing.domain_events import ValueChange
from eventsourcing.models import AggregateEventModel
from eventsourcing.models import AggregateModel
from winemaking.events.wine_lot import VolumeBlended
from winemaking.events.wine_lot import VolumeBottled
from winemaking.events.wine_lot import VolumeMoved
from winemaking.events.wine_lot import VolumeReceived
from winemaking.events.wine_lot import VolumeRemeasured
from winemaking.events.wine_lot import WineLotCreated
from winemaking.events.wine_lot import WineLotDeleted
from winemaking.events.wine_lot import WineLotUpdated
from winemaking.types import ComponentAmount
from winemaking.types import Composition

_CODE_REGEX = r"^[A-Z0-9]{1}[A-Z0-9_-]{0,48}[A-Z0-9]{1}$"
"""
A regular expression to validate wine lot codes.

They are made up of at least 2 uppercase alphanumeric characters, with optional hyphens
or underscores in between.
"""


class WineLotEventStore(AggregateEventModel):
    event_types = [
        WineLotCreated,
        WineLotUpdated,
        WineLotDeleted,
        VolumeBlended,
        VolumeReceived,
        VolumeRemeasured,
        VolumeMoved,
        VolumeBottled,
    ]


class WineLot(AggregateModel):
    code = models.CharField(max_length=100, unique=True)
    volume = models.DecimalField(max_digits=10, decimal_places=2, default=Decimal("0.00"))
    deleted_at = models.DateTimeField(null=True)

    def __str__(self):
        return f"{self.code}"

    def get_event_model(cls):
        return WineLotEventStore

    @classmethod
    def create(cls, code: str, composition: Composition) -> Self:
        cls._validate_code(code)

        event = WineLotCreated(
            aggregate_id=cls.next_id(),
            code=code,
            components=[ComponentAmount(component=comp, percent=percent) for comp, percent in composition.components.items()],
            occurred_at=timezone.make_aware(datetime(year=1970, month=1, day=1, hour=0, minute=0, second=0)),
            # Force our creation event to the front of any stream as a default.
            # There are other, more complex, solutions to keep creation at the front, but are out of scope for now.
        )

        instance = WineLot()

        instance.apply(event)

        return instance

    def update(self, code: str):
        if self.deleted_at is not None:
            raise ValueError("Cannot update a deleted wine lot.")

        self._validate_code(code)

        event = WineLotUpdated(aggregate_id=self.id, code=ValueChange(before=self.code, after=code))

        self.apply(event)

    def destroy(self):
        if self.deleted_at is not None:
            raise ValueError("Wine lot has already been deleted.")

        event = WineLotDeleted(aggregate_id=self.id, occurred_at=timezone.now())

        self.apply(event)

    def blend_in_volume(self, action_id: str, effective_at: datetime, volume_received: Decimal, volumes: dict[str, Decimal]):
        if self.deleted_at is not None:
            raise ValueError("Cannot blend into a deleted wine lot.")

        if volume_received <= 0:
            raise ValueError("Volume must be greater than zero.")

        event = VolumeBlended(
            aggregate_id=self.id,
            action_id=action_id,
            occurred_at=effective_at,
            volumes=volumes,
            volume_received=volume_received,
        )

        self.apply(event)

    def receive_volume(self, action_id: str, effective_at: datetime, volume: Decimal):
        if self.deleted_at is not None:
            raise ValueError("Cannot adjust volume of a deleted wine lot.")

        event = VolumeReceived(aggregate_id=self.id, action_id=action_id, occurred_at=effective_at, volume=volume)

        self.apply(event)

    def move_volume(self, action_id: str, effective_at: datetime, volume: Decimal, to_wine_lot_id: str):
        """
        Move (subtract) volume from the wine lot to a different lot.
        """
        if self.deleted_at is not None:
            raise ValueError("Cannot move volume from a deleted wine lot.")
        if volume < 0:
            raise ValueError("Volume must be non-negative.")

        event = VolumeMoved(
            aggregate_id=self.id,
            action_id=action_id,
            occurred_at=effective_at,
            volume=volume,
            to_wine_lot_id=to_wine_lot_id,
        )

        self.apply(event)

    def validate_volume_moved_context(self, event: VolumeBottled):
        if self.volume - event.volume < 0:
            raise ValueError(f"Moved volume cannot exceed current volume. Current volume: {self.volume}, moved: {event.volume}")

    def remeasure(self, action_id: str, effective_at: datetime, volume: Decimal):
        """
        Re-measure the volume of the wine lot.
        """
        if self.deleted_at is not None:
            raise ValueError("Cannot re-measure a deleted wine lot.")
        if volume < 0:
            raise ValueError("Volume must be non-negative.")

        event = VolumeRemeasured(aggregate_id=self.id, action_id=action_id, occurred_at=effective_at, volume=volume)

        self.apply(event)

    def validate_volume_bottled_context(self, event: VolumeBottled):
        if self.volume - event.volume < 0:
            raise ValueError(
                f"Bottled volume cannot exceed current volume. Current volume: {self.volume}, bottled: {event.volume}"
            )

    def bottle(self, action_id: str, effective_at: datetime, volume: Decimal):
        """
        Bottle the wine lot, adjusting the volume accordingly.
        """
        if self.deleted_at is not None:
            raise ValueError("Cannot bottle a deleted wine lot.")

        if volume <= 0:
            raise ValueError("Volume must be greater than zero.")

        event = VolumeBottled(aggregate_id=self.id, action_id=action_id, occurred_at=effective_at, volume=volume)

        self.apply(event)

    @classmethod
    def _validate_code(cls, code: str):
        if not code or not isinstance(code, str):
            raise ValueError("Code must be a non-empty string.")
        if len(code) < 2 or len(code) > 50:
            raise ValueError("Code must be between 2 and 50 characters long.")
        if not re.match(_CODE_REGEX, code):
            raise ValueError("Code must consist of uppercase alphanumeric characters, hyphens, or underscores.")

    def apply_wine_lot_created(self, event: WineLotCreated):
        self.id = event.aggregate_id
        self.code = event.code

    def apply_wine_lot_updated(self, event: WineLotUpdated):
        self.code = event.code.after

    def apply_wine_lot_deleted(self, event: WineLotDeleted):
        self.code = f"{self.code}!{uuid.uuid4().hex}"
        # Apply a unique identifier to the code to allow it for reuse in the future.

        self.deleted_at = event.occurred_at

    def apply_volume_received(self, event: VolumeReceived):
        self.volume += event.volume

    def apply_volume_remeasured(self, event: VolumeRemeasured):
        self.volume = event.volume

    def apply_volume_bottled(self, event: VolumeBottled):
        self.volume -= event.volume

    def apply_volume_blended(self, event: VolumeBlended):
        self.volume += event.volume_received

    def apply_volume_moved(self, event: VolumeMoved):
        self.volume -= event.volume


class WineLotComponent(models.Model):
    wine_lot = models.ForeignKey(WineLot, related_name="components", on_delete=models.CASCADE)
    component = SchemaField(ComponentAmount)
    percent = models.DecimalField(max_digits=5, decimal_places=2)
