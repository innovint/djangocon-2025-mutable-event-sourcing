from datetime import datetime
from decimal import Decimal
from typing import Annotated
from typing import Literal
from typing import Self
from typing import TypeVar
from typing import cast

from django.db import models
from django.utils import timezone
from django_pydantic_field import SchemaField
from pydantic import BaseModel
from pydantic import Discriminator

from eventsourcing.domain_events import ValueChange
from eventsourcing.models import AggregateEventModel
from eventsourcing.models import AggregateModel
from winemaking.events.action import ActionDeleted
from winemaking.events.action import ActionEdited
from winemaking.events.action import ActionRecorded
from winemaking.events.action import BlendEditedData
from winemaking.events.action import BlendRecordedData
from winemaking.events.action import BottleEditedData
from winemaking.events.action import BottleRecordedData
from winemaking.events.action import MeasureVolumeEditedData
from winemaking.events.action import MeasureVolumeRecordedData
from winemaking.events.action import ReceiveVolumeEditedData
from winemaking.events.action import ReceiveVolumeRecordedData
from winemaking.models.wine_lot import WineLot
from winemaking.types import ActionType

T = TypeVar("T", bound=BaseModel)


class ReceiveVolumeData(BaseModel):
    action_type: Literal[ActionType.RECEIVE_VOLUME] = ActionType.RECEIVE_VOLUME
    wine_lot_id: str
    volume: Decimal


class MeasureVolumeData(BaseModel):
    action_type: Literal[ActionType.REMEASURE] = ActionType.REMEASURE
    wine_lot_id: str
    volume: Decimal


class BlendData(BaseModel):
    action_type: Literal[ActionType.BLEND] = ActionType.BLEND
    blend_volumes: dict[str, Decimal]
    receiving_wine_lot_id: str
    blended_volume: Decimal


class BottleData(BaseModel):
    action_type: Literal[ActionType.BOTTLE] = ActionType.BOTTLE
    wine_lot_id: str
    volume_bottled: Decimal
    bottles: int


#
# ActionDetails = Annotated[
#     Union[
#         ReceiveVolumeData,
#         MeasureVolumeData,
#         BlendData,
#         BottleData
#     ],
#     Discriminator("action_type")
# ]
#
#
# ActionDetailsAdapter = TypeAdapter(ActionDetails)
#


class ActionEventStore(AggregateEventModel):
    event_types = [
        ActionRecorded,
        ActionEdited,
        ActionDeleted,
    ]


class ActionDetails(BaseModel):
    data: Annotated[
        ReceiveVolumeData | MeasureVolumeData | BlendData | BottleData,
        Discriminator("action_type"),
    ]


class Action(AggregateModel):
    effective_at = models.DateTimeField()
    deleted_at = models.DateTimeField(null=True)
    updated_at = models.DateTimeField(null=True)

    action_type = models.CharField(choices=ActionType, max_length=255, null=False)
    involved_wine_lot_ids = SchemaField(schema=list[str])
    revision_number = models.PositiveIntegerField(default=0)

    details = SchemaField(ActionDetails)

    def get_event_model(cls):
        return ActionEventStore

    @classmethod
    def record_receive_volume(cls, wine_lot: WineLot, volume: Decimal, effective_at: datetime = None) -> Self:
        event = ActionRecorded(
            aggregate_id=cls.next_id(),
            effective_at=effective_at or timezone.now(),
            recorded_at=timezone.now(),
            details=ReceiveVolumeRecordedData(
                wine_lot_id=wine_lot.id,
                volume=volume,
            ),
        )
        action = cls()
        action.apply(event)
        return action

    @classmethod
    def record_remeasure(cls, wine_lot: WineLot, volume: Decimal, effective_at: datetime = None) -> Self:
        event = ActionRecorded(
            aggregate_id=cls.next_id(),
            effective_at=effective_at or timezone.now(),
            recorded_at=timezone.now(),
            details=MeasureVolumeRecordedData(wine_lot_id=wine_lot.id, volume=volume),
        )
        action = cls()
        action.apply(event)
        return action

    @classmethod
    def record_blend(
        cls,
        blend_volumes: dict[WineLot, Decimal],
        receiving_wine_lot: WineLot,
        blended_volume: Decimal,
        effective_at: datetime = None,
    ) -> Self:
        if blended_volume <= 0:
            raise ValueError("Blended volume must be greater than zero.")
        total_moved_volume = sum(blend_volumes.values())
        if total_moved_volume == 0:
            raise ValueError("Total blended volume cannot be zero.")

        event = ActionRecorded(
            aggregate_id=cls.next_id(),
            effective_at=effective_at or timezone.now(),
            recorded_at=timezone.now(),
            details=BlendRecordedData(
                blend_volumes={wine_lot.id: volume for wine_lot, volume in blend_volumes.items()},
                receiving_wine_lot_id=receiving_wine_lot.id,
                blended_volume=blended_volume,
            ),
        )
        action = cls()
        action.apply(event)
        return action

    @classmethod
    def record_bottle(cls, wine_lot: WineLot, volume_bottled: Decimal, bottles: int, effective_at: datetime = None) -> Self:
        event = ActionRecorded(
            aggregate_id=cls.next_id(),
            effective_at=effective_at or timezone.now(),
            recorded_at=timezone.now(),
            details=BottleRecordedData(wine_lot_id=wine_lot.id, volume_bottled=volume_bottled, bottles=bottles),
        )
        action = cls()
        action.apply(event)
        return action

    def destroy(self):
        if self.deleted_at is not None:
            raise ValueError("Action has already been deleted.")

        event = ActionDeleted(aggregate_id=self.id, deleted_at=timezone.now())
        self.apply(event)

    def edit_receive_volume(self, wine_lot: WineLot, volume: Decimal):
        if self.action_type != ActionType.RECEIVE_VOLUME:
            raise ValueError(f"Cannot edit a {self.action_type} action as a volume receipt.")
        if self.deleted_at is not None:
            raise ValueError("Cannot edit a deleted action.")

        current_details = cast(ReceiveVolumeData, self.details.data)
        event = ActionEdited(
            aggregate_id=self.id,
            edited_at=timezone.now(),
            details=ReceiveVolumeEditedData(
                action_type=ActionType.RECEIVE_VOLUME,
                wine_lot_id=ValueChange(before=current_details.wine_lot_id, after=wine_lot.id),
                volume=ValueChange(before=current_details.volume, after=volume),
            ),
        )
        self.apply(event)

    def edit_remeasure(self, wine_lot: WineLot, volume: Decimal):
        if self.action_type != ActionType.REMEASURE:
            raise ValueError(f"Cannot edit a {self.action_type} action as a volume remeasurement.")
        if self.deleted_at is not None:
            raise ValueError("Cannot edit a deleted action.")

        current_details = cast(MeasureVolumeData, self.details.data)
        event = ActionEdited(
            aggregate_id=self.id,
            edited_at=timezone.now(),
            details=MeasureVolumeEditedData(
                action_type=ActionType.REMEASURE,
                wine_lot_id=ValueChange(before=current_details.wine_lot_id, after=wine_lot.id),
                volume=ValueChange(before=current_details.volume, after=volume),
            ),
        )
        self.apply(event)

    def edit_blend(self, blend_volumes: dict[WineLot, Decimal], receiving_wine_lot: WineLot, blended_volume: Decimal):
        if self.action_type != ActionType.BLEND:
            raise ValueError(f"Cannot edit a {self.action_type} action as a blend.")
        if self.deleted_at is not None:
            raise ValueError("Cannot edit a deleted action.")

        if blended_volume <= 0:
            raise ValueError("Blended volume must be greater than zero.")
        total_moved_volume = sum(blend_volumes.values())
        if total_moved_volume == 0:
            raise ValueError("Total blended volume cannot be zero.")

        current_details = cast(BlendData, self.details.data)
        event = ActionEdited(
            aggregate_id=self.id,
            edited_at=timezone.now(),
            details=BlendEditedData(
                action_type=ActionType.BLEND,
                blend_volumes=ValueChange(
                    before=current_details.blend_volumes,
                    after={wine_lot.id: volume for wine_lot, volume in blend_volumes.items()},
                ),
                receiving_wine_lot_id=ValueChange(before=current_details.receiving_wine_lot_id, after=receiving_wine_lot.id),
                blended_volume=ValueChange(before=current_details.blended_volume, after=blended_volume),
            ),
        )
        self.apply(event)

    def edit_bottle(self, wine_lot: WineLot, volume_bottled: Decimal, bottles: int):
        if self.action_type != ActionType.BOTTLE:
            raise ValueError(f"Cannot edit a {self.action_type} action as a bottling.")
        if self.deleted_at is not None:
            raise ValueError("Cannot edit a deleted action.")

        current_details: BottleData = cast(BottleData, self.details.data)
        event = ActionEdited(
            aggregate_id=self.id,
            edited_at=timezone.now(),
            details=BottleEditedData(
                action_type=ActionType.BOTTLE,
                wine_lot_id=ValueChange(before=current_details.wine_lot_id, after=wine_lot.id),
                volume_bottled=ValueChange(before=current_details.volume_bottled, after=volume_bottled),
                bottles=ValueChange(before=current_details.bottles, after=bottles),
            ),
        )
        self.apply(event)

    def apply_action_recorded(self, event: ActionRecorded):
        self.id = event.aggregate_id
        self.effective_at = event.effective_at
        self.recorded_at = event.recorded_at
        self.deleted_at = None
        self.revision_number = 0

        if isinstance(event.details, ReceiveVolumeRecordedData):
            self.action_type = ActionType.RECEIVE_VOLUME
            self.details = ActionDetails(
                data=ReceiveVolumeData(
                    action_type=ActionType.RECEIVE_VOLUME,
                    wine_lot_id=event.details.wine_lot_id,
                    volume=event.details.volume,
                )
            )
            self.involved_wine_lot_ids = [event.details.wine_lot_id]
        elif isinstance(event.details, MeasureVolumeRecordedData):
            self.action_type = ActionType.REMEASURE
            self.details = ActionDetails(
                data=MeasureVolumeData(
                    action_type=ActionType.REMEASURE, wine_lot_id=event.details.wine_lot_id, volume=event.details.volume
                )
            )
            self.involved_wine_lot_ids = [event.details.wine_lot_id]
        elif isinstance(event.details, BlendRecordedData):
            self.action_type = ActionType.BLEND
            self.details = ActionDetails(
                data=BlendData(
                    action_type=ActionType.BLEND,
                    blend_volumes=event.details.blend_volumes,
                    receiving_wine_lot_id=event.details.receiving_wine_lot_id,
                    blended_volume=event.details.blended_volume,
                )
            )
            self.involved_wine_lot_ids = [event.details.receiving_wine_lot_id, *event.details.blend_volumes.keys()]
        elif isinstance(event.details, BottleRecordedData):
            self.action_type = ActionType.BOTTLE
            self.details = ActionDetails(
                data=BottleData(
                    action_type=ActionType.BOTTLE,
                    wine_lot_id=event.details.wine_lot_id,
                    volume_bottled=event.details.volume_bottled,
                    bottles=event.details.bottles,
                )
            )
            self.involved_wine_lot_ids = [event.details.wine_lot_id]

    def apply_action_edited(self, event: ActionEdited):
        self.revision_number += 1
        self.updated_at = event.edited_at

        if isinstance(event.details, ReceiveVolumeEditedData):
            self.details = ActionDetails(
                data=ReceiveVolumeData(
                    action_type=ActionType.RECEIVE_VOLUME,
                    wine_lot_id=event.details.wine_lot_id.after,
                    volume=event.details.volume.after,
                )
            )
            self.involved_wine_lot_ids = [event.details.wine_lot_id.after]
        elif isinstance(event.details, MeasureVolumeEditedData):
            self.details = ActionDetails(
                data=MeasureVolumeData(
                    action_type=ActionType.REMEASURE,
                    wine_lot_id=event.details.wine_lot_id.after,
                    volume=event.details.volume.after,
                )
            )
            self.involved_wine_lot_ids = [event.details.wine_lot_id.after]
        elif isinstance(event.details, BlendEditedData):
            self.details = ActionDetails(
                data=BlendData(
                    action_type=ActionType.BLEND,
                    blend_volumes=event.details.blend_volumes.after,
                    receiving_wine_lot_id=event.details.receiving_wine_lot_id.after,
                    blended_volume=event.details.blended_volume.after,
                )
            )
            self.involved_wine_lot_ids = [event.details.receiving_wine_lot_id.after, *event.details.blend_volumes.after.keys()]
        elif isinstance(event.details, BottleEditedData):
            self.details = ActionDetails(
                data=BottleData(
                    action_type=ActionType.BOTTLE,
                    wine_lot_id=event.details.wine_lot_id.after,
                    volume_bottled=event.details.volume_bottled.after,
                    bottles=event.details.bottles.after,
                )
            )
            self.involved_wine_lot_ids = [event.details.wine_lot_id.after]

    def apply_action_deleted(self, event: ActionDeleted):
        self.deleted_at = event.deleted_at
