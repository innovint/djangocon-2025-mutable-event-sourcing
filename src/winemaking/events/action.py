from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import Annotated
from typing import Literal

from pydantic import BaseModel
from pydantic import Discriminator

from eventsourcing.domain_events import AggregateEvent
from eventsourcing.domain_events import ValueChange
from winemaking.types import ActionType


class ActionEvent(AggregateEvent):
    aggregate_type = "action"


class ActionEventType(StrEnum):
    ACTION_RECORDED = "ACTION_RECORDED"
    ACTION_EDITED = "ACTION_EDITED"
    ACTION_DELETED = "ACTION_DELETED"


class ReceiveVolumeRecordedData(BaseModel):
    action_type: Literal[ActionType.RECEIVE_VOLUME] = ActionType.RECEIVE_VOLUME
    wine_lot_id: str
    volume: Decimal


class MeasureVolumeRecordedData(BaseModel):
    action_type: Literal[ActionType.REMEASURE] = ActionType.REMEASURE
    wine_lot_id: str
    volume: Decimal


class BlendRecordedData(BaseModel):
    action_type: Literal[ActionType.BLEND] = ActionType.BLEND
    blend_volumes: dict[str, Decimal]
    """Mapping of wine lot IDs to their blend amounts."""
    receiving_wine_lot_id: str
    """ID of the wine lot receiving the blend."""
    blended_volume: Decimal
    """
    How much wine is blended into the receiving lot.
    
    There are often losses during blending, so this is not necessarily the same as the total volume of the blends.
    """


class BottleRecordedData(BaseModel):
    action_type: Literal[ActionType.BOTTLE] = ActionType.BOTTLE
    wine_lot_id: str
    volume_bottled: Decimal
    bottles: int


class ActionRecorded(ActionEvent):
    event_type = ActionEventType.ACTION_RECORDED
    effective_at: datetime
    recorded_at: datetime
    details: Annotated[
        ReceiveVolumeRecordedData | MeasureVolumeRecordedData | BlendRecordedData | BottleRecordedData,
        Discriminator("action_type"),
    ]


class ReceiveVolumeEditedData(BaseModel):
    action_type: Literal[ActionType.RECEIVE_VOLUME] = ActionType.RECEIVE_VOLUME
    wine_lot_id: ValueChange[str]
    volume: ValueChange[Decimal]


class MeasureVolumeEditedData(BaseModel):
    action_type: Literal[ActionType.REMEASURE] = ActionType.REMEASURE
    wine_lot_id: ValueChange[str]
    volume: ValueChange[Decimal]


class BlendEditedData(BaseModel):
    action_type: Literal[ActionType.BLEND] = ActionType.BLEND
    blend_volumes: ValueChange[dict[str, Decimal]]  # Mapping of wine lot IDs to their blend proportions
    receiving_wine_lot_id: ValueChange[str]  # ID of the wine lot receiving the blend
    blended_volume: ValueChange[Decimal]


class BottleEditedData(BaseModel):
    action_type: Literal[ActionType.BOTTLE] = ActionType.BOTTLE
    wine_lot_id: ValueChange[str]
    volume_bottled: ValueChange[Decimal]
    bottles: ValueChange[int]


class ActionEdited(ActionEvent):
    event_type = ActionEventType.ACTION_EDITED
    edited_at: datetime
    details: Annotated[
        ReceiveVolumeEditedData | MeasureVolumeEditedData | BlendEditedData | BottleEditedData,
        Discriminator("action_type"),
    ]


class ActionDeleted(ActionEvent):
    event_type = ActionEventType.ACTION_DELETED
    deleted_at: datetime
