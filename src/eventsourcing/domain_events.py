from datetime import datetime
from typing import ClassVar
from typing import Generic
from typing import TypeVar

from pydantic import BaseModel
from pydantic import ConfigDict

from eventsourcing.notification_bus import Notification

V = TypeVar("V")


class ValueChange(BaseModel, Generic[V]):
    before: V
    after: V


class AggregateEvent(BaseModel, Notification):
    aggregate_type: ClassVar[str]
    aggregate_id: str
    event_type: ClassVar[str]
    event_version: int = 1

    model_config = ConfigDict(
        from_attributes=True,  # aka orm_mode
        frozen=True,  # faux-immutable
    )


class Timestamped(BaseModel):
    occurred_at: datetime


class ActionSequenced(BaseModel):
    action_id: str

    @property
    def sequence_number(self) -> str:
        return self.action_id
