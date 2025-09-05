from datetime import datetime
from datetime import timedelta
from decimal import Decimal
from typing import cast

from django.utils import timezone

from eventsourcing.aggregate_repository import store_aggregate_changes
from eventsourcing.aggregates import load_editable_aggregates_at_time
from eventsourcing.aggregates import load_editable_aggregates_at_time_and_point
from eventsourcing.aggregates import reapply_downstream_events_from
from winemaking.models import Action
from winemaking.models import ReceiveVolumeData
from winemaking.models import WineLot
from winemaking.types import ActionType


@store_aggregate_changes
def record_receive_volume(lot_id: str, volume: Decimal, effective_at: datetime = None) -> Action:
    if effective_at is not None:
        effective_at = effective_at.replace(microsecond=0)
        if effective_at > timezone.now() - timedelta(seconds=2):
            raise ValueError("Effective date must be functionally in the past if provided.")

    lot = WineLot.objects.filter(id=lot_id).first()
    if not lot:
        raise ValueError(f"Wine lot with ID {lot_id} does not exist.")

    is_backdated = effective_at is not None

    if is_backdated:
        lot = load_editable_aggregates_at_time([lot], occurred_at=effective_at + timedelta(seconds=1))[lot_id]

    action = Action.record_receive_volume(wine_lot=lot, volume=volume, effective_at=effective_at)

    _process_action(action, lot)

    if is_backdated:
        reapply_downstream_events_from(aggregate=lot, occurred_at=action.effective_at, sequence_number=action.id)

    return action


@store_aggregate_changes
def edit_receive_volume(action_id: str, lot_id: str, volume: Decimal) -> Action:
    action = Action.objects.filter(id=action_id).first()
    if not action:
        raise ValueError(f"Action with ID {action_id} does not exist.")

    if action.action_type != ActionType.RECEIVE_VOLUME:
        raise ValueError(f"Action with ID {action_id} is not of type RECEIVE_VOLUME.")

    lot = WineLot.objects.filter(id=lot_id).first()
    if not lot:
        raise ValueError(f"Wine lot with ID {lot_id} does not exist.")

    # If we are dereferencing a lot, then we need to update it as well
    lots = [lot]
    if lot.id != action.details.data.wine_lot_id:
        old_lot = WineLot.objects.filter(id=action.details.data.wine_lot_id).first()
        lots.append(old_lot)

    lots_by_id = load_editable_aggregates_at_time_and_point(lots, occurred_at=action.effective_at, sequence_number=action.id)

    action.edit_receive_volume(wine_lot=lots_by_id[lot.id], volume=volume)

    _process_action(action, lots_by_id[lot.id])

    for to_replay in lots_by_id.values():
        reapply_downstream_events_from(aggregate=to_replay, occurred_at=action.effective_at, sequence_number=action.id)


def _process_action(action: Action, lot: WineLot):
    data = cast(ReceiveVolumeData, action.details.data)

    lot.receive_volume(action_id=action.id, effective_at=action.effective_at, volume=data.volume)
