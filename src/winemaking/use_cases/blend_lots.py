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
from winemaking.models import BlendData
from winemaking.models import WineLot
from winemaking.types import ActionType


@store_aggregate_changes
def record_blend_lot(
    lot_id: str, blend_volumes: dict[str, Decimal], blended_volume: Decimal, effective_at: datetime = None
) -> Action:
    if effective_at is not None:
        effective_at = effective_at.replace(microsecond=0)
        if effective_at > timezone.now() - timedelta(seconds=2):
            raise ValueError("Effective date must be functionally in the past if provided.")

    lots = WineLot.objects.filter(id__in=[lot_id] + list(blend_volumes.keys())).all()
    if len(lots) != len(blend_volumes) + 1:
        existing_ids = {lot.id for lot in lots}
        missing_ids = [lid for lid in [lot_id] + list(blend_volumes.keys()) if lid not in existing_ids]
        raise ValueError(f"Wine lots with IDs {', '.join(missing_ids)} do not exist.")

    is_backdated = effective_at is not None

    if is_backdated:
        lots = load_editable_aggregates_at_time(lots, occurred_at=effective_at + timedelta(seconds=1))
    else:
        lots = {lot.id: lot for lot in lots}

    action = Action.record_blend(
        blend_volumes={lots[blended_lot_id]: volume for blended_lot_id, volume in blend_volumes.items()},
        receiving_wine_lot=lots[lot_id],
        blended_volume=blended_volume,
        effective_at=effective_at,
    )

    _process_action(action, lots)

    if is_backdated:
        for lot in lots.values():
            reapply_downstream_events_from(aggregate=lot, occurred_at=action.effective_at, sequence_number=action.id)

    return action


@store_aggregate_changes
def edit_blend_lot(action_id: str, lot_id: str, blend_volumes: dict[WineLot, Decimal], blended_volume: Decimal) -> Action:
    action = Action.objects.filter(id=action_id).first()
    if not action:
        raise ValueError(f"Action with ID {action_id} does not exist.")

    if action.action_type != ActionType.BLEND:
        raise ValueError(f"Action with ID {action_id} is not of type BLEND.")

    lots = [lot_id] + list(blend_volumes.keys())
    existing_lots = WineLot.objects.filter(id__in=[lot.id for lot in lots]).all()
    if len(existing_lots) != len(lots):
        existing_ids = {lot.id for lot in existing_lots}
        missing_ids = [lot.id for lot in lots if lot.id not in existing_ids]
        raise ValueError(f"Wine lots with IDs {', '.join(missing_ids)} do not exist.")

    lots_by_id = load_editable_aggregates_at_time_and_point(
        existing_lots, occurred_at=action.effective_at, sequence_number=action.id
    )

    action = action.edit_blend(
        blend_volumes={lots_by_id[lot.id]: volume for lot, volume in blend_volumes.items()},
        receiving_wine_lot=lots_by_id[lot_id],
        blended_volume=blended_volume,
    )

    _process_action(action, lots_by_id)

    for lot in lots_by_id.values():
        reapply_downstream_events_from(aggregate=lot, occurred_at=action.effective_at, sequence_number=action.id)

    return action


def _process_action(action: Action, lots: dict[str, WineLot]):
    data = cast(BlendData, action.details.data)

    for blended_lot_id, volume in data.blend_volumes.items():
        blending_lot = lots[blended_lot_id]
        blending_lot.move_volume(
            action_id=action.id,
            effective_at=action.effective_at,
            volume=volume,
            to_wine_lot_id=data.receiving_wine_lot_id,
        )

    receiving_lot = lots[data.receiving_wine_lot_id]
    receiving_lot.blend_in_volume(
        action_id=action.id,
        effective_at=action.effective_at,
        volume_received=data.blended_volume,
        volumes=data.blend_volumes,
    )
