from datetime import datetime
from decimal import Decimal
from typing import cast

from django.db.models import Q

from winemaking.events.wine_lot import VolumeBlended
from winemaking.events.wine_lot import WineLotEventType
from winemaking.models import WineLot
from winemaking.models import WineLotEventStore
from winemaking.types import Composition


def calculate_composition(lot_id: str, effective_at: datetime | None = None, action_id: str | None = None) -> Composition:
    """
    Compute the composition of a wine lot by replaying its blending history up to an optional cutoff.

    Given a target lot ID, this function:
    - Verifies the lot exists.
    - Discovers all upstream source lots that have contributed volume to the target through blends,
      constrained to events up to the provided cutoff (if any).
    - Replays all relevant events in chronological order for the target and its sources, updating the
      target lot’s composition by volume-weighting contributions from existing contents and each
      blended-in lot.

    This function is read-only and does not modify the persisted state.

    Parameters:
    - lot_id: Unique identifier of the wine lot to evaluate.
    - effective_at: Optional timestamp at which to evaluate the composition. If provided, only events
      with occurred_at <= effective_at are considered. If action_id is also provided, then events with
      occurred_at < effective_at are included, plus only those at exactly effective_at up to the matching
      action_id.
    - action_id: Optional action identifier. If provided, effective_at must also be provided.

    Returns:
    - Composition describing the fraction of each component in the target lot after all relevant blends
      up to the cutoff. Fractions are volume-weighted and typically sum to 1.0; the mapping may be empty
      if the lot has no volume.

    Raises:
    - ValueError: If the specified lot does not exist, or if action_id is provided without effective_at.

    """
    # The database stores timestamps without microsecond precision, so we need to remove it from the cutoff.
    effective_at = effective_at.replace(microsecond=0) if effective_at is not None else None

    if action_id is not None and effective_at is None:
        raise ValueError("effective_at must be provided when action_id is specified.")

    lot = WineLot.objects.filter(id=lot_id).first()

    if not lot:
        raise ValueError(f"Wine lot with ID {lot_id} does not exist.")

    lot_ids = _get_all_lot_ids(lot_id, effective_at=effective_at, action_id=action_id)

    lot_compositions = _build_lot_compositions(lot_ids, effective_at=effective_at, action_id=action_id)

    return lot_compositions[lot_id]


def _build_lot_compositions(
    lot_ids: set[str],
    effective_at: datetime | None = None,
    action_id: str | None = None,
) -> dict[str, Composition]:
    relevant_events = WineLotEventStore.objects.filter(aggregate_id__in=lot_ids)

    if effective_at is not None:
        if action_id is not None:
            # Before the cutoff time, include all events; at the cutoff time, include only up to the matching action.
            relevant_events = relevant_events.filter(
                Q(occurred_at__lt=effective_at) | Q(occurred_at=effective_at, sequence_number__lte=action_id)
            )
        else:
            # Include everything up to and including the cutoff time.
            relevant_events = relevant_events.filter(occurred_at__lte=effective_at)

    # Now build up the composition for each lot as we go
    lot_compositions: dict[str, Composition] = {}
    lots: dict[str, WineLot] = {}
    for event in relevant_events:
        if event.event_type == WineLotEventType.CREATED:
            lot_compositions[event.aggregate_id] = event.get_event_data().composition
            lot = WineLot()
            lot.load(event.get_event_data())
            lots[event.aggregate_id] = lot
            continue

        lot = lots[event.aggregate_id]
        if event.event_type == WineLotEventType.VOLUME_BLENDED:
            event_data = cast(VolumeBlended, event.get_event_data())
            lot_composition = lot_compositions[event.aggregate_id]
            lot_volume = lot.volume

            # Each of `volumes` is a different lot that is being blended into `lot` by volume. This should update
            # the final composition of `lot` based on the blended volumes.
            blended_total = sum(event_data.volumes.values(), start=Decimal("0"))
            new_total_volume = lot_volume + blended_total

            new_components = {}

            # Contribution from the existing lot contents
            if lot_volume > 0:
                current_weight = lot_volume / new_total_volume
                for comp, pct in lot_composition.components.items():
                    new_components[comp] = new_components.get(comp, Decimal("0")) + pct * current_weight

            # Contributions from each blended-in lot
            for blended_lot_id, blend_volume in event_data.volumes.items():
                if blend_volume <= 0:
                    continue
                blend_weight = blend_volume / new_total_volume
                blend_composition = lot_compositions[blended_lot_id]
                for comp, pct in blend_composition.components.items():
                    new_components[comp] = new_components.get(comp, Decimal("0")) + pct * blend_weight

            new_composition = Composition(components=new_components)

            lot_compositions[event.aggregate_id] = new_composition

        # Update the state of the lot before moving on
        lot.load(event.get_event_data())

    return lot_compositions


def _get_all_lot_ids(
    lot_id: str,
    effective_at: datetime | None = None,
    action_id: str | None = None,
) -> set[str]:
    """
    Iteratively find all lot IDs that are involved in the calculation of the target lot composition,
    constrained to blend events up to an optional cutoff.

    This performs a breadth-first search, because each of the lots that are directly blended into the target lot may
    have blends into them as well.
    """
    discovered: set[str] = {lot_id}
    queue: list[str] = [lot_id]

    while queue:
        current = queue.pop(0)

        # Find all blend events where this lot received volume from other lots
        blend_events = WineLotEventStore.objects.filter(aggregate_id=current, event_type=WineLotEventType.VOLUME_BLENDED)

        if effective_at is not None:
            if action_id is not None:
                blend_events = blend_events.filter(
                    Q(occurred_at__lt=effective_at) | Q(occurred_at=effective_at, sequence_number__lte=action_id)
                )
            else:
                blend_events = blend_events.filter(occurred_at__lte=effective_at)

        for event in blend_events:
            event_data = cast(VolumeBlended, event.get_event_data())
            for source_lot_id in event_data.volumes.keys():
                if source_lot_id not in discovered:
                    discovered.add(source_lot_id)
                    queue.append(source_lot_id)

    return discovered
