from datetime import timedelta
from decimal import Decimal

import pytest
from django.utils import timezone

from eventsourcing.aggregate_repository import store_aggregate_changes
from winemaking.models import WineLot
from winemaking.types import Composition
from winemaking.types import LotComponent
from winemaking.use_cases.blend_lots import record_blend_lot
from winemaking.use_cases.calculate_composition import calculate_composition
from winemaking.use_cases.receive_volume import record_receive_volume

# Django DB test marker
pytestmark = [pytest.mark.django_db]


@store_aggregate_changes
def _create_wine_lot(code: str, vintage: int | None = None) -> WineLot:
    return WineLot.create(
        code=code,
        composition=Composition(
            components={
                LotComponent(variety="Test", appellation="Test", vintage=vintage or 2023): Decimal("1.0"),
            },
        ),
    )


def test_calculate_composition_raises_for_missing_lot():
    with pytest.raises(ValueError, match=r"Wine lot with ID missing does not exist\."):
        calculate_composition(lot_id="missing")


def test_calculate_composition_simple_blend():
    receiving = _create_wine_lot(code="CC-REC-1", vintage=2022)
    blended = _create_wine_lot(code="CC-BLD-1", vintage=2023)

    # Seed volumes
    record_receive_volume(lot_id=blended.id, volume=Decimal("10.00"))
    record_receive_volume(lot_id=receiving.id, volume=Decimal("5.00"))

    # Blend 5.00 from blended into receiving (no losses)
    record_blend_lot(lot_id=receiving.id, blend_volumes={blended.id: Decimal("5.00")}, blended_volume=Decimal("5.00"))

    composition = calculate_composition(lot_id=receiving.id)
    comp_2022 = LotComponent(variety="Test", appellation="Test", vintage=2022)
    comp_2023 = LotComponent(variety="Test", appellation="Test", vintage=2023)

    assert composition.components[comp_2022] == Decimal("0.5")
    assert composition.components[comp_2023] == Decimal("0.5")


def test_calculate_composition_multiple_blends():
    receiving = _create_wine_lot(code="CC-REC-2", vintage=2022)
    blended1 = _create_wine_lot(code="CC-BLD-2A", vintage=2023)
    blended2 = _create_wine_lot(code="CC-BLD-2B", vintage=2024)

    # Seed volumes
    record_receive_volume(lot_id=blended1.id, volume=Decimal("10.00"))
    record_receive_volume(lot_id=blended2.id, volume=Decimal("20.00"))
    record_receive_volume(lot_id=receiving.id, volume=Decimal("5.00"))

    # Blend 5.00 from blended1 and 10.00 from blended2 into receiving
    record_blend_lot(
        lot_id=receiving.id,
        blend_volumes={blended1.id: Decimal("5.00"), blended2.id: Decimal("10.00")},
        blended_volume=Decimal("15.00"),
    )

    composition = calculate_composition(lot_id=receiving.id)
    comp_2022 = LotComponent(variety="Test", appellation="Test", vintage=2022)
    comp_2023 = LotComponent(variety="Test", appellation="Test", vintage=2023)
    comp_2024 = LotComponent(variety="Test", appellation="Test", vintage=2024)

    # receiving: 5.00 existing out of 20.00 total => 0.25
    # blended1: 5.00 out of 20.00 total => 0.25
    # blended2: 10.00 out of 20.00 total => 0.5
    assert composition.components[comp_2022] == Decimal("0.25")
    assert composition.components[comp_2023] == Decimal("0.25")
    assert composition.components[comp_2024] == Decimal("0.5")


def test_calculate_composition_traverses_nested_blends():
    # Create a receiving lot with no initial volume and an intermediate lot
    receiving = _create_wine_lot(code="CC-REC-NEST", vintage=2025)
    intermediate = _create_wine_lot(code="CC-INT", vintage=2024)

    # Source lots that feed the intermediate
    source1 = _create_wine_lot(code="CC-SRC1", vintage=2020)
    source2 = _create_wine_lot(code="CC-SRC2", vintage=2021)

    # Seed volumes for sources
    record_receive_volume(lot_id=source1.id, volume=Decimal("10.00"))
    record_receive_volume(lot_id=source2.id, volume=Decimal("10.00"))

    # Blend into intermediate: 4 from source1 and 6 from source2 => 40%/60% composition
    record_blend_lot(
        lot_id=intermediate.id,
        blend_volumes={source1.id: Decimal("4.00"), source2.id: Decimal("6.00")},
        blended_volume=Decimal("10.00"),
    )

    # Now blend intermediate into receiving: 8.00 from intermediate
    record_blend_lot(
        lot_id=receiving.id,
        blend_volumes={intermediate.id: Decimal("8.00")},
        blended_volume=Decimal("8.00"),
    )

    # Since receiving had no prior volume, its composition should mirror the intermediate's
    composition = calculate_composition(lot_id=receiving.id)
    comp_2020 = LotComponent(variety="Test", appellation="Test", vintage=2020)
    comp_2021 = LotComponent(variety="Test", appellation="Test", vintage=2021)

    assert composition.components[comp_2020] == Decimal("0.4")
    assert composition.components[comp_2021] == Decimal("0.6")


def test_calculate_composition_requires_effective_at_when_action_id():
    lot = _create_wine_lot(code="CC-REQ-1", vintage=2022)
    with pytest.raises(ValueError, match=r"effective_at must be provided when action_id is specified\."):
        calculate_composition(lot_id=lot.id, action_id="some-action")


def test_calculate_composition_as_of_time_before_and_at_blend():
    # Setup lots
    receiving = _create_wine_lot(code="CC-REC-TIME", vintage=2022)
    blended = _create_wine_lot(code="CC-BLD-TIME", vintage=2023)

    # Establish times
    t1 = timezone.now() - timedelta(days=7)  # seed volumes
    t2 = timezone.now() - timedelta(days=6)  # perform blend

    # Seed volumes at t1
    record_receive_volume(lot_id=blended.id, volume=Decimal("10.00"), effective_at=t1)
    record_receive_volume(lot_id=receiving.id, volume=Decimal("5.00"), effective_at=t1)

    # Blend at t2
    record_blend_lot(
        lot_id=receiving.id,
        blend_volumes={blended.id: Decimal("5.00")},
        blended_volume=Decimal("5.00"),
        effective_at=t2,
    )

    # As of t1 (before blend): receiving should be 100% its own vintage
    comp_before = calculate_composition(lot_id=receiving.id, effective_at=t1)
    comp_2022 = LotComponent(variety="Test", appellation="Test", vintage=2022)
    comp_2023 = LotComponent(variety="Test", appellation="Test", vintage=2023)
    assert comp_before.components[comp_2022] == Decimal("1.0")
    assert comp_2023 not in comp_before.components

    # As of t2 (including the blend at exactly t2): receiving should be 50/50
    comp_at = calculate_composition(lot_id=receiving.id, effective_at=t2)
    assert comp_at.components[comp_2022] == Decimal("0.5")
    assert comp_at.components[comp_2023] == Decimal("0.5")


def test_calculate_composition_action_filter_at_same_timestamp():
    # Setup lots
    receiving = _create_wine_lot(code="CC-REC-ACT", vintage=2022)
    blended1 = _create_wine_lot(code="CC-BLD-ACT1", vintage=2023)
    blended2 = _create_wine_lot(code="CC-BLD-ACT2", vintage=2024)

    # Establish times
    t1 = timezone.now() - timedelta(days=7)  # seed volumes
    t_same = timezone.now() - timedelta(days=6)  # two blends at the same timestamp

    # Seed volumes at t1
    record_receive_volume(lot_id=blended1.id, volume=Decimal("10.00"), effective_at=t1)
    record_receive_volume(lot_id=blended2.id, volume=Decimal("20.00"), effective_at=t1)
    record_receive_volume(lot_id=receiving.id, volume=Decimal("5.00"), effective_at=t1)

    # Two blends at the same effective_at timestamp
    action1 = record_blend_lot(
        lot_id=receiving.id,
        blend_volumes={blended1.id: Decimal("5.00")},
        blended_volume=Decimal("5.00"),
        effective_at=t_same,
    )
    record_blend_lot(
        lot_id=receiving.id,
        blend_volumes={blended2.id: Decimal("10.00")},
        blended_volume=Decimal("10.00"),
        effective_at=t_same,
    )

    comp_2022 = LotComponent(variety="Test", appellation="Test", vintage=2022)
    comp_2023 = LotComponent(variety="Test", appellation="Test", vintage=2023)
    comp_2024 = LotComponent(variety="Test", appellation="Test", vintage=2024)

    # Without action filter, both blends at the same timestamp are included
    comp_all = calculate_composition(lot_id=receiving.id, effective_at=t_same)
    assert comp_all.components[comp_2022] == Decimal("0.25")
    assert comp_all.components[comp_2023] == Decimal("0.25")
    assert comp_all.components[comp_2024] == Decimal("0.5")

    # With action filter for action1, only the first blend at t_same should be included
    comp_action1 = calculate_composition(lot_id=receiving.id, effective_at=t_same, action_id=action1.id)
    assert comp_action1.components[comp_2022] == Decimal("0.5")
    assert comp_action1.components[comp_2023] == Decimal("0.5")
    assert comp_2024 not in comp_action1.components
