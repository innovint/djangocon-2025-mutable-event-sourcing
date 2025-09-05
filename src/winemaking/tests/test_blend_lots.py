from decimal import Decimal

import pytest

from eventsourcing.aggregate_repository import store_aggregate_changes
from winemaking.models import WineLot
from winemaking.models.action import Action
from winemaking.types import ActionType
from winemaking.types import Composition
from winemaking.types import LotComponent
from winemaking.use_cases.blend_lots import record_blend_lot
from winemaking.use_cases.receive_volume import record_receive_volume

# Django DB test marker
pytestmark = [pytest.mark.django_db]


@store_aggregate_changes
def _create_wine_lot(code: str, vintage: int = None) -> WineLot:
    return WineLot.create(
        code=code,
        composition=Composition(
            components={
                LotComponent(variety="Test", appellation="Test", vintage=vintage or 2023): Decimal("1.0"),
            },
        ),
    )


def test_record_blend_lot_raises_for_missing_lots():
    lot = _create_wine_lot(code="BL-MISSING")
    with pytest.raises(ValueError, match=r"Wine lots with IDs missing do not exist\."):
        record_blend_lot(lot_id=lot.id, blend_volumes={"missing": Decimal("1.00")}, blended_volume=Decimal("1.00"))


def test_record_blend_lot_happy_path():
    receiving = _create_wine_lot(code="BL-REC", vintage=2022)
    blended = _create_wine_lot(code="BL-BLD", vintage=2023)

    record_receive_volume(lot_id=blended.id, volume=Decimal("10.00"))
    record_receive_volume(lot_id=receiving.id, volume=Decimal("5.00"))

    record_blend_lot(lot_id=receiving.id, blend_volumes={blended.id: Decimal("5.00")}, blended_volume=Decimal("5.00"))

    actions = Action.objects.all()  # Adjust if Action is imported differently in this project
    assert actions.count() == 3
    action = actions.last()
    assert action.action_type == ActionType.BLEND
    data = action.details.data
    assert data.receiving_wine_lot_id == receiving.id

    # Ensure the blend_volumes mapping contains the correct paired lot and volume
    blended_lot_id, vol = next(iter(data.blend_volumes.items()))
    assert blended_lot_id == blended.id
    assert vol == Decimal("5.00")

    refetched_receiving = WineLot.objects.get(id=receiving.id)
    refetched_blended = WineLot.objects.get(id=blended.id)

    assert refetched_receiving.volume == Decimal("10.00")  # 5.00 original + 5.00 blended in
    assert refetched_blended.volume == Decimal("5.00")  # 10.00 original - 5.00 blended out


def test_record_blend_lot_happy_path_multiple_blends():
    receiving = _create_wine_lot(code="BL-REC2", vintage=2022)
    blended1 = _create_wine_lot(code="BL-BLD1", vintage=2023)
    blended2 = _create_wine_lot(code="BL-BLD2", vintage=2024)

    record_receive_volume(lot_id=blended1.id, volume=Decimal("10.00"))
    record_receive_volume(lot_id=blended2.id, volume=Decimal("20.00"))
    record_receive_volume(lot_id=receiving.id, volume=Decimal("5.00"))

    record_blend_lot(
        lot_id=receiving.id,
        blend_volumes={blended1.id: Decimal("5.00"), blended2.id: Decimal("10.00")},
        blended_volume=Decimal("15.00"),
    )

    actions = Action.objects.all()  # Adjust if Action is imported differently in this project
    assert actions.count() == 4
    action = actions.last()
    assert action.action_type == ActionType.BLEND
    data = action.details.data
    assert data.receiving_wine_lot_id == receiving.id

    # Ensure the blend_volumes mapping contains the correct paired lots and volumes
    blend_volumes_dict = data.blend_volumes
    assert blend_volumes_dict[blended1.id] == Decimal("5.00")
    assert blend_volumes_dict[blended2.id] == Decimal("10.00")

    refetched_receiving = WineLot.objects.get(id=receiving.id)
    refetched_blended1 = WineLot.objects.get(id=blended1.id)
    refetched_blended2 = WineLot.objects.get(id=blended2.id)

    assert refetched_receiving.volume == Decimal("20.00")  # 5.00 original + 15.00 blended in
    assert refetched_blended1.volume == Decimal("5.00")  # 10.00 original - 5.00 blended out
    assert refetched_blended2.volume == Decimal("10.00")  # 20.00 original - 10.00 blended out


def test_record_blend_lot_raises_for_missing_receiving_lot():
    blended = _create_wine_lot(code="BL-BLD-MISS", vintage=2023)
    with pytest.raises(ValueError, match=r"Wine lots with IDs missing do not exist\."):
        record_blend_lot(lot_id="missing", blend_volumes={blended.id: Decimal("1.00")}, blended_volume=Decimal("1.00"))


def test_record_blend_lot_raises_for_missing_blended_lot():
    receiving = _create_wine_lot(code="BL-REC-MISS", vintage=2022)
    with pytest.raises(ValueError, match=r"Wine lots with IDs missing do not exist\."):
        record_blend_lot(lot_id=receiving.id, blend_volumes={"missing": Decimal("1.00")}, blended_volume=Decimal("1.00"))


def test_it_blends_with_losses():
    receiving = _create_wine_lot(code="BL-REC2", vintage=2022)
    blended1 = _create_wine_lot(code="BL-BLD1", vintage=2023)
    blended2 = _create_wine_lot(code="BL-BLD2", vintage=2024)

    record_receive_volume(lot_id=blended1.id, volume=Decimal("10.00"))
    record_receive_volume(lot_id=blended2.id, volume=Decimal("10.00"))
    record_receive_volume(lot_id=receiving.id, volume=Decimal("5.00"))

    record_blend_lot(
        lot_id=receiving.id,
        blend_volumes={
            blended1.id: Decimal("1.00"),
            blended2.id: Decimal("4.00"),
        },
        blended_volume=Decimal("4.00"),
    )

    # Ensure the blended composition is correct
    refetched_receiving = WineLot.objects.get(id=receiving.id)
    assert refetched_receiving.volume == Decimal("9.00")  # 5.00 original + 4.00 blended in
