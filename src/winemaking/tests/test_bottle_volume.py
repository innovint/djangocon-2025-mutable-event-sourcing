from datetime import timedelta
from decimal import Decimal

import pytest
from django.utils import timezone

from eventsourcing.aggregate_repository import store_aggregate_changes
from winemaking.models import WineLotEventStore
from winemaking.models.action import Action
from winemaking.models.wine_lot import WineLot
from winemaking.types import ActionType
from winemaking.types import Composition
from winemaking.types import LotComponent
from winemaking.use_cases.bottle_volume import edit_bottle_volume
from winemaking.use_cases.bottle_volume import record_bottle_volume
from winemaking.use_cases.receive_volume import record_receive_volume

pytestmark = [pytest.mark.django_db]


@store_aggregate_changes
def _create_wine_lot(code: str) -> WineLot:
    return WineLot.create(
        code=code,
        composition=Composition(
            components={
                LotComponent(variety="Test", appellation="Test", vintage=2023): Decimal("1.0"),
            },
        ),
    )


def test_record_bottle_volume_raises_for_missing_lot():
    with pytest.raises(ValueError, match=r"Wine lot with ID missing does not exist\."):
        record_bottle_volume(lot_id="missing", volume_bottled=Decimal("1.00"), bottles=1)


def test_record_bottle_volume_happy_path():
    lot = _create_wine_lot(code="BV-1")

    record_receive_volume(lot_id=lot.id, volume=Decimal("5.00"))

    record_bottle_volume(lot_id=lot.id, volume_bottled=Decimal("2.50"), bottles=3)

    lot_refetched = WineLot.objects.get(id=lot.id)
    assert lot_refetched.volume == Decimal("2.50")

    actions = Action.objects.all()
    assert actions.count() == 2
    action = actions.last()
    assert action.action_type == ActionType.BOTTLE
    assert action.details.data.wine_lot_id == lot.id
    assert action.details.data.volume_bottled == Decimal("2.50")
    assert action.effective_at is not None


def test_record_bottle_volume_backdated_reapplies():
    lot = _create_wine_lot(code="BV-2")
    events = WineLotEventStore.objects.filter(aggregate_id=lot.id).all()
    assert events.count() == 1

    now = timezone.now()

    record_receive_volume(lot_id=lot.id, volume=Decimal("5.00"), effective_at=now - timedelta(hours=2))

    record_bottle_volume(lot_id=lot.id, volume_bottled=Decimal("2.50"), bottles=3)

    events = WineLotEventStore.objects.filter(aggregate_id=lot.id).all()
    assert events.count() == 3

    record_bottle_volume(lot_id=lot.id, volume_bottled=Decimal("1.00"), bottles=1, effective_at=now - timedelta(hours=1))

    lot_refetched = WineLot.objects.get(id=lot.id)
    # With backdated bottle, the final volume should reflect the later churn, resulting in the most recent value (here 2.50)
    assert lot_refetched.volume == Decimal("1.50")
    assert Action.objects.filter(action_type=ActionType.BOTTLE).count() == 2


def test_edit_bottle_volume_change_volume_same_lot():
    lot = _create_wine_lot(code="BV-3")

    now = timezone.now()

    record_receive_volume(lot_id=lot.id, volume=Decimal("5.00"), effective_at=now - timedelta(hours=2))

    record_bottle_volume(lot_id=lot.id, volume_bottled=Decimal("2.00"), bottles=2, effective_at=now - timedelta(minutes=10))

    action = Action.objects.get(action_type=ActionType.BOTTLE)

    edit_bottle_volume(action_id=action.id, lot_id=lot.id, volume_bottled=Decimal("4.00"), bottles=3)

    lot_refetched = WineLot.objects.get(id=lot.id)
    assert lot_refetched.volume == Decimal("1.00")

    action.refresh_from_db()
    assert action.revision_number == 1
    assert action.details.data.wine_lot_id == lot.id
    assert action.details.data.volume_bottled == Decimal("4.00")
    assert action.updated_at is not None


def test_edit_bottle_volume_move_to_other_lot():
    lot_a = _create_wine_lot(code="BV-4A")
    lot_b = _create_wine_lot(code="BV-4B")

    now = timezone.now()

    record_receive_volume(lot_id=lot_a.id, volume=Decimal("5.00"), effective_at=now - timedelta(hours=2))
    record_receive_volume(lot_id=lot_b.id, volume=Decimal("4.00"), effective_at=now - timedelta(hours=1, minutes=30))

    record_bottle_volume(lot_id=lot_a.id, volume_bottled=Decimal("4.00"), bottles=4, effective_at=now - timedelta(minutes=10))
    action = Action.objects.get(action_type=ActionType.BOTTLE)

    edit_bottle_volume(action_id=action.id, lot_id=lot_b.id, volume_bottled=Decimal("1.50"), bottles=2)

    lot_a_refetched = WineLot.objects.get(id=lot_a.id)
    lot_b_refetched = WineLot.objects.get(id=lot_b.id)
    assert lot_a_refetched.volume == Decimal("5.00")
    assert lot_b_refetched.volume == Decimal("2.50")

    action.refresh_from_db()
    assert action.details.data.wine_lot_id == lot_b.id
    assert action.details.data.volume_bottled == Decimal("1.50")


def test_edit_bottle_volume_raises_for_missing_action():
    with pytest.raises(ValueError, match=r"Action with ID missing does not exist\."):
        edit_bottle_volume(action_id="missing", lot_id="irrelevant", volume_bottled=Decimal("1.00"), bottles=1)
