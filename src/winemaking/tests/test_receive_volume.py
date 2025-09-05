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
from winemaking.use_cases.receive_volume import edit_receive_volume
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


def test_record_receive_volume_raises_for_missing_lot():
    with pytest.raises(ValueError, match=r"Wine lot with ID missing does not exist\."):
        record_receive_volume(lot_id="missing", volume=Decimal("1.00"))


def test_record_receive_volume_happy_path():
    lot = _create_wine_lot(code="RV-1")

    record_receive_volume(lot_id=lot.id, volume=Decimal("10.00"))

    lot_refetched = WineLot.objects.get(id=lot.id)
    assert lot_refetched.volume == Decimal("10.00")

    actions = Action.objects.all()
    assert actions.count() == 1
    action = actions.first()
    assert action.action_type == ActionType.RECEIVE_VOLUME
    assert action.details.data.wine_lot_id == lot.id
    assert action.details.data.volume == Decimal("10.00")
    assert action.effective_at is not None


def test_record_receive_volume_backdated_reapplies():
    lot = _create_wine_lot(code="RV-2")
    events = WineLotEventStore.objects.filter(aggregate_id=lot.id).all()
    assert events.count() == 1

    record_receive_volume(lot_id=lot.id, volume=Decimal("5.00"))

    events = WineLotEventStore.objects.filter(aggregate_id=lot.id).all()
    assert events.count() == 2

    t1 = timezone.now() - timedelta(hours=1)
    record_receive_volume(lot_id=lot.id, volume=Decimal("3.00"), effective_at=t1)

    lot_refetched = WineLot.objects.get(id=lot.id)
    assert lot_refetched.volume == Decimal("8.00")
    assert Action.objects.filter(action_type=ActionType.RECEIVE_VOLUME).count() == 2


def test_edit_receive_volume_change_volume_same_lot():
    lot = _create_wine_lot(code="EV-1")
    eff = timezone.now() - timedelta(minutes=5)
    record_receive_volume(lot_id=lot.id, volume=Decimal("5.00"), effective_at=eff)

    action = Action.objects.get(action_type=ActionType.RECEIVE_VOLUME)

    edit_receive_volume(action_id=action.id, lot_id=lot.id, volume=Decimal("7.00"))

    lot_refetched = WineLot.objects.get(id=lot.id)
    assert lot_refetched.volume == Decimal("7.00")

    action.refresh_from_db()
    assert action.revision_number == 1
    assert action.details.data.wine_lot_id == lot.id
    assert action.details.data.volume == Decimal("7.00")
    assert action.updated_at is not None


def test_edit_receive_volume_move_to_other_lot():
    lot_a = _create_wine_lot(code="EV-2A")
    lot_b = _create_wine_lot(code="EV-2B")
    eff = timezone.now() - timedelta(minutes=10)

    record_receive_volume(lot_id=lot_a.id, volume=Decimal("10.00"), effective_at=eff)
    action = Action.objects.get(action_type=ActionType.RECEIVE_VOLUME)

    edit_receive_volume(action_id=action.id, lot_id=lot_b.id, volume=Decimal("4.00"))

    lot_a_refetched = WineLot.objects.get(id=lot_a.id)
    lot_b_refetched = WineLot.objects.get(id=lot_b.id)
    assert lot_a_refetched.volume == Decimal("0.00")
    assert lot_b_refetched.volume == Decimal("4.00")

    action.refresh_from_db()
    assert action.details.data.wine_lot_id == lot_b.id
    assert action.details.data.volume == Decimal("4.00")


def test_edit_receive_volume_raises_for_missing_action():
    with pytest.raises(ValueError, match=r"Action with ID missing does not exist\."):
        edit_receive_volume(action_id="missing", lot_id="irrelevant", volume=Decimal("1.00"))


def test_edit_receive_volume_raises_for_missing_lot():
    lot = _create_wine_lot(code="EV-4")
    record_receive_volume(lot_id=lot.id, volume=Decimal("1.50"))
    rv_action = Action.objects.filter(action_type=ActionType.RECEIVE_VOLUME).first()

    with pytest.raises(ValueError, match=r"Wine lot with ID missing does not exist\."):
        edit_receive_volume(action_id=rv_action.id, lot_id="missing", volume=Decimal("2.00"))
