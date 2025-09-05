from decimal import Decimal

import pytest
from django.utils import timezone
from ulid import ULID

from winemaking.models import WineLot
from winemaking.types import Composition
from winemaking.types import LotComponent


@pytest.fixture
def simple_composition():
    comp = Composition(
        components={
            LotComponent(variety="Cabernet", appellation="Napa", vintage=2021): Decimal("1.0"),
        }
    )
    return comp


def test_create_wine_lot(simple_composition):
    lot = WineLot.create(code="LOT001", composition=simple_composition)
    assert lot.code == "LOT001"
    assert lot.volume == Decimal("0.0")
    assert lot.deleted_at is None


def test_update_wine_lot(simple_composition):
    lot = WineLot.create(code="LOT001", composition=simple_composition)
    lot.update(code="LOT002")
    assert lot.code == "LOT002"


def test_destroy_wine_lot(simple_composition):
    lot = WineLot.create(code="LOT001", composition=simple_composition)
    lot.destroy()
    assert lot.deleted_at is not None


def test_blend_in_volume_adds_correctly(simple_composition):
    lot = WineLot.create(code="LOT001", composition=simple_composition)
    lot.receive_volume(action_id=str(ULID()), effective_at=timezone.now(), volume=Decimal("100.0"))
    lot.blend_in_volume(
        action_id=str(ULID()),
        effective_at=timezone.now(),
        volume_received=Decimal("50.0"),
        volumes={str(ULID()): Decimal("50.0")},
    )
    # Expect the volume to have increased by 50 (simple add; adjust if logic is fancier!)
    assert lot.volume == Decimal("150.0")
