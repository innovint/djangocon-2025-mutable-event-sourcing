from datetime import datetime
from decimal import Decimal

import pytest

from winemaking.models import Action
from winemaking.models import WineLot
from winemaking.types import ActionType
from winemaking.types import Composition
from winemaking.types import LotComponent


@pytest.fixture
def wine_lot():
    comp = Composition(
        components={
            LotComponent(variety="Cabernet", appellation="Napa", vintage=2021): Decimal("1.0"),
        }
    )
    return WineLot.create(code="LOT001", composition=comp)


@pytest.fixture
def another_wine_lot():
    comp = Composition(
        components={
            LotComponent(variety="Merlot", appellation="Sonoma", vintage=2022): Decimal("1.0"),
        }
    )
    return WineLot.create(code="LOT002", composition=comp)


@pytest.fixture
def fixed_datetime():
    return datetime(2025, 7, 21, 12, 0, 0)


def test_record_remeasure(wine_lot, fixed_datetime):
    # When a remeasure is recorded
    action = Action.record_remeasure(wine_lot=wine_lot, volume=Decimal("75.5"), effective_at=fixed_datetime)

    # Then the action should be created with correct details
    assert action.action_type == ActionType.REMEASURE
    assert action.details.data.wine_lot_id == wine_lot.id
    assert action.details.data.volume == Decimal("75.5")
    assert action.effective_at == fixed_datetime
    assert action.deleted_at is None
    assert action.revision_number == 0


def test_record_remeasure_default_time(wine_lot):
    # When a remeasure is recorded without specifying time
    action = Action.record_remeasure(wine_lot=wine_lot, volume=Decimal("80.0"))

    # Then the current time should be used
    assert action.effective_at is not None
    assert isinstance(action.effective_at, datetime)


def test_record_blend(wine_lot, another_wine_lot, fixed_datetime):
    # When a blend is recorded
    blend_volumes = {wine_lot: Decimal("20.0"), another_wine_lot: Decimal("30.0")}

    action = Action.record_blend(
        blend_volumes=blend_volumes,
        receiving_wine_lot=wine_lot,
        blended_volume=Decimal("45.0"),  # Account for some loss during blending
        effective_at=fixed_datetime,
    )

    # Then the action should be created with correct details
    assert action.action_type == ActionType.BLEND
    assert action.details.data.receiving_wine_lot_id == wine_lot.id
    assert action.details.data.blended_volume == Decimal("45.0")

    # Check blend volumes are correctly mapped by wine lot ID
    expected_blend_volumes = {wine_lot.id: Decimal("20.0"), another_wine_lot.id: Decimal("30.0")}
    assert action.details.data.blend_volumes == expected_blend_volumes

    assert action.effective_at == fixed_datetime
    assert action.deleted_at is None
    assert action.revision_number == 0


def test_record_blend_default_time(wine_lot, another_wine_lot):
    # When a blend is recorded without specifying time
    blend_volumes = {wine_lot: Decimal("10.0"), another_wine_lot: Decimal("15.0")}

    action = Action.record_blend(blend_volumes=blend_volumes, receiving_wine_lot=wine_lot, blended_volume=Decimal("24.5"))

    # Then the current time should be used
    assert action.effective_at is not None
    assert isinstance(action.effective_at, datetime)


def test_record_bottle(wine_lot, fixed_datetime):
    # When a bottling is recorded
    action = Action.record_bottle(
        wine_lot=wine_lot,
        volume_bottled=Decimal("76.0"),
        bottles=100,  # Assuming 75 bottles of 750 mL each
        effective_at=fixed_datetime,
    )

    # Then the action should be created with correct details
    assert action.action_type == ActionType.BOTTLE
    assert action.details.data.wine_lot_id == wine_lot.id
    assert action.details.data.volume_bottled == Decimal("76.0")
    assert action.details.data.bottles == 100
    assert action.effective_at == fixed_datetime
    assert action.deleted_at is None
    assert action.revision_number == 0


def test_record_bottle_default_time(wine_lot):
    # When a bottling is recorded without specifying time
    action = Action.record_bottle(
        wine_lot=wine_lot,
        volume_bottled=Decimal("76.0"),
        bottles=75,
    )

    # Then the current time should be used
    assert action.effective_at is not None
    assert isinstance(action.effective_at, datetime)


def test_edit_remeasure(wine_lot, another_wine_lot, fixed_datetime):
    # Given a remeasure action
    action = Action.record_remeasure(wine_lot=wine_lot, volume=Decimal("75.5"), effective_at=fixed_datetime)
    original_id = action.id

    # When the action is edited
    action.edit_remeasure(wine_lot=another_wine_lot, volume=Decimal("80.25"))

    # Then the action should be updated with new details
    assert action.id == original_id  # ID remains the same
    assert action.action_type == ActionType.REMEASURE  # Type remains the same
    assert action.details.data.wine_lot_id == another_wine_lot.id  # Updated wine lot
    assert action.details.data.volume == Decimal("80.25")  # Updated volume
    assert action.revision_number == 1  # Revision number incremented
    assert action.updated_at is not None  # Updated timestamp set


def test_edit_blend(wine_lot, another_wine_lot, fixed_datetime):
    # Given a blend action
    original_blend_volumes = {wine_lot: Decimal("20.0"), another_wine_lot: Decimal("30.0")}

    action = Action.record_blend(
        blend_volumes=original_blend_volumes,
        receiving_wine_lot=wine_lot,
        blended_volume=Decimal("45.0"),
        effective_at=fixed_datetime,
    )
    original_id = action.id

    # When the action is edited with updated values
    updated_blend_volumes = {wine_lot: Decimal("25.0"), another_wine_lot: Decimal("35.0")}

    action.edit_blend(
        blend_volumes=updated_blend_volumes,
        receiving_wine_lot=another_wine_lot,  # Change receiving lot
        blended_volume=Decimal("55.0"),
    )

    # Then the action should be updated with new details
    assert action.id == original_id  # ID remains the same
    assert action.action_type == ActionType.BLEND  # Type remains the same

    # Check updated blend volumes
    expected_blend_volumes = {wine_lot.id: Decimal("25.0"), another_wine_lot.id: Decimal("35.0")}
    assert action.details.data.blend_volumes == expected_blend_volumes

    # Check other updated fields
    assert action.details.data.receiving_wine_lot_id == another_wine_lot.id
    assert action.details.data.blended_volume == Decimal("55.0")
    assert action.revision_number == 1  # Revision number incremented
    assert action.updated_at is not None  # Updated timestamp set


def test_edit_bottle(wine_lot, another_wine_lot, fixed_datetime):
    # Given a bottle action
    action = Action.record_bottle(wine_lot=wine_lot, volume_bottled=Decimal("76.0"), bottles=100, effective_at=fixed_datetime)
    original_id = action.id

    # When the action is edited
    action.edit_bottle(wine_lot=another_wine_lot, volume_bottled=Decimal("85.5"), bottles=114)

    # Then the action should be updated with new details
    assert action.id == original_id  # ID remains the same
    assert action.action_type == ActionType.BOTTLE  # Type remains the same
    assert action.details.data.wine_lot_id == another_wine_lot.id  # Updated wine lot
    assert action.details.data.volume_bottled == Decimal("85.5")  # Updated volume
    assert action.details.data.bottles == 114  # Updated bottle count
    assert action.revision_number == 1  # Revision number incremented
    assert action.updated_at is not None  # Updated timestamp set


def test_edit_wrong_action_type():
    # Given different action types
    comp = Composition(
        components={
            LotComponent(variety="Cabernet", appellation="Napa", vintage=2021): Decimal("1.0"),
        }
    )
    lot = WineLot.create(code="LOT_TEST", composition=comp)

    # When trying to edit an action with the wrong edit method

    # Volume adjustment action with remeasure edit method
    volume_action = Action.record_receive_volume(
        wine_lot=lot,
        volume=Decimal("10.0"),
    )

    # Then an exception should be raised
    with pytest.raises(ValueError, match="Cannot edit a RECEIVE_VOLUME action as a volume remeasurement"):
        volume_action.edit_remeasure(wine_lot=lot, volume=Decimal("20.0"))

    # Remeasure action with bottle edit method
    remeasure_action = Action.record_remeasure(wine_lot=lot, volume=Decimal("30.0"))

    with pytest.raises(ValueError, match="Cannot edit a REMEASURE action as a bottling"):
        remeasure_action.edit_bottle(wine_lot=lot, volume_bottled=Decimal("40.0"), bottles=50)


def test_edit_deleted_action(wine_lot):
    # Given a deleted action
    action = Action.record_receive_volume(
        wine_lot=wine_lot,
        volume=Decimal("5.0"),
    )

    # When the action is deleted
    action.destroy()

    # Then editing it should raise an error
    with pytest.raises(ValueError, match="Cannot edit a deleted action"):
        action.edit_receive_volume(
            wine_lot=wine_lot,
            volume=Decimal("10.0"),
        )
