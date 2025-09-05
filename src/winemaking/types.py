from decimal import Decimal

from django.db.models.enums import TextChoices
from pydantic import BaseModel
from pydantic import Field
from pydantic import model_validator


class ActionType(TextChoices):
    RECEIVE_VOLUME = "RECEIVE_VOLUME"
    BLEND = "BLEND"
    REMEASURE = "REMEASURE"
    BOTTLE = "BOTTLE"


class LotComponent(BaseModel):
    variety: str
    appellation: str
    vintage: int = Field(..., ge=1900, le=2100)

    def __hash__(self):
        return hash((self.variety, self.appellation, self.vintage))

    def __eq__(self, other):
        if not isinstance(other, LotComponent):
            return NotImplemented

        return self.variety == other.variety and self.appellation == other.appellation and self.vintage == other.vintage


class Composition(BaseModel):
    components: dict[LotComponent, Decimal] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_percentages(cls, obj):
        total = sum(percent for percent in obj.components.values())
        # Check tolerance for floating point/decimal precision issues
        if not (Decimal("0.9999") < total < Decimal("1.0001")):
            raise ValueError(f"Total percentage must be 100 but got {total * 100:.2f}%")
        return obj


class ComponentAmount(BaseModel):
    """
    A database-friendly way of storing a lot's components.

    This version is less helpful for validations than Cmoposition, but can be serialized into a JSON value easily.

    TODO Consider if validating here and switching entire to this is better
    """

    component: LotComponent
    percent: Decimal
