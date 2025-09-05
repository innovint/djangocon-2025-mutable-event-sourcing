import logging
import re
import uuid
from typing import Self

import ulid
from django.core.exceptions import ImproperlyConfigured
from django.db import models
from django.db.models import F
from django.db.models.functions import Now
from django.utils import timezone

from eventsourcing.aggregate_repository import AggregateRepository
from eventsourcing.domain_events import AggregateEvent
from eventsourcing.notification_bus import get_notification_bus

logger = logging.getLogger(__name__)


_class_to_func_pattern = re.compile(r"(?<!^)(?=[A-Z])")
"""
A regular expression for splitting a pascal case string (class name) into chunks. These chunks can then be combined using
underscores to create a snake case string.

This is compiled once on import for all uses in the future.
"""


class OutOfDateVersionException(Exception):
    def __init__(self, model_name: str):
        self.model_name = model_name

    def get_message(self) -> str:
        return f"The {self.model_name} you are trying to update is out of date. Please refresh and try again."


class CannotPersistAggregateView(Exception):
    def __init__(self, model_name: str):
        self.model_name = model_name

    def get_message(self) -> str:
        return f"The {self.model_name} you are trying to persist is a view and cannot be persisted."


class AggregateModelManagerMixin:
    def bulk_persist(self, instances: list["AggregateModel"]):
        events = [event for instance in instances for event in instance.get_recorded_events()]
        self.model.get_event_model().objects.store(events)

        self.bulk_create(instances)
        get_notification_bus().dispatch_all(events)


class AggregateModelManager(AggregateModelManagerMixin, models.Manager):
    pass


class AggregateModel(models.Model):
    id = models.CharField(primary_key=True, max_length=26, editable=False)
    version = models.PositiveIntegerField(editable=False)

    class Meta:
        abstract = True

    def confirm_version(self, version):
        if self.version != version:
            raise OutOfDateVersionException(self.__class__._meta.verbose_name)

    @classmethod
    def next_id(cls) -> str:
        # We need a unique, monotonically increasing ID for the aggregate.
        return str(ulid.ULID())

    @classmethod
    def get_event_model(cls):
        if hasattr(cls, "event_model"):
            return cls.event_model

        raise ImproperlyConfigured(
            "The event model must be set on the Aggregate or the get_event_model function should be overwritten."
        )

    def get_events_queryset(self, aggregate_id: str | None = None) -> models.QuerySet[AggregateEvent]:
        """Returns a queryset of aggregate events for the aggregate."""
        # Sometimes (inventory line items) the aggregate id is not the same as the pk.
        if aggregate_id is None:
            aggregate_id = self.pk
        return self.get_event_model().objects.filter(aggregate_id=aggregate_id)

    def identity(self) -> Self:
        """
        Create a new instance of the aggregate representing the blank "identity" of the aggregate.

        The new instance will be set as if it is already added. This is useful for rebuilding aggregates from events for
        persistence.

        If aggregates require any other fields to represent their identity, then this function should be overridden by the
        implementing class.
        """
        instance = type(self)()
        instance.pk = self.pk
        instance.version = self.version
        instance._state.adding = False

        return instance

    def get_aggregate_id(self) -> uuid.UUID:
        return self.pk

    def apply(self, event: AggregateEvent):
        self.load(event)

        self.get_recorded_events().append(event)
        if self.is_persistable():
            AggregateRepository().add(self)

    def load(self, event: AggregateEvent):
        """
        Load the event onto the aggregate.

        This function is similar to the `apply` function but does not interact with the aggregate's into event memory or the
        AggregateRepository. It is useful for rebuilding aggregates from events without persisting the event. The most common
        use cases for this are:

        * reapplying downstream events after an edit to calculate the aggregates new current state
        * replaying events to create a view of the aggregate at a point in time
        """
        self._validate_event_context(event)
        self._apply_event(event)

    def _is_before_creation(self) -> bool:
        return getattr(self, "__is_before_creation__", False)

    def _set_is_before_creation(self, value: bool = True):
        self.__is_before_creation__ = value

    def mark_for_backdating(self):
        """
        Mark the aggregate for backdating.

        If desirable, each aggregate
        """
        self._set_is_before_creation()

    def _validate_event_context(self, event: AggregateEvent):
        """
        Validate the context of the event before applying it onto the aggregate.

        Any checks that may be impacted by editing an upstream action MUST be included in a context validation function!

        This function is automatically called by `apply` and `reapply_reverted`. It will call a function based on the name of
        the event. For example, if the event is `SkuInventoryAdded`, it will call `validate_sku_inventory_added_context`. If
        this function does not exist on the aggregate, nothing will be run.

        The validation functions should raise domain exceptions in the case of errors. The validation functions should be
        limited to the domain validation around the context and results of the event, and they must be restricted to the data
        available on the current state of the aggregate or the event. For this reason, it is more restricted than checks
        available during the DDD functions on our aggregates.

        An example of how this might function is to check that when applying or reapplying a `SkuInventoryAdded` event,
        the total units of the SKU in the inventory do not exceed the maximum units allowed for the SKU.
        """
        name = _class_to_func_pattern.sub("_", event.__class__.__name__).lower()
        fn_name = f"validate_{name}_context"

        if hasattr(self, fn_name):
            getattr(self, fn_name)(event)

    def _apply_event(self, event: AggregateEvent):
        name = _class_to_func_pattern.sub("_", event.__class__.__name__).lower()
        fn_name = f"apply_{name}"

        if not hasattr(self, fn_name):
            raise NotImplementedError(f"Event handling method named {fn_name} not implemented")

        getattr(self, fn_name)(event)

    def persist(self):
        if hasattr(self, "_persistable") and not self._persistable:
            raise CannotPersistAggregateView(self.__class__._meta.verbose_name)

        if self._state.adding:
            self.version = 1
            super().save(force_insert=True)
        else:
            # For updates, we skip the "save" function and directly update the fields while checking the version. This is
            # similar to what the "save" function does, but is NOT the exact same. There are edge cases around relationships
            # and deferred values. The assumption is that we can ignore these edge cases specifically for aggregates.
            current_version = self.version
            self.version += 1
            updated_rows = self.__class__.objects.filter(pk=self.pk, version=current_version).update(
                **{field.name: getattr(self, field.name) for field in self._meta.fields if field.editable}
            )
            if updated_rows == 0:
                raise OutOfDateVersionException(self.__class__._meta.verbose_name)

    def is_persistable(self) -> bool:
        return not hasattr(self, "_persistable") or self._persistable

    def get_recorded_events(self) -> list[AggregateEvent]:
        if not hasattr(self, "_recorded_events"):
            self._recorded_events = []

        return self._recorded_events

    def get_deleted_event_models(self) -> list["AggregateEventModel"]:
        if not hasattr(self, "_deleted_event_models"):
            self._deleted_event_models = []

        return self._deleted_event_models

    def save(self, *args, **kwargs):
        logger.warning("The save method should not be used directly on aggregates. Use persist instead.")
        self.persist()


class EventStoreManagerMixin:
    def store(self, events: list[AggregateEvent]):
        instances = []
        for event in events:
            instances.append(
                self.model(
                    aggregate_id=event.aggregate_id,
                    event_type=event.event_type,
                    event_data=event.model_dump(mode="json"),
                    occurred_at=getattr(event, "occurred_at", timezone.now()),
                    sequence_number=getattr(event, "sequence_number", None),
                )
            )

        self.bulk_create(instances)


class AggregateEventModelQuerySet(models.QuerySet):
    def reverse(self):
        return self.order_by("-occurred_at", F("sequence_number").desc(nulls_last=True), "-id")


class AggregateEventModelManager(EventStoreManagerMixin, models.Manager.from_queryset(AggregateEventModelQuerySet)):
    pass


class AggregateEventOrderingMixin:
    """
    A Meta class mixin defining the default aggregate event ordering.

    This mixin should be used in the Meta class of a child of the AggregateEventModel to define the default ordering of
    events. This is required because non-abstract child models will not inherit the `ordering` key of an abstract parent
    model if the child defines a `Meta` class at all.

    Example:

    ```python
    class MyEventModel(AggregateEventModel):
        class Meta(AggregateEventOrderingMixin):
            app_label = "my_app
    ```
    """

    ordering = ["occurred_at", F("sequence_number").asc(nulls_first=True), "id"]


class AggregateEventModel(models.Model):
    aggregate_id = models.CharField(db_index=True)
    event_type = models.CharField()
    event_data = models.JSONField()
    created_at = models.DateTimeField(auto_created=True, db_default=Now())
    occurred_at = models.DateTimeField()
    sequence_number = models.CharField(null=True)

    objects = AggregateEventModelManager()

    class Meta(AggregateEventOrderingMixin):
        abstract = True

    @classmethod
    def get_event_class(cls, event_type) -> type[AggregateEvent]:
        if not hasattr(cls, "event_types"):
            raise ImproperlyConfigured(
                "The event_types attribute must be set on the AggregateEventModel or override `get_event_class`."
            )

        if not hasattr(cls, "event_types_dict"):
            cls.event_types_dict = {c.event_type: c for c in cls.event_types}

        return cls.event_types_dict[event_type]

    def get_event_data(self) -> AggregateEvent:
        if not hasattr(self, "_event_data"):
            self._event_data = self.get_event_class(self.event_type).model_validate(self.event_data)

        return self._event_data
