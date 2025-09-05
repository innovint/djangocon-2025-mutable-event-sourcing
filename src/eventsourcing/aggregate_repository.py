from collections import defaultdict
from contextlib import contextmanager
from functools import wraps
from typing import TYPE_CHECKING

from django.db.models import Model
from django.db.transaction import atomic

from eventsourcing.domain_events import AggregateEvent
from eventsourcing.notification_bus import get_notification_bus

if TYPE_CHECKING:
    from eventsourcing.models import AggregateEventModel
    from eventsourcing.models import AggregateModel


class _Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(_Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class AggregateRepository(metaclass=_Singleton):
    def __init__(self):
        self._events = []
        self._new_aggregate_events: dict[AggregateModel, list[AggregateEvent]] = defaultdict(list)
        self._event_stores: dict[type[Model], list[AggregateEvent]] = defaultdict(list)
        self._deleted_event_models: dict[type[Model], list[AggregateEventModel]] = defaultdict(list)

    def add(self, aggregate):
        new_events = self._new_aggregate_events[aggregate]
        added_events = aggregate.get_recorded_events()[len(new_events) :]

        self._new_aggregate_events[aggregate] += added_events
        self._event_stores[aggregate.get_event_model()] += added_events
        self._deleted_event_models[aggregate.get_event_model()] += aggregate.get_deleted_event_models()

        self._events += added_events

    def mark_aggregate_event_edited(self, aggregate: "AggregateModel", event: "AggregateEventModel"):
        """
        Mark an event as edited.

        Edited events will be removed from the event store at `persist` time.
        """
        if aggregate not in self._new_aggregate_events:
            # Ensure each of the aggregates is marked as updated to be persisted
            self._new_aggregate_events[aggregate] = []

        self._deleted_event_models[type(event)].append(event)

    def clear(self):
        self._events = []
        self._new_aggregate_events = defaultdict(list)
        self._event_stores = defaultdict(list)
        self._deleted_event_models = defaultdict(list)

    def persist(self):
        # Save the changes for each aggregate, incrementing the version as it goes to check for collisions.
        for aggregate in self._new_aggregate_events.keys():
            aggregate.persist()

        # Store the events into the persistent event store
        for event_model, events in self._event_stores.items():
            event_model.objects.store(events)

        for event_model, events in self._deleted_event_models.items():
            if ids := [event.id for event in events]:
                event_model.objects.filter(id__in=ids).delete()

        # Publish the events as notifications on the event bus
        get_notification_bus().dispatch_all(self._events)

        self.clear()


def store_aggregate_changes(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        # Create the aggregate repository singleton for this unit of work
        aggregate_repository = AggregateRepository()

        with atomic():
            try:
                response = f(*args, **kwargs)

                # On success persist the aggregate changes and dispatch all notifications
                aggregate_repository.persist()
            except Exception as e:
                # On failure clear the aggregate repository
                aggregate_repository.clear()

                raise e
        return response

    return wrapper


@contextmanager
def aggregate_store():
    aggregate_repository = AggregateRepository()
    with atomic():
        try:
            yield aggregate_repository
            aggregate_repository.persist()
        except Exception:
            aggregate_repository.clear()
            raise
