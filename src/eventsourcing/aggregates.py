from datetime import datetime
from typing import Any
from typing import Callable
from typing import Iterable
from typing import Type
from typing import TypeVar

from django.db import transaction
from django.db.models import F
from django.db.models import Q

from eventsourcing.aggregate_repository import AggregateRepository
from eventsourcing.models import AggregateEventModel
from eventsourcing.models import AggregateModel
from lib.db.iterators import cursor
from lib.iter import chunk

A = TypeVar("A", bound=AggregateModel)


def load_editable_aggregates_at_time_and_point(
    aggregates: Iterable[A],
    occurred_at: datetime,
    sequence_number: str,
) -> dict[str, A]:
    """
    Load editable versions of the aggregates at a specific point in time, returning persistable representations of before the
    time.

    Any events that occurred on the aggregates with the provided sequence number will be marked for removal by the aggregate
    repository.

    For any aggregates that already exist within the event store, newly initialized aggregates with the correct ID and version
    will be returned for each of the aggregates passed in. The provided aggregates must include all fields necessary to
    create the `identity` of the aggregate.

    For aggregates that are not yet persisted, the aggregate will be marked for backdating and the original version, not a copy,
    will be returned. This is to ensure any aggregates already tracked by the AggregateRepository are not duplicated.
    """
    event_store = None
    aggregates_by_id = {}
    for agg in aggregates:
        event_store = event_store or agg.get_event_model()
        if agg._state.adding:
            # This is a new aggregate that has not been persisted yet. We want to keep using it but mark it for backdating
            # since we are acting on the aggregate at a specific time in the past and the creation will be recorded at the
            # time of insertion.
            agg.mark_for_backdating()
            aggregates_by_id[agg.get_aggregate_id()] = agg
        else:
            aggregates_by_id[agg.get_aggregate_id()] = agg.identity()

    if not aggregates_by_id:
        return {}

    repository = AggregateRepository()

    events_after_and_including = event_store.objects.filter(aggregate_id__in=aggregates_by_id.keys()).filter(
        Q(occurred_at__lt=occurred_at) | Q(occurred_at=occurred_at, sequence_number__lte=sequence_number)
    )

    # For all aggregates with events at or before the provided timestamp/sequence number, rebuild them using that information
    rebuilt_aggregate_ids = set()
    for event in events_after_and_including:
        if event.sequence_number == sequence_number:
            # Capture the particular sequence number we are reverting
            repository.mark_aggregate_event_edited(aggregates_by_id[event.aggregate_id], event)
        else:
            rebuilt_aggregate_ids.add(event.aggregate_id)
            aggregates_by_id[event.aggregate_id].load(event.get_event_data())

    # For all aggregates that have not been rebuilt, we need to load the initial event for them and apply it.
    # These are aggregates that did not exist at this time, but we will be backdating the initial reference to.
    not_rebuilt_ids = set(aggregates_by_id.keys()) - rebuilt_aggregate_ids
    initial_events_before = (
        event_store.objects.filter(aggregate_id__in=not_rebuilt_ids)
        .exclude(sequence_number=sequence_number)
        .order_by("aggregate_id", "occurred_at", F("sequence_number").asc(nulls_first=True), "id")
        .distinct("aggregate_id")
    )

    for event in initial_events_before:
        aggregate = aggregates_by_id[event.aggregate_id]
        aggregate.load(event.get_event_data())
        aggregate.mark_for_backdating()

    return aggregates_by_id


def load_editable_aggregates_at_time(aggregates: Iterable[A], occurred_at: datetime) -> dict[str, A]:
    """
    Load editable versions of the aggregates at the end of a specific point in time, returning persistable
    representations at the time, but after any events that may have occurred at the exact time.
    """
    event_store = None
    aggregates_by_id = {}
    for agg in aggregates:
        event_store = event_store or agg.get_event_model()
        if agg._state.adding:
            # This is a new aggregate that has not been persisted yet. We want to keep using it but mark it for backdating
            # since we are acting on the aggregate at a specific time in the past and the creation will be recorded at the
            # time of insertion.
            agg.mark_for_backdating()
            aggregates_by_id[agg.get_aggregate_id()] = agg
        else:
            aggregates_by_id[agg.get_aggregate_id()] = agg.identity()

    if not aggregates_by_id:
        return {}

    events_before_or_including = event_store.objects.filter(aggregate_id__in=aggregates_by_id.keys()).filter(
        occurred_at__lte=occurred_at
    )

    # For all aggregates with events at or before the provided timestamp/sequence number, rebuild them using that information
    rebuilt_aggregate_ids = set()
    for event in events_before_or_including:
        rebuilt_aggregate_ids.add(event.aggregate_id)
        aggregates_by_id[event.aggregate_id].load(event.get_event_data())

    # For all aggregates that have not been rebuilt, we need to load the initial event for them and apply it.
    # These are aggregates that did not exist at this time, but we will be backdating the initial reference to.
    not_rebuilt_ids = set(aggregates_by_id.keys()) - rebuilt_aggregate_ids
    initial_events_before = (
        event_store.objects.filter(aggregate_id__in=not_rebuilt_ids)
        .order_by("aggregate_id", "occurred_at", F("sequence_number").asc(nulls_first=True), "id")
        .distinct("aggregate_id")
    )

    for event in initial_events_before:
        aggregate = aggregates_by_id[event.aggregate_id]
        aggregate.load(event.get_event_data())
        aggregate.mark_for_backdating()

    return aggregates_by_id


def load_aggregate_states_before(
    aggregates: Iterable[A],
    occurred_at: datetime,
    sequence_number: str | None = None,
) -> dict[str, A]:
    """
    Load the state of the aggregates at a specific point in time, returning non-persistable representations of the aggregates at
    the time.

    This function creates copies of the aggregates passed to it by pulling them from the database fresh and then reverting
    them. This "full copy" behavior should always be assumed for this function, and all the referenced aggregates must
    already exist within the event store for this function to succeed.

    Because of this, thin representations of the aggregates are acceptable. For example, passing in the results of
    `MyAgg.objects.all().only("id")` is acceptable.
    """
    aggregate_ids = set()
    model: A | None = None
    event_store: Type[AggregateEventModel] | None = None
    for agg in aggregates:
        model = model or type(agg)
        aggregate_ids.add(agg.pk)
        event_store = event_store or agg.get_event_model()

    if not aggregate_ids:
        return {}

    reloaded_aggs = model.objects.filter(pk__in=aggregate_ids)
    identities = {}
    for reloaded_agg in reloaded_aggs:
        # Force the aggregates to not be persistable.
        identity = reloaded_agg.identity()
        identity._persistable = False
        identities[reloaded_agg.get_aggregate_id()] = identity

    aggregate_events = event_store.objects.filter(aggregate_id__in=identities.keys())

    sequence_filter = Q(occurred_at__lt=occurred_at)
    if sequence_number is not None:
        sequence_filter = Q(occurred_at__lt=occurred_at) | Q(occurred_at=occurred_at, sequence_number__lt=sequence_number)

    events_before = aggregate_events.filter(sequence_filter)

    # For all aggregates with events before the provided timestamp/sequence number, rebuild them using that information
    rebuilt_aggregate_ids = set()
    for event in events_before:
        rebuilt_aggregate_ids.add(event.aggregate_id)
        identities[event.aggregate_id].load(event.get_event_data())

    # For all aggregates that have not been rebuilt, we need to load the initial event for them and apply it.
    # These are aggregates that did not exist at this time, but we will be backdating the initial reference to.
    not_rebuilt_ids = set(identities.keys()) - rebuilt_aggregate_ids
    first_events_after = (
        event_store.objects.filter(aggregate_id__in=not_rebuilt_ids)
        .order_by("aggregate_id", "occurred_at", F("sequence_number").asc(nulls_first=True), "id")
        .distinct("aggregate_id")
    )

    for event in first_events_after:
        aggregate = identities[event.aggregate_id]
        aggregate.load(event.get_event_data())
        aggregate.mark_for_backdating()

    return identities


def rebuild_aggregates(
    model_class: Type[A],
    *,
    chunk_size=1000,
    prebuild_callback: Callable[[int], None] = None,
    chunk_callback: Callable[[int], None] = None,
    model_id: Any | None = None,
):
    event_model: Type[AggregateEventModel] = model_class.get_event_model()

    all_instances = model_class.objects.all().only("pk", "version")

    if model_id:
        all_instances = all_instances.filter(id=model_id)

    if prebuild_callback:
        total = all_instances.count()
        prebuild_callback(total)

    index = 1

    for chunk_instances in chunk(cursor(all_instances, "pk"), chunk_size):
        rebuilt_instances = {}
        ids = []
        for instance in chunk_instances:
            rebuilt_instances[instance.pk] = instance.identity()
            ids.append(instance.pk)

        events = event_model.objects.filter(aggregate_id__in=ids)
        for event in events:
            rebuilt_instances[event.aggregate_id].load(event.get_event_data())

        with transaction.atomic():
            for instance in rebuilt_instances.values():
                instance.persist()

        if chunk_callback:
            chunk_callback(index)
        index += 1


def reapply_downstream_events_from(aggregate: AggregateModel, occurred_at: datetime, sequence_number: str):
    """
    Reapply all events that occurred after the given timestamp and sequence number.
    """
    # Find all the events that come after any events matching the occurred_at and sequence_number
    events = (
        aggregate.get_event_model()
        .objects.filter(aggregate_id=aggregate.get_aggregate_id())
        .filter(Q(occurred_at__gt=occurred_at) | Q(occurred_at=occurred_at, sequence_number__gt=sequence_number))
    )

    # Reapply each downstream event back onto the new state of the aggregate to build the latest current state to be
    # persisted by the AggregateRepository.
    for event in events:
        aggregate.load(event.get_event_data())
