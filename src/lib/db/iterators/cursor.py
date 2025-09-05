from typing import Any
from typing import Generator
from typing import TypeVar

from django.db.models import Model
from django.db.models import Q
from django.db.models import QuerySet

T = TypeVar("T", bound=Model)
QS = TypeVar("QS", bound=QuerySet[T])


def cursor(qset: QS, primary_key: str, tiebreaker_key: str | None = None, size: int = 1000) -> Generator[T, Any, Any]:
    """
    Iterate over a queryset using cursor pagination.

    Using cursor pagination can help when there is the potential for a large number of results to be returned, but in general
    the number should remain relatively small. This fits well between offset pagination, which performs poorly in large
    datasets, and DB cursors, which are optimized when the results are always expected to be large.

    The ordering of the queryset will use the primary key and the optional tiebreaker. The tiebreaker must be unique and
    should be used when the primary key is not unique. An example of this is iterating over actions by effective at,
    which is not guaranteed to be unique, so we would then also use the ID.

    See this great post for a description of these behaviors: https://brunoscheufler.com/blog/2022-01-01-paginating-large-ordered-datasets-with-cursor-based-pagination

    Args:
        qset: The queryset to iterate over
        primary_key: The primary key to use for ordering. For descending order, prepend the key with a "-".
        tiebreaker_key: The tiebreaker key to use for ordering. For descending order, prepend the key with a "-".
        size: The number of results to return per iteration. This should be optimized based on the expected number of results.

    Returns:

    """
    primary_lookup = primary_key.replace("-", "")
    primary_comparison = "gt" if "-" not in primary_key else "lt"
    if tiebreaker_key:
        tiebreaker_lookup = tiebreaker_key.replace("-", "")
        tiebreaker_comparison = "gt" if "-" not in tiebreaker_key else "lt"
        qset = qset.order_by(primary_key, tiebreaker_key)
    else:
        tiebreaker_lookup = None
        qset = qset.order_by(primary_key)

    results = list(qset[: size + 1])

    yield from results[:size]

    while len(results) > size:
        last_used = results[-2]
        last_used_primary = getattr(last_used, primary_lookup)

        if tiebreaker_key:
            last_used_tiebreaker = getattr(last_used, tiebreaker_lookup)
            next_qset = qset.filter(
                # The primary key is greater than the last used primary key
                Q(**{f"{primary_lookup}__{primary_comparison}": last_used_primary})
                # Or the primary key is the same as the last used primary key, but the tiebreaker is greater
                | Q(
                    **{
                        f"{primary_lookup}": last_used_primary,
                        f"{tiebreaker_lookup}__{tiebreaker_comparison}": last_used_tiebreaker,
                    }
                )
            )
        else:
            next_qset = qset.filter(**{f"{primary_lookup}__{primary_comparison}": last_used_primary})

        results = list(next_qset[: size + 1])
        yield from results[:size]
