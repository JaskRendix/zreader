from __future__ import annotations

from collections.abc import Callable, Collection
from typing import Any, AsyncIterator

from pydantic import BaseModel

JsonObj = dict[str, Any]
PredicateFn = Callable[[JsonObj], bool]


class NDJSONFilter:
    """
    Applies filtering rules to NDJSON objects.
    Filters are pure predicates: dict[str, Any] -> bool.
    All registered predicates must match for an object to pass (AND logic).
    """

    def __init__(self, predicates: list[PredicateFn] | None = None) -> None:
        self.predicates: list[PredicateFn] = predicates or []

    def add(self, fn: PredicateFn) -> None:
        """Register a new predicate."""
        self.predicates.append(fn)

    def match(self, obj: BaseModel | JsonObj) -> bool:
        """
        Check whether an object satisfies all predicates.
        Accepts either a Pydantic model or a raw dict.
        """
        data: JsonObj = obj.model_dump() if isinstance(obj, BaseModel) else obj
        return all(fn(data) for fn in self.predicates)

    async def filter_stream(
        self,
        stream: AsyncIterator[BaseModel | JsonObj],
    ) -> AsyncIterator[BaseModel | JsonObj]:
        """Yield only objects that satisfy all predicates."""
        async for obj in stream:
            if self.match(obj):
                yield obj


def field_equals(field: str, value: Any) -> PredicateFn:
    """Keep objects where obj[field] == value."""

    def _fn(obj: JsonObj) -> bool:
        return obj.get(field) == value

    return _fn


def field_in(field: str, values: Collection[Any]) -> PredicateFn:
    """
    Keep objects where obj[field] is in values.

    values is converted to a frozenset at predicate creation time so that
    membership tests are O(1) regardless of collection size.
    """
    value_set = frozenset(values)

    def _fn(obj: JsonObj) -> bool:
        return obj.get(field) in value_set

    return _fn


def field_exists(field: str) -> PredicateFn:
    """Keep objects where field is present."""

    def _fn(obj: JsonObj) -> bool:
        return field in obj

    return _fn


def field_not_exists(field: str) -> PredicateFn:
    """Keep objects where field is absent."""

    def _fn(obj: JsonObj) -> bool:
        return field not in obj

    return _fn


def numeric_range(
    field: str,
    *,
    min_val: float | None = None,
    max_val: float | None = None,
) -> PredicateFn:
    """
    Keep objects where min_val <= obj[field] <= max_val.

    Objects missing the field or with a non-numeric value are excluded.
    Either bound may be omitted (None = unbounded on that side).
    """

    def _fn(obj: JsonObj) -> bool:
        if field not in obj:
            return False
        try:
            val = float(obj[field])
        except (TypeError, ValueError):
            return False
        if min_val is not None and val < min_val:
            return False
        if max_val is not None and val > max_val:
            return False
        return True

    return _fn
