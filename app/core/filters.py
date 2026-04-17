from __future__ import annotations

from collections.abc import Callable
from typing import Any, AsyncIterator

from pydantic import BaseModel

JsonObj = dict[str, Any]
PredicateFn = Callable[[JsonObj], bool]


class NDJSONFilter:
    """
    Applies filtering rules to NDJSON objects.
    Filters are pure predicates: Dict[str, Any] -> bool.
    """

    def __init__(self, predicates: list[PredicateFn] | None = None) -> None:
        self.predicates = predicates or []

    def add(self, fn: PredicateFn) -> None:
        """
        Register a new predicate.
        """
        self.predicates.append(fn)

    def match(self, obj: BaseModel | JsonObj) -> bool:
        """
        Check whether an object satisfies all predicates.
        Accepts either a Pydantic model or a raw dict.
        """

        if isinstance(obj, BaseModel):
            data = obj.model_dump()
        else:
            data = obj

        for fn in self.predicates:
            if not fn(data):
                return False

        return True

    async def filter_stream(
        self,
        stream: AsyncIterator[BaseModel | JsonObj],
    ) -> AsyncIterator[BaseModel | JsonObj]:
        """
        Yield only objects that satisfy all predicates.
        """
        async for obj in stream:
            if self.match(obj):
                yield obj


def field_equals(field: str, value: Any) -> PredicateFn:
    """
    Keep objects where obj[field] == value.
    """

    def _fn(obj: JsonObj) -> bool:
        return obj.get(field) == value

    return _fn


def field_in(field: str, values: list[Any]) -> PredicateFn:
    """
    Keep objects where obj[field] is in values.
    """

    def _fn(obj: JsonObj) -> bool:
        return obj.get(field) in values

    return _fn


def field_exists(field: str) -> PredicateFn:
    """
    Keep objects where the field exists.
    """

    def _fn(obj: JsonObj) -> bool:
        return field in obj

    return _fn


def field_not_exists(field: str) -> PredicateFn:
    """
    Keep objects where the field does NOT exist.
    """

    def _fn(obj: JsonObj) -> bool:
        return field not in obj

    return _fn


def numeric_range(
    field: str, *, min: float | None = None, max: float | None = None
) -> PredicateFn:
    """
    Keep objects where min <= obj[field] <= max.
    """

    def _fn(obj: JsonObj) -> bool:
        if field not in obj:
            return False

        try:
            val = float(obj[field])
        except (TypeError, ValueError):
            return False

        if min is not None and val < min:
            return False
        if max is not None and val > max:
            return False

        return True

    return _fn
