from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any, AsyncIterator, Literal

from pydantic import BaseModel

logger = logging.getLogger(__name__)

JsonObj = dict[str, Any]
TransformFn = Callable[[JsonObj], JsonObj]

# Policy controlling what happens when a transform raises an exception.
ErrorPolicy = Literal["raise", "skip", "log"]


class NDJSONTransformer:
    """
    Applies a chain of transformation functions to NDJSON objects.

    Each transform receives a mutable dict and returns the same dict
    (after in-place mutation). The transformer guarantees that the input
    object is shallow-copied exactly once, so user data is never mutated
    and the pipeline avoids repeated allocations.

    Parameters
    ----------
    transforms:
        Initial list of transform functions. More can be added via .add().
    on_error:
        What to do when a transform raises:
        - "raise" (default) — propagate the exception immediately.
        - "skip"            — drop the offending object silently.
        - "log"             — log a warning and drop the object.
    """

    def __init__(
        self,
        transforms: list[TransformFn] | None = None,
        on_error: ErrorPolicy = "raise",
    ) -> None:
        self.transforms: list[TransformFn] = transforms or []
        self.on_error: ErrorPolicy = on_error

    def add(self, fn: TransformFn) -> None:
        """Append a transform function to the chain."""
        self.transforms.append(fn)

    def apply(self, obj: BaseModel | JsonObj) -> JsonObj:
        """
        Apply all transforms to a single object and return the result.

        A single shallow copy of the input is created up front. All
        transform functions mutate this working copy in place, which
        avoids repeated dict allocations and reduces GC pressure.
        """
        out: JsonObj = obj.model_dump() if isinstance(obj, BaseModel) else dict(obj)

        for fn in self.transforms:
            out = fn(out)
        return out

    async def apply_stream(
        self,
        stream: AsyncIterator[BaseModel | JsonObj],
    ) -> AsyncIterator[JsonObj]:
        """
        Apply transforms to every object in an async stream.

        Error handling is governed by the on_error policy:
        - "raise": propagate the exception
        - "log":   log and drop the object
        - "skip":  silently drop the object
        """
        async for obj in stream:
            try:
                yield self.apply(obj)
            except Exception as exc:  # noqa: BLE001
                if self.on_error == "raise":
                    raise
                if self.on_error == "log":
                    logger.warning("Transform error, dropping object: %s", exc)
                # "skip" and "log" both drop the object.


def rename_field(old: str, new: str) -> TransformFn:
    """
    Rename a field in place.

    If the field does not exist, the object is returned unchanged.
    """

    def _fn(obj: JsonObj) -> JsonObj:
        if old in obj:
            obj[new] = obj.pop(old)
        return obj

    return _fn


def drop_fields(fields: list[str]) -> TransformFn:
    """
    Remove a list of fields in place.

    Missing fields are silently ignored.
    """

    def _fn(obj: JsonObj) -> JsonObj:
        for f in fields:
            obj.pop(f, None)
        return obj

    return _fn


def add_field(name: str, value: Any, *, overwrite: bool = True) -> TransformFn:
    """
    Add or update a field in place.

    Parameters
    ----------
    name:
        Field name to set.
    value:
        Value to assign.
    overwrite:
        If False, existing values are preserved.
    """

    def _fn(obj: JsonObj) -> JsonObj:
        if not overwrite and name in obj:
            return obj
        obj[name] = value
        return obj

    return _fn


def map_field(name: str, fn: Callable[[Any], Any]) -> TransformFn:
    """
    Apply a function to the value of a field in place.

    If the field is absent, the object is returned unchanged.
    """

    def _fn(obj: JsonObj) -> JsonObj:
        if name in obj:
            obj[name] = fn(obj[name])
        return obj

    return _fn
