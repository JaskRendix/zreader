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
    Applies a chain of pure transformation functions to NDJSON objects.
    Each function receives a dict and returns a (new) dict.

    Parameters
    ----------
    transforms:
        Initial list of transform functions. More can be added via .add().
    on_error:
        What to do when a transform raises:
        - "raise" (default) — propagate the exception immediately.
        - "skip"            — silently drop the offending object.
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

        Always operates on a shallow copy of the input so the original is
        never mutated. Each transform in the chain receives the output of
        the previous one.
        """
        data: JsonObj = obj.model_dump() if isinstance(obj, BaseModel) else dict(obj)

        # Pass the working copy through each transform.  Transform helpers
        # also copy internally, but we own the initial copy here so helpers
        # can skip their own defensive copy if needed in the future.
        out = data
        for fn in self.transforms:
            out = fn(out)
        return out

    async def apply_stream(
        self,
        stream: AsyncIterator[BaseModel | JsonObj],
    ) -> AsyncIterator[JsonObj]:
        """
        Apply transforms to every object in an async stream.
        Error handling is governed by the on_error policy.
        """
        async for obj in stream:
            try:
                yield self.apply(obj)
            except Exception as exc:  # noqa: BLE001
                if self.on_error == "raise":
                    raise
                if self.on_error == "log":
                    logger.warning("Transform error, dropping object: %s", exc)
                # "skip" and "log" both drop the object — just don't yield.


def rename_field(old: str, new: str) -> TransformFn:
    """Rename a field. If old is absent the object is returned unchanged."""

    def _fn(obj: JsonObj) -> JsonObj:
        if old not in obj:
            return obj
        result = dict(obj)
        result[new] = result.pop(old)
        return result

    return _fn


def drop_fields(fields: list[str]) -> TransformFn:
    """Remove a list of fields. Missing fields are silently ignored."""

    def _fn(obj: JsonObj) -> JsonObj:
        result = dict(obj)
        for f in fields:
            result.pop(f, None)
        return result

    return _fn


def add_field(name: str, value: Any, *, overwrite: bool = True) -> TransformFn:
    """
    Add or update a field.

    Parameters
    ----------
    name:
        The field name to set.
    value:
        The value to assign.
    overwrite:
        If False, existing values are preserved and no error is raised.
        Defaults to True (existing values are replaced).
    """

    def _fn(obj: JsonObj) -> JsonObj:
        if not overwrite and name in obj:
            return obj
        result = dict(obj)
        result[name] = value
        return result

    return _fn


def map_field(name: str, fn: Callable[[Any], Any]) -> TransformFn:
    """Apply fn to the value of field name. If the field is absent, no-op."""

    def _fn(obj: JsonObj) -> JsonObj:
        if name not in obj:
            return obj
        result = dict(obj)
        result[name] = fn(result[name])
        return result

    return _fn
