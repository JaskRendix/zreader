from __future__ import annotations

from collections.abc import Callable
from typing import Any, AsyncIterator

from pydantic import BaseModel

JsonObj = dict[str, Any]
TransformFn = Callable[[JsonObj], JsonObj]


class NDJSONTransformer:
    """
    Applies transformations to NDJSON objects.
    Transformations are pure functions: Dict[str, Any] -> Dict[str, Any].
    """

    def __init__(self, transforms: list[TransformFn] | None = None) -> None:
        self.transforms = transforms or []

    def add(self, fn: TransformFn) -> None:
        self.transforms.append(fn)

    def apply(self, obj: BaseModel | JsonObj) -> JsonObj:
        """
        Apply all transformations to a single object.
        Always returns a *new* dict, never mutates input.
        """

        # Convert Pydantic model to dict
        if isinstance(obj, BaseModel):
            data = obj.model_dump()
        else:
            data = obj

        # Start from a fresh copy to avoid mutation
        out = dict(data)

        # Apply transformations
        for fn in self.transforms:
            out = fn(out)

        return out

    async def apply_stream(
        self,
        stream: AsyncIterator[BaseModel | JsonObj],
    ) -> AsyncIterator[JsonObj]:
        async for obj in stream:
            yield self.apply(obj)


def rename_field(old: str, new: str) -> TransformFn:
    def _fn(obj: JsonObj) -> JsonObj:
        if old in obj:
            obj = dict(obj)
            obj[new] = obj.pop(old)
        return obj

    return _fn


def drop_fields(fields: list[str]) -> TransformFn:
    def _fn(obj: JsonObj) -> JsonObj:
        obj = dict(obj)
        for f in fields:
            obj.pop(f, None)
        return obj

    return _fn


def add_field(name: str, value: Any) -> TransformFn:
    def _fn(obj: JsonObj) -> JsonObj:
        obj = dict(obj)
        obj[name] = value
        return obj

    return _fn


def map_field(name: str, fn: Callable[[Any], Any]) -> TransformFn:
    def _fn(obj: JsonObj) -> JsonObj:
        obj = dict(obj)
        if name in obj:
            obj[name] = fn(obj[name])
        return obj

    return _fn
