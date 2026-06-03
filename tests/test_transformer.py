import asyncio

import pytest
from pydantic import BaseModel

from app.core.transformers import (
    NDJSONTransformer,
    add_field,
    drop_fields,
    map_field,
    rename_field,
)


def test_default_transformer_returns_copy():
    t = NDJSONTransformer()
    obj = {"a": 1, "b": 2}
    out = t.apply(obj)
    assert out == obj
    assert out is not obj  # must be a fresh copy


def test_transformer_accepts_pydantic_model():
    class M(BaseModel):
        a: int
        b: int

    t = NDJSONTransformer()
    m = M(a=1, b=2)
    out = t.apply(m)
    assert out == {"a": 1, "b": 2}
    assert isinstance(out, dict)


@pytest.mark.parametrize(
    "transform,obj,expected",
    [
        (rename_field("old", "new"), {"old": 1, "k": 2}, {"new": 1, "k": 2}),
        (drop_fields(["a", "b"]), {"a": 1, "b": 2, "c": 3}, {"c": 3}),
        (add_field("x", 99), {"a": 1}, {"a": 1, "x": 99}),
        (add_field("x", 99, overwrite=False), {"x": 1}, {"x": 1}),  # no-op
        (map_field("n", lambda v: v * 10), {"n": 3, "k": 1}, {"n": 30, "k": 1}),
        (map_field("missing", lambda v: v), {"a": 1}, {"a": 1}),  # no-op
    ],
)
def test_single_transform(transform, obj, expected):
    t = NDJSONTransformer([transform])
    out = t.apply(obj)
    assert out == expected
    assert out is not obj  # must always be a fresh dict


def test_transformer_composition_order():
    t = NDJSONTransformer(
        [
            rename_field("a", "x"),
            map_field("x", lambda v: v + 1),
            add_field("flag", True),
            drop_fields(["remove"]),
        ]
    )
    obj = {"a": 10, "remove": 1}
    out = t.apply(obj)
    assert out == {"x": 11, "flag": True}
    assert obj == {"a": 10, "remove": 1}  # original untouched


@pytest.mark.asyncio
async def test_transformer_error_policy_skip():
    def bad(obj):
        raise RuntimeError("boom")

    t = NDJSONTransformer([bad], on_error="skip")

    async def gen():
        yield {"a": 1}

    out = [o async for o in t.apply_stream(gen())]
    assert out == []  # dropped silently


@pytest.mark.asyncio
async def test_transformer_error_policy_log(caplog):
    def bad(obj):
        raise RuntimeError("boom")

    t = NDJSONTransformer([bad], on_error="log")

    async def gen():
        yield {"a": 1}

    async for _ in t.apply_stream(gen()):
        pass  # should not raise

    assert "Transform error" in caplog.text


@pytest.mark.asyncio
async def test_transformer_error_policy_raise():
    def bad(obj):
        raise RuntimeError("boom")

    t = NDJSONTransformer([bad], on_error="raise")

    async def gen():
        yield {"a": 1}

    with pytest.raises(RuntimeError):
        async for _ in t.apply_stream(gen()):
            pass


@pytest.mark.asyncio
async def test_transformer_apply_stream_purity_and_order():
    t = NDJSONTransformer([add_field("z", 1)])

    async def gen():
        yield {"v": 1}
        yield {"v": 2}

    out = [o async for o in t.apply_stream(gen())]

    assert out == [{"v": 1, "z": 1}, {"v": 2, "z": 1}]
    assert out[0] is not out[1]  # distinct objects
