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
    assert out is not obj


def test_transformer_apply_stream():
    t = NDJSONTransformer()

    async def gen():
        yield {"x": 1}
        yield {"x": 2}

    out = []

    async def collect():
        async for o in t.apply_stream(gen()):
            out.append(o)

    import asyncio

    asyncio.run(collect())

    assert out == [{"x": 1}, {"x": 2}]


def test_rename_field():
    t = NDJSONTransformer([rename_field("old", "new")])
    obj = {"old": 1, "keep": 2}
    out = t.apply(obj)
    assert out == {"new": 1, "keep": 2}
    assert "old" not in out
    assert obj == {"old": 1, "keep": 2}


def test_drop_fields():
    t = NDJSONTransformer([drop_fields(["a", "b"])])
    obj = {"a": 1, "b": 2, "c": 3}
    out = t.apply(obj)
    assert out == {"c": 3}
    assert obj == {"a": 1, "b": 2, "c": 3}


def test_add_field():
    t = NDJSONTransformer([add_field("x", 99)])
    obj = {"a": 1}
    out = t.apply(obj)
    assert out == {"a": 1, "x": 99}
    assert obj == {"a": 1}


def test_map_field():
    t = NDJSONTransformer([map_field("n", lambda v: v * 10)])
    obj = {"n": 3, "keep": 1}
    out = t.apply(obj)
    assert out == {"n": 30, "keep": 1}
    assert obj == {"n": 3, "keep": 1}


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
    assert obj == {"a": 10, "remove": 1}


class M(BaseModel):
    a: int
    b: int


def test_transformer_accepts_pydantic_model():
    t = NDJSONTransformer()
    m = M(a=1, b=2)
    out = t.apply(m)
    assert out == {"a": 1, "b": 2}
    assert isinstance(out, dict)


def test_transformer_identity_behavior():
    t = NDJSONTransformer()
    obj = {"k": 1}
    out = t.apply(obj)
    assert out == obj
    assert out is not obj


def test_transformer_add_method():
    t = NDJSONTransformer()
    t.add(add_field("x", 5))
    out = t.apply({"a": 1})
    assert out == {"a": 1, "x": 5}


def test_transformer_prevents_mutation():
    def mutator(obj):
        obj["x"] = 99
        return obj

    t = NDJSONTransformer([mutator])
    obj = {"a": 1}
    out = t.apply(obj)
    assert out == {"a": 1, "x": 99}
    assert obj == {"a": 1}


def test_transformer_apply_stream_purity_and_order():
    t = NDJSONTransformer([add_field("z", 1)])

    async def gen():
        yield {"v": 1}
        yield {"v": 2}

    async def collect():
        return [o async for o in t.apply_stream(gen())]

    import asyncio

    out = asyncio.run(collect())

    assert out == [{"v": 1, "z": 1}, {"v": 2, "z": 1}]
    assert out[0] is not out[1]
