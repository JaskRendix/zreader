import pytest
from pydantic import BaseModel, RootModel

from app.core.validators import NDJSONValidator, ValidationError_, ValidationStats


class StrictRecord(RootModel[dict]):
    class Model(BaseModel):
        author: str
        score: int

    root: Model


@pytest.mark.parametrize(
    "line,expected",
    [
        ("", None),
        ("   ", None),
        ("\n", None),
    ],
)
def test_blank_lines(line, expected):
    v = NDJSONValidator()
    stats = ValidationStats()
    assert v.validate(line, stats) is expected
    assert stats.empty == 1


def test_valid_json():
    v = NDJSONValidator()
    stats = ValidationStats()
    out = v.validate('{"a": 1}', stats)
    assert out == {"a": 1}
    assert stats.valid == 1
    assert stats.invalid_json == 0
    assert stats.schema_errors == 0


def test_invalid_json_non_strict():
    v = NDJSONValidator(strict=False)
    stats = ValidationStats()
    out = v.validate("INVALID JSON", stats)
    assert out is None
    assert stats.invalid_json == 1


def test_invalid_json_strict_raises():
    v = NDJSONValidator(strict=True)
    stats = ValidationStats()
    with pytest.raises(ValidationError_):
        v.validate("INVALID JSON", stats)


def test_schema_error_non_strict():
    v = NDJSONValidator(model=StrictRecord, strict=False)
    stats = ValidationStats()
    out = v.validate('{"author": "a"}', stats)  # missing score
    assert out is None
    assert stats.schema_errors == 1


def test_schema_error_strict_raises():
    v = NDJSONValidator(model=StrictRecord, strict=True)
    stats = ValidationStats()
    with pytest.raises(ValidationError_):
        v.validate('{"author": "a"}', stats)


@pytest.mark.asyncio
async def test_validate_stream_mixed_lines():
    v = NDJSONValidator()
    stats = ValidationStats()

    async def gen():
        yield ""
        yield "INVALID"
        yield '{"a": 1}'
        yield '{"b": 2}'

    out = [o async for o in v.validate_stream(gen(), stats)]

    assert out == [{"a": 1}, {"b": 2}]
    assert stats.empty == 1
    assert stats.invalid_json == 1
    assert stats.valid == 2


@pytest.mark.asyncio
async def test_validate_stream_strict_raises_on_first_invalid():
    v = NDJSONValidator(strict=True)

    async def gen():
        yield '{"a": 1}'
        yield "INVALID JSON"
        yield '{"b": 2}'

    with pytest.raises(ValidationError_):
        async for _ in v.validate_stream(gen()):
            pass


def test_stats_isolation_between_streams():
    v = NDJSONValidator()

    s1 = ValidationStats()
    s2 = ValidationStats()

    v.validate('{"a": 1}', s1)
    v.validate("INVALID", s2)

    assert s1.valid == 1
    assert s1.invalid_json == 0

    assert s2.valid == 0
    assert s2.invalid_json == 1

    assert v.stats.valid in (0, 1)
