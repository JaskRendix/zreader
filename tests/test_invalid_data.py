import pytest

from app.core.filters import NDJSONFilter, field_equals
from app.core.transformers import NDJSONTransformer
from app.core.validators import NDJSONValidator
from app.services.stream_service import StreamService


@pytest.mark.asyncio
async def test_invalid_lines_are_counted():
    svc = StreamService(chunk_size=8192)
    objs = []

    async for obj in svc.process_file("tests/data/invalid.zst"):
        objs.append(obj)

    assert len(objs) == 2
    assert all(isinstance(o, dict) for o in objs)

    for o in objs:
        assert "author" in o or "subreddit" in o or "score" in o


@pytest.mark.asyncio
async def test_invalid_lines_stats():
    svc = StreamService()

    async for _ in svc.process_file("tests/data/invalid.zst"):
        pass

    stats = svc.validator.stats

    assert stats.valid == 2
    assert stats.invalid_json == 2
    assert stats.schema_errors == 0
    assert stats.empty == 0


@pytest.mark.asyncio
async def test_invalid_lines_strict_mode_does_not_raise():
    svc = StreamService(validator=NDJSONValidator(strict=True))

    objs = [o async for o in svc.process_file("tests/data/invalid.zst")]

    assert len(objs) == 2
    assert all(isinstance(o, dict) for o in objs)


@pytest.mark.asyncio
async def test_filtering_with_invalid_lines():
    f = NDJSONFilter([field_equals("subreddit", "s1")])
    svc = StreamService(filters=f)

    objs = [o async for o in svc.process_file("tests/data/invalid.zst")]

    assert objs == [{"author": "a1", "subreddit": "s1"}]


@pytest.mark.asyncio
async def test_transformer_with_invalid_lines():
    t = NDJSONTransformer([lambda o: {**o, "x": 1}])
    svc = StreamService(transformers=t)

    objs = [o async for o in svc.process_file("tests/data/invalid.zst")]

    assert len(objs) == 2
    assert all(o["x"] == 1 for o in objs)
