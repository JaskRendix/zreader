import pytest

from app.core.filters import (
    NDJSONFilter,
    field_equals,
    field_exists,
    field_in,
    numeric_range,
)
from app.core.transformers import NDJSONTransformer
from app.core.validators import NDJSONValidator
from app.services.stream_service import StreamService


@pytest.mark.asyncio
async def test_filtering_basic_equals():
    f = NDJSONFilter([field_equals("subreddit", "python")])
    svc = StreamService(filters=f)

    objs = [o async for o in svc.process_file("tests/data/filtered.zst")]

    assert len(objs) == 2
    assert objs[0]["author"] == "keep1"
    assert objs[1]["author"] == "keep2"


@pytest.mark.asyncio
async def test_filtering_multiple_predicates():
    f = NDJSONFilter(
        [
            field_equals("subreddit", "python"),
            numeric_range("score", min_val=150),
        ]
    )
    svc = StreamService(filters=f)

    objs = [o async for o in svc.process_file("tests/data/filtered.zst")]

    assert objs == [{"author": "keep2", "subreddit": "python", "score": 200}]


@pytest.mark.asyncio
async def test_filtering_field_in():
    f = NDJSONFilter([field_in("subreddit", ["python"])])
    svc = StreamService(filters=f)

    objs = [o async for o in svc.process_file("tests/data/filtered.zst")]

    assert len(objs) == 2


@pytest.mark.asyncio
async def test_filtering_field_exists():
    f = NDJSONFilter([field_exists("score")])
    svc = StreamService(filters=f)

    objs = [o async for o in svc.process_file("tests/data/filtered.zst")]

    assert len(objs) == 3


@pytest.mark.asyncio
async def test_filtering_no_matches():
    f = NDJSONFilter([field_equals("subreddit", "nonexistent")])
    svc = StreamService(filters=f)

    objs = [o async for o in svc.process_file("tests/data/filtered.zst")]

    assert objs == []


@pytest.mark.asyncio
async def test_filtering_with_transformer():
    t = NDJSONTransformer([lambda o: {**o, "flag": True}])
    f = NDJSONFilter([field_equals("subreddit", "python")])

    svc = StreamService(filters=f, transformers=t)

    objs = [o async for o in svc.process_file("tests/data/filtered.zst")]

    assert len(objs) == 2
    assert all(o["flag"] is True for o in objs)


@pytest.mark.asyncio
async def test_filtering_with_validator_strict_schema():
    validator = NDJSONValidator(strict=False)
    f = NDJSONFilter([numeric_range("score", min_val=50)])

    svc = StreamService(filters=f, validator=validator)

    objs = [o async for o in svc.process_file("tests/data/filtered.zst")]

    assert len(objs) == 2
    assert objs[0]["score"] == 100
    assert objs[1]["score"] == 200


@pytest.mark.asyncio
async def test_filtering_transforming_and_validating_full_pipeline():
    f = NDJSONFilter([field_equals("subreddit", "python")])
    t = NDJSONTransformer([lambda o: {**o, "boosted": o["score"] * 2}])
    v = NDJSONValidator(strict=False)

    svc = StreamService(filters=f, transformers=t, validator=v)

    objs = [o async for o in svc.process_file("tests/data/filtered.zst")]

    assert len(objs) == 2
    assert objs[0]["boosted"] == 200
    assert objs[1]["boosted"] == 400


@pytest.mark.asyncio
async def test_filtering_preserves_order():
    f = NDJSONFilter([field_equals("subreddit", "python")])
    svc = StreamService(filters=f)

    objs = [o async for o in svc.process_file("tests/data/filtered.zst")]

    assert [o["author"] for o in objs] == ["keep1", "keep2"]
