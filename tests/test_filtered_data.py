import pytest

from app.core.filters import NDJSONFilter, field_equals
from app.services.stream_service import StreamService


@pytest.mark.asyncio
async def test_filtering_behavior():
    f = NDJSONFilter()
    f.add(field_equals("subreddit", "python"))

    svc = StreamService(chunk_size=8192, filters=f)
    objs = []

    async for obj in svc.process_file("tests/data/filtered.zst"):
        objs.append(obj)

    assert len(objs) == 2
    assert objs[0]["author"] == "keep1"
    assert objs[1]["author"] == "keep2"
