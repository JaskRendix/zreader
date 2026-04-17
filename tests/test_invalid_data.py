import pytest

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
