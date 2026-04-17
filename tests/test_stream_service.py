import pytest

from app.services.stream_service import StreamService


@pytest.mark.asyncio
async def test_stream_service_valid_sample():
    svc = StreamService(chunk_size=8192)
    objs = []

    async for obj in svc.process_file("tests/data/sample.zst"):
        objs.append(obj)

    # basic shape
    assert len(objs) == 3

    # ensure objects are plain dicts
    assert all(isinstance(o, dict) for o in objs)

    # ensure expected keys / values on first two objects
    assert objs[0]["author"] == "a1"
    assert objs[1]["subreddit"] == "s2"

    # third object: just ensure it's a dict and not empty
    assert isinstance(objs[2], dict)
    assert objs[2]  # not empty

    # ensure no unexpected fields leak in (only check known ones)
    allowed_keys = {"author", "subreddit", "score"}
    for o in objs:
        assert set(o.keys()) <= allowed_keys

    # ensure ordering is preserved (streaming guarantee)
    assert objs[0]["author"] == "a1"
    assert objs[1]["subreddit"] == "s2"
