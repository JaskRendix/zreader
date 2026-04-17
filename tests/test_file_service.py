import asyncio

import pytest

from app.services.file_service import FileService


@pytest.mark.asyncio
async def test_file_service_reads_zst():
    fs = FileService(chunk_size=8192)
    objs = []

    async for obj in fs.process_file("tests/data/sample.zst"):
        objs.append(obj)

    assert len(objs) == 3
    assert objs[0]["author"] == "a1"


@pytest.mark.asyncio
async def test_file_service_iter_file_bytes(tmp_path):
    p = tmp_path / "x.bin"
    p.write_bytes(b"abcdef")

    fs = FileService(chunk_size=2)
    out = []

    async for chunk in fs.iter_file_bytes(str(p)):
        out.append(chunk)

    assert out == [b"ab", b"cd", b"ef"]


@pytest.mark.asyncio
async def test_file_service_empty_file(tmp_path):
    p = tmp_path / "empty.bin"
    p.write_bytes(b"")

    fs = FileService(chunk_size=4)
    out = []

    async for chunk in fs.iter_file_bytes(str(p)):
        out.append(chunk)

    assert out == []


@pytest.mark.asyncio
async def test_file_service_process_file_as_ndjson():
    fs = FileService(chunk_size=8192)
    out = []

    async for line in fs.process_file_as_ndjson("tests/data/sample.zst"):
        out.append(line)

    assert len(out) == 3

    import json

    first = json.loads(out[0])
    assert first["author"] == "a1"
