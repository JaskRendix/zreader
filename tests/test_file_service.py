import json
from pathlib import Path

import pytest
import zstandard as zstd

from app.services.file_service import FileService


def make_zst(records: list[dict]) -> bytes:
    """Serialize a list of dicts to a compressed NDJSON .zst blob."""
    ndjson = "\n".join(json.dumps(r) for r in records) + "\n"
    return zstd.ZstdCompressor().compress(ndjson.encode())


def write_zst(path: Path, records: list[dict]) -> None:
    path.write_bytes(make_zst(records))


@pytest.mark.asyncio
async def test_process_file_reads_sample_zst():
    fs = FileService(chunk_size=8192)
    objs = [o async for o in fs.process_file("tests/data/sample.zst")]

    assert len(objs) == 3
    assert objs[0]["author"] == "a1"


@pytest.mark.asyncio
async def test_process_file_returns_dicts(tmp_path: Path):
    records = [{"id": 1, "val": "a"}, {"id": 2, "val": "b"}]
    p = tmp_path / "data.zst"
    write_zst(p, records)

    fs = FileService()
    out = [o async for o in fs.process_file(str(p))]

    assert out == records


@pytest.mark.asyncio
async def test_process_file_single_record(tmp_path: Path):
    p = tmp_path / "single.zst"
    write_zst(p, [{"x": 42}])

    fs = FileService()
    out = [o async for o in fs.process_file(str(p))]

    assert out == [{"x": 42}]


@pytest.mark.asyncio
async def test_process_file_many_records(tmp_path: Path):
    records = [{"i": i} for i in range(200)]
    p = tmp_path / "many.zst"
    write_zst(p, records)

    fs = FileService(chunk_size=256)
    out = [o async for o in fs.process_file(str(p))]

    assert out == records


@pytest.mark.asyncio
async def test_process_file_preserves_types(tmp_path: Path):
    record = {
        "int": 1,
        "float": 3.14,
        "bool": True,
        "null": None,
        "list": [1, 2],
        "nested": {"k": "v"},
    }
    p = tmp_path / "types.zst"
    write_zst(p, [record])

    fs = FileService()
    out = [o async for o in fs.process_file(str(p))]

    assert out == [record]


@pytest.mark.asyncio
async def test_process_file_not_found_raises():
    fs = FileService()
    with pytest.raises(FileNotFoundError):
        async for _ in fs.process_file("tests/data/does_not_exist.zst"):
            pass


@pytest.mark.asyncio
async def test_process_file_as_ndjson_reads_sample_zst():
    fs = FileService(chunk_size=8192)
    out = [line async for line in fs.process_file_as_ndjson("tests/data/sample.zst")]

    assert len(out) == 3
    first = json.loads(out[0])
    assert first["author"] == "a1"


@pytest.mark.asyncio
async def test_process_file_as_ndjson_valid_json_lines(tmp_path: Path):
    records = [{"id": i, "name": f"item{i}"} for i in range(10)]
    p = tmp_path / "data.zst"
    write_zst(p, records)

    fs = FileService()
    out = [line async for line in fs.process_file_as_ndjson(str(p))]

    assert len(out) == 10
    parsed = [json.loads(line) for line in out]
    assert parsed == records


@pytest.mark.asyncio
async def test_process_file_as_ndjson_no_trailing_newline(tmp_path: Path):
    p = tmp_path / "data.zst"
    write_zst(p, [{"a": 1}])

    fs = FileService()
    async for line in fs.process_file_as_ndjson(str(p)):
        assert "\n" not in line


@pytest.mark.asyncio
async def test_iter_file_bytes_chunk_boundary(tmp_path: Path):
    p = tmp_path / "x.bin"
    p.write_bytes(b"abcdef")

    fs = FileService(chunk_size=2)
    out = [chunk async for chunk in fs._iter_file_bytes(str(p))]

    assert out == [b"ab", b"cd", b"ef"]


@pytest.mark.asyncio
async def test_iter_file_bytes_empty_file(tmp_path: Path):
    p = tmp_path / "empty.bin"
    p.write_bytes(b"")

    fs = FileService(chunk_size=4)
    out = [chunk async for chunk in fs._iter_file_bytes(str(p))]

    assert out == []


@pytest.mark.asyncio
async def test_iter_file_bytes_chunk_size_larger_than_file(tmp_path: Path):
    p = tmp_path / "small.bin"
    p.write_bytes(b"hi")

    fs = FileService(chunk_size=8192)
    out = [chunk async for chunk in fs._iter_file_bytes(str(p))]

    assert b"".join(out) == b"hi"
