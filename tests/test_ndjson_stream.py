from __future__ import annotations

import pytest

from app.core.ndjson_stream import AsyncByteStream, NDJSONStream, iter_bytes_from_file


async def str_chunks(*chunks: str):
    """Yield string chunks as an async iterator — matches NDJSONStream contract."""
    for c in chunks:
        yield c


async def byte_chunks(*chunks: bytes):
    """Yield byte chunks as an async iterator."""
    for c in chunks:
        yield c


@pytest.mark.asyncio
async def test_iter_lines_splits_basic():
    stream = NDJSONStream()
    out = []
    async for line in stream.iter_lines(str_chunks('{"a":1}\n{"b":2', '}\n{"c":3}\n')):
        out.append(line)
    assert out == ['{"a":1}', '{"b":2}', '{"c":3}']


@pytest.mark.asyncio
async def test_iter_lines_partial_final_line():
    """Content with no trailing newline: last line is still yielded."""
    stream = NDJSONStream()
    out = []
    async for line in stream.iter_lines(str_chunks('{"a":1}\n{"b":2}\n{"c":3}')):
        out.append(line)
    assert out == ['{"a":1}', '{"b":2}', '{"c":3}']


@pytest.mark.asyncio
async def test_iter_lines_ignores_blank_lines():
    stream = NDJSONStream()
    out = []
    async for line in stream.iter_lines(str_chunks('\n{"a":1}\n\n{"b":2}\n')):
        out.append(line)
    assert out == ['{"a":1}', '{"b":2}']


@pytest.mark.asyncio
async def test_iter_lines_no_trailing_newline():
    stream = NDJSONStream()
    out = []
    async for line in stream.iter_lines(str_chunks('{"a":1}\n{"b":2}\n{"c":3}')):
        out.append(line)
    assert out == ['{"a":1}', '{"b":2}', '{"c":3}']


@pytest.mark.asyncio
async def test_iter_lines_single_line_with_newline():
    stream = NDJSONStream()
    out = []
    async for line in stream.iter_lines(str_chunks('{"x":1}\n')):
        out.append(line)
    assert out == ['{"x":1}']


@pytest.mark.asyncio
async def test_iter_lines_single_line_no_newline():
    stream = NDJSONStream()
    out = []
    async for line in stream.iter_lines(str_chunks('{"x":1}')):
        out.append(line)
    assert out == ['{"x":1}']


@pytest.mark.asyncio
async def test_iter_lines_empty_input():
    stream = NDJSONStream()
    out = []
    async for line in stream.iter_lines(str_chunks()):
        out.append(line)
    assert out == []


@pytest.mark.asyncio
async def test_iter_lines_only_newlines():
    """A stream of only newlines yields nothing."""
    stream = NDJSONStream()
    out = []
    async for line in stream.iter_lines(str_chunks("\n\n\n")):
        out.append(line)
    assert out == []


@pytest.mark.asyncio
async def test_iter_lines_split_across_many_chunks():
    """A single JSON object split across many one-character chunks."""
    stream = NDJSONStream()
    payload = '{"key":"value"}\n'
    out = []
    async for line in stream.iter_lines(str_chunks(*list(payload))):
        out.append(line)
    assert out == ['{"key":"value"}']


@pytest.mark.asyncio
async def test_iter_lines_many_lines():
    lines = [f'{{"i":{i}}}' for i in range(300)]
    combined = "\n".join(lines) + "\n"
    stream = NDJSONStream()
    out = []
    async for line in stream.iter_lines(str_chunks(combined)):
        out.append(line)
    assert out == lines


@pytest.mark.asyncio
async def test_iter_lines_whitespace_only_lines_ignored():
    """Lines containing only spaces or tabs are treated as blank and skipped."""
    stream = NDJSONStream()
    out = []
    async for line in stream.iter_lines(str_chunks('{"a":1}\n   \n{"b":2}\n')):
        out.append(line)
    assert out == ['{"a":1}', '{"b":2}']


@pytest.mark.asyncio
async def test_iter_lines_unicode():
    stream = NDJSONStream()
    out = []
    async for line in stream.iter_lines(
        str_chunks('{"msg":"héllo"}\n{"msg":"wörld"}\n')
    ):
        out.append(line)
    assert out == ['{"msg":"héllo"}', '{"msg":"wörld"}']


class FakeRequest:
    def __init__(self, chunks: list[bytes]) -> None:
        self._chunks = chunks

    async def stream(self):
        for c in self._chunks:
            yield c


@pytest.mark.asyncio
async def test_async_byte_stream_yields_chunks():
    s = AsyncByteStream(FakeRequest([b"a", b"b", b"c"]))
    out = []
    async for chunk in s:
        out.append(chunk)
    assert out == [b"a", b"b", b"c"]


@pytest.mark.asyncio
async def test_async_byte_stream_skips_empty_chunks():
    s = AsyncByteStream(FakeRequest([b"a", b"", b"b", b"", b"c"]))
    out = []
    async for chunk in s:
        out.append(chunk)
    assert out == [b"a", b"b", b"c"]


@pytest.mark.asyncio
async def test_async_byte_stream_empty_request():
    s = AsyncByteStream(FakeRequest([]))
    out = []
    async for chunk in s:
        out.append(chunk)
    assert out == []


@pytest.mark.asyncio
async def test_async_byte_stream_all_empty_chunks():
    """A stream that yields only empty bytes produces nothing."""
    s = AsyncByteStream(FakeRequest([b"", b"", b""]))
    out = []
    async for chunk in s:
        out.append(chunk)
    assert out == []


@pytest.mark.asyncio
async def test_async_byte_stream_single_large_chunk():
    data = b"x" * 65536
    s = AsyncByteStream(FakeRequest([data]))
    out = []
    async for chunk in s:
        out.append(chunk)
    assert b"".join(out) == data


@pytest.mark.asyncio
async def test_iter_bytes_from_file_chunk_boundary(tmp_path):
    p = tmp_path / "x.bin"
    p.write_bytes(b"abcdef")
    out = []
    async for chunk in iter_bytes_from_file(str(p), chunk_size=2):
        out.append(chunk)
    assert out == [b"ab", b"cd", b"ef"]


@pytest.mark.asyncio
async def test_iter_bytes_from_file_empty_file(tmp_path):
    p = tmp_path / "empty.bin"
    p.write_bytes(b"")
    out = []
    async for chunk in iter_bytes_from_file(str(p), chunk_size=4):
        out.append(chunk)
    assert out == []


@pytest.mark.asyncio
async def test_iter_bytes_from_file_chunk_size_larger_than_file(tmp_path):
    p = tmp_path / "small.bin"
    p.write_bytes(b"hi")
    out = []
    async for chunk in iter_bytes_from_file(str(p), chunk_size=8192):
        out.append(chunk)
    assert b"".join(out) == b"hi"


@pytest.mark.asyncio
async def test_iter_bytes_from_file_reassembles_correctly(tmp_path):
    data = bytes(range(256)) * 100
    p = tmp_path / "data.bin"
    p.write_bytes(data)
    out = []
    async for chunk in iter_bytes_from_file(str(p), chunk_size=64):
        out.append(chunk)
    assert b"".join(out) == data


@pytest.mark.asyncio
async def test_iter_bytes_from_file_not_found_raises():
    with pytest.raises(FileNotFoundError):
        async for _ in iter_bytes_from_file("does/not/exist.bin"):
            pass
