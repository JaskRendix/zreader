import pytest

from app.core.ndjson_stream import AsyncByteStream, NDJSONStream, iter_bytes_from_file


@pytest.mark.asyncio
async def test_ndjson_stream_splits_lines():
    stream = NDJSONStream()
    chunks = [b'{"a":1}\n{"b":2', b'}\n{"c":3}\n']

    async def async_chunks():
        for c in chunks:
            yield c

    out = []
    async for line in stream.iter_lines(async_chunks()):
        out.append(line)

    assert out == ['{"a":1}', '{"b":2}', '{"c":3}']


@pytest.mark.asyncio
async def test_ndjson_stream_handles_partial_final_line():
    stream = NDJSONStream()
    chunks = [b'{"a":1}\n{"b":2', b'}\n{"c":3}']

    async def async_chunks():
        for c in chunks:
            yield c

    out = []
    async for line in stream.iter_lines(async_chunks()):
        out.append(line)

    assert out == ['{"a":1}', '{"b":2}', '{"c":3}']


@pytest.mark.asyncio
async def test_ndjson_stream_ignores_empty_lines():
    stream = NDJSONStream()
    chunks = [b'\n{"a":1}\n\n{"b":2}\n']

    async def async_chunks():
        for c in chunks:
            yield c

    out = []
    async for line in stream.iter_lines(async_chunks()):
        out.append(line)

    assert out == ['{"a":1}', '{"b":2}']


@pytest.mark.asyncio
async def test_ndjson_stream_no_trailing_newline():
    stream = NDJSONStream()
    chunks = [b'{"a":1}\n{"b":2}\n{"c":3}']

    async def async_chunks():
        for c in chunks:
            yield c

    out = []
    async for line in stream.iter_lines(async_chunks()):
        out.append(line)

    assert out == ['{"a":1}', '{"b":2}', '{"c":3}']


@pytest.mark.asyncio
async def test_async_byte_stream_yields_request_chunks():
    class FakeRequest:
        def __init__(self, chunks):
            self._chunks = chunks

        async def stream(self):
            for c in self._chunks:
                yield c

    req = FakeRequest([b"a", b"b", b"c"])
    s = AsyncByteStream(req)

    out = []
    async for chunk in s:
        out.append(chunk)

    assert out == [b"a", b"b", b"c"]


@pytest.mark.asyncio
async def test_async_byte_stream_skips_empty_chunks():
    class FakeRequest:
        def __init__(self, chunks):
            self._chunks = chunks

        async def stream(self):
            for c in self._chunks:
                yield c

    req = FakeRequest([b"a", b"", b"b", b"", b"c"])
    s = AsyncByteStream(req)

    out = []
    async for chunk in s:
        out.append(chunk)

    assert out == [b"a", b"b", b"c"]


@pytest.mark.asyncio
async def test_iter_bytes_from_file(tmp_path):
    p = tmp_path / "x.bin"
    p.write_bytes(b"abcdef")

    out = []
    async for chunk in iter_bytes_from_file(str(p), chunk_size=2):
        out.append(chunk)

    assert out == [b"ab", b"cd", b"ef"]
