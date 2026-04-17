import pytest
import zstandard as zstd

from app.core.decompressor import AsyncZstdDecompressor


async def make_stream(chunks):
    for c in chunks:
        yield c


def compress(data: bytes) -> bytes:
    return zstd.ZstdCompressor().compress(data)


@pytest.mark.asyncio
async def test_decompress_stream_single_chunk():
    raw = b"hello world"
    comp = compress(raw)

    d = AsyncZstdDecompressor(chunk_size=5)
    out = []

    async for chunk in d.decompress_stream(make_stream([comp])):
        out.append(chunk)

    assert "".join(out) == "hello world"


@pytest.mark.asyncio
async def test_decompress_stream_multiple_chunks():
    raw = b"abcdefghij"
    comp = compress(raw)

    d = AsyncZstdDecompressor(chunk_size=3)
    out = []

    async for chunk in d.decompress_stream(make_stream([comp[:5], comp[5:]])):
        out.append(chunk)

    assert "".join(out) == "abcdefghij"


@pytest.mark.asyncio
async def test_decompress_lines_basic():
    raw = b"one\ntwo\nthree\n"
    comp = compress(raw)

    d = AsyncZstdDecompressor()
    out = []

    async for line in d.decompress_lines(make_stream([comp])):
        out.append(line)

    assert out == ["one", "two", "three"]


@pytest.mark.asyncio
async def test_decompress_lines_partial_final_line():
    raw = b"one\ntwo\nthree"
    comp = compress(raw)

    d = AsyncZstdDecompressor()
    out = []

    async for line in d.decompress_lines(make_stream([comp])):
        out.append(line)

    assert out == ["one", "two", "three"]


@pytest.mark.asyncio
async def test_decompress_empty_input():
    d = AsyncZstdDecompressor()
    out = []

    async for chunk in d.decompress_stream(make_stream([])):
        out.append(chunk)

    assert out == []


@pytest.mark.asyncio
async def test_decompress_lines_empty_input():
    d = AsyncZstdDecompressor()
    out = []

    async for line in d.decompress_lines(make_stream([])):
        out.append(line)

    assert out == []


@pytest.mark.asyncio
async def test_decompress_stream_multiple_async_chunks():
    raw = b"abcdefghijklmnopqrstuvwxyz"
    comp = compress(raw)

    chunks = [comp[i : i + 7] for i in range(0, len(comp), 7)]

    d = AsyncZstdDecompressor(chunk_size=4)
    out = []

    async for chunk in d.decompress_stream(make_stream(chunks)):
        out.append(chunk)

    assert "".join(out) == raw.decode()
