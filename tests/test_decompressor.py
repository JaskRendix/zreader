import pytest
import zstandard as zstd

from app.core.decompressor import AsyncZstdDecompressor


async def make_stream(chunks):
    """Yield each item in chunks as an async iterator."""
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
async def test_decompress_stream_multiple_input_chunks():
    raw = b"abcdefghij"
    comp = compress(raw)
    d = AsyncZstdDecompressor(chunk_size=3)
    out = []
    async for chunk in d.decompress_stream(make_stream([comp[:5], comp[5:]])):
        out.append(chunk)
    assert "".join(out) == "abcdefghij"


@pytest.mark.asyncio
async def test_decompress_stream_many_small_input_chunks():
    raw = b"abcdefghijklmnopqrstuvwxyz"
    comp = compress(raw)
    chunks = [comp[i : i + 7] for i in range(0, len(comp), 7)]
    d = AsyncZstdDecompressor(chunk_size=4)
    out = []
    async for chunk in d.decompress_stream(make_stream(chunks)):
        out.append(chunk)
    assert "".join(out) == raw.decode()


@pytest.mark.asyncio
async def test_decompress_stream_chunk_size_larger_than_data():
    raw = b"short"
    comp = compress(raw)
    d = AsyncZstdDecompressor(chunk_size=65536)
    out = []
    async for chunk in d.decompress_stream(make_stream([comp])):
        out.append(chunk)
    assert "".join(out) == "short"


@pytest.mark.asyncio
async def test_decompress_stream_empty_input():
    d = AsyncZstdDecompressor()
    out = []
    async for chunk in d.decompress_stream(make_stream([])):
        out.append(chunk)
    assert out == []


@pytest.mark.asyncio
async def test_decompress_stream_skips_empty_chunks():
    raw = b"hello"
    comp = compress(raw)
    d = AsyncZstdDecompressor()
    out = []
    async for chunk in d.decompress_stream(make_stream([b"", comp, b""])):
        out.append(chunk)
    assert "".join(out) == "hello"


@pytest.mark.asyncio
async def test_decompress_stream_large_payload():
    raw = ("x" * 100_000).encode()
    comp = compress(raw)
    d = AsyncZstdDecompressor(chunk_size=1024)
    out = []
    async for chunk in d.decompress_stream(make_stream([comp])):
        out.append(chunk)
    assert "".join(out) == raw.decode()


@pytest.mark.asyncio
async def test_decompress_stream_invalid_data_raises():
    d = AsyncZstdDecompressor()
    with pytest.raises(Exception):
        async for _ in d.decompress_stream(make_stream([b"not zstd data"])):
            pass


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
async def test_decompress_lines_no_trailing_newline():
    raw = b"one\ntwo\nthree"
    comp = compress(raw)
    d = AsyncZstdDecompressor()
    out = []
    async for line in d.decompress_lines(make_stream([comp])):
        out.append(line)
    assert out == ["one", "two", "three"]


@pytest.mark.asyncio
async def test_decompress_lines_empty_input():
    d = AsyncZstdDecompressor()
    out = []
    async for line in d.decompress_lines(make_stream([])):
        out.append(line)
    assert out == []


@pytest.mark.asyncio
async def test_decompress_lines_single_line():
    raw = b"just one line"
    comp = compress(raw)
    d = AsyncZstdDecompressor()
    out = []
    async for line in d.decompress_lines(make_stream([comp])):
        out.append(line)
    assert out == ["just one line"]


@pytest.mark.asyncio
async def test_decompress_lines_single_line_with_newline():
    raw = b"just one line\n"
    comp = compress(raw)
    d = AsyncZstdDecompressor()
    out = []
    async for line in d.decompress_lines(make_stream([comp])):
        out.append(line)
    assert out == ["just one line"]


@pytest.mark.asyncio
async def test_decompress_lines_many_lines():
    lines = [f"line{i}" for i in range(500)]
    raw = "\n".join(lines).encode()
    comp = compress(raw)
    d = AsyncZstdDecompressor(chunk_size=64)
    out = []
    async for line in d.decompress_lines(make_stream([comp])):
        out.append(line)
    assert out == lines


@pytest.mark.asyncio
async def test_decompress_lines_blank_lines_are_yielded():
    raw = b"a\n\nb\n"
    comp = compress(raw)
    d = AsyncZstdDecompressor()
    out = []
    async for line in d.decompress_lines(make_stream([comp])):
        out.append(line)
    assert out == ["a", "", "b"]


@pytest.mark.asyncio
async def test_decompress_lines_unicode():
    raw = "héllo\nwörld\n".encode("utf-8")
    comp = compress(raw)
    d = AsyncZstdDecompressor()
    out = []
    async for line in d.decompress_lines(make_stream([comp])):
        out.append(line)
    assert out == ["héllo", "wörld"]


@pytest.mark.asyncio
async def test_decompress_lines_unicode_split_across_chunks():
    raw = ("café\n" * 50).encode("utf-8")
    comp = compress(raw)
    # Split compressed bytes into small pieces to stress the boundary handling.
    chunks = [comp[i : i + 11] for i in range(0, len(comp), 11)]
    d = AsyncZstdDecompressor(chunk_size=8)
    out = []
    async for line in d.decompress_lines(make_stream(chunks)):
        out.append(line)
    assert all(line == "café" for line in out)
    assert len(out) == 50
