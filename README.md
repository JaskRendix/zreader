# zreader

Async service for reading and processing compressed NDJSON `.zst` files.

## Features

- Async Zstandard decompression
- NDJSON line splitting
- Pydantic validation with strict mode and per-stream error stats
- Filtering and transformation pipelines
- Streaming HTTP responses
- Local file processing
- Configurable via environment variables

## Project layout

```
app/core/       — decompressor, NDJSON stream, filters, transformers, validators
app/services/   — stream service, file service, stats service
app/api/        — FastAPI routers
tests/          — full test suite with synthetic .zst fixtures
```

## Local usage

```bash
make install   # install in editable mode with test dependencies
make run       # start API on :8000
make dev       # start API with auto-reload
make test      # run the full test suite
make clean     # remove Python cache files
make docker    # build the Docker image
```

Process a local `.zst` file directly:

```python
import asyncio
from app.services.file_service import FileService

async def main():
    fs = FileService()
    async for obj in fs.process_file("path/to/file.zst"):
        print(obj)

asyncio.run(main())
```

## API

All endpoints accept compressed NDJSON streams (`.zst`).

| Method | Path         | Description                              |
|--------|--------------|------------------------------------------|
| POST   | `/upload`    | Upload and validate a `.zst` file        |
| POST   | `/process`   | Decompress, validate, and return objects |
| POST   | `/stream`    | Stream decompressed NDJSON line by line  |
| POST   | `/filter`    | Apply field filters and return matches   |
| POST   | `/transform` | Apply transformations and return results |
| GET    | `/stats`     | Processing statistics snapshot           |
| GET    | `/health`    | Service health check                     |

## Configuration

All settings are read from environment variables at startup. Invalid values
fall back to defaults with a logged warning.

| Variable                  | Default            | Description                        |
|---------------------------|--------------------|------------------------------------|
| `ZREADER_CHUNK_SIZE`      | `16384`            | Decompression buffer size (bytes)  |
| `ZREADER_MAX_QUEUE_SIZE`  | `8`                | Max internal queue depth           |
| `ZREADER_MAX_UPLOAD_MB`   | `512`              | Max upload size (MB)               |
| `ZREADER_LOG_LEVEL`       | `INFO`             | One of DEBUG / INFO / WARNING / ERROR / CRITICAL |
| `ZREADER_SERVICE_NAME`    | `zreader-service`  | Service name in logs and API docs  |
| `ZREADER_VERSION`         | `1.0.0`            | Version string in API docs         |

## Validation

`NDJSONValidator` separates two distinct failure modes:

- `json.JSONDecodeError` — the line is not valid JSON (corrupt input)
- `pydantic.ValidationError` — valid JSON that does not match the schema

By default invalid lines are logged and skipped. Pass `strict=True` to raise
instead. After iterating a stream, error counts are available on `validator.stats`:

```python
from app.core.validators import NDJSONValidator

validator = NDJSONValidator()
async for obj in validator.validate_stream(lines):
    ...

print(validator.stats)
# ValidationStats(valid=98, invalid_json=1, schema_errors=1, empty=2)
```

## Transformation error policy

`NDJSONTransformer` accepts an `on_error` parameter controlling what happens
when a transform function raises:

```python
from app.core.transformers import NDJSONTransformer

# "raise" (default) — propagate immediately
# "skip"            — drop the object silently
# "log"             — log a warning and drop the object
transformer = NDJSONTransformer(on_error="log")
```

## Stats

`StatsService.snapshot()` returns an immutable `StatsSnapshot` safe to
serialise directly into API responses:

```python
snapshot = stats_service.snapshot()
# StatsSnapshot(uptime_seconds=4.2, lines_total=500, lines_valid=498,
#               lines_invalid=2, lines_filtered_out=10, lines_emitted=488,
#               throughput_lps=116.19)
```

## Docker

```bash
docker build -t zreader .
docker run -p 8000:8000 zreader
```
