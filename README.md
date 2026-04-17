# zreader

Async service for reading and processing compressed NDJSON `.zst` files.

The system provides:

- async Zstandard decompression  
- NDJSON line splitting  
- validation  
- filtering  
- transformation  
- streaming responses  
- local file processing  
- HTTP API endpoints  

The codebase is organized into:

- `app/core` — decompressor, NDJSON stream, filters, transformers, validators  
- `app/services` — stream service, file service, stats service  
- `app/api` — FastAPI routers for upload, process, stream, filter, transform, stats  
- `tests` — full test suite with synthetic `.zst` fixtures  

## Local usage

Run the API:

```
make run
```

Run the test suite:

```
make test
```

Process a local `.zst` file:

```python
from app.services.file_service import FileService

fs = FileService()
async for obj in fs.process_file("path/to/file.zst"):
    print(obj)
```

## API

The service exposes:

- `POST /upload`  
- `POST /process`  
- `POST /stream`  
- `POST /filter`  
- `POST /transform`  
- `GET /stats`  
- `GET /health`  

All endpoints operate on compressed NDJSON streams.

## Docker

Build and run:

```
docker build -t zreader .
docker run -p 8000:8000 zreader
```

## Makefile commands

The project includes a Makefile with common tasks:

- `make install` — install the project in editable mode and install test tools
- `make run` — start the API with Uvicorn
- `make dev` — start the API with auto‑reload
- `make test` — run the full test suite
- `make clean` — remove Python cache files
- `make docker` — build the Docker image
