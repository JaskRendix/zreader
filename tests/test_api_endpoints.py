import pytest
from httpx import ASGITransport, AsyncClient

from app.config import settings
from app.main import create_app
from app.services.stats_service import StatsService


@pytest.mark.asyncio
async def test_health_endpoint():
    app = create_app()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        r = await ac.get("/health")
        assert r.status_code == 200
        assert r.json()["status"] == "ok"


@pytest.mark.asyncio
async def test_health_response_shape():
    app = create_app()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        r = await ac.get("/health")
        body = r.json()
        assert set(body.keys()) == {"status", "service", "version"}


@pytest.mark.asyncio
async def test_stats_endpoint():
    app = create_app()
    app.state.stats = StatsService()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        r = await ac.get("/stats")
        assert r.status_code == 200
        body = r.json()
        assert "uptime_seconds" in body
        assert "lines_total" in body


@pytest.mark.asyncio
async def test_all_routes_exist():
    app = create_app()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        for route in ["/health"]:
            r = await ac.get(route)
            assert r.status_code != 404


@pytest.mark.asyncio
async def test_filter_rejects_large_payload():
    app = create_app()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        headers = {"content-length": str(settings.max_upload_size_bytes + 1)}
        r = await ac.post("/filter", headers=headers, json={})
        assert r.status_code == 413


@pytest.mark.asyncio
async def test_filter_accepts_empty_stream():
    app = create_app()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        payload = {
            "equals": None,
            "in_set": None,
            "exists": None,
            "not_exists": None,
            "numeric": None,
        }
        r = await ac.post("/filter", json=payload)
        assert r.status_code == 200


@pytest.mark.asyncio
async def test_process_rejects_large_payload():
    app = create_app()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        headers = {"content-length": str(settings.max_upload_size_bytes + 1)}
        r = await ac.post("/process", headers=headers)
        assert r.status_code == 413


@pytest.mark.asyncio
async def test_upload_rejects_wrong_extension():
    app = create_app()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        files = {"file": ("data.txt", b"dummy", "application/octet-stream")}
        r = await ac.post("/upload", files=files)
        assert r.status_code == 400


@pytest.mark.asyncio
async def test_upload_rejects_unsupported_media_type():
    app = create_app()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        files = {"file": ("data.zst", b"dummy", "text/plain")}
        r = await ac.post("/upload", files=files)
        assert r.status_code == 415


@pytest.mark.asyncio
async def test_transform_rejects_unknown_map_op():
    app = create_app()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        payload = {"map": {"field": "does_not_exist"}}
        r = await ac.post("/transform", json=payload)
        assert r.status_code == 400
        assert "Unknown map operation" in r.json()["detail"]


@pytest.mark.asyncio
async def test_transform_rejects_large_payload():
    app = create_app()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        headers = {"content-length": str(settings.max_upload_size_bytes + 1)}
        r = await ac.post("/transform", headers=headers, json={})
        assert r.status_code == 413
