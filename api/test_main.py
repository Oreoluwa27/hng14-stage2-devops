import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from fastapi.testclient import TestClient

# ── Patch Redis before the app module is imported ──────────────────────────────
# The module-level `r = Redis(...)` runs at import time, so we need to
# intercept it before our app code executes.

@pytest.fixture(autouse=True)
def mock_redis():
    """
    Single fixture that stubs out every Redis method used by the app.
    `autouse=True` means every test in this file gets it automatically.
    """
    mock = AsyncMock()

    # pipeline() returns a context manager whose __aenter__ gives back
    # another AsyncMock (the actual pipeline object).
    pipeline_mock = AsyncMock()
    pipeline_mock.__aenter__ = AsyncMock(return_value=pipeline_mock)
    pipeline_mock.__aexit__ = AsyncMock(return_value=False)
    mock.pipeline.return_value = pipeline_mock

    with patch("main.r", mock):
        yield mock


@pytest.fixture
def client(mock_redis):
    # Import AFTER the patch is in place so the app sees the mock.
    from main import app
    # `raise_server_exceptions=True` (default) re-raises 500s in tests.
    with TestClient(app, raise_server_exceptions=False) as c:
        yield c


# ── POST /jobs ─────────────────────────────────────────────────────────────────

class TestCreateJob:
    def test_returns_202_with_job_id(self, client):
        response = client.post("/jobs")

        assert response.status_code == 202
        body = response.json()
        assert "job_id" in body
        # UUID4 format sanity-check
        import uuid
        uuid.UUID(body["job_id"], version=4)  # raises if invalid

    def test_pipeline_called_with_correct_commands(self, client, mock_redis):
        response = client.post("/jobs")
        job_id = response.json()["job_id"]

        pipe = mock_redis.pipeline.return_value
        pipe.lpush.assert_awaited_once_with("myapp:jobs:queue", job_id)
        pipe.hset.assert_awaited_once_with(f"job:{job_id}", "status", "queued")
        pipe.expire.assert_awaited_once_with(f"job:{job_id}", 86400)
        pipe.execute.assert_awaited_once()

    def test_each_job_gets_unique_id(self, client):
        id_a = client.post("/jobs").json()["job_id"]
        id_b = client.post("/jobs").json()["job_id"]
        assert id_a != id_b


# ── GET /jobs/{job_id} ─────────────────────────────────────────────────────────

class TestGetJob:
    def test_returns_status_when_job_exists(self, client, mock_redis):
        mock_redis.hget = AsyncMock(return_value="queued")

        response = client.get("/jobs/abc-123")

        assert response.status_code == 200
        assert response.json() == {"job_id": "abc-123", "status": "queued"}

    def test_returns_404_when_job_missing(self, client, mock_redis):
        mock_redis.hget = AsyncMock(return_value=None)

        response = client.get("/jobs/does-not-exist")

        assert response.status_code == 404
        assert response.json()["detail"] == "Job not found"

    def test_hget_called_with_correct_key(self, client, mock_redis):
        mock_redis.hget = AsyncMock(return_value="processing")

        client.get("/jobs/xyz-789")

        mock_redis.hget.assert_awaited_once_with("job:xyz-789", "status")


# ── GET /health ────────────────────────────────────────────────────────────────

class TestHealthCheck:
    def test_healthy_when_redis_responds(self, client, mock_redis):
        mock_redis.ping = AsyncMock(return_value=True)

        response = client.get("/health")

        assert response.status_code == 200
        assert response.json() == {"status": "healthy"}

    def test_unhealthy_when_redis_unreachable(self, client, mock_redis):
        mock_redis.ping = AsyncMock(side_effect=ConnectionError("refused"))

        response = client.get("/health")

        assert response.status_code == 200          # endpoint itself still 200
        assert response.json() == {"status": "unhealthy"}