import pytest

pytest.importorskip("gradio", reason="Space界面在独立的锁定依赖环境验证")

from fastapi.testclient import TestClient  # noqa: E402
from test_space_runtime import LocalLease, settings, worker_command  # noqa: E402

from deploy.hf_space.app import create_app  # noqa: E402
from deploy.hf_space.runtime import Runner  # noqa: E402


def test_public_health_reports_real_checkpoint_status_and_does_not_start_work(tmp_path):
    payload = {
        "errors": {}, "checkpoint": {"generation": "verified"},
        "status": {"phases": {"collecting": 1}, "source_event_counts": {"collected": 2}},
    }
    runner = Runner(settings(tmp_path), LocalLease(), command_factory=lambda path: worker_command(path, payload))
    with TestClient(create_app(runner, start_worker=False)) as client:
        response = client.get("/health")
        assert response.status_code == 503
        assert response.headers["cache-control"] == "no-store"
        assert runner.thread is None
        assert client.post("/health").status_code in {404, 405}
        runner.run_cycle()
        response = client.get("/health")
        assert response.status_code == 200 and response.json()["checkpoint"] == "verified"
        assert client.get("/").status_code == 200
        private = runner.settings.control_dir / "secret.txt"
        private.write_text("private-data-must-stay-private")
        response = client.get(f"/gradio_api/file={private}")
        assert response.status_code in {403, 404}
        assert "private-data-must-stay-private" not in response.text
