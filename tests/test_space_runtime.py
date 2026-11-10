import json
import sys
import threading
from types import SimpleNamespace

import httpx
from huggingface_hub.errors import EntryNotFoundError, HfHubHTTPError

from deploy.hf_space.runtime import HubLease, Runner, Settings, report_error


class Hub:
    def __init__(self, root):
        self.root, self.head, self.body = root, "0", None
        self.race = False

    def create_branch(self, *args, **kwargs):
        pass

    def repo_info(self, *args, **kwargs):
        return SimpleNamespace(sha=self.head)

    def hf_hub_download(self, *args, **kwargs):
        if self.body is None:
            raise EntryNotFoundError("missing")
        path = self.root / f"{self.head}.json"
        path.write_bytes(self.body)
        return str(path)

    def create_commit(self, repo_id, *, operations, parent_commit, **kwargs):
        if self.race:
            self.race = False
            self.head = str(int(self.head) + 1)
        if parent_commit != self.head:
            raise HfHubHTTPError("conflict", response=httpx.Response(409, request=httpx.Request("POST", "https://hub.invalid")))
        self.body = operations[0].path_or_fileobj
        self.head = str(int(self.head) + 1)
        return SimpleNamespace(oid=self.head)


def test_hub_lease_rejects_competing_workers_and_stale_releases(tmp_path):
    hub = Hub(tmp_path)
    first = HubLease("owner/space", "production", api=hub, clock=lambda: 100)
    second = HubLease("owner/space", "production", api=hub, clock=lambda: 101)
    assert first.acquire("first", 20)
    assert not second.acquire("second", 20)
    expired = HubLease("owner/space", "production", api=hub, clock=lambda: 121)
    assert expired.acquire("second", 20)
    assert not first.release("first")
    assert json.loads(hub.body)["production"]["owner"] == "second"
    assert expired.release("second")
    hub.race = True
    assert not first.acquire("raced", 20)


def test_controller_diagnostics_redact_credentials_and_signed_urls(capsys):
    report_error("test", RuntimeError("hf_sensitive_token Bearer abc123 https://example.invalid/?secret=value"))
    output = capsys.readouterr().out
    assert "sensitive_token" not in output and "abc123" not in output and "secret=value" not in output
    assert "RuntimeError" in output


class LocalLease:
    def __init__(self):
        self.owner = None

    def acquire(self, owner, seconds):
        if self.owner is not None:
            return False
        self.owner = owner
        return True

    def release(self, owner):
        assert self.owner == owner
        self.owner = None
        return True


def settings(tmp_path, **extra):
    return Settings(
        worker_root=tmp_path,
        python=sys.executable,
        control_dir=tmp_path / "control",
        space_id="owner/space",
        mode="qualification",
        version="release-1",
        **extra,
    )


def worker_command(receipt, payload, delay=0):
    script = (
        "import json,time,pathlib;"
        f"time.sleep({delay});"
        f"pathlib.Path({str(receipt)!r}).write_text({json.dumps(payload)!r})"
    )
    return [sys.executable, "-c", script]


def test_runner_health_requires_a_verified_receipt_and_keeps_private_fields_out(tmp_path):
    payload = {
        "errors": {},
        "checkpoint": {"generation": "verified-generation"},
        "status": {"phases": {"collecting": 1}, "source_event_counts": {"collected": 2}},
        "private_debug": "hf_secret_should_not_be_exposed",
    }
    lease = LocalLease()
    runner = Runner(settings(tmp_path), lease, command_factory=lambda path: worker_command(path, payload))
    assert not runner.snapshot()["healthy"]
    runner.run_cycle()
    state = runner.snapshot()
    assert state["healthy"] and state["checkpoint"] == "verified-generation"
    assert state["counts"] == {"collected": 2}
    assert "hf_secret" not in json.dumps(state)
    assert lease.owner is None
    runner.command_factory = lambda path: [sys.executable, "-c", "raise SystemExit(0)"]
    runner.run_cycle()
    assert not runner.snapshot()["healthy"]
    assert runner.snapshot()["error"] == "missing_receipt"


def test_runner_kills_timed_out_worker_before_releasing_the_lease(tmp_path):
    lease = LocalLease()
    runner = Runner(
        settings(tmp_path, hard_seconds=0.2, stop_seconds=0.2),
        lease,
        command_factory=lambda path: worker_command(path, {}, delay=30),
    )
    runner.run_cycle()
    assert runner.snapshot()["error"] == "worker_timeout"
    assert not runner.snapshot()["worker_running"]
    assert lease.owner is None


def test_waiting_for_control_io_and_closed_runner_are_not_reported_healthy(tmp_path):
    entered, release = threading.Event(), threading.Event()

    class BlockingLease(LocalLease):
        calls = 0

        def acquire(self, owner, seconds):
            self.calls += 1
            if self.calls == 2:
                entered.set()
                assert release.wait(timeout=5)
            return super().acquire(owner, seconds)

    payload = {
        "errors": {}, "checkpoint": {"generation": "verified"},
        "status": {"phases": {}, "source_event_counts": {}},
    }
    runner = Runner(settings(tmp_path), BlockingLease(), command_factory=lambda path: worker_command(path, payload))
    runner.run_cycle()
    assert runner.snapshot()["healthy"]
    thread = threading.Thread(target=runner.run_cycle)
    thread.start()
    try:
        assert entered.wait(timeout=2)
        assert not runner.snapshot()["healthy"]
    finally:
        release.set()
        thread.join(timeout=5)
    runner.close()
    assert not runner.snapshot()["healthy"]


def test_an_old_successful_receipt_does_not_hide_a_stalled_control_loop(tmp_path, monkeypatch):
    from deploy.hf_space import runtime

    clock = [100.0]
    monkeypatch.setattr(runtime.time, "monotonic", lambda: clock[0])
    payload = {
        "errors": {}, "checkpoint": {"generation": "verified"},
        "status": {"phases": {}, "source_event_counts": {}},
    }
    runner = Runner(settings(tmp_path), LocalLease(), command_factory=lambda path: worker_command(path, payload))
    runner.run_cycle()
    assert runner.snapshot()["healthy"]
    clock[0] += 400
    assert not runner.snapshot()["healthy"]
