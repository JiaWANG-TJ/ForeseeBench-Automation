"""Space监督层：复用既有云端入口，检查回执，并保持单一写入者。"""

from __future__ import annotations

import json
import math
import os
import re
import signal
import subprocess
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

import httpx2
from huggingface_hub import CommitOperationAdd, HfApi, set_client_factory
from huggingface_hub.errors import EntryNotFoundError, HfHubHTTPError


def timestamp() -> str:
    return datetime.now(UTC).isoformat()


def report_error(phase: str, error: Exception) -> None:
    detail = re.sub(r"https?://\S+", "<url>", str(error))
    detail = re.sub(r"hf_[A-Za-z0-9_-]+|sk-[A-Za-z0-9_-]{8,}", "<redacted>", detail)
    detail = re.sub(r"(?i)bearer\s+\S+", "Bearer <redacted>", detail)
    print(json.dumps({"phase": phase, "error_type": type(error).__name__, "detail": detail[:400]}), flush=True)


class HubLease:
    """在本Space的独立控制分支上，以parent_commit竞争执行权。"""

    branch = "foreseebench-runtime"

    def __init__(self, space_id: str, mode: str, *, api: Any = None, clock: Callable[[], float] = time.time):
        self.space_id, self.mode = space_id, mode
        if api is None:
            set_client_factory(lambda: httpx2.Client(timeout=15, follow_redirects=True))
        self.api = api if api is not None else HfApi()
        self.clock = clock
        self.initialized = False

    def _read(self) -> tuple[str, dict[str, Any]]:
        if not self.initialized:
            self.api.create_branch(self.space_id, repo_type="space", branch=self.branch, exist_ok=True)
            self.initialized = True
        head = self.api.repo_info(self.space_id, repo_type="space", revision=self.branch, timeout=15).sha
        if not isinstance(head, str) or not head:
            raise ValueError("lease_revision_missing")
        try:
            path = self.api.hf_hub_download(
                self.space_id, "lease.json", repo_type="space", revision=head, etag_timeout=15
            )
            value = json.loads(Path(path).read_text())
        except EntryNotFoundError:
            value = {}
        if not isinstance(value, dict):
            raise ValueError("lease_state_invalid")
        return head, value

    def _write(self, head: str, value: dict[str, Any]) -> bool:
        try:
            self.api.create_commit(
                self.space_id,
                repo_type="space",
                revision=self.branch,
                parent_commit=head,
                operations=[CommitOperationAdd("lease.json", json.dumps(value, sort_keys=True).encode())],
                commit_message="chore: update ForeseeBench execution lease",
            )
            return True
        except HfHubHTTPError as error:
            if error.response is not None and error.response.status_code in {409, 412}:
                return False
            raise

    def acquire(self, owner: str, seconds: float) -> bool:
        if not math.isfinite(seconds) or seconds <= 0:
            raise ValueError("lease_duration_invalid")
        head, value = self._read()
        current = value.get(self.mode, {})
        if not isinstance(current, dict):
            raise ValueError("lease_entry_invalid")
        if current and (
            current.get("owner") is not None and not isinstance(current.get("owner"), str)
            or isinstance(current.get("expires_at"), bool)
            or not isinstance(current.get("expires_at"), int | float)
            or not math.isfinite(current["expires_at"])
        ):
            raise ValueError("lease_entry_invalid")
        if current.get("owner") not in {None, owner} and current["expires_at"] > self.clock():
            return False
        value[self.mode] = {"owner": owner, "expires_at": self.clock() + seconds}
        if not self._write(head, value):
            return False
        _, confirmed = self._read()
        grant = confirmed.get(self.mode, {})
        return grant.get("owner") == owner and grant["expires_at"] - self.clock() >= max(1, seconds - 60)

    def release(self, owner: str) -> bool:
        head, value = self._read()
        if value.get(self.mode, {}).get("owner") != owner:
            return False
        value[self.mode] = {"owner": None, "expires_at": 0}
        return self._write(head, value)


@dataclass(frozen=True)
class Settings:
    worker_root: Path
    python: str
    control_dir: Path
    space_id: str
    mode: str
    version: str
    bucket: str = "SII-JiaWANG/ForeseeBench-state"
    dataset: str = "SII-JiaWANG/ForeseeBench"
    poll_seconds: float = 300
    hard_seconds: float = 3600
    stop_seconds: float = 30

    def __post_init__(self) -> None:
        if self.mode not in {"qualification", "production"}:
            raise ValueError("run_mode_invalid")
        if not self.version or "/" not in self.space_id:
            raise ValueError("deployment_identity_missing")
        for value, maximum in ((self.poll_seconds, 300), (self.hard_seconds, 3600), (self.stop_seconds, 60)):
            if not math.isfinite(value) or not 0 < value <= maximum:
                raise ValueError("runner_budget_invalid")

    def command(self, receipt: Path) -> list[str]:
        command = [
            self.python, "-m", "foreseebench.cloud", "--config", str(self.worker_root / "foreseebench.toml"),
            "--bucket", self.bucket, "--work-seconds", "600" if self.mode == "qualification" else "2700",
            "--checkpoint-seconds", "600", "--receipt-output", str(receipt),
        ]
        if self.mode == "qualification":
            import hashlib

            suffix = hashlib.sha256(self.version.encode()).hexdigest()[:16]
            command += ["--qualification", "--namespace", f"qualification/space-{suffix}"]
        else:
            command.append("--bootstrap")
        return command


class Runner:
    def __init__(
        self,
        settings: Settings,
        lease: Any,
        *,
        command_factory: Callable[[Path], list[str]] | None = None,
    ):
        self.settings, self.lease = settings, lease
        self.command_factory = command_factory or settings.command
        self.owner = uuid4().hex
        self.stopping = threading.Event()
        self.lock = threading.RLock()
        self.cycle_lock = threading.Lock()
        self.process: subprocess.Popen[bytes] | None = None
        self.thread: threading.Thread | None = None
        self.state: dict[str, Any] = {
            "phase": "starting", "checkpoint": None, "last_completed_at": None,
            "counts": {}, "batch_phases": {}, "error": None,
            "monitor_requests": 0, "monitor_first_seen_at": None, "monitor_last_seen_at": None,
        }
        self.changed_at = time.monotonic()
        settings.control_dir.mkdir(parents=True, exist_ok=True, mode=0o700)

    def _state(self, **values: Any) -> None:
        with self.lock:
            self.state.update(values)
            self.changed_at = time.monotonic()

    def snapshot(self) -> dict[str, Any]:
        with self.lock:
            running = self.process is not None and self.process.poll() is None
            state = dict(self.state, counts=dict(self.state["counts"]), batch_phases=dict(self.state["batch_phases"]))
            age = time.monotonic() - self.changed_at
            alive = self.thread is None or self.thread.is_alive()
        active = (
            state["phase"] == "running" and running and age <= self.settings.hard_seconds
            or state["phase"] == "waiting" and age <= self.settings.poll_seconds + 60
        )
        return state | {
            "healthy": bool(
                state["checkpoint"] and state["error"] is None and active and alive and not self.stopping.is_set()
            ),
            "mode": self.settings.mode,
            "version": self.settings.version,
            "worker_running": running,
            "instance": self.owner,
        }

    def observe_monitor(self) -> None:
        with self.lock:
            now = timestamp()
            self.state["monitor_requests"] += 1
            self.state["monitor_first_seen_at"] = self.state["monitor_first_seen_at"] or now
            self.state["monitor_last_seen_at"] = now

    @staticmethod
    def _counts(value: Any, allowed: set[str]) -> dict[str, int]:
        if not isinstance(value, dict) or any(
            key not in allowed or isinstance(count, bool) or not isinstance(count, int) or count < 0
            for key, count in value.items()
        ):
            raise ValueError("receipt_counts_invalid")
        return value

    def _terminate(self, process: subprocess.Popen[bytes]) -> None:
        if process.poll() is not None:
            return
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except ProcessLookupError:
            return
        try:
            process.wait(timeout=self.settings.stop_seconds)
        except subprocess.TimeoutExpired:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            process.wait(timeout=10)

    def run_cycle(self) -> None:
        if not self.cycle_lock.acquire(blocking=False):
            return
        acquired = False
        successful_state: dict[str, Any] | None = None
        receipt_path = self.settings.control_dir / f"receipt-{uuid4().hex}.json"
        try:
            if self.stopping.is_set():
                return
            self._state(phase="acquiring_lease", error=None)
            acquired = self.lease.acquire(self.owner, self.settings.hard_seconds + self.settings.stop_seconds + 120)
            if not acquired:
                self._state(phase="waiting_lease", error="execution_lease_unavailable")
                return
            run_dir = self.settings.control_dir / "temporary"
            run_dir.mkdir(exist_ok=True)
            environment = dict(os.environ)
            for key in ("PYTHONPATH", "PYTHONHOME", "VIRTUAL_ENV"):
                environment.pop(key, None)
            environment.update(
                HF_REPO_ID=self.settings.dataset,
                HF_HUB_DISABLE_PROGRESS_BARS="1",
                PYTHONUNBUFFERED="1",
                TMPDIR=str(run_dir),
                HF_HOME=str(self.settings.control_dir / "huggingface"),
            )
            # 每轮仅保留一份私有日志；健康接口只返回经过筛选的回执字段。
            with (self.settings.control_dir / "worker.log").open("wb") as output:
                with self.lock:
                    if self.stopping.is_set():
                        return
                    process = subprocess.Popen(
                        self.command_factory(receipt_path), cwd=self.settings.worker_root, env=environment,
                        stdin=subprocess.DEVNULL, stdout=output, stderr=output, start_new_session=True,
                    )
                    self.process = process
                    self._state(phase="running", error=None)
                try:
                    code = process.wait(timeout=self.settings.hard_seconds)
                except subprocess.TimeoutExpired:
                    self._terminate(process)
                    self._state(phase="error", error="worker_timeout")
                    return
            if self.stopping.is_set():
                self._state(phase="stopped", error=None)
                return
            if code != 0:
                self._state(phase="error", error="worker_failed")
                print(json.dumps({"phase": "space_worker", "exit_code": code}), flush=True)
                return
            if not receipt_path.is_file():
                self._state(phase="error", error="missing_receipt")
                return
            receipt = json.loads(receipt_path.read_text())
            generation = (receipt.get("checkpoint") or {}).get("generation")
            if receipt.get("errors") != {} or not isinstance(generation, str) or not generation:
                raise ValueError("receipt_not_verified")
            status = receipt["status"]
            counts = self._counts(
                status["source_event_counts"], {"candidate", "collecting", "collected", "rejected", "failed"}
            )
            phases = self._counts(status["phases"], {"collecting", "awaiting_gt", "closed", "expired_incomplete"})
            successful_state = {
                "phase": "waiting", "error": None, "checkpoint": generation, "last_completed_at": timestamp(),
                "counts": counts, "batch_phases": phases,
            }
        except Exception as error:
            self._state(phase="error", error="controller_failed")
            report_error("space_controller", error)
        finally:
            with self.lock:
                remaining_process = self.process
            if remaining_process is not None and remaining_process.poll() is None:
                self._terminate(remaining_process)
            if acquired:
                prior_phase = self.state["phase"]
                self._state(phase="releasing_lease")
                try:
                    if not self.lease.release(self.owner):
                        self._state(phase="error", error="lease_release_unconfirmed")
                    elif self.stopping.is_set():
                        self._state(phase="stopped")
                    elif successful_state is not None:
                        self._state(**successful_state)
                    else:
                        self._state(phase=prior_phase)
                except Exception as error:
                    self._state(phase="error", error="lease_release_failed")
                    report_error("lease_release", error)
            receipt_path.unlink(missing_ok=True)
            self.cycle_lock.release()

    def start(self) -> None:
        with self.lock:
            if self.thread is not None:
                raise RuntimeError("runner_already_started")

            def loop() -> None:
                while not self.stopping.is_set():
                    self.run_cycle()
                    if self.stopping.wait(self.settings.poll_seconds):
                        break

            self.thread = threading.Thread(target=loop, name="foreseebench-controller", daemon=True)
            self.thread.start()

    def close(self) -> None:
        self.stopping.set()
        with self.lock:
            process = self.process
        if process is not None:
            self._terminate(process)
        if self.thread is not None:
            self.thread.join(timeout=self.settings.stop_seconds + 15)
        self._state(phase="stopped")
