import json
import os
import subprocess
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import pytest


@pytest.mark.parametrize("stop_mode", ["deadline", "signal"])
def test_daemon_interrupts_retry_after_before_waiting_for_another_request(tmp_path, stop_mode):
    requests = threading.Event()

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            requests.set()
            self.send_response(429)
            self.send_header("Retry-After", "30")
            self.send_header("Content-Length", "0")
            self.end_headers()

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    root = tmp_path / "data"
    config = tmp_path / "retry.toml"
    config.write_text(
        f"""[storage]
root = {json.dumps(str(root))}
[scheduler]
publish = false
poll_seconds = {0.2 if stop_mode == 'deadline' else 300}
[publication]
repo_id = "owner/unused"
[network]
gamma_url = "http://127.0.0.1:{server.server_port}"
data_url = "http://127.0.0.1:{server.server_port}"
timeout_seconds = 1
attempts = 2
requests_per_second = 1000
workers = 1
"""
    )
    executable = str(Path(sys.executable).with_name("foreseebench"))
    command = [executable, "--config", str(config), "--bootstrap", "run"]
    environment = dict(os.environ, NO_PROXY="127.0.0.1,localhost", no_proxy="127.0.0.1,localhost")
    with (tmp_path / "retry.log").open("w") as output:
        process = subprocess.Popen(command, stdout=output, stderr=output, env=environment)
        try:
            assert requests.wait(timeout=5)
            if stop_mode == "deadline":
                limit = time.monotonic() + 2
                while not (root / "scheduler.json").exists() and time.monotonic() < limit:
                    time.sleep(0.02)
                assert (root / "scheduler.json").is_file()
            process.terminate()
            assert process.wait(timeout=2) == 0
        finally:
            if process.poll() is None:
                process.kill()
                process.wait(timeout=5)
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)


def test_service_wrapper_waits_for_shutdown_before_starting_again(tmp_path):
    import hashlib
    import shutil

    source = Path(__file__).resolve().parents[1]
    root = tmp_path / "host"
    scripts, binaries = root / "scripts", root / ".venv" / "bin"
    scripts.mkdir(parents=True)
    binaries.mkdir(parents=True)
    for name in ("service.sh", "supervisord.conf"):
        shutil.copy2(source / "scripts" / name, scripts / name)
    for name in ("supervisord", "supervisorctl"):
        (binaries / name).symlink_to(Path(sys.executable).with_name(name))

    def executable(name, body):
        path = binaries / name
        path.write_text(body)
        path.chmod(0o755)

    executable("uv", "#!/bin/sh\nexit 0\n")
    executable("python", "#!/bin/sh\nexit 0\n")
    executable(
        "foreseebench",
        f"#!{sys.executable}\n"
        "import signal,time\n"
        "def stop(*args):\n    time.sleep(2)\n    raise SystemExit(0)\n"
        "signal.signal(signal.SIGTERM,stop)\n"
        "while True: time.sleep(0.1)\n",
    )
    environment = dict(os.environ, PATH=str(binaries) + os.pathsep + os.environ["PATH"], HF_BUCKET="owner/state")
    socket = Path(f"/tmp/foreseebench-{os.getuid()}-{hashlib.sha256(str(root).encode()).hexdigest()[:12]}.sock")

    def invoke(action):
        return subprocess.run(
            ["bash", str(scripts / "service.sh"), action], capture_output=True, text=True, timeout=15, env=environment
        )

    try:
        for action in ("start", "stop", "start", "status"):
            result = invoke(action)
            assert result.returncode == 0, result.stdout + result.stderr
    finally:
        invoke("stop")
        deadline = time.monotonic() + 10
        while socket.exists() and time.monotonic() < deadline:
            time.sleep(0.05)
        assert not socket.exists(), "隔离Supervisor实例未退出"


def test_daemon_emits_bounded_heartbeats_and_resumes_after_a_graceful_restart(tmp_path):
    boundaries = []
    ranges = []

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            ids = parse_qs(urlparse(self.path).query).get("id", [])
            if ids:
                ranges.append(int(ids[0]))
            else:
                boundaries.append(True)
            payload = json.dumps({"events": [{"id": value} for value in ids or ["1000001"]]}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    root = tmp_path / "data"
    config = tmp_path / "service.toml"
    config.write_text(
        f"""[storage]
root = {json.dumps(str(root))}
[scheduler]
publish = false
poll_seconds = 0.2
[publication]
repo_id = "owner/unused"
revision = "main"
[network]
gamma_url = "http://127.0.0.1:{server.server_port}"
data_url = "http://127.0.0.1:{server.server_port}"
timeout_seconds = 1
attempts = 1
requests_per_second = 1000
workers = 1
"""
    )
    executable = str(Path(sys.executable).with_name("foreseebench"))
    command = [executable, "--config", str(config), "--bootstrap", "run"]
    environment = dict(os.environ, NO_PROXY="127.0.0.1,localhost", no_proxy="127.0.0.1,localhost")

    def heartbeat():
        path = root / "scheduler.json"
        return json.loads(path.read_text())["last_tick"] if path.exists() else None

    def wait_for_tick(process, previous):
        deadline = time.monotonic() + 8
        while time.monotonic() < deadline:
            assert process.poll() is None, "服务在写入心跳前退出"
            current = heartbeat()
            if current is not None and current != previous:
                return current
            time.sleep(0.02)
        raise AssertionError("持续采集未按工作预算交出执行权并写入心跳")

    try:
        for iteration in range(2):
            previous = heartbeat()
            before = len(ranges)
            with (tmp_path / f"service-{iteration}.log").open("w") as output:
                process = subprocess.Popen(command, stdout=output, stderr=output, env=environment)
                try:
                    first = wait_for_tick(process, previous)
                    wait_for_tick(process, first)
                    process.terminate()
                    assert process.wait(timeout=8) == 0
                finally:
                    if process.poll() is None:
                        process.kill()
                        process.wait(timeout=8)
            if iteration:
                assert ranges[before] > 1
        assert len(boundaries) == 1
        result = subprocess.run(
            [executable, "--config", str(config), "status"],
            capture_output=True,
            text=True,
            check=True,
            env=environment,
        )
        batches = json.loads(result.stdout)["batches"]
        assert len([batch for batch in batches if batch["kind"] == "bootstrap"]) == 1
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)
