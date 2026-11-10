"""打包已提交源码，并只部署到ForeseeBench专用Space。"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
import tempfile
import time
from pathlib import Path

from huggingface_hub import CommitOperationAdd, HfApi, SpaceHardware, get_token
from huggingface_hub.errors import RepositoryNotFoundError

SPACE_ID = "SII-JiaWANG/ForeseeBench-Automation"


def build_bundle(root: Path, destination: Path) -> dict:
    if subprocess.run(["git", "diff", "--quiet", "HEAD"], cwd=root).returncode:
        raise ValueError("deployment_requires_committed_source")
    revision = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip()
    names = subprocess.check_output(["git", "ls-files", "-z"], cwd=root).decode().split("\0")
    allowed_files = {"pyproject.toml", "uv.lock", "foreseebench.toml"}
    allowed_files.update(
        f"deploy/hf_space/{name}" for name in ("app.py", "runtime.py", "README.md", "requirements.txt")
    )
    selected = {
        name for name in names if name in allowed_files or name.startswith("src/foreseebench/")
    }
    required = {
        "deploy/hf_space/app.py", "deploy/hf_space/runtime.py", "deploy/hf_space/requirements.txt",
        "deploy/hf_space/README.md", "src/foreseebench/cloud.py", "uv.lock", "foreseebench.toml",
    }
    if not required <= selected:
        raise ValueError("deployment_files_not_committed")
    files = {}
    for name in sorted(selected):
        if name.startswith(("data/", ".venv/", ".env")):
            raise ValueError("private_file_in_deployment")
        path = root / name
        if path.is_symlink():
            raise ValueError("deployment_symlink_not_allowed")
        body = path.read_bytes()
        if len(body) > 50_000_000 or re.search(rb"hf_[A-Za-z0-9]{20,}|sk-[A-Za-z0-9_-]{20,}", body):
            raise ValueError(f"deployment_content_rejected:{name}")
        target = destination / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(body)
    for name in ("README.md", "requirements.txt"):
        (destination / name).write_bytes((root / "deploy/hf_space" / name).read_bytes())
    for path in sorted(destination.rglob("*")):
        if path.is_file():
            files[path.relative_to(destination).as_posix()] = hashlib.sha256(path.read_bytes()).hexdigest()
    bundle = {"source_revision": revision, "files": files}
    (destination / "bundle.json").write_text(json.dumps(bundle, sort_keys=True, indent=2) + "\n")
    return bundle


def pause(api: HfApi) -> None:
    if str(api.get_space_runtime(SPACE_ID).stage) in {"NO_APP_FILE", "PAUSED"}:
        return
    api.pause_space(SPACE_ID)
    deadline = time.monotonic() + 180
    while str(api.get_space_runtime(SPACE_ID).stage) != "PAUSED":
        if time.monotonic() >= deadline:
            raise TimeoutError("space_pause_not_confirmed")
        time.sleep(2)


def deploy(root: Path) -> dict:
    with tempfile.TemporaryDirectory(prefix="foreseebench-space-bundle-") as directory:
        folder = Path(directory)
        bundle = build_bundle(root, folder)
        api = HfApi(endpoint="https://huggingface.co")
        if api.whoami()["name"] != SPACE_ID.split("/")[0]:
            raise ValueError("deployment_account_mismatch")
        token = get_token()
        if not token:
            raise ValueError("hf_authentication_missing")
        try:
            info = api.repo_info(SPACE_ID, repo_type="space")
            runtime = api.get_space_runtime(SPACE_ID)
            if info.private:
                raise ValueError("space_configuration_mismatch")
            if str(runtime.requested_hardware) != "cpu-basic":
                raise ValueError("space_requires_cpu_basic")
        except RepositoryNotFoundError:
            api.create_repo(
                SPACE_ID, repo_type="space", space_sdk="gradio", private=False,
                space_hardware=SpaceHardware.CPU_BASIC, exist_ok=False,
            )
        # 配置变化也可能重启Space，先切资格模式，并在上传前再次确认暂停。
        pause(api)
        api.add_space_variable(SPACE_ID, key="FORESEEBENCH_MODE", value="qualification")
        api.add_space_secret(SPACE_ID, key="HF_TOKEN", value=token)
        pause(api)
        operations = [
            CommitOperationAdd(path.relative_to(folder).as_posix(), path)
            for path in sorted(folder.rglob("*")) if path.is_file()
        ]
        head = api.repo_info(SPACE_ID, repo_type="space").sha
        commit = api.create_commit(
            SPACE_ID, repo_type="space", operations=operations, parent_commit=head,
            commit_message=f"Deploy ForeseeBench {bundle['source_revision'][:12]}",
        )
        api.restart_space(SPACE_ID)
    return {"space": SPACE_ID, "space_revision": commit.oid, "source_revision": bundle["source_revision"]}


def main() -> None:
    parser = argparse.ArgumentParser(description="部署独立Space，默认只运行隔离资格验证")
    parser.add_argument("--output", type=Path, required=True, help="保存部署回执，不包含凭据")
    args = parser.parse_args()
    result = deploy(Path(__file__).resolve().parents[2])
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result))


if __name__ == "__main__":
    main()
