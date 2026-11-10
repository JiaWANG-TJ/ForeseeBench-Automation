import subprocess
from pathlib import Path
from types import SimpleNamespace

import httpx2
import pytest

from deploy.hf_space import deploy


def repository(root: Path) -> None:
    files = {
        "README.md": "source readme",
        "pyproject.toml": "[project]\nname='fixture'\nversion='0.1.0'\n",
        "foreseebench.toml": "[storage]\nroot='data'\n",
        "uv.lock": "version=1\n",
        "src/foreseebench/cloud.py": "# source\n",
        "deploy/hf_space/app.py": "# application\n",
        "deploy/hf_space/runtime.py": "# runtime\n",
        "deploy/hf_space/README.md": "---\nsdk: gradio\n---\n",
        "deploy/hf_space/requirements.txt": "uv==0.13.7\n",
        "AGENTS.md": "private agent instructions",
        "docs/agents/private.md": "private agent conventions",
        "notes/internal-review.txt": "unapproved private note",
    }
    for name, body in files.items():
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(body)
    for command in (["git", "init", "-q"], ["git", "add", "."], [
        "git", "-c", "user.name=Test", "-c", "user.email=test@example.invalid", "commit", "-qm", "fixture",
    ]):
        subprocess.run(command, cwd=root, check=True)


def test_bundle_excludes_private_instructions_and_keeps_the_core_lock(tmp_path):
    root, target = tmp_path / "repo", tmp_path / "bundle"
    root.mkdir()
    repository(root)
    bundle = deploy.build_bundle(root, target)
    assert not (target / "AGENTS.md").exists()
    assert not (target / "docs/agents").exists()
    assert not (target / "notes/internal-review.txt").exists()
    assert (target / "uv.lock").read_bytes() == (root / "uv.lock").read_bytes()
    assert (target / "README.md").read_text().startswith("---\nsdk: gradio")
    assert "src/foreseebench/cloud.py" in bundle["files"]


def test_dirty_source_cannot_pause_or_modify_a_remote_space(tmp_path, monkeypatch):
    repository(tmp_path)
    (tmp_path / "src/foreseebench/cloud.py").write_text("# uncommitted\n")
    writes = []

    class API:
        def whoami(self):
            return {"name": "SII-JiaWANG"}

        def repo_info(self, *args, **kwargs):
            return SimpleNamespace(private=False)

        def get_space_runtime(self, *args):
            return SimpleNamespace(stage="PAUSED" if writes else "RUNNING", requested_hardware="zero-a10g")

        def pause_space(self, *args):
            writes.append("pause")

        def add_space_secret(self, *args, **kwargs):
            writes.append("secret")

        def add_space_variable(self, *args, **kwargs):
            writes.append("variable")

    monkeypatch.setattr(deploy, "HfApi", lambda **kwargs: API())
    monkeypatch.setattr(deploy, "get_token", lambda: "fixture")
    with pytest.raises(ValueError, match="deployment_requires_committed_source"):
        deploy.deploy(tmp_path)
    assert writes == []


@pytest.mark.parametrize("hardware", ["zero-a10g", "cpu-upgrade"])
def test_cpu_pipeline_rejects_incompatible_hardware_before_remote_writes(tmp_path, monkeypatch, hardware):
    repository(tmp_path)
    writes = []

    class API:
        def whoami(self):
            return {"name": "SII-JiaWANG"}

        def repo_info(self, *args, **kwargs):
            return SimpleNamespace(private=False)

        def get_space_runtime(self, *args):
            return SimpleNamespace(stage="PAUSED", requested_hardware=hardware)

        def __getattr__(self, name):
            def unexpected(*args, **kwargs):
                writes.append(name)
                raise AssertionError("incompatible_space_must_not_be_modified")
            return unexpected

    monkeypatch.setattr(deploy, "HfApi", lambda **kwargs: API())
    monkeypatch.setattr(deploy, "get_token", lambda: "fixture")
    with pytest.raises(ValueError, match="space_requires_cpu_basic"):
        deploy.deploy(tmp_path)
    assert writes == []


def test_new_space_requests_cpu_basic_and_keeps_creation_denial(tmp_path, monkeypatch):
    repository(tmp_path)
    requests = []

    class API:
        def whoami(self):
            return {"name": "SII-JiaWANG"}

        def repo_info(self, *args, **kwargs):
            raise deploy.RepositoryNotFoundError(
                "missing", response=httpx2.Response(404, request=httpx2.Request("GET", "https://huggingface.co"))
            )

        def create_repo(self, name, **kwargs):
            requests.append((name, kwargs))
            raise RuntimeError("account_creation_denied")

    monkeypatch.setattr(deploy, "HfApi", lambda **kwargs: API())
    monkeypatch.setattr(deploy, "get_token", lambda: "fixture")
    with pytest.raises(RuntimeError, match="account_creation_denied"):
        deploy.deploy(tmp_path)
    assert len(requests) == 1
    assert requests[0][0] == deploy.SPACE_ID
    assert requests[0][1]["space_hardware"] == deploy.SpaceHardware.CPU_BASIC
