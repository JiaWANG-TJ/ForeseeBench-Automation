"""托管 runner 的本地执行与经过校验的 HF Bucket 检查点。"""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import os
import signal
import tempfile
import threading
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path, PurePosixPath
from typing import Any
from uuid import uuid4

from huggingface_hub import CommitOperationAdd, CommitOperationDelete, HfApi

from .contract import Record, dumps, stamp
from .store import SCHEMAS, Store, atomic_json, file_hash, read_manifest

DEFAULT_STORAGE_LIMIT_BYTES = 90_000_000_000


class StorageLimit(RuntimeError):
    """用量未知或余量不足时，在写入前暂停，不自动购买容量。"""

    def __init__(self, report: Record):
        self.report = report
        super().__init__(f"storage_limit:{report['reason']}")


class StorageGuard:
    """累计可见逻辑用量与本轮写入预留；不把它称作 Xet 物理计费容量。"""

    def __init__(self, api: Any, bucket_id: str, *, limit_bytes: int = DEFAULT_STORAGE_LIMIT_BYTES):
        if (
            isinstance(limit_bytes, bool)
            or not isinstance(limit_bytes, int)
            or not 0 <= limit_bytes <= DEFAULT_STORAGE_LIMIT_BYTES
        ):
            raise ValueError("storage_limit_out_of_range")
        self.api, self.bucket_id, self.limit_bytes = api, bucket_id, limit_bytes
        self.namespaces = {bucket_id.split("/")[0]}
        self.datasets: set[str] = set()
        self.measurement: Record | None = None
        self.reserved: dict[str, int] = {}
        self.blocked: StorageLimit | None = None

    def include_namespace(self, namespace: str) -> None:
        if namespace not in self.namespaces:
            self.namespaces.add(namespace)
            self.measurement = None

    def include_dataset(self, repo_id: str) -> None:
        self.include_namespace(repo_id.split("/")[0])
        if repo_id not in self.datasets:
            self.datasets.add(repo_id)
            self.measurement = None

    @staticmethod
    def _bytes(value: Any) -> int:
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ValueError("storage_usage_missing_or_invalid")
        return value

    def _measure(self) -> Record:
        identity = self.api.whoami(cache=True)
        if not isinstance(identity.get("name"), str) or not identity["name"]:
            raise ValueError("storage_account_identity_missing")
        namespaces = self.namespaces | {identity["name"]}
        repositories = {}
        measured = set()
        buckets = {self.bucket_id}
        for namespace in sorted(namespaces):
            for kind, listing in (
                ("model", self.api.list_models),
                ("dataset", self.api.list_datasets),
                ("space", self.api.list_spaces),
            ):
                for repo in listing(author=namespace):
                    if not isinstance(repo.id, str) or repo.id.split("/")[0] != namespace:
                        raise ValueError("storage_repository_identity_mismatch")
                    info = self.api.repo_info(repo.id, repo_type=kind)
                    measured.add((kind, repo.id))
                    if info.private is True:
                        repositories[f"{kind}:{repo.id}"] = self._bytes(info.used_storage)
                    elif info.private is not False:
                        raise ValueError("storage_repository_visibility_unknown")
            for bucket in self.api.list_buckets(namespace=namespace):
                if not isinstance(bucket.id, str) or bucket.id.split("/")[0] != namespace:
                    raise ValueError("storage_bucket_identity_mismatch")
                buckets.add(bucket.id)
        for repo_id in self.datasets:
            if ("dataset", repo_id) not in measured:
                info = self.api.repo_info(repo_id, repo_type="dataset")
                if info.private is True:
                    repositories[f"dataset:{repo_id}"] = self._bytes(info.used_storage)
                elif info.private is not False:
                    raise ValueError("storage_repository_visibility_unknown")
        sizes = [self._bytes(self.api.bucket_info(bucket).size) for bucket in sorted(buckets)]
        return {
            "visible_used_bytes": sum(repositories.values()) + sum(sizes),
            "visible_private_repositories": len(repositories),
            "visible_buckets": len(buckets),
            "namespaces": len(namespaces),
            "measured_at": stamp(datetime.now(UTC)),
            "measurement": "visible_repository_used_storage_plus_bucket_size",
        }

    def reserve(self, additions: dict[str, int]) -> Record:
        if self.blocked is not None:
            raise self.blocked
        planned = {key: self._bytes(size) for key, size in additions.items() if key not in self.reserved}
        report: Record = {
            "status": "storage_limit",
            "limit_bytes": self.limit_bytes,
            "visible_used_bytes": None,
            "reserved_bytes": sum(self.reserved.values()),
            "planned_new_bytes": sum(planned.values()),
        }
        try:
            if self.measurement is None:
                self.measurement = self._measure()
        except Exception as error:
            self.blocked = StorageLimit(report | {"reason": "usage_unknown", "error_type": type(error).__name__})
            raise self.blocked from None
        report.update(self.measurement)
        projected = report["visible_used_bytes"] + report["reserved_bytes"] + report["planned_new_bytes"]
        report["projected_bytes"] = projected
        if projected >= self.limit_bytes:
            self.blocked = StorageLimit(report | {"reason": "limit_reached"})
            raise self.blocked
        self.reserved.update(planned)
        return report | {"status": "available"}

    def before_publish(self, repo_id: str, operations: list[CommitOperationAdd | CommitOperationDelete]) -> Record:
        self.include_dataset(repo_id)
        additions = {
            f"dataset:{repo_id}:{op.path_in_repo}:{op.upload_info.sha256.hex()}": op.upload_info.size
            for op in operations
            if isinstance(op, CommitOperationAdd)
        }
        return self.reserve(additions)


class CheckpointDeadline(BaseException):
    """上传宽限期已耗尽；保留最后完成指针，禁止启动另一轮上传。"""


class BucketCheckpoint:
    """显式上传关闭的数据库；completed 指针是唯一恢复入口。"""

    def __init__(
        self,
        bucket_id: str,
        *,
        api: Any = None,
        namespace: str = "",
        storage_limit_bytes: int = DEFAULT_STORAGE_LIMIT_BYTES,
    ):
        self.namespace = namespace.strip("/")
        if namespace and (namespace.startswith("/") or ".." in PurePosixPath(namespace).parts):
            raise ValueError("checkpoint_namespace_invalid")
        self.bucket_id = bucket_id
        self.api = api if api is not None else HfApi()
        info = self.api.bucket_info(bucket_id)
        if not info.private:
            raise ValueError("checkpoint_bucket_must_be_private")
        self.manifest: Record = {"files": {}}
        self.pointer: Record = {"generations": []}
        self.verified: set[str] = set()
        self.storage_guard = StorageGuard(self.api, bucket_id, limit_bytes=storage_limit_bytes)

    def _key(self, relative: str) -> str:
        return f"{self.namespace}/{relative}" if self.namespace else relative

    @staticmethod
    def _path(root: Path, relative: str) -> Path:
        path = PurePosixPath(relative)
        if path.is_absolute() or ".." in path.parts or not path.parts:
            raise ValueError("checkpoint_path_invalid")
        local = root / relative
        if not local.resolve().is_relative_to(root.resolve()):
            raise ValueError("checkpoint_path_invalid")
        return local

    def _download(self, remote: str, local: Path, sha256: str | None = None) -> None:
        local.parent.mkdir(parents=True, exist_ok=True)
        self.api.download_bucket_files(self.bucket_id, [(self._key(remote), local)], raise_on_missing_files=True)
        if not local.is_file() or sha256 is not None and file_hash(local) != sha256:
            raise ValueError(f"checkpoint_checksum_mismatch:{remote}")

    def _upload_verified(self, source: Path | bytes, remote: str, sha256: str) -> Record:
        self.api.batch_bucket_files(self.bucket_id, add=[(source, self._key(remote))])
        with tempfile.TemporaryDirectory(prefix="foreseebench-verify-") as directory:
            self._download(remote, Path(directory) / "object", sha256)
        info = list(self.api.get_bucket_paths_info(self.bucket_id, [self._key(remote)]))
        if len(info) != 1:
            raise ValueError("checkpoint_object_metadata_missing")
        return {"object": remote, "sha256": sha256, "bytes": info[0].size, "xet_hash": info[0].xet_hash}

    def save(self, root: Path) -> Record:
        """调用方先关闭所有 Store/Polymarket，避免数据库及归档的撕裂快照。"""
        root.mkdir(parents=True, exist_ok=True)
        with (root / "writer.lock").open("a+") as lock:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise RuntimeError("checkpoint_requires_closed_store") from None
            if (root / "state.duckdb.wal").exists() or any(root.rglob("*.sqlite-journal")):
                raise RuntimeError("checkpoint_requires_closed_databases")
            return self._save_closed(root)

    def _require_parent(self) -> None:
        entries = list(self.api.get_bucket_paths_info(self.bucket_id, [self._key("completed.json")]))
        current: Record = {"generations": []}
        if entries:
            with tempfile.TemporaryDirectory(prefix="foreseebench-parent-") as directory:
                path = Path(directory) / "pointer.json"
                self._download("completed.json", path)
                current = read_manifest(path)
        if current != self.pointer:
            raise RuntimeError("checkpoint_parent_changed")

    def _save_closed(self, root: Path) -> Record:
        self._require_parent()
        generation = f"{datetime.now(UTC):%Y%m%dT%H%M%S}-{uuid4().hex}"
        files = dict(self.manifest["files"])
        changed: list[tuple[Path, str, str]] = []
        additions = {}
        for path in sorted(root.rglob("*")):
            if not path.is_file() or path.is_symlink():
                continue
            relative = path.relative_to(root).as_posix()
            if relative not in {"state.duckdb", "scheduler.json", "submissions.sqlite"} and not (
                relative.startswith(("batches/", "stages/"))
                and path.suffix in {".json", ".jsonl", ".sqlite", ".parquet"}
            ):
                continue
            sha256 = file_hash(path)
            previous = files.get(relative)
            if previous and previous["sha256"] == sha256:
                continue
            size = path.stat().st_size
            files[relative] = {"object": f"objects/{sha256}", "sha256": sha256, "bytes": size, "xet_hash": "0" * 64}
            additions[f"bucket:{self.bucket_id}:{self._key(f'objects/{sha256}')}"] = size
            changed.append((path, relative, sha256))
        # 原件按未压缩文件长度预留；manifest 与两个完成引用另留充足 JSON 空间。
        additions[f"bucket:{self.bucket_id}:{self._key(f'generations/{generation}.json')}"] = (
            len(dumps({"files": files}).encode()) + 4096
        )
        storage = self.storage_guard.reserve(additions)
        for path, relative, sha256 in changed:
            files[relative] = self._upload_verified(path, f"objects/{sha256}", sha256)
        manifest = {"version": 1, "generation": generation, "created_at": stamp(datetime.now(UTC)), "files": files}
        self._verify_generation(manifest)
        payload = dumps(manifest).encode()
        manifest_hash = hashlib.sha256(payload).hexdigest()
        reference = {"path": f"generations/{generation}.json", "sha256": manifest_hash}
        self._upload_verified(payload, reference["path"], manifest_hash)
        pointer = {"version": 1, "generations": [reference, *self.pointer["generations"][:1]]}
        pointer_bytes = dumps(pointer).encode()
        self._require_parent()
        self._upload_verified(pointer_bytes, "completed.json", hashlib.sha256(pointer_bytes).hexdigest())
        self.manifest, self.pointer = manifest, pointer
        self.collect_garbage()
        return {"generation": generation, "files": len(files), "manifest": reference, "storage": storage}

    def _verify_generation(self, manifest: Record) -> None:
        references = {self._key(entry["object"]): entry for entry in manifest["files"].values()}
        metadata = {entry.path: entry for entry in self.api.get_bucket_paths_info(self.bucket_id, references)}
        for path, reference in references.items():
            info = metadata.get(path)
            if info is None:
                raise ValueError(f"checkpoint_generation_incomplete:{path}")
            if info.size != reference["bytes"] or info.xet_hash != reference["xet_hash"]:
                raise ValueError(f"checkpoint_checksum_mismatch:{path}")

    def collect_garbage(self) -> None:
        """完成指针提交后，才删除两个完整代次均不引用的临时对象。"""
        self._require_parent()
        retained = {self._key("completed.json")}
        with tempfile.TemporaryDirectory(prefix="foreseebench-gc-") as directory:
            for reference in self.pointer["generations"]:
                retained.add(self._key(reference["path"]))
                path = Path(directory) / "manifest.json"
                self._download(reference["path"], path, reference["sha256"])
                retained.update(self._key(entry["object"]) for entry in read_manifest(path)["files"].values())
        for prefix in ("objects/", "generations/"):
            obsolete = [
                entry.path
                for entry in self.api.list_bucket_tree(self.bucket_id, prefix=self._key(prefix), recursive=True)
                if entry.path not in retained and hasattr(entry, "size")
            ]
            if obsolete:
                self.api.batch_bucket_files(self.bucket_id, delete=obsolete)

    def restore(self, root: Path) -> Record | None:
        entries = list(self.api.get_bucket_paths_info(self.bucket_id, [self._key("completed.json")]))
        if not entries:
            return None
        root.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(prefix="foreseebench-restore-", dir=root.parent) as directory:
            temporary = Path(directory)
            self._download("completed.json", temporary / "pointer.json")
            pointer = read_manifest(temporary / "pointer.json")
            reference = pointer["generations"][0]
            self._download(reference["path"], temporary / "manifest.json", reference["sha256"])
            manifest = read_manifest(temporary / "manifest.json")
            self._verify_generation(manifest)
            controls = {}
            for relative, entry in manifest["files"].items():
                self._path(root, relative)
                if relative not in {"state.duckdb", "scheduler.json", "submissions.sqlite"} and not (
                    relative.startswith(("batches/", "stages/")) and Path(relative).suffix == ".json"
                ):
                    continue
                controls[relative] = entry
                local = self._path(temporary / "data", relative)
                self._download(entry["object"], local, entry["sha256"])
            for relative in controls:
                destination = self._path(root, relative)
                destination.parent.mkdir(parents=True, exist_ok=True)
                (temporary / "data" / relative).replace(destination)
        self.manifest, self.pointer = manifest, pointer
        return {"generation": manifest["generation"], "files": len(controls)}

    def resolve_file(self, root: Path, relative: str) -> Path:
        entry = self.manifest["files"].get(relative)
        if entry is None:
            raise ValueError(f"checkpoint_dependency_missing:{relative}")
        destination = self._path(root, relative)
        if destination.is_file() and file_hash(destination) == entry["sha256"]:
            return destination
        temporary = destination.with_name(destination.name + ".download")
        self._download(entry["object"], temporary, entry["sha256"])
        temporary.replace(destination)
        return destination

    def verify_immutable(self, entry: Record) -> bool:
        reference = self.manifest["files"].get(entry["path"])
        if reference is None or reference["sha256"] != entry["sha256"] or entry["table"] in SCHEMAS:
            return False
        if reference["object"] not in self.verified:
            metadata = list(self.api.get_bucket_paths_info(self.bucket_id, [self._key(reference["object"])]))
            if len(metadata) != 1 or (
                metadata[0].size != reference["bytes"] or metadata[0].xet_hash != reference["xet_hash"]
            ):
                return False
            self.verified.add(reference["object"])
        return True

    def hydrate_batch(self, root: Path, batch_id: str, *, frozen: bool) -> None:
        prefix = f"batches/{batch_id}/"
        if not frozen:
            paths = [path for path in self.manifest["files"] if path.startswith(prefix)]
        else:
            base_path = root / prefix / "V1/manifest.json"
            current = root / prefix / "latest.json"
            manifests = [read_manifest(base_path)]
            if current.exists():
                manifests.append(read_manifest(root / read_manifest(current)["manifest"]))
            paths = [
                entry["path"]
                for manifest in manifests
                for entry in manifest["files"]
                if entry["table"] in SCHEMAS or entry["table"] == "source_metadata"
            ]
            if prefix + "raw-v3.sqlite" in self.manifest["files"]:
                paths.append(prefix + "raw-v3.sqlite")
        for relative in sorted(set(paths)):
            if relative in self.manifest["files"]:
                self.resolve_file(root, relative)


def run_once(
    config: Record,
    checkpoint: BucketCheckpoint,
    *,
    bootstrap: bool = False,
    qualification: bool = False,
    work_seconds: int = 2700,
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> Record:
    """一次恢复、按真实截止推进、关闭所有数据库，然后提交完整检查点。"""
    from .cli import latest_path, tick
    from .delivery import publish
    from .source import WorkYield

    if not 1 <= work_seconds <= 2700:
        raise ValueError("runner_work_seconds_out_of_range")
    if qualification != checkpoint.namespace.startswith("qualification/"):
        raise ValueError("checkpoint_namespace_mode_mismatch")
    stop_at = clock() + timedelta(seconds=work_seconds)
    root = Path(config["storage"]["root"])
    restored = checkpoint.restore(root)
    runtime = dict(config, storage=dict(config["storage"]))
    runtime["storage"].update(
        immutable_verifier=checkpoint.verify_immutable,
        file_resolver=lambda relative: checkpoint.resolve_file(root, relative),
    )
    publish_enabled = config["scheduler"]["publish"] and not qualification
    # 外部提交前必须先耐久保存冻结输入；进程失联不能重新生成 V1。
    runtime["scheduler"] = dict(config["scheduler"], publish=False)
    guard = StorageGuard(checkpoint.api, checkpoint.bucket_id, limit_bytes=checkpoint.storage_guard.limit_bytes)
    checkpoint.storage_guard = guard
    cancel_event = config.get("cancel_event") or threading.Event()
    runtime["cancel_event"] = cancel_event
    if publish_enabled:
        repo_id = config["publication"]["repo_id"]
        guard.include_dataset(repo_id)

        def before_publish(operations: list[CommitOperationAdd | CommitOperationDelete]) -> None:
            try:
                guard.before_publish(repo_id, operations)
            except StorageLimit:
                cancel_event.set()
                raise

        runtime["storage"]["before_publish"] = before_publish

    def paused(error: StorageLimit) -> Record:
        return {
            "phase": "scheduler",
            "last_tick": stamp(clock()),
            "errors": {},
            "yielded": False,
            "storage": error.report,
            "restored": restored,
            "checkpoint": {"status": "not_written", "generation": checkpoint.manifest.get("generation")},
            "status": {
                "date": clock().date().isoformat(),
                "phases": {"storage_limit": 1},
                "source_event_counts": {},
                "stages": {"V2": "not_configured", "V4": "not_configured"},
                "storage": {
                    key: error.report.get(key)
                    for key in (
                        "reason",
                        "limit_bytes",
                        "visible_used_bytes",
                        "planned_new_bytes",
                        "visible_private_repositories",
                        "visible_buckets",
                    )
                },
            },
        }

    def hydrate(store: Store, batch: Record) -> None:
        checkpoint.hydrate_batch(store.root, batch["id"], frozen=batch["frozen"])

    publication_errors: dict[str, str] = {}
    publication_yielded = False
    attempted_paths: set[Path] = set()

    def publish_pending() -> bool:
        nonlocal publication_yielded
        if not publish_enabled:
            return False
        attempted = False
        with Store(**runtime["storage"]) as store:
            for batch in sorted(store.status(), key=lambda row: store.week(row["id"]).evidence_cutoff):
                if cancel_event.is_set() or clock() >= stop_at:
                    publication_yielded = True
                    break
                if (
                    not batch["frozen"]
                    or batch["kind"] not in {"formal", "bootstrap"}
                    or store.week(batch["id"]).policy.read_only
                ):
                    continue
                path = latest_path(store, batch["id"])
                if path in attempted_paths or (path.parent / "publication.json").exists():
                    continue
                attempted = True
                attempted_paths.add(path)
                try:
                    publish(
                        store,
                        path,
                        **config["publication"],
                        stop_at=stop_at,
                        clock=clock,
                        cancel_event=cancel_event,
                    )
                    publication_errors.pop(batch["id"], None)
                except WorkYield:
                    publication_yielded = True
                    break
                except StorageLimit:
                    raise
                except Exception as error:
                    publication_errors[batch["id"]] = f"{type(error).__name__}:{error}"
        return attempted

    try:
        guard.reserve({})
        if restored:
            # 这批输入已在上一 runner 耐久保存，先补发布，避免新来源扫描耗尽整轮预算。
            publish_pending()
        result = tick(
            runtime, clock=clock, stop_at=stop_at, bootstrap=bootstrap, qualification=qualification, hydrate=hydrate
        )
        if guard.blocked is not None:
            raise guard.blocked
    except StorageLimit as error:
        return paused(error)
    except CheckpointDeadline:
        raise
    except BaseException:
        try:
            checkpoint.save(root)
        except StorageLimit as error:
            return paused(error)
        raise
    tick_errors = dict(result["errors"])

    def record_publication_state() -> None:
        result["errors"] = tick_errors | publication_errors
        result["yielded"] = result["yielded"] or publication_yielded
        state = read_manifest(root / "scheduler.json")
        state.update(last_error=result["errors"] or None, yielded=result["yielded"])
        atomic_json(root / "scheduler.json", state)

    record_publication_state()
    try:
        receipt = checkpoint.save(root)
    except StorageLimit as error:
        return paused(error)
    try:
        if publish_pending():
            record_publication_state()
            receipt = checkpoint.save(root)
    except StorageLimit as error:
        return paused(error)
    result["yielded"] = result["yielded"] or cancel_event.is_set() or clock() >= stop_at
    return result | {"restored": restored, "checkpoint": receipt}


def main() -> None:
    from .cli import log, settings

    parser = argparse.ArgumentParser(description="在一次托管任务内恢复、推进并保存 ForeseeBench")
    parser.add_argument("--config", type=Path, default=Path("foreseebench.toml"))
    parser.add_argument("--bucket", default=os.environ.get("HF_BUCKET"), required=not os.environ.get("HF_BUCKET"))
    parser.add_argument("--bootstrap", action="store_true", help="空状态下启动完整初始批次")
    parser.add_argument("--qualification", action="store_true", help="隔离检查点，不发布正式数据集")
    parser.add_argument("--namespace", default="", help="工程核验的独立 Bucket 前缀")
    parser.add_argument("--work-seconds", type=int, default=2700)
    parser.add_argument("--checkpoint-seconds", type=int, default=600)
    parser.add_argument("--status-output", type=Path, help="写出仅含日期、阶段与汇总计数的公开状态")
    parser.add_argument("--receipt-output", type=Path, help="向监督进程写出本次运行与已确认检查点回执")
    args = parser.parse_args()
    if not 1 <= args.work_seconds <= 2700 or not 1 <= args.checkpoint_seconds <= 600:
        parser.error("工作预算须为 1—2700 秒，检查点宽限期须为 1—600 秒")
    if args.namespace and not args.qualification:
        parser.error("独立 namespace 仅用于 qualification")
    namespace = (args.namespace or "qualification/default") if args.qualification else ""
    if args.qualification and not namespace.startswith("qualification/"):
        parser.error("工程核验 namespace 必须以 qualification/ 开头")
    cancel_event = threading.Event()

    def request_stop(signum: int, frame: Any) -> None:
        cancel_event.set()

    def hard_deadline(signum: int, frame: Any) -> None:
        cancel_event.set()
        raise CheckpointDeadline("checkpoint_grace_exhausted")

    previous_term = signal.signal(signal.SIGTERM, request_stop)
    previous_int = signal.signal(signal.SIGINT, request_stop)
    previous_alarm = signal.signal(signal.SIGALRM, hard_deadline)
    signal.setitimer(signal.ITIMER_REAL, args.work_seconds + args.checkpoint_seconds)
    try:
        config = settings(args.config)
        config["cancel_event"] = cancel_event
        if os.environ.get("HF_REPO_ID"):
            config["publication"] = dict(config["publication"], repo_id=os.environ["HF_REPO_ID"])
        with tempfile.TemporaryDirectory(prefix="foreseebench-run-") as directory:
            config["storage"] = dict(config["storage"], root=Path(directory) / "state")
            checkpoint = BucketCheckpoint(args.bucket, namespace=namespace)
            receipt = run_once(
                config,
                checkpoint,
                bootstrap=args.bootstrap or args.qualification,
                qualification=args.qualification,
                work_seconds=args.work_seconds,
            )
            log(receipt)
            if args.receipt_output:
                atomic_json(args.receipt_output, receipt)
            if args.status_output and not args.qualification:
                atomic_json(args.status_output, receipt["status"])
            if receipt["errors"]:
                raise SystemExit(1)
    except CheckpointDeadline:
        log({"phase": "checkpoint", "error": "checkpoint_grace_exhausted", "checkpoint_commit": "unconfirmed"})
        raise SystemExit(1) from None
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGTERM, previous_term)
        signal.signal(signal.SIGINT, previous_int)
        signal.signal(signal.SIGALRM, previous_alarm)


if __name__ == "__main__":
    main()
