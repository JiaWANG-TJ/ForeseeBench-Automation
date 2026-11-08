"""操作入口与 UTC 周批调度。"""

from __future__ import annotations

import argparse
import importlib
import shutil
import sys
import tomllib
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger

from .benchmark import accept_submission, finalize_benchmark, release_benchmark, stage_output, validate_stage_result
from .contract import Policy, Record, Week, dumps, instant, stamp
from .delivery import download_view, export_jsonl, publish
from .pipeline import Pipeline, stage_task
from .source import Polymarket, WorkYield
from .store import Store, atomic_json, file_hash, read_manifest


def log(value: Any) -> None:
    print(dumps(value), flush=True)


def settings(path: Path, qualification: bool = False) -> Record:
    with path.open("rb") as stream:
        config = tomllib.load(stream)
    root = Path(config["storage"]["root"])
    if not root.is_absolute():
        root = path.resolve().parent / root
    config["storage"]["root"] = root / "qualification" if qualification else root
    config["config_hash"] = file_hash(path)
    if Policy.load(config.get("protocol")).read_only:
        raise ValueError("legacy_protocol_configuration_read_only")
    return config


def open_source(
    store: Store,
    config: Record,
    week_id: str,
    phase: str,
    *,
    clock: Callable[[], datetime] | None = None,
    stop_at: datetime | None = None,
) -> Polymarket:
    network = config["network"]
    return Polymarket(
        store.root / "batches" / week_id / f"raw-{phase.lower()}.sqlite",
        gamma_url=network["gamma_url"],
        data_url=network["data_url"],
        timeout=network["timeout_seconds"],
        attempts=network["attempts"],
        rate=network["requests_per_second"],
        workers=network.get("workers", 1),
        clock=clock or (lambda: datetime.now(UTC)),
        stop_at=stop_at,
        cancel_event=config.get("cancel_event"),
        transport_factory=network.get("transport_factory"),
    )


def latest_path(store: Store, week_id: str) -> Path:
    pointer = read_manifest(store.root / "batches" / week_id / "latest.json")
    return store.root / pointer["manifest"]


def receive_stage_result(store: Store, task_id: str, artifact_path: Path) -> Record:
    row = store.db.execute("SELECT input_hash,payload FROM tasks WHERE task_id=?", [task_id]).fetchone()
    if row is None:
        raise ValueError("stage_task_missing")
    artifact = read_manifest(artifact_path)
    if artifact.get("input_hash") != row[0] or not artifact.get("handler_version"):
        raise ValueError("stage_result_provenance_mismatch")
    if not isinstance(artifact.get("files"), list) or not artifact["files"]:
        raise ValueError("stage_output_files_missing")
    for entry in artifact["files"]:
        relative = Path(entry["path"])
        if relative.is_absolute() or ".." in relative.parts or relative == Path("manifest.json"):
            raise ValueError("stage_output_path_invalid")
        file = (artifact_path.parent / entry["path"]).resolve()
        if not file.is_relative_to(artifact_path.parent.resolve()) or file_hash(file) != entry["sha256"]:
            raise ValueError("stage_output_checksum_mismatch")
    import simplejson

    task = simplejson.loads(row[1])
    if task["status"] == "validated":
        raise ValueError("validated_stage_output_is_immutable")
    retained = store.root / "stages" / task_id / file_hash(artifact_path)
    retained.mkdir(parents=True, exist_ok=True)
    for entry in artifact["files"]:
        destination = retained / entry["path"]
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(artifact_path.parent / entry["path"], destination)
    retained_manifest = retained / "manifest.json"
    shutil.copyfile(artifact_path, retained_manifest)
    task.update(
        status="awaiting_handler_validation",
        handler_version=artifact["handler_version"],
        output={"manifest": str(retained_manifest), "sha256": file_hash(retained_manifest)},
    )
    store.db.execute(
        "UPDATE tasks SET status=?,payload=? WHERE task_id=?",
        [task["status"], dumps(task), task_id],
    )
    return task


def tick(
    config: Record,
    *,
    clock: Callable[[], datetime] | None = None,
    stop_at: datetime | None = None,
    bootstrap: bool = False,
    qualification: bool = False,
    hydrate: Callable[[Store, Record], None] | None = None,
) -> Record:
    clock = clock or (lambda: datetime.now(UTC))
    now = clock()
    week = Week.current(now, Policy.load(config.get("protocol")))

    def expired() -> bool:
        return bool(config.get("cancel_event") and config["cancel_event"].is_set()) or (
            stop_at is not None and clock() >= stop_at
        )

    with Store(**config["storage"]) as store:
        state_path = store.root / "scheduler.json"
        state = read_manifest(state_path) if state_path.exists() else {"last_refresh": {}}
        errors: dict[str, str] = {}
        batches = store.status()
        if bootstrap and not batches and not expired():
            initial = Week.bootstrap(now, Policy.load(config.get("protocol")))
            store.begin(initial, "qualification" if qualification else "bootstrap", config["config_hash"])

        def advance_existing(batch: Record) -> None:
            try:
                advance_batch(store, config, batch, clock(), state, clock=clock, stop_at=stop_at, hydrate=hydrate)
            except WorkYield:
                return
            except Exception as error:
                errors[batch["id"]] = f"{type(error).__name__}:{error}"

        kinds = {"qualification"} if qualification else {"formal", "bootstrap"}
        frozen = [batch for batch in batches if batch["kind"] in kinds and batch["frozen"]]
        for batch in sorted(frozen, key=lambda row: store.week(row["id"]).evidence_cutoff):
            if expired():
                break
            advance_existing(batch)
        current = next((b for b in batches if b["id"] == week.id), None)
        needs_collection = current is None or (
            current["kind"] == "formal"
            and not current["frozen"]
            and (
                not current["scan_complete"]
                or any(s not in {"collected", "rejected"} for s, _ in current["event_states"])
            )
        )
        if not qualification and week.collection_start <= now < week.freeze_at and needs_collection and not expired():
            try:
                if hydrate and current:
                    hydrate(store, current)
                with open_source(store, config, week.id, "V1", clock=clock, stop_at=stop_at) as pm:
                    log(
                        Pipeline(store, pm, clock=clock).collect(
                            week, "formal", config["config_hash"], progress=log, refresh_completed=False
                        )
                    )
            except Exception as error:
                errors[week.id] = f"{type(error).__name__}:{error}"
        for batch in store.status():
            if expired():
                break
            if batch["kind"] not in kinds or batch["frozen"]:
                continue
            advance_existing(batch)
        now = clock()
        state.update(last_tick=stamp(now), last_error=errors or None, yielded=expired())
        atomic_json(state_path, state)
        result = {"phase": "scheduler", "last_tick": stamp(now), "errors": errors, "yielded": expired()}
        phases: dict[str, int] = {}
        counts: dict[str, int] = {}
        for batch in store.status():
            phase = (
                "expired_incomplete"
                if batch["id"] in state.get("collection_closed", {})
                else "collecting"
                if not batch["frozen"]
                else "closed"
                if batch["id"] in state.get("gt_closed", {})
                else "awaiting_gt"
            )
            phases[phase] = phases.get(phase, 0) + 1
            for status, count in batch["event_states"]:
                counts[status] = counts.get(status, 0) + count
        result["status"] = {
            "date": now.date().isoformat(),
            "phases": phases,
            "source_event_counts": counts,
            "stages": {"V2": "not_configured", "V4": "not_configured"},
        }
        log(result)
        return result


def advance_batch(
    store: Store,
    config: Record,
    batch: Record,
    now: datetime,
    state: Record,
    *,
    clock: Callable[[], datetime] | None = None,
    stop_at: datetime | None = None,
    hydrate: Callable[[Store, Record], None] | None = None,
) -> None:
    clock = clock or (lambda: datetime.now(UTC))

    def require_budget() -> None:
        if (stop_at is not None and clock() >= stop_at) or (
            config.get("cancel_event") and config["cancel_event"].is_set()
        ):
            raise WorkYield("runner_work_budget")

    week = store.week(batch["id"])
    if week.policy.read_only:
        state.setdefault("legacy_read_only", {})[week.id] = {
            "protocol_version": week.record()["protocol_version"],
            "frozen": batch["frozen"],
        }
        return
    incomplete = not batch["scan_complete"] or any(
        status not in {"collected", "rejected"} for status, _ in batch["event_states"]
    )
    if not batch["frozen"] and incomplete and now >= week.freeze_at and batch["kind"] != "qualification":
        closed_collections = state.setdefault("collection_closed", {})
        if week.id not in closed_collections:
            store.failure(week.id, None, "V1", now, "collection_incomplete_at_deadline")
            closed_collections[week.id] = {
                "deadline": stamp(week.freeze_at),
                "closed_at": stamp(now),
                "status": "expired_incomplete",
            }
        log({"phase": "collection_closed", "batch_id": week.id, **closed_collections[week.id]})
        return
    release_path = store.root / "batches" / week.id / "benchmark" / "release.json"
    effective = Week.restore(read_manifest(release_path)["protocol"]) if release_path.exists() else week
    pointer = store.root / "batches" / week.id / "latest.json"
    closed = state.setdefault("gt_closed", {}).get(week.id, {})
    if batch["frozen"] and closed.get("deadline") == stamp(effective.evidence_cutoff) and pointer.exists():
        latest = latest_path(store, week.id)
        if not config["scheduler"]["publish"] or (latest.parent / "publication.json").exists():
            state.setdefault("finalization", {})[week.id] = {
                "deadline": stamp(effective.finalize_at),
                "status": "completed"
                if release_path.with_name("final.json").exists()
                else "deadline_passed"
                if now >= effective.finalize_at
                else "pending",
            }
            return
    if hydrate:
        hydrate(store, batch)
    require_budget()
    if batch["kind"] in {"bootstrap", "qualification"} and not batch["frozen"]:
        incomplete = not batch["scan_complete"] or any(
            status not in {"collected", "rejected"} for status, _ in batch["event_states"]
        )
        if incomplete:
            with open_source(store, config, week.id, "V1", clock=clock, stop_at=stop_at) as pm:
                result = Pipeline(store, pm, clock=clock).collect(
                    week.id, batch["kind"], config["config_hash"], progress=log, refresh_completed=False
                )
                log(result)
                if result.get("yielded"):
                    raise WorkYield("runner_work_budget")
                if not result["construction_complete"]:
                    raise ValueError("construction_incomplete")
    now = clock()
    require_budget()
    pointer_missing = not (store.root / "batches" / week.id / "latest.json").exists()
    if (not batch["frozen"] or pointer_missing) and (
        now >= week.freeze_at or batch["kind"] in {"bootstrap", "qualification"}
    ):
        manifest = store.freeze(week, now)
        log({"phase": "freeze", "batch_id": week.id, "counts": manifest["counts"]})
        batch["frozen"] = True
    if not batch["frozen"]:
        return
    release_path = store.root / "batches" / week.id / "benchmark" / "release.json"
    if release_path.exists():
        week = Week.restore(read_manifest(release_path)["protocol"])
    require_budget()
    if config["scheduler"]["publish"]:
        manifest_path = latest_path(store, week.id)
        if not (manifest_path.parent / "publication.json").exists():
            log(
                publish(
                    store,
                    manifest_path,
                    **config["publication"],
                    stop_at=stop_at,
                    clock=clock,
                    cancel_event=config.get("cancel_event"),
                )
            )
    if now >= week.freeze_at:
        closed = state.setdefault("gt_closed", {})
        if now >= week.evidence_cutoff:
            if closed.get(week.id, {}).get("deadline") != stamp(week.evidence_cutoff):
                store.release_results(week, now)
                closed[week.id] = {"deadline": stamp(week.evidence_cutoff), "closed_at": stamp(now)}
        else:
            last = state["last_refresh"].get(week.id)
            if last is None or (now - instant(last)).total_seconds() >= config["scheduler"]["poll_seconds"]:
                with open_source(store, config, week.id, "V3", clock=clock, stop_at=stop_at) as pm:
                    refresh = Pipeline(store, pm, clock=clock).refresh(week.id, progress=log, resume=True)
                    log(refresh)
                if refresh.get("yielded"):
                    raise WorkYield("runner_work_budget")
                state["last_refresh"][week.id] = stamp(clock())
            require_budget()
            store.release_results(week, clock())
        state.setdefault("finalization", {})[week.id] = {
            "deadline": stamp(week.finalize_at),
            "status": "completed"
            if release_path.with_name("final.json").exists()
            else "deadline_passed"
            if now >= week.finalize_at
            else "pending",
        }
    require_budget()
    if config["scheduler"]["publish"]:
        manifest_path = latest_path(store, week.id)
        if not (manifest_path.parent / "publication.json").exists():
            log(
                publish(
                    store,
                    manifest_path,
                    **config["publication"],
                    stop_at=stop_at,
                    clock=clock,
                    cancel_event=config.get("cancel_event"),
                )
            )


def parser() -> argparse.ArgumentParser:
    cli = argparse.ArgumentParser(prog="foreseebench", description="无LLM的Polymarket原始数据管线")
    cli.add_argument("--config", type=Path, default=Path("foreseebench.toml"))
    cli.add_argument("--qualification", action="store_true", help="使用隔离的工程核验数据目录")
    cli.add_argument("--bootstrap", action="store_true", help="完整初始源数据：实际观察与冻结时间，不倒填正式周批")
    commands = cli.add_subparsers(dest="command", required=True)
    download = commands.add_parser("download-view", help="从HF分片按需导出累计JSONL，不依赖本地采集状态")
    download.add_argument("--repo", required=True)
    download.add_argument("--revision", default="main")
    download.add_argument("--format", choices=["basic", "extended"], default="basic")
    download.add_argument("--view", choices=["full", "balanced"], default="full")
    download.add_argument("--output", type=Path, required=True)
    for command in (
        "collect",
        "freeze",
        "refresh",
        "release",
        "verify",
        "publish",
        "export",
        "stage",
    ):
        sub = commands.add_parser(command)
        sub.add_argument("--week", required=True, help="UTC周一 YYYY-MM-DD，或已创建的 bootstrap 批次 ID")
        if command == "collect":
            sub.add_argument("--max-events", type=int, help="仅工程核验允许限量")
        if command == "publish":
            sub.add_argument("--revision", default=None)
        if command == "export":
            sub.add_argument("--extended", action="store_true")
            sub.add_argument("--view", choices=["full", "balanced"], default="full")
            sub.add_argument("--output", type=Path, required=True)
            sub.add_argument("--stage", choices=["V1", "latest"], default="V1")
        if command == "stage":
            sub.add_argument("--stage", choices=["V2", "V4"], required=True)
            sub.add_argument("--v2-task-id", help="V4绑定同批次已接收的V2产物")
    receive = commands.add_parser("stage-receive")
    receive.add_argument("--task-id", required=True)
    receive.add_argument("--artifact", type=Path, required=True)
    for command in ("stage-validate", "benchmark-release", "benchmark-finalize"):
        sub = commands.add_parser(command)
        sub.add_argument("--task-id", required=True)
    submit = commands.add_parser("submit")
    submit.add_argument("--week", required=True)
    submit.add_argument("--participant", required=True)
    submit.add_argument("--file", type=Path, required=True)
    for command in ("status", "tick", "run"):
        commands.add_parser(command)
    return cli


def run_service(config: Record, *, bootstrap: bool = False) -> None:
    """定时器只唤醒主线程；采集与发布在主线程内按预算推进。"""
    import math
    import os
    import signal
    import threading
    from datetime import timedelta
    from functools import partial

    interval = float(config["scheduler"]["poll_seconds"])
    if not math.isfinite(interval) or interval <= 0:
        raise ValueError("scheduler_interval_invalid")
    due, cancelled = threading.Event(), threading.Event()
    runtime = dict(config, cancel_event=cancelled)
    scheduler = BackgroundScheduler(timezone="UTC")
    scheduler.add_job(
        due.set,
        IntervalTrigger(seconds=interval, start_date=datetime.now(UTC)),
        max_instances=1,
        coalesce=True,
        misfire_grace_time=60,
    )

    def stop(signum: int, frame: Any) -> None:
        cancelled.set()
        due.set()

    previous = {kind: signal.signal(kind, stop) for kind in (signal.SIGTERM, signal.SIGINT)}
    try:
        scheduler.start()
        due.set()
        while not cancelled.is_set():
            due.wait()
            due.clear()
            if cancelled.is_set():
                break
            if config["scheduler"]["publish"] and (bucket_id := os.environ.get("HF_BUCKET")):
                from huggingface_hub import HfApi

                from .cloud import StorageGuard

                guard = StorageGuard(HfApi(), bucket_id)
                repo_id = config["publication"]["repo_id"]
                guard.include_dataset(repo_id)
                runtime["storage"] = dict(
                    config["storage"], before_publish=partial(guard.before_publish, repo_id)
                )
            tick(runtime, bootstrap=bootstrap, stop_at=datetime.now(UTC) + timedelta(seconds=interval))
    finally:
        cancelled.set()
        if scheduler.running:
            scheduler.shutdown(wait=True)
        for kind, handler in previous.items():
            signal.signal(kind, handler)


def main() -> None:
    args = parser().parse_args()
    if args.command == "download-view":
        try:
            log(download_view(args.repo, args.output, revision=args.revision, format=args.format, view=args.view))
        except Exception as error:
            print(dumps({"error": f"{type(error).__name__}:{error}"}), file=sys.stderr)
            raise SystemExit(1) from error
        return
    config = settings(args.config, args.qualification)
    if args.command in {"run", "tick"}:
        if args.qualification:
            raise SystemExit("定时服务仅运行正式管线")
        if args.command == "run":
            run_service(config, bootstrap=args.bootstrap)
        else:
            tick(config, bootstrap=args.bootstrap)
        return
    try:
        if args.command == "submit":
            log(accept_submission(config["storage"]["root"], args.week, args.participant, read_manifest(args.file)))
            return
        with Store(**config["storage"]) as store:
            if args.command == "status":
                log(
                    {
                        "batches": store.status(),
                        "stages": {"V2": "not_configured", "V4": "not_configured"},
                    }
                )
                return
            if args.command == "stage-receive":
                log(receive_stage_result(store, args.task_id, args.artifact))
                return
            if args.command in {"stage-validate", "benchmark-release", "benchmark-finalize"}:
                task, _, base, _ = stage_output(store, args.task_id)
                if args.command == "stage-validate":
                    reference = config.get("stages", {}).get(task["stage"].lower() + "_validator")
                    validator = None
                    if reference:
                        module, name = reference.split(":", 1)
                        validator = getattr(importlib.import_module(module), name)
                        if not callable(validator):
                            raise ValueError("invalid_stage_validator")
                    log(validate_stage_result(store, args.task_id, validator=validator))
                elif args.command == "benchmark-release":
                    with open_source(store, config, base["batch_id"], "V2") as pm:
                        log(release_benchmark(store, pm, args.task_id))
                    store.release_results(store.week(base["batch_id"]), datetime.now(UTC))
                else:
                    log(finalize_benchmark(store, args.task_id))
                    store.release_results(store.week(base["batch_id"]), datetime.now(UTC))
                return
            now = datetime.now(UTC)
            week = store.week(args.week)
            if args.command == "collect":
                if args.bootstrap and "-bootstrap-" not in args.week:
                    if week.open_at != Week.current(now).open_at:
                        raise ValueError("bootstrap_must_use_current_week")
                    unfinished = [
                        b
                        for b in store.status()
                        if b["kind"] == "bootstrap"
                        and not b["frozen"]
                        and b["id"].startswith(args.week + "-bootstrap-")
                        and not store.week(b["id"]).policy.read_only
                    ]
                    week = (
                        store.week(unfinished[-1]["id"])
                        if unfinished
                        else Week.bootstrap(now, Policy.load(config.get("protocol")))
                    )
                elif not (store.root / "batches" / week.id / "protocol.json").exists():
                    week = Week.parse(args.week, Policy.load(config.get("protocol")))
                if (store.root / "batches" / week.id / "V1" / "manifest.json").exists():
                    raise ValueError("batch_already_frozen")
                if week.policy.read_only:
                    raise ValueError("legacy_batch_read_only")
                with open_source(store, config, week.id, "V1") as pm:
                    log(
                        Pipeline(store, pm).collect(
                            week,
                            "qualification" if args.qualification else "bootstrap" if args.bootstrap else "formal",
                            config["config_hash"],
                            max_events=args.max_events,
                            progress=log,
                        )
                    )
            elif args.command == "freeze":
                log(store.freeze(week, now))
            elif args.command == "refresh":
                if week.policy.read_only:
                    raise ValueError("legacy_batch_read_only")
                with open_source(store, config, week.id, "V3") as pm:
                    log(Pipeline(store, pm).refresh(week.id, progress=log))
            elif args.command == "release":
                log(store.release_results(week, now))
            elif args.command == "verify":
                log(store.verify(read_manifest(latest_path(store, week.id))))
            elif args.command == "publish":
                log(
                    publish(
                        store,
                        latest_path(store, week.id),
                        config["publication"]["repo_id"],
                        args.revision or config["publication"]["revision"],
                    )
                )
            elif args.command == "export":
                manifest_path = (
                    (store.root / "batches" / week.id / "V1" / "manifest.json")
                    if args.stage == "V1"
                    else latest_path(store, week.id)
                )
                manifest = read_manifest(manifest_path)
                store.verify(manifest)
                args.output.parent.mkdir(parents=True, exist_ok=True)
                temporary = args.output.with_suffix(".partial")
                with temporary.open("w", encoding="utf-8") as output:
                    export_jsonl(store.root, manifest, output, extended=args.extended, view=args.view)
                temporary.replace(args.output)
                log({"output": str(args.output), "sha256": file_hash(args.output)})
            elif args.command == "stage":
                input_path = (
                    store.root / "batches" / week.id / "V1" / "manifest.json"
                    if args.stage == "V2"
                    else latest_path(store, week.id)
                )
                log(stage_task(store, args.stage, str(input_path.relative_to(store.root)), v2_task_id=args.v2_task_id))
    except Exception as error:
        print(dumps({"error": f"{type(error).__name__}:{error}"}), file=sys.stderr)
        raise SystemExit(1) from error
