"""操作入口与 UTC 周批调度。"""

from __future__ import annotations

import argparse
import importlib
import shutil
import sys
import tomllib
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.interval import IntervalTrigger

from .benchmark import accept_submission, finalize_benchmark, release_benchmark, stage_output, validate_stage_result
from .contract import Policy, Record, Week, dumps, instant, stamp
from .delivery import export_jsonl, publish
from .pipeline import Pipeline, stage_task
from .source import Polymarket
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


def open_source(store: Store, config: Record, week_id: str, phase: str) -> Polymarket:
    network = config["network"]
    return Polymarket(
        store.root / "batches" / week_id / f"raw-{phase.lower()}.sqlite",
        gamma_url=network["gamma_url"],
        data_url=network["data_url"],
        timeout=network["timeout_seconds"],
        attempts=network["attempts"],
        rate=network["requests_per_second"],
        workers=network.get("workers", 1),
        clock=lambda: datetime.now(UTC),
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


def tick(config: Record) -> None:
    now = datetime.now(UTC)
    week = Week.current(now, Policy.load(config.get("protocol")))
    with Store(**config["storage"]) as store:
        state_path = store.root / "scheduler.json"
        state = read_manifest(state_path) if state_path.exists() else {"last_refresh": {}}
        errors: dict[str, str] = {}
        current = next((b for b in store.status() if b["id"] == week.id), None)
        needs_collection = current is None or (
            current["kind"] == "formal"
            and not current["frozen"]
            and (
                not current["scan_complete"]
                or any(s not in {"collected", "rejected"} for s, _ in current["event_states"])
            )
        )
        if week.collection_start <= now < week.freeze_at and needs_collection:
            try:
                with open_source(store, config, week.id, "V1") as pm:
                    log(
                        Pipeline(store, pm, clock=lambda: datetime.now(UTC)).collect(
                            week, "formal", config["config_hash"], progress=log, refresh_completed=False
                        )
                    )
            except Exception as error:
                errors[week.id] = f"{type(error).__name__}:{error}"
        now = datetime.now(UTC)
        for batch in store.status():
            if batch["kind"] not in {"formal", "bootstrap"}:
                continue
            try:
                advance_batch(store, config, batch, now, state)
            except Exception as error:
                errors[batch["id"]] = f"{type(error).__name__}:{error}"
        state.update(last_tick=stamp(now), last_error=errors or None)
        atomic_json(state_path, state)
        log({"phase": "scheduler", "last_tick": stamp(now), "errors": errors})


def advance_batch(store: Store, config: Record, batch: Record, now: datetime, state: Record) -> None:
    week = store.week(batch["id"])
    if week.policy.read_only:
        state.setdefault("legacy_read_only", {})[week.id] = {
            "protocol_version": week.record()["protocol_version"],
            "frozen": batch["frozen"],
        }
        return
    if batch["kind"] == "bootstrap" and not batch["frozen"]:
        with open_source(store, config, week.id, "V1") as pm:
            result = Pipeline(store, pm, clock=lambda: datetime.now(UTC)).collect(
                week.id, "bootstrap", config["config_hash"], progress=log
            )
            log(result)
            if not result["construction_complete"]:
                raise ValueError("construction_incomplete")
    pointer_missing = not (store.root / "batches" / week.id / "latest.json").exists()
    if (not batch["frozen"] or pointer_missing) and now >= week.freeze_at:
        manifest = store.freeze(week, now)
        log({"phase": "freeze", "batch_id": week.id, "counts": manifest["counts"]})
        batch["frozen"] = True
    if not batch["frozen"]:
        return
    release_path = store.root / "batches" / week.id / "benchmark" / "release.json"
    if release_path.exists():
        week = Week.restore(read_manifest(release_path)["protocol"])
    if config["scheduler"]["publish"]:
        manifest_path = latest_path(store, week.id)
        if not (manifest_path.parent / "publication.json").exists():
            log(publish(store, manifest_path, **config["publication"]))
    if now >= week.freeze_at:
        closed = state.setdefault("gt_closed", {})
        if now >= week.evidence_cutoff:
            if closed.get(week.id, {}).get("deadline") != stamp(week.evidence_cutoff):
                store.release_results(week, now)
                closed[week.id] = {"deadline": stamp(week.evidence_cutoff), "closed_at": stamp(now)}
        else:
            last = state["last_refresh"].get(week.id)
            if last is None or (now - instant(last)).total_seconds() >= config["scheduler"]["poll_seconds"]:
                with open_source(store, config, week.id, "V3") as pm:
                    log(Pipeline(store, pm, clock=lambda: datetime.now(UTC)).refresh(week.id, progress=log))
                state["last_refresh"][week.id] = stamp(datetime.now(UTC))
            store.release_results(week, datetime.now(UTC))
        state.setdefault("finalization", {})[week.id] = {
            "deadline": stamp(week.finalize_at),
            "status": "completed"
            if release_path.with_name("final.json").exists()
            else "deadline_passed"
            if now >= week.finalize_at
            else "pending",
        }
    if config["scheduler"]["publish"]:
        manifest_path = latest_path(store, week.id)
        if not (manifest_path.parent / "publication.json").exists():
            log(publish(store, manifest_path, **config["publication"]))


def parser() -> argparse.ArgumentParser:
    cli = argparse.ArgumentParser(prog="foreseebench", description="无LLM的Polymarket原始数据管线")
    cli.add_argument("--config", type=Path, default=Path("foreseebench.toml"))
    cli.add_argument("--qualification", action="store_true", help="使用隔离的工程核验数据目录")
    cli.add_argument("--bootstrap", action="store_true", help="完整初始源数据：实际观察与冻结时间，不倒填正式周批")
    commands = cli.add_subparsers(dest="command", required=True)
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


def main() -> None:
    args = parser().parse_args()
    config = settings(args.config, args.qualification)
    if args.command in {"run", "tick"}:
        if args.qualification:
            raise SystemExit("定时服务仅运行正式管线")
        tick(config)
        if args.command == "run":
            scheduler = BlockingScheduler(timezone="UTC")
            scheduler.add_job(
                tick,
                IntervalTrigger(
                    seconds=config["scheduler"]["poll_seconds"],
                    start_date=datetime(2020, 1, 1, tzinfo=UTC),
                ),
                args=[config],
                max_instances=1,
                coalesce=True,
                misfire_grace_time=60,
            )
            scheduler.start()
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
