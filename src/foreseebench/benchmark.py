"""冻结周基准成员、外部阶段契约和服务端提交记录。"""

from __future__ import annotations

import shutil
import sqlite3
from collections import Counter, defaultdict, deque
from collections.abc import Callable, Iterable, Iterator
from contextlib import closing
from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal
from itertools import islice
from pathlib import Path
from typing import TYPE_CHECKING

import simplejson

from .contract import (
    ANSWER_STATES,
    Record,
    Week,
    admit_event,
    digest,
    dumps,
    has_known_answer,
    instant,
    require_unresolved,
    settle_event,
    source_binding,
    stamp,
)
from .domains import PRIMARY_DOMAINS, event_group, grouping_record
from .source import EvidenceError, evidence_observations
from .store import atomic_json, file_hash, read_manifest, read_records

if TYPE_CHECKING:
    from .source import Polymarket
    from .store import Store


BALANCED_SELECTION_ALGORITHM = "domain_cap_series_day_v1"


def round_robin(groups: Iterable[Iterable[Record]]) -> Iterator[Record]:
    queues = deque(iter(group) for group in groups)
    while queues:
        group = queues.popleft()
        row = next(group, None)
        if row is not None:
            yield row
            queues.append(group)


def select_events(events: Iterable[Record], week: Week) -> Record:
    """各域独立封顶，域内依次轮询系列及UTC结束日；不回补其他领域。"""
    pools: dict[str, list[Record]] = {group: [] for group in (*PRIMARY_DOMAINS, "Unclassified")}
    seen: set[str] = set()
    tags: Counter[str] = Counter()
    for event in events:
        identifier = event["event_id"]
        if identifier in seen:
            raise ValueError("duplicate_selection_event")
        seen.add(identifier)
        group = event_group(event)
        series = sorted(set(event.get("series_ids") or []))
        tag_ids = sorted(set(event.get("tag_ids") or []))
        stratum = (
            {"basis": "series", "ids": series}
            if series
            else ({"basis": "tags", "ids": tag_ids} if tag_ids else {"basis": "event", "ids": [identifier]})
        )
        end = event.get("time", {}).get("end_at")
        pools.setdefault(group, []).append(
            {
                "event_id": identifier,
                "source_id": event["source_id"],
                "group": group,
                "rank": digest([week.policy.sampling_seed, week.id, identifier]),
                "stratum": stratum,
                "end_day": instant(end).date().isoformat() if end else "unknown",
            }
        )
        tags.update(set(tag for tag in event.get("domain") or [] if isinstance(tag, str)))
    members = []
    strata = {}
    for group in sorted(pools):
        buckets: dict[str, dict[str, list[Record]]] = defaultdict(lambda: defaultdict(list))
        for row in pools[group]:
            buckets[dumps(row["stratum"])][row["end_day"]].append(row)
        ordered = sorted(buckets, key=lambda key: (digest([week.policy.sampling_seed, week.id, group, key]), key))
        streams = [
            round_robin(
                [
                    sorted(buckets[key][day], key=lambda row: (row["rank"], row["event_id"]))
                    for day in sorted(
                        buckets[key],
                        key=lambda day: (digest([week.policy.sampling_seed, week.id, group, key, day]), day),
                    )
                ]
            )
            for key in ordered
        ]
        chosen = list(islice(round_robin(streams), week.policy.domain_event_cap))
        members.extend(chosen)
        selected_counts = Counter((dumps(row["stratum"]), row["end_day"]) for row in chosen)
        strata[group] = [
            {
                **simplejson.loads(key),
                "days": {
                    day: {"available": len(rows), "selected": selected_counts[key, day]}
                    for day, rows in sorted(buckets[key].items())
                },
            }
            for key in sorted(buckets)
        ]
    quotas = Counter(row["group"] for row in members)
    return {
        "algorithm": BALANCED_SELECTION_ALGORITHM,
        "seed": week.policy.sampling_seed,
        "domain_event_cap": week.policy.domain_event_cap,
        "available_count": len(seen),
        "selected_count": len(members),
        "available_groups": {group: len(pools[group]) for group in sorted(pools)},
        "selected_groups": {group: quotas[group] for group in sorted(pools)},
        "raw_tag_coverage": dict(sorted(tags.items())),
        "grouping": grouping_record(),
        "strata": strata,
        "members": members,
    }


def stage_output(store: Store, task_id: str) -> tuple[Record, Record, Record, Path]:
    saved = store.db.execute("SELECT payload FROM tasks WHERE task_id=?", [task_id]).fetchone()
    if saved is None:
        raise ValueError("stage_task_missing")
    task = simplejson.loads(saved[0])
    if not task.get("output"):
        raise ValueError("stage_output_missing")
    path = Path(task["output"]["manifest"])
    if file_hash(path) != task["output"]["sha256"]:
        raise ValueError("stage_output_changed")
    artifact = read_manifest(path)
    if artifact.get("input_hash") != task["input_hash"] or artifact.get("stage") != task["stage"]:
        raise ValueError("stage_result_provenance_mismatch")
    for entry in artifact["files"]:
        file = (path.parent / entry["path"]).resolve()
        if not file.is_relative_to(path.parent.resolve()) or file_hash(file) != entry["sha256"]:
            raise ValueError("stage_output_checksum_mismatch")
    base = read_manifest(store.root / task["input_manifest"])
    store.verify(base)
    return task, artifact, base, path


def artifact_rows(artifact: Record, path: Path) -> Iterator[Record]:
    for entry in artifact["files"]:
        with (path.parent / entry["path"]).open(encoding="utf-8") as stream:
            for line in stream:
                value = simplejson.loads(line, use_decimal=True)
                if not isinstance(value, dict):
                    raise ValueError("stage_row_must_be_object")
                yield value


def v2_records(store: Store, artifact: Record, base: Record, path: Path) -> dict[str, Record]:
    sources = {row["event_id"]: row for row in read_records(store.root, base, "events")}
    result: dict[str, Record] = {}
    questions: set[str] = set()
    for row in artifact_rows(artifact, path):
        identifier = row.get("event_id")
        if not isinstance(identifier, str) or identifier not in sources or identifier in result:
            raise ValueError("v2_event_missing_or_duplicate")
        source = sources[identifier]
        if row.get("source_id") != source["source_id"] or row.get("source_hash") != source["source_hash"]:
            raise ValueError("v2_source_binding_mismatch")
        if not isinstance(row.get("eligible"), bool) or not isinstance(row.get("questions"), list):
            raise ValueError("v2_eligibility_missing")
        if not row["eligible"]:
            if not row.get("reason") or row["questions"]:
                raise ValueError("v2_rejection_requires_reason_and_no_questions")
        else:
            review = row.get("review", {})
            if not review.get("evidence_urls") or not row["questions"]:
                raise ValueError("v2_semantic_review_missing")
            instant(review["answer_available_after"])
            conditions = {market["condition_id"] for market in source["markets"]}
            for question in row["questions"]:
                qa_id = question.get("qa_id")
                if not isinstance(qa_id, str) or not qa_id or qa_id in questions:
                    raise ValueError("v2_question_id_missing_or_duplicate")
                if not isinstance(question.get("question"), str) or not question["question"].strip():
                    raise ValueError("v2_question_missing")
                if not isinstance(question.get("answer_spec"), dict) or not question["answer_spec"]:
                    raise ValueError("v2_answer_rule_missing")
                identifiers = question.get("condition_ids", [])
                if len(identifiers) != len(conditions) or set(identifiers) != conditions:
                    raise ValueError("v2_requires_complete_market_set")
                if any(field in question for field in ("groundtruth", "answer", "resolution", "correct_option")):
                    raise ValueError("v2_contains_answer_field")
                questions.add(qa_id)
        result[identifier] = row
    if set(result) != set(sources):
        raise ValueError("v2_incomplete_source_denominator")
    return result


def validate_stage_result(
    store: Store, task_id: str, *, validator: Callable[[Record, Record, Path], Record] | None = None
) -> Record:
    """只提供外部语义审核端口；没有处理器时不能授予通过状态。"""
    if validator is None:
        raise ValueError("stage_handler_not_configured")
    task, artifact, base, path = stage_output(store, task_id)
    if task["stage"] == "V2":
        v2_records(store, artifact, base, path)
    else:
        v4_records(store, task, artifact, base, path)
    receipt = validator(task, artifact, path)
    if receipt.get("passed") is not True or not receipt.get("validator_version") or not receipt.get("evidence"):
        raise ValueError("stage_semantic_validation_failed")
    task.update(
        status="validated",
        validation={
            **receipt,
            "validated_at": stamp(datetime.now(UTC)),
            "artifact_sha256": file_hash(path),
        },
    )
    store.db.execute("UPDATE tasks SET status=?,payload=? WHERE task_id=?", ["validated", dumps(task), task_id])
    return task


def release_benchmark(
    store: Store, pm: Polymarket, task_id: str, *, clock: Callable[[], datetime] = lambda: datetime.now(UTC)
) -> Record:
    task, artifact, base, path = stage_output(store, task_id)
    if task["stage"] != "V2" or task["status"] != "validated":
        raise ValueError("v2_not_validated")
    if base["kind"] == "qualification" or "protocol" not in base:
        raise ValueError("benchmark_requires_production_protocol")
    destination = store.root / "batches" / base["batch_id"] / "benchmark"
    if (destination / "release.json").exists():
        raise ValueError("benchmark_already_released")
    records = v2_records(store, artifact, base, path)
    scheduled = Week.restore(base["protocol"])
    if scheduled.policy.read_only:
        raise ValueError("legacy_batch_read_only")
    week = replace(scheduled, publication_at=instant(clock()), collection_cutoff=instant(base["frozen_at"]))
    week.require_cycle_budget()
    if week.publish_at < instant(base["frozen_at"]):
        raise ValueError("publication_before_freeze")
    if base["kind"] == "formal" and week.publish_at < scheduled.publish_at:
        raise ValueError("publication_not_due")
    accepted, excluded, observations = [], [], []
    for source in read_records(store.root, base, "events"):
        row = records[source["event_id"]]
        reason = row["reason"] if not row["eligible"] else None
        if store.previously_published(source["event_id"], week.id):
            reason = "already_published"
        previous = store.latest_resolution(week.id, source["event_id"])
        if previous and any(
            fact.get("status") in {"verified", "conflicting_evidence", "fractional_or_invalid_payout"}
            or fact.get("lifecycle", {}).get("status") in ANSWER_STATES
            or fact.get("first_answer_observed_at") is not None
            or any(token.get("resolved") is True for token in fact.get("tokens", []))
            for fact in previous.get("markets", [])
        ):
            reason = "answer_previously_observed"
        if reason is None:
            raw, receipt = pm.event(source["event_id"])
            try:
                current = admit_event(raw, week, instant(receipt["observed_at"]))
                if source_binding(current) != source_binding(source):
                    raise ValueError("source_structure_changed")
            except (ValueError, TypeError, KeyError) as error:
                reason = str(error)
            if reason is None:
                fetch_error = None
                try:
                    resolutions, tokens, receipts = pm.evidence(source)
                except EvidenceError as error:
                    fetch_error = error
                    resolutions, tokens, receipts = error.partial
                    store.failure(week.id, source["event_id"], "V2", clock(), str(error))
                if has_known_answer(source, resolutions, tokens):
                    observed = instant(clock())
                    result = settle_event(
                        source,
                        resolutions,
                        tokens,
                        week,
                        observed,
                        previous=previous,
                        observations=evidence_observations(source, resolutions, tokens, receipts, observed),
                    )
                    result["receipt_ids"] = [r["request_id"] for r in receipts]
                    store.add_resolution(week.id, result)
                elif fetch_error is not None:
                    raise fetch_error
                try:
                    require_unresolved(source, resolutions, tokens)
                except ValueError as error:
                    if str(error) != "market_resolved_during_collection":
                        raise
                    reason = str(error)
                observations.append({"event_id": source["event_id"], "metadata": receipt, "native": receipts})
        if reason is None:
            accepted.append(source)
        else:
            excluded.append({"event_id": source["event_id"], "reason": reason})
    retained_files = artifact_files(store, task_id)
    archive = store.root / "batches" / week.id / "raw-v2.sqlite"
    if archive.exists():
        destination.mkdir(parents=True, exist_ok=True)
        retained_archive = destination / "prepublication.sqlite"
        shutil.copyfile(archive, retained_archive)
        retained_files.append(
            {
                "table": "evidence",
                "path": str(retained_archive.relative_to(store.root)),
                "sha256": file_hash(retained_archive),
                "bytes": retained_archive.stat().st_size,
                "rows": 0,
            }
        )
    # 所有网络工作及大文件核验完成后确定服务端发布时刻，完整保留 72 小时。
    week = replace(week, publication_at=instant(clock()))
    week.require_cycle_budget()
    eligible = []
    for source in accepted:
        review = records[source["event_id"]]["review"]
        if instant(review["answer_available_after"]) <= week.prediction_cutoff:
            reason = "answer_available_before_submission"
        elif any(not week.end_min < instant(m["end_at"]) <= week.end_max for m in source["markets"]):
            reason = "market_end_outside_actual_publication_window"
        else:
            eligible.append(source)
            continue
        excluded.append({"event_id": source["event_id"], "reason": reason})
    selection = select_events(eligible, week)
    attempt = {
        "checked_at": stamp(clock()),
        "selection": selection,
        "excluded": excluded,
        "observations": observations,
        "v2_manifest_sha256": file_hash(path),
    }
    atomic_json(destination / "attempts" / f"{digest(attempt)}.json", attempt)
    if not selection["members"]:
        raise ValueError("no_eligible_benchmark_events")
    destination.mkdir(parents=True, exist_ok=True)
    questions = [
        {**q, "event_id": member["event_id"], "source_id": member["source_id"], "group": member["group"]}
        for member in selection["members"]
        for q in records[member["event_id"]]["questions"]
    ]
    questions_path = destination / "questions.jsonl"
    temporary = questions_path.with_suffix(".partial")
    with temporary.open("w", encoding="utf-8") as output:
        for question in questions:
            output.write(dumps(question) + "\n")
    temporary.replace(questions_path)
    release = {
        "schema_version": "2.0",
        "batch_id": week.id,
        "protocol": week.record(),
        "selection": selection,
        "v2_task_id": task_id,
        "v2_manifest_sha256": file_hash(path),
        "validation": task["validation"],
        "v1_manifest": task["input_manifest"],
        "v1_manifest_sha256": file_hash(store.root / task["input_manifest"]),
        "question_ids": [q["qa_id"] for q in questions],
        "question_count": len(questions),
        "excluded": excluded,
        "prepublication_observations": observations,
        "files": [
            {
                "table": "benchmark_questions",
                "path": str(questions_path.relative_to(store.root)),
                "sha256": file_hash(questions_path),
                "bytes": questions_path.stat().st_size,
                "rows": len(questions),
            }
        ]
        + retained_files,
    }
    atomic_json(destination / "release.json", release)
    store.index_publication(week.id)
    return release


def accept_submission(
    store: Store | Path,
    batch_id: str,
    participant_id: str,
    payload: Record,
    *,
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> Record:
    """服务端生成回执时间；分批与更新提交保留完整历史，不接受客户端时钟。"""
    root = store.resolve() if isinstance(store, Path) else store.root
    if "/" in batch_id or "\\" in batch_id or batch_id in {"", ".", ".."}:
        raise ValueError("invalid_batch_id")
    received_at = instant(clock())
    path = root / "batches" / batch_id / "benchmark" / "release.json"
    release = read_manifest(path)
    week = Week.restore(release["protocol"])
    if week.id != batch_id or release["batch_id"] != batch_id:
        raise ValueError("submission_batch_mismatch")
    if not week.publish_at <= received_at < week.prediction_cutoff:
        raise ValueError("submission_closed")
    if (
        payload.get("release_sha256") != file_hash(path)
        or payload.get("v2_manifest_sha256") != release["v2_manifest_sha256"]
    ):
        raise ValueError("submission_version_mismatch")
    predictions = payload.get("predictions")
    if not isinstance(predictions, dict) or not predictions:
        raise ValueError("predictions_missing")
    if set(predictions) - set(release["question_ids"]):
        raise ValueError("unknown_question_id")
    for prediction in predictions.values():
        if not isinstance(prediction, dict) or set(prediction) != {"answer", "confidence"}:
            raise ValueError("prediction_requires_answer_and_confidence")
        if prediction["answer"] is None:
            raise ValueError("prediction_answer_missing")
        confidence = prediction["confidence"]
        if isinstance(confidence, bool) or not isinstance(confidence, (int, float, Decimal)):
            raise ValueError("submission_confidence_invalid")
        value = Decimal(str(confidence))
        if not value.is_finite() or not 0 <= value <= 1:
            raise ValueError("submission_confidence_invalid")
    if not participant_id or len(participant_id) > 128:
        raise ValueError("participant_id_invalid")
    receipt = {
        "batch_id": batch_id,
        "participant_id": participant_id,
        "received_at": stamp(received_at),
        "release_sha256": file_hash(path),
        "v2_manifest_sha256": release["v2_manifest_sha256"],
        "predictions": predictions,
    }
    # 提交回执使用独立短事务，不能被大批量采集的 DuckDB 写入锁阻塞。
    with closing(sqlite3.connect(root / "submissions.sqlite", timeout=30)) as db, db:
        db.execute("PRAGMA journal_mode=DELETE")
        db.execute("PRAGMA synchronous=FULL")
        db.execute("""CREATE TABLE IF NOT EXISTS submissions (
            sequence INTEGER PRIMARY KEY AUTOINCREMENT, id TEXT UNIQUE,
            batch TEXT NOT NULL, participant TEXT NOT NULL, received_at TEXT NOT NULL, payload TEXT)""")
        db.execute("BEGIN IMMEDIATE")
        cursor = db.execute(
            "INSERT INTO submissions(batch,participant,received_at) VALUES (?,?,?)",
            [batch_id, participant_id, receipt["received_at"]],
        )
        receipt["sequence"] = cursor.lastrowid
        receipt["submission_id"] = digest(receipt)
        db.execute(
            "UPDATE submissions SET id=?,payload=? WHERE sequence=?",
            [receipt["submission_id"], dumps(receipt), receipt["sequence"]],
        )
    return receipt


def v4_records(store: Store, task: Record, artifact: Record, base: Record, path: Path) -> list[Record]:
    release_path = store.root / "batches" / base["batch_id"] / "benchmark" / "release.json"
    release = read_manifest(release_path)
    upstream = task.get("upstream_artifacts", {}).get("V2")
    if not upstream or upstream.get("task_id") != release["v2_task_id"]:
        raise ValueError("v4_requires_released_v2")
    if upstream["sha256"] != release["v2_manifest_sha256"] or artifact.get("release_sha256") != file_hash(release_path):
        raise ValueError("v4_release_binding_mismatch")
    native = {row["event_id"]: row for row in read_records(store.root, base, "resolutions")}
    questions = {}
    for entry in release["files"]:
        if entry["table"] != "benchmark_questions":
            continue
        file = store.root / entry["path"]
        if file_hash(file) != entry["sha256"]:
            raise ValueError("released_questions_changed")
        with file.open(encoding="utf-8") as stream:
            for line in stream:
                question = simplejson.loads(line)
                questions[question["qa_id"]] = question
    result = {}
    for row in artifact_rows(artifact, path):
        identifier = row.get("qa_id")
        if not isinstance(identifier, str) or identifier not in questions or identifier in result:
            raise ValueError("v4_question_missing_or_duplicate")
        question = questions[identifier]
        if any(row.get(key) != question[key] for key in ("source_id", "event_id")):
            raise ValueError("v4_question_source_mismatch")
        evidence = native.get(question["event_id"], {})
        if evidence.get("validity") not in {"valid", "invalid"}:
            raise ValueError("v3_event_not_terminal")
        validity = row.get("validity")
        if validity not in {"valid", "invalid"} or (evidence["validity"] == "invalid" and validity != "invalid"):
            raise ValueError("v4_event_validity_mismatch")
        reason = evidence["eligibility"]
        if evidence["validity"] == "valid" and validity == "invalid":
            reason = row.get("invalid_reason")
            if reason not in {"early_information", "invalid_question"} or not row.get("evidence_urls"):
                raise ValueError("v4_semantic_invalidation_requires_evidence")
        if (validity == "valid") != (row.get("groundtruth") is not None):
            raise ValueError("v4_answer_completeness_mismatch")
        result[identifier] = {**row, "reason": reason, "group": question["group"]}
    if set(result) != set(release["question_ids"]):
        raise ValueError("v4_incomplete_question_denominator")
    for member in release["selection"]["members"]:
        statuses = {row["validity"] for row in result.values() if row["event_id"] == member["event_id"]}
        if len(statuses) != 1:
            raise ValueError("v4_requires_whole_event_invalidation")
    return [result[identifier] for identifier in release["question_ids"]]


def finalize_benchmark(
    store: Store, task_id: str, *, clock: Callable[[], datetime] = lambda: datetime.now(UTC)
) -> Record:
    task, artifact, base, path = stage_output(store, task_id)
    if task["stage"] != "V4" or task["status"] != "validated":
        raise ValueError("v4_not_validated")
    questions = v4_records(store, task, artifact, base, path)
    valid = {q["event_id"] for q in questions if q["validity"] == "valid"}
    invalid = {q["event_id"] for q in questions if q["validity"] == "invalid"}
    destination = store.root / "batches" / base["batch_id"] / "benchmark" / "final.json"
    if destination.exists():
        raise ValueError("benchmark_already_finalized")
    release_path = destination.with_name("release.json")
    release = read_manifest(release_path)
    week = Week.restore(release["protocol"])
    if week.policy.read_only:
        raise ValueError("legacy_batch_read_only")
    input_manifest_path = store.root / task["input_manifest"]
    dependencies = [
        dict(entry, table="stage_input", rows=0)
        for entry in base["files"]
        if entry["table"] == "resolutions" or Path(entry["path"]).name == "evidence.sqlite"
    ]
    dependencies.append(
        {
            "table": "stage_input",
            "path": task["input_manifest"],
            "sha256": file_hash(input_manifest_path),
            "bytes": input_manifest_path.stat().st_size,
            "rows": 0,
        }
    )
    groups = (*PRIMARY_DOMAINS, "Unclassified")
    by_domain = {}
    for status, identifiers in (("valid", valid), ("invalid", invalid)):
        counts = Counter(m["group"] for m in release["selection"]["members"] if m["event_id"] in identifiers)
        by_domain[status] = {group: counts[group] for group in groups}
    completed = instant(clock())
    timing_ok = completed <= week.finalize_at
    report = {
        "schema_version": "2.0",
        "batch_id": week.id,
        "v4_task_id": task_id,
        "release_sha256": file_hash(release_path),
        "v3_manifest": task["input_manifest"],
        "v3_manifest_sha256": file_hash(store.root / task["input_manifest"]),
        "v4_manifest_sha256": file_hash(path),
        "validation": task["validation"],
        "event_count": len(valid | invalid),
        "question_count": len(questions),
        "valid_events": len(valid),
        "invalid_events": len(invalid),
        "status": "completed",
        "by_domain": by_domain,
        "finalization_deadline_at": stamp(week.finalize_at),
        "processing_within_budget": timing_ok,
        "cycle_hours": (completed - week.collection_start).total_seconds() / 3600,
        "completed_at": stamp(completed),
        "questions": questions,
        "files": artifact_files(store, task_id) + dependencies,
    }
    atomic_json(destination, report)
    return report


def artifact_files(store: Store, task_id: str) -> list[Record]:
    _, artifact, _, path = stage_output(store, task_id)
    files = [path, *(path.parent / entry["path"] for entry in artifact["files"])]
    if any(not file.resolve().is_relative_to(store.root) for file in files):
        raise ValueError("stage_output_not_retained")
    return [
        {
            "table": "stage_artifact",
            "path": str(file.relative_to(store.root)),
            "sha256": file_hash(file),
            "bytes": file.stat().st_size,
            "rows": 0,
        }
        for file in files
    ]


def benchmark_artifacts(store: Store, batch_id: str) -> list[Record]:
    files: dict[str, Record] = {}
    for name in ("release.json", "final.json"):
        path = store.root / "batches" / batch_id / "benchmark" / name
        if not path.exists():
            continue
        report = read_manifest(path)
        for entry in report["files"]:
            files[entry["path"]] = entry
        relative = str(path.relative_to(store.root))
        files[relative] = {
            "table": "benchmark_manifest",
            "path": relative,
            "sha256": file_hash(path),
            "bytes": path.stat().st_size,
            "rows": 0,
        }
    return [files[key] for key in sorted(files)]
