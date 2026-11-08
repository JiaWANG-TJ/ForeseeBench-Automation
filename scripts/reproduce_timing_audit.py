"""复现2026-11-07冻结来源的数量与3＋5日期窗口；不回填历史GT。"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import resource
import sqlite3
import sys
import time
import tomllib
from collections import Counter, defaultdict
from contextlib import closing
from datetime import UTC, datetime, timedelta
from pathlib import Path

import foreseebench.contract
import foreseebench.domains
from foreseebench.contract import Policy, Week
from foreseebench.domains import PRIMARY_DOMAINS, event_group, grouping_record


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(4 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def connect(path: Path) -> sqlite3.Connection:
    return sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True)


def epoch_ns(value: datetime) -> int:
    return int(value.timestamp()) * 1_000_000_000


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-root", type=Path, required=True)
    parser.add_argument("--study-root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--config", type=Path, default=Path("foreseebench.toml"))
    args = parser.parse_args()
    started = time.monotonic()
    with args.config.open("rb") as stream:
        policy = Policy.load(tomllib.load(stream)["protocol"])
    if policy.legacy_gt_grace_hours is not None:
        raise ValueError("audit_requires_current_protocol")
    source_files = [args.source_root / f"part-{i:02d}.sqlite" for i in range(16)]
    source_files.append(args.source_root / "detail-enrichment.sqlite")
    coverage_path = args.source_root / "final-source-coverage.json"
    coverage = json.loads(coverage_path.read_text())
    lower, upper = coverage["integer_ID_interval"]
    seen = bytearray(upper + 1)
    unique_events = 0
    for path in source_files:
        with closing(connect(path)) as db:
            for (identifier,) in db.execute("SELECT event_id FROM events"):
                number = int(identifier)
                if not lower <= number <= upper or seen[number]:
                    raise ValueError("source_identity_outside_boundary_or_duplicate")
                seen[number] = 1
                unique_events += 1
    if (
        unique_events != coverage["all_returned_events"]
        or upper - lower + 1 - unique_events != coverage["confirmed_404_IDs"]
    ):
        raise ValueError("source_coverage_count_mismatch")
    del seen

    feature_files = sorted(args.study_root.glob("features-*.sqlite"))
    stocks: Counter[str] = Counter()
    all_domains: Counter[str] = Counter()
    new_by_week: list[Counter[str]] = [Counter(), Counter()]
    week_starts = [datetime(2026, 10, 19, tzinfo=UTC), datetime(2026, 10, 26, tzinfo=UTC)]
    boundaries = [epoch_ns(value) for value in [*week_starts, datetime(2026, 11, 2, tzinfo=UTC)]]
    for path in feature_files:
        with closing(connect(path)) as db:
            for closed, count in db.execute("SELECT event_closed,count(*) FROM features GROUP BY event_closed"):
                stocks[{0: "closed_false", 1: "closed_true", None: "closed_unknown"}[closed]] += count
            for (payload,) in db.execute("SELECT payload FROM features"):
                event = json.loads(payload)
                group = event_group(event)
                all_domains[group] += 1
                created = event["event_created_ns"]
                if created is not None and boundaries[0] <= created < boundaries[2]:
                    new_by_week[0 if created < boundaries[1] else 1][group] += 1
    if sum(stocks.values()) != unique_events or sum(all_domains.values()) != unique_events:
        raise ValueError("feature_projection_denominator_mismatch")

    schedules = []
    for start in week_starts:
        week = Week.parse(start.date().isoformat(), policy)
        for gt_days in (3, policy.ending_window_days):
            schedules.append(
                {
                    "week": week.id,
                    "freeze_at": week.freeze_at.isoformat(),
                    "publication_at": week.publish_at.isoformat(),
                    "submission_deadline": week.prediction_cutoff.isoformat(),
                    "gt_deadline_at": (week.prediction_cutoff + timedelta(days=gt_days)).isoformat(),
                    "gt_days": gt_days,
                }
            )
    counts = [defaultdict(Counter) for _ in schedules]
    thresholds = [
        (
            epoch_ns(datetime.fromisoformat(s["freeze_at"])),
            epoch_ns(datetime.fromisoformat(s["submission_deadline"])),
            epoch_ns(datetime.fromisoformat(s["gt_deadline_at"])),
        )
        for s in schedules
    ]
    members_path = args.study_root / "historical-memberships.sqlite"
    args.output.mkdir(parents=True, exist_ok=True)
    audit_path = args.output / "event-decisions.csv"
    cohort = 0
    with closing(connect(members_path)) as db, audit_path.open("w", newline="") as output:
        writer = csv.writer(output)
        writer.writerow(["event_id", "primary_domain", "week", "gt_days", "reason", "frozen_markets"])
        for (payload,) in db.execute("SELECT payload FROM events ORDER BY event_id"):
            event = json.loads(payload)
            cohort += 1
            group = event_group(event)
            for i, (freeze, submit, deadline) in enumerate(thresholds):
                frozen = []
                if event["created_ns"] is not None and (
                    event["created_ns"] > freeze or event["published_ns"] is not None and event["published_ns"] > freeze
                ):
                    reason = "not_created_by_freeze"
                elif event["created_ns"] is None or any(m["created_ns"] is None for m in event["markets"]):
                    reason = "membership_unknown"
                else:
                    frozen = [m for m in event["markets"] if m["created_ns"] <= freeze]
                    if not frozen:
                        reason = "empty_set"
                    elif any(m["end_ns"] is None for m in frozen):
                        reason = "endDate_missing"
                    elif any(m["end_ns"] <= submit for m in frozen):
                        reason = "early_end"
                    elif any(m["end_ns"] > deadline for m in frozen):
                        reason = "late_end"
                    else:
                        reason = "passed"
                counts[i][group][reason] += 1
                writer.writerow(
                    [event["event_id"], group, schedules[i]["week"], schedules[i]["gt_days"], reason, len(frozen)]
                )
    for schedule, grouped in zip(schedules, counts, strict=True):
        totals = sum(grouped.values(), Counter())
        if sum(totals.values()) != cohort:
            raise ValueError("decision_denominator_mismatch")
        schedule["counts"] = dict(totals)
        schedule["domains"] = {group: dict(grouped[group]) for group in (*PRIMARY_DOMAINS, "Unclassified")}
        schedule["named_non_sports_pass"] = sum(grouped[g]["passed"] for g in PRIMARY_DOMAINS if g != "Sports")
    inputs = [coverage_path, *source_files, *feature_files, members_path, args.config]
    report = {
        "kind": "historical_date_reconstruction_only",
        "raw_unique_events": unique_events,
        "stocks": dict(stocks),
        "raw_primary_domains": dict(all_domains),
        "new_by_createdAt": {
            str(week.date()): dict(values) for week, values in zip(week_starts, new_by_week, strict=True)
        },
        "cohort_events": cohort,
        "policy": policy.record(),
        "grouping": grouping_record(),
        "schedules": schedules,
        "input_hashes": {str(path): sha256(path) for path in inputs},
        "implementation": {
            str(path): sha256(path)
            for path in [
                Path(__file__),
                Path(foreseebench.contract.__file__),
                Path(foreseebench.domains.__file__),
                Path(__file__).resolve().parents[1] / "uv.lock",
            ]
        },
        "python": sys.version.split()[0],
        "max_rss_kib": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
        "decisions": {"path": audit_path.name, "sha256": sha256(audit_path)},
        "limitations": [
            "当前元数据不能恢复历史删除、移动和日期修订。",
            "createdAt不能证明历史公开状态。",
            "这些是日期候选，不是已核验的历史未决或按期GT数量。",
        ],
        "elapsed_seconds": round(time.monotonic() - started, 3),
    }
    (args.output / "reproduction.json").write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
    print(
        json.dumps(
            {
                "output": str(args.output),
                "raw_unique_events": unique_events,
                "cycles": [
                    {"week": s["week"], "gt_days": s["gt_days"], "named_non_sports_pass": s["named_non_sports_pass"]}
                    for s in schedules
                ],
            },
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    main()
