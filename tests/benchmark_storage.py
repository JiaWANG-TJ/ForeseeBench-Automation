"""基于真实冻结样本重复构造工程负载；结果不属于正式数据集。"""

from __future__ import annotations

import argparse
import resource
import shutil
import time
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path

from foreseebench.contract import Week, dumps
from foreseebench.delivery import export_jsonl
from foreseebench.store import Store, atomic_json, file_hash, read_manifest, read_records


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--rows", type=int, default=200000)
    args = parser.parse_args()
    if args.output.exists():
        raise SystemExit("输出目录必须尚不存在，避免覆盖已有测量")
    manifest = read_manifest(args.manifest)
    sources = list(read_records(args.root, manifest, "events"))
    samples = defaultdict(list)
    for row in read_records(args.root, manifest, "trades"):
        if len(samples[row["event_id"]]) < 1000:
            samples[row["event_id"]].append(row)
    sources = [s for s in sources if samples[s["event_id"]]]
    if not sources or args.rows < len(sources):
        raise SystemExit("需要含成交的真实样本，且负载行数不能小于事件数")
    week = Week.parse(manifest["batch_id"])
    started = time.perf_counter()
    with Store(args.output) as store:
        store.begin(week, "qualification", file_hash(args.manifest))
        for entry in manifest["files"]:
            if Path(entry["path"]).name == "raw-v1.sqlite":
                archive = store.root / "batches" / week.id / "raw-v1.sqlite"
                archive.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(args.root / entry["path"], archive)
        remaining = args.rows
        for position, source in enumerate(sources):
            event_id = source["event_id"]
            count = remaining if position == len(sources) - 1 else args.rows // len(sources)
            remaining -= count
            store.event(week.id, event_id, source, "collected")
            original = samples[event_id]
            records = (dict(original[i % len(original)], row_id=f"load-{event_id}-{i}") for i in range(count))
            store.page(week.id, event_id, "trades", "synthetic-repeat-load", records, None, True)
            store.page(week.id, event_id, "comments", "empty", [], None, True)
        store.discovery_complete(week.id)
        staged = time.perf_counter()
        output_manifest = store.freeze(week, datetime.now(UTC))
        frozen = time.perf_counter()
        store.verify(output_manifest)
        count = sum(1 for _ in read_records(store.root, output_manifest, "trades"))
        assert count == args.rows
        scanned = time.perf_counter()
        export = args.output / "extended.jsonl"
        with export.open("w", encoding="utf-8") as stream:
            export_jsonl(store.root, output_manifest, stream, extended=True)
        ended = time.perf_counter()
    report = {
        "purpose": "synthetic_repeat_load_only_not_observed_market_data",
        "input_manifest_sha256": file_hash(args.manifest),
        "rows": count,
        "events": len(sources),
        "stage_seconds": staged - started,
        "freeze_seconds": frozen - staged,
        "verify_and_scan_seconds": scanned - frozen,
        "export_seconds": ended - scanned,
        "parquet_trade_bytes": sum(f["bytes"] for f in output_manifest["files"] if f["table"] == "trades"),
        "jsonl_bytes": export.stat().st_size,
        "parquet_trade_files": sum(f["table"] == "trades" for f in output_manifest["files"]),
        "peak_rss_kib": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
    }
    atomic_json(args.output / "measurements.json", report)
    print(dumps(report))


if __name__ == "__main__":
    main()
