"""两种读取视图与 Hugging Face 完整快照提交。"""

from __future__ import annotations

import hashlib
import os
import re
import sqlite3
import tempfile
import time
import zlib
from collections.abc import Iterable, Iterator
from contextlib import closing
from decimal import Decimal
from importlib.resources import files as resource_files
from itertools import groupby
from pathlib import Path
from typing import Any, Literal, TextIO

import httpx
import ijson
import simplejson
from huggingface_hub import CommitOperationAdd, CommitOperationDelete, HfApi, hf_hub_download

from .contract import Record, Week, bound_tokens, digest, dumps, instant, settled_payouts, token_matches
from .domains import PRIMARY_DOMAINS, event_group, grouping_record
from .store import (
    PARQUET_SCHEMA_VERSION,
    Store,
    atomic_json,
    file_hash,
    implementation_provenance,
    read_manifest,
    read_records,
)

CARD_FILES: dict[Literal["zh", "en"], str] = {"zh": "README.md", "en": "README.en.md"}
JSONL_SCHEMA_VERSION = "3.0"
EXPORT_PATHS = {f"{view}/{name}.jsonl" for view in ("full", "balanced") for name in ("basic", "extended")}


def write_comment_trees(output: TextIO, records: Iterable[Record]) -> None:
    """用临时索引流式输出回复树；内存只随嵌套深度增长。"""
    with tempfile.TemporaryDirectory(prefix="foreseebench-comments-") as temporary:
        db = sqlite3.connect(Path(temporary) / "comments.sqlite")
        try:
            db.execute("CREATE TABLE comments (id TEXT PRIMARY KEY,parent TEXT,created TEXT,payload TEXT)")
            total = 0
            for row in records:
                if not row.get("comment_id"):
                    raise ValueError("comment_id_missing")
                try:
                    db.execute(
                        "INSERT INTO comments VALUES (?,?,?,?)",
                        [row["comment_id"], row.get("parent_comment_id") or None, row.get("createdAt"), dumps(row)],
                    )
                except sqlite3.IntegrityError as error:
                    raise ValueError("duplicate_comment_id") from error
                total += 1
            db.execute("CREATE INDEX comment_children ON comments(parent,created,id)")
            visited = 0
            for field, condition in (
                ("comments", "parent IS NULL"),
                ("orphan_replies", "parent IS NOT NULL AND NOT EXISTS (SELECT 1 FROM comments p WHERE p.id=c.parent)"),
            ):
                output.write("," + dumps(field) + ":[")
                cursor = db.execute(f"SELECT id,payload FROM comments c WHERE {condition} ORDER BY created,id")
                stack = [(cursor, False)]
                while stack:
                    cursor, emitted = stack[-1]
                    entry = cursor.fetchone()
                    if entry is None:
                        cursor.close()
                        stack.pop()
                        output.write("]")
                        if stack:
                            output.write("}")
                        continue
                    if emitted:
                        output.write(",")
                    stack[-1] = (cursor, True)
                    identifier, payload = entry
                    visited += 1
                    output.write(payload[:-1] + ',"replies":[')
                    children = db.execute(
                        "SELECT id,payload FROM comments WHERE parent=? ORDER BY created,id", [identifier]
                    )
                    stack.append((children, False))
            if visited != total:
                raise ValueError("comment_reply_cycle")
        finally:
            db.close()


def frozen_event_metadata(root: Path, manifest: Record) -> dict[str, Record]:
    """读取冻结来源的 Event 文本及 slug，供导出核对，不改写输入快照。"""
    expected = {event["source_hash"] for event in read_records(root, manifest, "events")}
    if not expected:
        return {}
    archives = [entry for entry in manifest["files"] if Path(entry["path"]).name == "raw-v1.sqlite"]
    if not archives:
        raise ValueError("v1_evidence_missing")
    if len(archives) != 1:
        raise ValueError("ambiguous_v1_evidence")
    entry = archives[0]
    relative = Path(entry["path"])
    if relative.is_absolute() or ".." in relative.parts:
        raise ValueError("invalid_export_data_path")
    path = root / relative
    if file_hash(path) != entry["sha256"]:
        raise ValueError("export_input_checksum_mismatch")
    metadata: dict[str, Record] = {}
    query = """SELECT content FROM bodies WHERE sha256 IN (
        SELECT json_extract(payload,'$.body_sha256') FROM receipts
        WHERE json_extract(payload,'$.status')=200
        AND json_extract(payload,'$.url') GLOB '*/events/[0-9]*')"""
    with closing(sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True)) as archive:
        for (body,) in archive.execute(query):
            try:
                raw = simplejson.loads(zlib.decompress(body), use_decimal=True)
            except simplejson.JSONDecodeError:
                continue
            if not isinstance(raw, dict) or (source_hash := digest(raw)) not in expected:
                continue
            slug = raw.get("slug")
            if slug is not None and not isinstance(slug, str):
                raise ValueError("invalid_event_slug")
            title = raw.get("title")
            if not isinstance(title, str) or not title.strip():
                raise ValueError("event_title_missing")
            metadata[source_hash] = {
                "event_id": str(raw.get("id")),
                "question": title,
                "description": raw.get("description"),
                "slug": slug,
                "series_ids": sorted(
                    {str(s["id"]) for s in raw.get("series", []) if isinstance(s, dict) and s.get("id") is not None}
                )
                if isinstance(raw.get("series"), list)
                else None,
                "resolution_sources": {
                    m["conditionId"]: m.get("resolutionSource")
                    for m in raw.get("markets", [])
                    if isinstance(m, dict) and m.get("conditionId")
                },
            }
    if expected - metadata.keys():
        raise ValueError("frozen_event_source_missing")
    return metadata


def native_groundtruth(source: Record, result: Record | None) -> Record | None:
    """按冻结资产对齐全部原生支付；分数支付完整性不冒充唯一赢家。"""
    if result is None:
        return None
    by_condition: dict[str, list[Record]] = {}
    for fact in result.get("markets", []):
        by_condition.setdefault(fact["condition_id"], []).append(fact)
    markets = []
    for market in source["markets"]:
        facts = by_condition.get(market["condition_id"], [])
        fact = facts[0] if len(facts) == 1 else {}
        by_token: dict[str, list[Record]] = {}
        for token in fact.get("tokens", []):
            if token.get("token_id"):
                by_token.setdefault(token["token_id"], []).append(token)
        selected, binding = bound_tokens(market, by_token)
        payouts: list[Decimal | None] = []
        malformed = False
        for i, token in enumerate(selected):
            value = None
            if (
                token_matches(market, i, token)
                and token.get("resolved") is True
                and token.get("final_price") is not None
            ):
                try:
                    value = Decimal(str(token["final_price"]))
                    if not value.is_finite() or not 0 <= value <= 1:
                        value, malformed = None, True
                except ArithmeticError:
                    malformed = True
            payouts.append(value)
        lifecycle = fact.get("lifecycle", {}).get("status")
        paid = [value for value in payouts if value is not None]
        complete_payouts = settled_payouts(market, fact)
        if len(facts) > 1 or fact.get("status") in {"ambiguous_evidence", "conflicting_evidence"}:
            status = "conflict"
        elif malformed:
            status = "invalid_payout"
        elif not binding:
            status = "missing_or_invalid_binding"
        elif complete_payouts is not None:
            status = "resolved" if sorted(complete_payouts) == [Decimal(0), Decimal(1)] else "fractional"
        elif lifecycle == "resolved" and len(paid) == len(payouts):
            status = "invalid_payout"
        elif not lifecycle:
            status = "missing_lifecycle"
        else:
            status = "pending"
        markets.append(
            {
                "condition_id": market["condition_id"],
                "status": status,
                "resolution_status": lifecycle,
                "payouts": payouts,
                "resolved_at": fact.get("resolved_at"),
                "observed_at": fact.get("observed_at"),
            }
        )
    status = (
        "complete"
        if markets and all(m["status"] in {"resolved", "fractional"} for m in markets)
        else (
            "conflict"
            if any(m["status"] in {"conflict", "invalid_payout"} for m in markets)
            else "partial"
            if any(value is not None for m in markets for value in m["payouts"])
            else "pending"
        )
    )
    return {
        "status": status,
        "qualification": {"validity": result.get("validity"), "reason": result.get("eligibility")},
        "deadline_at": result.get("gt_deadline_at"),
        "markets": markets,
    }


def event_results(root: Path, manifest: Record) -> Iterator[tuple[Record, Record | None]]:
    """按有序Event流对齐标签，避免把整个批次的取证JSON同时加载进内存。"""
    labels = iter(read_records(root, manifest, "resolutions"))
    head = next(labels, None)
    for event in read_records(root, manifest, "events"):
        if head is not None and head["event_id"] < event["event_id"]:
            raise ValueError("orphaned_or_duplicate_resolution")
        label = None
        if head is not None and head["event_id"] == event["event_id"]:
            label, head = head, next(labels, None)
        yield event, label
    if head is not None:
        raise ValueError("orphaned_or_duplicate_resolution")


def export_jsonl(
    root: Path,
    manifest: Record,
    output: TextIO,
    *,
    extended: bool = False,
    view: Literal["full", "balanced"] = "full",
    include_event_ids: set[str] | None = None,
) -> None:
    """逐条写入大数组；内存不随单题评论或成交总量增长。"""
    if view not in {"full", "balanced"}:
        raise ValueError("unknown_dataset_view")
    if view == "balanced":
        selected = {row["event_id"] for row in snapshot_selection(root, manifest)[0]["members"]}
        include_event_ids = selected if include_event_ids is None else include_event_ids & selected
    metadata = frozen_event_metadata(root, manifest)
    event_ids: set[str] = set()
    source_ids: set[str] = set()
    streams: dict[str, Any] = {}
    heads: dict[str, Any] = {}
    if extended:
        for table in ("comments", "trades"):
            streams[table] = groupby(read_records(root, manifest, table), key=lambda r: r["event_id"])
            heads[table] = next(streams[table], (None, ()))
    for event, label in event_results(root, manifest):
        original = metadata[event["source_hash"]]
        if any(event[key] != original[key] for key in ("event_id", "question", "description")):
            raise ValueError("frozen_event_metadata_mismatch")
        if event["event_id"] in event_ids:
            raise ValueError("duplicate_event_in_export")
        if event["source_id"] in source_ids:
            raise ValueError("duplicate_source_in_export")
        event_ids.add(event["event_id"])
        source_ids.add(event["source_id"])
        if include_event_ids is not None and event["event_id"] not in include_event_ids:
            if extended:
                for table in ("comments", "trades"):
                    key, _ = heads[table]
                    if key is not None and key < event["event_id"]:
                        raise ValueError("orphaned_payload_event")
                    if key == event["event_id"]:
                        heads[table] = next(streams[table], (None, ()))
            continue
        basic = {
            "source_id": event["source_id"],
            "event_id": event["event_id"],
            "batch_id": manifest["batch_id"],
            "slug": original["slug"],
            "time": {
                **event["time"],
                "observed_at": event["observed_at"],
                "frozen_at": manifest["frozen_at"],
            },
            "title": event["question"],
            "markets": [
                {
                    "market_id": m["market_id"],
                    "condition_id": m["condition_id"],
                    "label": m["group_item_title"],
                    "question": m["question"],
                    "version": m["version"],
                    "end_at": m["end_at"],
                    "outcomes": m["outcomes"],
                    **(
                        {
                            "description": m.get("description"),
                            "resolution_source": m.get(
                                "resolution_source", original["resolution_sources"].get(m["condition_id"])
                            ),
                            "neg_risk": m.get("neg_risk"),
                            "game_start_at": m.get("game_start_at"),
                        }
                        if extended
                        else {}
                    ),
                }
                for m in event["markets"]
            ],
            "domain": event["domain"],
            "primary_domain": event_group(event),
            "native_gt": native_groundtruth(event, label),
        }
        if not extended:
            output.write(dumps(basic) + "\n")
            continue
        basic["description"] = event["description"]
        output.write(dumps(basic)[:-1])
        for table, field in (("comments", "comments"), ("trades", "token_trade_history")):
            key, rows = heads[table]
            if key is not None and key < event["event_id"]:
                raise ValueError("orphaned_payload_event")
            if table == "comments":
                if key == event["event_id"]:
                    write_comment_trees(output, rows)
                else:
                    output.write(',"comments":[],"orphan_replies":[]')
            else:
                output.write("," + dumps(field) + ":[")
                first = True
                for row in rows if key == event["event_id"] else ():
                    item = dict(row)
                    for number in ("price_per_share", "shares"):
                        item[number] = Decimal(item[number]) if item[number] is not None else None
                    if not first:
                        output.write(",")
                    output.write(dumps(item))
                    first = False
                output.write("]")
            if key == event["event_id"]:
                heads[table] = next(streams[table], (None, ()))
        output.write("}\n")
    if extended and any(key is not None for key, _ in heads.values()):
        raise ValueError("orphaned_payload_event")


def validate_export(path: Path, expected: Record) -> Record:
    counts = {"events": 0, "comments": 0, "trades": 0}
    if path.stat().st_size == 0:
        if counts != expected:
            raise ValueError("export_count_mismatch")
        return counts
    ids = set()
    event_ids = set()
    comment_node = re.compile(r"(?:comments|orphan_replies)\.item(?:\.replies\.item)*")
    with path.open("rb") as stream:
        for prefix, event, value in ijson.parse(stream, multiple_values=True, use_float=False):
            if prefix == "" and event == "end_map":
                counts["events"] += 1
            elif event == "end_map" and comment_node.fullmatch(prefix):
                counts["comments"] += 1
            elif prefix == "token_trade_history.item" and event == "end_map":
                counts["trades"] += 1
            elif prefix == "source_id" and event == "string":
                if value in ids:
                    raise ValueError("duplicate_source_in_export")
                ids.add(value)
            elif prefix == "event_id" and event == "string":
                if value in event_ids:
                    raise ValueError("duplicate_event_in_export")
                event_ids.add(value)
    if counts != expected or len(ids) != counts["events"] or len(event_ids) != counts["events"]:
        raise ValueError("export_count_mismatch")
    return counts


def snapshot_selection(root: Path, manifest: Record) -> tuple[Record, str]:
    from .benchmark import BALANCED_SELECTION_ALGORITHM, select_events

    selection = manifest.get("selection", {})
    if selection.get("algorithm") == BALANCED_SELECTION_ALGORITHM:
        return selection, "frozen_v1"
    # 历史输入仅做确定性的读取投影，不改写旧名单或追认前瞻采样。
    week = Week.restore(manifest["protocol"]) if "protocol" in manifest else Week.parse(manifest["batch_id"][:10])
    metadata = frozen_event_metadata(root, manifest)
    events = (
        dict(event, series_ids=event.get("series_ids") or metadata[event["source_hash"]]["series_ids"])
        for event in read_records(root, manifest, "events")
    )
    return select_events(events, week), "historical_projection"


def batch_statistics(root: Path, manifest: Record, selection: Record, origin: str) -> Record:
    selected = {row["event_id"] for row in selection["members"]}
    inventory = manifest.get("source_inventory") or {}
    domains: dict[str, Record] = {
        group: {
            "source": (inventory.get("by_primary_domain") or {}).get(group),
            "date_passed": (inventory.get("screening", {}).get(group) or {}).get("date_passed"),
            "native_unresolved_verified": (inventory.get("screening", {}).get(group) or {}).get(
                "native_unresolved_verified"
            ),
            "full": 0,
            "balanced": 0,
            "full_gt": {},
            "balanced_gt": {},
            "full_validity": {},
            "balanced_validity": {},
            "full_reasons": {},
            "balanced_reasons": {},
        }
        for group in (*PRIMARY_DOMAINS, "Unclassified")
    }
    for event, result in event_results(root, manifest):
        group = event_group(event)
        native = native_groundtruth(event, result)
        status = native["status"] if native is not None else "not_observed"
        validity = result.get("validity", "pending") if result else "pending"
        reason = result.get("eligibility", "not_observed") if result else "not_observed"
        for view in ("full", "balanced") if event["event_id"] in selected else ("full",):
            domains[group][view] += 1
            for name, value in ((f"{view}_gt", status), (f"{view}_validity", validity), (f"{view}_reasons", reason)):
                domains[group][name][value] = domains[group][name].get(value, 0) + 1
    return {
        "protocol_version": manifest.get("protocol", {}).get("protocol_version", "legacy"),
        "frozen_at": manifest["frozen_at"],
        "source_coverage": manifest.get("source_coverage"),
        "domains": domains,
        "selection_origin": origin,
        "selection": selection,
        "source_manifest": manifest.get("parent_manifest") or f"batches/{manifest['batch_id']}/V1/manifest.json",
        "source_manifest_sha256": manifest.get("parent_manifest_sha256")
        or hashlib.sha256((dumps(manifest) + "\n").encode()).hexdigest(),
    }


def build_exports(store: Store, index: Record, repo_id: str, revision: str) -> list[Record]:
    """累计视图每Event保留完整单一快照；所有历史输入继续保留在各批次。"""
    snapshots = []
    for _batch_id, entry in sorted(index["batches"].items()):
        path = (store.root / entry["manifest"]).resolve()
        if not path.is_relative_to(store.root):
            raise ValueError("invalid_export_manifest_path")
        local_manifest = read_manifest(path) if path.exists() else None
        local_complete = bool(
            local_manifest
            and digest(local_manifest) == entry["manifest_hash"]
            and all(
                (store.root / f["path"]).is_file()
                for f in local_manifest["files"]
                if f["table"] in {"events", "comments", "trades", "resolutions"}
                or Path(f["path"]).name == "raw-v1.sqlite"
            )
        )
        if local_complete:
            root = store.root
            manifest = local_manifest
        else:
            cached = Path(hf_hub_download(repo_id, entry["manifest"], repo_type="dataset", revision=revision))
            manifest = read_manifest(cached)
            if digest(manifest) != entry["manifest_hash"]:
                raise ValueError("remote_snapshot_mismatch")
            root = cached.parents[len(Path(entry["manifest"]).parts) - 1]
        assert manifest is not None
        for file in manifest["files"]:
            if (
                file["table"] not in {"events", "comments", "trades", "resolutions"}
                and Path(file["path"]).name != "raw-v1.sqlite"
            ):
                continue
            relative = Path(file["path"])
            if relative.is_absolute() or ".." in relative.parts:
                raise ValueError("invalid_export_data_path")
            if local_complete:
                local = (root / relative).resolve()
                if not local.is_relative_to(root.resolve()):
                    raise ValueError("invalid_export_data_path")
            else:
                local = Path(hf_hub_download(repo_id, file["path"], repo_type="dataset", revision=revision))
            if file_hash(local) != file["sha256"]:
                raise ValueError("export_input_checksum_mismatch")
        snapshots.append((root, manifest))
    chosen: dict[str, Record] = {}
    snapshot_event_rows = 0
    for number, (root, manifest) in enumerate(snapshots):
        formal = {row["event_id"] for row in (manifest.get("benchmark_selection") or {}).get("members", [])}
        seen: set[str] = set()
        for event in read_records(root, manifest, "events"):
            identifier = event["event_id"]
            if identifier in seen:
                raise ValueError("duplicate_event_in_export")
            seen.add(identifier)
            rank = (identifier in formal, instant(manifest["frozen_at"]), manifest["batch_id"])
            previous = chosen.get(identifier)
            if previous and previous["rank"][0] and rank[0] and previous["rank"][2] != rank[2]:
                raise ValueError("event_released_in_multiple_batches")
            if previous is None or rank > previous["rank"]:
                chosen[identifier] = {"rank": rank, "snapshot": number}
        expected_events = sum(f["rows"] for f in manifest["files"] if f["table"] == "events")
        if len(seen) != expected_events:
            raise ValueError("export_input_count_mismatch")
        snapshot_event_rows += len(seen)
    included: list[set[str]] = [set() for _ in snapshots]
    for identifier, choice in chosen.items():
        included[choice["snapshot"]].add(identifier)
    selections = [snapshot_selection(root, manifest) for root, manifest in snapshots]
    included_balanced = [
        included[number] & {row["event_id"] for row in selection["members"]}
        for number, (selection, _) in enumerate(selections)
    ]
    view_ids = {"full": included, "balanced": included_balanced}
    selected_counts: dict[str, list[Record]] = {"full": [], "balanced": []}
    for number, (root, manifest) in enumerate(snapshots):
        counts = {view: {"events": len(ids[number]), "comments": 0, "trades": 0} for view, ids in view_ids.items()}
        for table in ("comments", "trades"):
            all_rows = 0
            for row in read_records(root, manifest, table):
                all_rows += 1
                for view, ids in view_ids.items():
                    counts[view][table] += row["event_id"] in ids[number]
            if all_rows != sum(f["rows"] for f in manifest["files"] if f["table"] == table):
                raise ValueError("export_input_count_mismatch")
        for view in view_ids:
            selected_counts[view].append(counts[view])
    destination = store.root / "exports"
    destination.mkdir(parents=True, exist_ok=True)
    exports = []
    for view, extended, name in [
        (view, extended, f"{view}/{name}.jsonl")
        for view in view_ids
        for extended, name in ((False, "basic"), (True, "extended"))
    ]:
        path = destination / name
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_suffix(".partial")
        expected = {"events": 0, "comments": 0, "trades": 0}
        with temporary.open("w", encoding="utf-8") as output:
            for number, (root, manifest) in enumerate(snapshots):
                if not view_ids[view][number]:
                    continue
                export_jsonl(root, manifest, output, extended=extended, include_event_ids=view_ids[view][number])
                expected["events"] += selected_counts[view][number]["events"]
                if extended:
                    for key in ("comments", "trades"):
                        expected[key] += selected_counts[view][number][key]
            output.flush()
            os.fsync(output.fileno())
        validate_export(temporary, expected)
        os.replace(temporary, path)
        exports.append(
            {
                "path": name,
                "local_path": str(path.relative_to(store.root)),
                "sha256": file_hash(path),
                "bytes": path.stat().st_size,
                "counts": expected,
                "view": view,
                "format": "extended" if extended else "basic",
                "coverage": "all_qualified_source_events" if view == "full" else "frozen_domain_capped_subset",
                "snapshot_policy": "formal_release_first_else_latest_complete_snapshot",
                "source_snapshots": len(snapshots),
                "snapshot_event_rows": snapshot_event_rows,
                "schema_version": JSONL_SCHEMA_VERSION,
            }
        )
    statistics: Record = {
        "schema_version": "1.0",
        "grouping": grouping_record(),
        "batches": {
            manifest["batch_id"]: batch_statistics(root, manifest, *selections[number])
            for number, (root, manifest) in enumerate(snapshots)
        },
        "cumulative": {e["view"]: e["counts"] for e in exports if e["format"] == "extended"},
        "snapshot_policy": "formal_release_first_else_latest_complete_snapshot",
    }
    statistics_path = destination / "statistics.json"
    atomic_json(statistics_path, statistics)
    index["statistics"] = {
        "path": "statistics.json",
        "local_path": str(statistics_path.relative_to(store.root)),
        "sha256": file_hash(statistics_path),
        "bytes": statistics_path.stat().st_size,
    }
    for batch_id, report in statistics["batches"].items():
        index["batches"][batch_id]["view_summary"] = {
            "selection_origin": report["selection_origin"],
            "domains": report["domains"],
            "balanced_events": report["selection"]["selected_count"],
            "selection_sha256": digest(report["selection"]),
        }
    index["export_implementation_hash"] = implementation_provenance()["implementation_hash"]
    return exports


def publication_index(previous: Record, manifest: Record, path: str) -> Record:
    batches = dict(previous.get("batches", {}))
    old = batches.get(manifest["batch_id"])
    if old and (
        instant(old["created_at"]) > instant(manifest["created_at"])
        or old["stage"] == "V3"
        and manifest["stage"] == "V1"
    ):
        raise ValueError("publication_regression")
    input_hash = (
        manifest.get("parent_manifest_sha256")
        if manifest["stage"] == "V3"
        else hashlib.sha256((dumps(manifest) + "\n").encode()).hexdigest()
    )
    if old and old.get("input_manifest_sha256") != input_hash:
        raise ValueError("frozen_input_conflict")
    batches[manifest["batch_id"]] = {
        "manifest": path,
        "manifest_hash": digest(manifest),
        "stage": manifest["stage"],
        "created_at": manifest["created_at"],
        "kind": manifest.get("kind"),
        "schema_version": manifest.get("schema_version", "1.1"),
        "protocol_version": manifest.get("protocol", {}).get("protocol_version", "legacy"),
        "source_coverage": manifest.get("source_coverage"),
        "counts": manifest.get("counts", {}),
        "selection_summary": {
            key: value
            for key, value in manifest.get("selection", {}).items()
            if key in {"selected_count", "available_groups", "selected_groups"}
        },
        "files": manifest.get("files", []),
        "input_manifest_sha256": input_hash,
    }
    return {"schema_version": "1.0", "batches": batches}


def dataset_card(index: Record, language: Literal["zh", "en"] = "zh") -> str:
    """生成共享数据配置的双语卡片；正文随包分发。"""
    lines = ["---", "pretty_name: ForeseeBench", "configs:"]
    has_views = bool(index.get("exports") and any(e.get("view") == "full" for e in index["exports"]))
    if has_views:
        for name in ("basic", "extended"):
            lines.append(f"- config_name: {name}")
            if name == "basic":
                lines.append("  default: true")
            lines.append("  data_files:")
            for view in ("full", "balanced"):
                paths = [e["path"] for e in index["exports"] if e.get("view") == view and e.get("format") == name]
                lines.extend([f"  - split: {view}", "    path:"])
                lines.extend("    - " + dumps(path) for path in paths)
    batches = list(index["batches"].values())
    versions = sorted({str(batch.get("schema_version") or "legacy") for batch in batches})
    newest = PARQUET_SCHEMA_VERSION if PARQUET_SCHEMA_VERSION in versions else (versions[-1] if versions else "legacy")
    older = [version for version in versions if version != newest]
    groups = [("", [batch for batch in batches if str(batch.get("schema_version") or "legacy") == newest])]
    groups.extend(
        (
            "_legacy" if len(older) == 1 else "_legacy_" + version.replace(".", "_"),
            [batch for batch in batches if str(batch.get("schema_version") or "legacy") == version],
        )
        for version in older
    )
    for suffix, selected in groups:
        for table in ("events", "comments", "trades", "resolutions"):
            paths = sorted({entry["path"] for batch in selected for entry in batch["files"] if entry["table"] == table})
            lines.extend(
                [f"- config_name: {table}{suffix}"]
                + (["  default: true"] if table == "events" and not suffix and not has_views else [])
            )
            lines.extend(["  data_files:", "  - split: data", "    path:"])
            lines.extend("    - " + dumps(path) for path in paths)
    if has_views:
        # 官方Json特性保留嵌套对象，避免可空支付数组和任意深度回复树的Arrow类型推断问题。
        lines.append("dataset_info:")
        for name in ("basic", "extended"):
            lines.extend([f"- config_name: {name}", "  features:"])
            strings = ["source_id", "event_id", "batch_id", "slug", "title", "primary_domain"]
            objects = ["domain", "time", "markets", "native_gt"]
            if name == "extended":
                strings.append("description")
                objects.extend(["comments", "orphan_replies", "token_trade_history"])
            for field in strings:
                lines.extend([f"  - name: {field}", "    dtype: string"])
            for field in objects:
                lines.extend([f"  - name: {field}", "    dtype: json"])
    lines.extend(["---", ""])
    name = CARD_FILES[language]
    body = resource_files("foreseebench").joinpath("cards", name).read_text(encoding="utf-8")
    summary = (
        [
            "| 批次 | 时间协议 | 原始来源Event | 完整集Event | 评论 | 成交 | 采样Event |",
            "| --- | --- | ---: | ---: | ---: | ---: | ---: |",
        ]
        if language == "zh"
        else [
            "| Batch | Timing protocol | Raw source Events | Full Events | Comments | Trades | Balanced Events |",
            "| --- | --- | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for identifier, batch in sorted(index["batches"].items()):
        counts = batch.get("counts") or {
            t: sum(f["rows"] for f in batch["files"] if f["table"] == t) for t in ("events", "comments", "trades")
        }
        selected = batch.get("view_summary", {}).get("balanced_events", "—")
        coverage = batch.get("source_coverage") or {}
        raw_count = coverage.get("retained_events", coverage.get("events"))
        raw_text = f"{raw_count:,}" if raw_count is not None else "—"
        summary.append(
            f"| {identifier} | {batch.get('protocol_version', 'legacy')} | {raw_text} | {counts['events']:,} | "
            f"{counts['comments']:,} | {counts['trades']:,} | {selected} |"
        )
    body = body.replace("{{BATCH_SUMMARY}}", "\n".join(summary))
    domains = []
    if index["batches"]:
        latest = index["batches"][sorted(index["batches"])[-1]]
        distribution = latest.get("view_summary", {}).get("domains", {})
        if distribution:
            domains = [
                "| 主领域 | 完整集 | 采样集 | 完整集原生GT齐备 | 采样集原生GT齐备 |"
                if language == "zh"
                else "| Primary domain | Full | Balanced | Full native GT complete | Balanced native GT complete |",
                "| --- | ---: | ---: | ---: | ---: |",
            ]
            domains.extend(
                f"| {group} | {row['full']} | {row['balanced']} | {row['full_gt'].get('complete', 0)} | "
                f"{row['balanced_gt'].get('complete', 0)} |"
                for group, row in distribution.items()
            )
    body = body.replace("{{DOMAIN_SUMMARY}}", "\n".join(domains))
    return "\n".join(lines) + body


def publish(store: Store, manifest_path: Path, repo_id: str, revision: str = "main") -> Record:
    """从不可变快照重试发布；提交后断连通过远端manifest恢复回执。"""
    for attempt in range(5):
        try:
            return _publish_once(store, manifest_path, repo_id, revision)
        except Exception as error:
            response = getattr(error, "response", None)
            retryable = isinstance(error, httpx.TransportError) or (
                response is not None and (response.status_code in {409, 429} or response.status_code >= 500)
            )
            if not retryable or attempt == 4:
                raise
            time.sleep(min(30, 2**attempt))
    raise RuntimeError("publication_retry_exhausted")


def verify_published_views(repo_id: str, revision: str, index: Record) -> dict[str, str]:
    verified = {}
    for entry in [*index["exports"], index["statistics"]]:
        path = Path(hf_hub_download(repo_id, entry["path"], repo_type="dataset", revision=revision))
        if file_hash(path) != entry["sha256"]:
            raise ValueError(f"remote_view_readback_failed:{entry['path']}")
        verified[entry["path"]] = entry["sha256"]
    return verified


def _publish_once(store: Store, manifest_path: Path, repo_id: str, revision: str = "main") -> Record:
    manifest_path = manifest_path.resolve()
    if not manifest_path.is_relative_to(store.root):
        raise ValueError("manifest_outside_store")
    manifest = read_manifest(manifest_path)
    store.verify(manifest)
    if manifest["kind"] == "qualification" and revision == "main":
        raise ValueError("qualification_cannot_publish_to_main")
    if manifest["kind"] != "qualification" and not manifest.get("construction_complete"):
        raise ValueError("construction_incomplete")
    api = HfApi()
    info = api.repo_info(repo_id, repo_type="dataset", revision=revision)
    if info.sha is None:
        raise ValueError("remote_revision_missing")
    previous: Record = {"batches": {}}
    if api.file_exists(repo_id, "manifest.json", repo_type="dataset", revision=info.sha):
        previous = read_manifest(
            Path(hf_hub_download(repo_id, "manifest.json", repo_type="dataset", revision=info.sha))
        )
    relative_manifest = str(manifest_path.relative_to(store.root))
    if (
        previous.get("batches", {}).get(manifest["batch_id"], {}).get("manifest_hash") == digest(manifest)
        and {e["path"] for e in previous.get("exports", [])} == EXPORT_PATHS
        and all(e.get("schema_version") == JSONL_SCHEMA_VERSION for e in previous["exports"])
        and previous.get("statistics")
        and previous.get("export_implementation_hash") == implementation_provenance()["implementation_hash"]
    ):
        cards = {name: dataset_card(previous, language).encode() for language, name in CARD_FILES.items()}
        card_updates = []
        for name, content in cards.items():
            if api.file_exists(repo_id, name, repo_type="dataset", revision=info.sha):
                current = Path(hf_hub_download(repo_id, name, repo_type="dataset", revision=info.sha))
                if current.read_bytes() == content:
                    continue
            card_updates.append(CommitOperationAdd(path_in_repo=name, path_or_fileobj=content))
        head = info.sha
        if card_updates:
            commit = api.create_commit(
                repo_id,
                repo_type="dataset",
                revision=revision,
                parent_commit=head,
                operations=card_updates,
                commit_message="docs: update Chinese and English dataset cards",
            )
            head = commit.oid
            for name, content in cards.items():
                remote_card = Path(hf_hub_download(repo_id, name, repo_type="dataset", revision=head))
                if remote_card.read_bytes() != content:
                    raise ValueError("remote_card_readback_failed")
        receipt = {
            "repo_id": repo_id,
            "revision": head,
            "manifest_hash": digest(manifest),
            "branch": revision,
            "unchanged": not card_updates,
            "verified_artifacts": verify_published_views(repo_id, head, previous),
        }
        atomic_json(manifest_path.parent / "publication.json", receipt)
        return receipt
    index = publication_index(previous, manifest, relative_manifest)
    index["exports"] = build_exports(store, index, repo_id, info.sha)
    known = {f["path"]: f["sha256"] for b in previous.get("batches", {}).values() for f in b.get("files", [])}
    manifests = {relative_manifest: manifest}
    node = manifest
    while parent := node.get("parent_manifest"):
        parent_path = (store.root / parent).resolve()
        if parent in manifests or not parent_path.is_relative_to(store.root):
            raise ValueError("invalid_parent_manifest_chain")
        if file_hash(parent_path) != node.get("parent_manifest_sha256"):
            raise ValueError("parent_manifest_checksum_mismatch")
        node = read_manifest(parent_path)
        store.verify(node)
        manifests[parent] = node
    files: dict[str, Record] = {}
    for node in manifests.values():
        for entry in node["files"]:
            if entry["path"] in files and files[entry["path"]]["sha256"] != entry["sha256"]:
                raise ValueError("conflicting_snapshot_file")
            files[entry["path"]] = entry
    operations: list[CommitOperationAdd | CommitOperationDelete] = []
    for entry in files.values():
        if entry["path"] in known and known[entry["path"]] != entry["sha256"]:
            raise ValueError("immutable_remote_file_changed")
        if known.get(entry["path"]) == entry["sha256"]:
            continue
        operation = CommitOperationAdd(path_in_repo=entry["path"], path_or_fileobj=store.root / entry["path"])
        api.preupload_lfs_files(repo_id, additions=[operation], repo_type="dataset", revision=revision)
        operations.append(operation)
    operations.extend(CommitOperationAdd(path_in_repo=path, path_or_fileobj=store.root / path) for path in manifests)
    for entry in [*index["exports"], index["statistics"]]:
        operation = CommitOperationAdd(path_in_repo=entry["path"], path_or_fileobj=store.root / entry["local_path"])
        api.preupload_lfs_files(repo_id, additions=[operation], repo_type="dataset", revision=revision)
        operations.append(operation)
    for entry in previous.get("exports", []):
        if entry["path"] in {"basic.jsonl", "extended.jsonl"} and api.file_exists(
            repo_id, entry["path"], repo_type="dataset", revision=info.sha
        ):
            operations.append(CommitOperationDelete(path_in_repo=entry["path"]))
    operations.extend(
        [
            CommitOperationAdd(path_in_repo="manifest.json", path_or_fileobj=(dumps(index) + "\n").encode()),
        ]
    )
    operations.extend(
        CommitOperationAdd(path_in_repo=name, path_or_fileobj=dataset_card(index, language).encode())
        for language, name in CARD_FILES.items()
    )
    commit = api.create_commit(
        repo_id,
        repo_type="dataset",
        revision=revision,
        parent_commit=info.sha,
        operations=operations,
        commit_message=f"{manifest['batch_id']} {manifest['stage']} verified snapshot",
    )
    remote = Path(hf_hub_download(repo_id, "manifest.json", repo_type="dataset", revision=commit.oid))
    if read_manifest(remote) != index:
        raise ValueError("remote_manifest_readback_failed")
    receipt = {
        "repo_id": repo_id,
        "revision": commit.oid,
        "manifest_hash": digest(manifest),
        "branch": revision,
        "unchanged": False,
        "verified_artifacts": verify_published_views(repo_id, commit.oid, index),
    }
    atomic_json(manifest_path.parent / "publication.json", receipt)
    return receipt
