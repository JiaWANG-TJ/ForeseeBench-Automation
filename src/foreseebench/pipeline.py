"""无 LLM 的采集、冻结前核验和结算刷新。"""

from __future__ import annotations

import sqlite3
import tempfile
from collections.abc import Callable, Iterator
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from contextlib import contextmanager
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import httpx
import simplejson

from .contract import (
    Record,
    Week,
    admit_event,
    digest,
    dumps,
    has_known_answer,
    instant,
    market_end_date,
    require_unresolved,
    settle_event,
    source_binding,
    stamp,
)
from .source import EvidenceError, Page, Polymarket, WorkYield, evidence_observations
from .store import Store, atomic_json, file_hash, read_manifest, read_records


def source_metadata(raw: Record, receipt: Record, week: Week) -> Record:
    """即使来源不合格，仍保存其领域和原件定位信息。"""
    tags = raw.get("tags")
    try:
        if not isinstance(raw.get("markets"), list) or not raw["markets"]:
            raise ValueError("markets_missing")
        for market in raw["markets"]:
            market_end_date(market, week)
        date_status = "passed"
    except (ValueError, TypeError, KeyError) as error:
        date_status = str(error)
    return {
        "event_id": str(raw["id"]),
        "domain": [t.get("label") if isinstance(t, dict) else None for t in tags] if isinstance(tags, list) else None,
        "tag_ids": [str(t["id"]) for t in tags if isinstance(t, dict) and t.get("id") is not None]
        if isinstance(tags, list)
        else None,
        "source_hash": digest(raw),
        "source_receipt_id": receipt["request_id"],
        "source_body_sha256": receipt["body_sha256"],
        "observed_at": stamp(receipt["observed_at"]),
        "date_window_status": date_status,
    }


def fact_content(value: object) -> object:
    if isinstance(value, dict):
        return {
            k: fact_content(v)
            for k, v in value.items()
            if k
            not in {
                "observed_at",
                "receipt_ids",
                "lifecycle_observed_at",
                "lifecycle_receipt_ids",
                "token_observations",
                "last_unresolved_at",
                "last_unresolved_verified_at",
            }
        }
    if isinstance(value, list):
        return [fact_content(v) for v in value]
    return value


def comment_rows(page: Page, event_id: str, cutoff: datetime | None, pm: Polymarket) -> Iterator[Record]:
    for row in page.rows:
        if not row.get("id"):
            raise ValueError("comment_id_missing")
        if row.get("parentEntityType") not in (None, "Event") or (
            row.get("parentEntityID") is not None and str(row["parentEntityID"]) != event_id
        ):
            raise ValueError("comment_event_mismatch")
        created, updated = row.get("createdAt"), row.get("updatedAt")
        if cutoff and any(value and instant(value) >= cutoff for value in (created, updated)):
            continue
        profile = dict(row.get("profile") or {})
        user_id = row.get("user_id")
        profile_observed_at = None
        if address := row.get("userAddress"):
            public, receipt = pm.public_profile(address, deadline=cutoff)
            profile_observed_at = receipt["observed_at"]
            if cutoff and instant(profile_observed_at) >= cutoff:
                raise TimeoutError("freeze_deadline")
            users = public.get("users") or []
            if not isinstance(users, list) or any(not isinstance(user, dict) for user in users):
                raise ValueError("invalid_profile_users")
            identifiers = {str(user["id"]) for user in users if user.get("id")}
            if len(identifiers) == 1:
                official_id = next(iter(identifiers))
                if user_id is not None and str(user_id) != official_id:
                    raise ValueError("conflicting_comment_user_id")
                user_id = official_id
            for field in ("name", "bio"):
                if profile.get(field) is None:
                    profile[field] = public.get(field)
        yield {
            "event_id": event_id,
            "comment_id": str(row["id"]),
            "parent_comment_id": str(row["parentCommentID"]) if row.get("parentCommentID") else None,
            "user_id": str(user_id) if user_id is not None else None,
            "userAddress": row.get("userAddress"),
            "body": row.get("body"),
            "createdAt": stamp(created) if created else None,
            "updatedAt": stamp(updated) if updated else None,
            "observed_at": page.receipt["observed_at"],
            "profile_observed_at": profile_observed_at,
            "profile": {"name": profile.get("name"), "bio": profile.get("bio")},
        }


@contextmanager
def prepared_comment_rows(
    page: Page, event_id: str, cutoff: datetime | None, pm: Polymarket
) -> Iterator[Iterator[Record]]:
    """先在存储事务外完成网络补全，再从有界磁盘缓存交付整页。"""
    with tempfile.TemporaryDirectory(prefix="foreseebench-comment-page-") as temporary:
        db = sqlite3.connect(Path(temporary) / "page.sqlite")
        try:
            db.execute("CREATE TABLE comments (id TEXT PRIMARY KEY,payload TEXT)")
            for row in comment_rows(page, event_id, cutoff, pm):
                payload = dumps(row)
                inserted = db.execute("INSERT OR IGNORE INTO comments VALUES (?,?)", [row["comment_id"], payload])
                if not inserted.rowcount:
                    previous = db.execute("SELECT payload FROM comments WHERE id=?", [row["comment_id"]]).fetchone()
                    if previous[0] != payload:
                        raise ValueError("conflicting_comment_duplicate")
            yield (
                simplejson.loads(row[0], use_decimal=True)
                for row in db.execute("SELECT payload FROM comments ORDER BY rowid")
            )
        finally:
            db.close()


def trade_rows(page: Page, source: Record, cutoff: datetime | None) -> Iterator[Record]:
    mapping = {m["condition_id"]: m for m in source["markets"]}
    for i, row in enumerate(page.rows):
        condition = row.get("condition_id")
        if condition not in mapping:
            raise ValueError("trade_condition_not_in_frozen_event")
        timestamp = Decimal(str(row["timestamp"]))
        if not timestamp.is_finite() or timestamp != timestamp.to_integral_value():
            raise ValueError("invalid_trade_timestamp")
        traded_at = datetime.fromtimestamp(int(timestamp), UTC)
        if cutoff and traded_at >= cutoff:
            continue
        price, size = Decimal(str(row["price"])), Decimal(str(row["size"]))
        if not price.is_finite() or not size.is_finite() or not 0 <= price <= 1 or size <= 0:
            raise ValueError("invalid_trade_amount")
        if row.get("side") not in {"BUY", "SELL"} or not row.get("token_id") or not row.get("transaction_hash"):
            raise ValueError("trade_identity_missing")
        yield {
            "event_id": source["event_id"],
            "row_id": digest([page.receipt["query_hash"], i]),
            "transaction_hash": row["transaction_hash"],
            "condition_id": condition,
            "token_id": row["token_id"],
            "option_label": mapping[condition]["group_item_title"],
            "outcome": row.get("outcome"),
            "side": row["side"],
            "price_per_share": str(price),
            "shares": str(size),
            "traded_at": stamp(traded_at),
            "observed_at": page.receipt["observed_at"],
        }


class Pipeline:
    def __init__(
        self,
        store: Store,
        pm: Polymarket,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ):
        self.store, self.pm, self.clock = store, pm, clock
        self.week = Week.current(clock())

    def collect(
        self,
        week_id: str | Week,
        kind: str,
        config_hash: str,
        *,
        max_events: int | None = None,
        progress: Callable[[Record], None] | None = None,
        refresh_completed: bool = True,
    ) -> Record:
        self.week = week_id if isinstance(week_id, Week) else self.store.week(week_id)
        week = self.week
        if week.policy.read_only:
            raise ValueError("legacy_batch_read_only")
        if max_events is not None and kind != "qualification":
            raise ValueError("formal_collection_cannot_be_sampled")
        if kind == "formal" and not week.collection_start <= self.clock() < week.freeze_at:
            raise ValueError("outside_formal_collection_window")
        self.store.begin(week, kind, config_hash)
        self.store.mutable(week.id)
        deadline = week.freeze_at if kind in {"formal", "bootstrap"} else None
        yielded = False
        if (kind == "formal" and refresh_completed) or not self.store.batch(week.id)["scan_complete"]:
            try:
                self._discover_source(week, deadline, kind == "formal" and refresh_completed, progress)
            except WorkYield:
                return self.store.batch(week.id) | {
                    "construction_complete": False,
                    "yielded": True,
                    "event_states": self.store.event_counts(week.id),
                    "source_coverage": self.store.source_coverage(week.id),
                }
            except Exception as error:
                self.store.failure(week.id, None, "discovery", self.clock(), f"{type(error).__name__}:{error}")
                raise
        refresh_all = kind == "formal" and refresh_completed
        statuses = (
            ("candidate", "collecting", "failed", "collected") if refresh_all else ("candidate", "collecting", "failed")
        )
        pending = self.store.iter_events(week.id, statuses)
        states = self.store.event_counts(week.id)
        total = sum(states.get(status, 0) for status in statuses)
        collected = states.get("collected", 0) if not refresh_all else 0
        if max_events is not None:
            for entry in pending:
                if self.pm.work_expired:
                    yielded = True
                    break
                if collected >= max_events:
                    break
                collected += self._collect_event(entry, week, deadline, kind)
                if progress:
                    progress({"phase": "collect", "event_id": entry["event_id"], "collected": collected})
        else:
            with ThreadPoolExecutor(max_workers=self.pm.workers) as pool:
                futures = {}
                processed = 0
                for _ in range(self.pm.workers * 2):
                    next_entry = next(pending, None)
                    if next_entry is None:
                        break
                    futures[pool.submit(self._collect_event, next_entry, week, deadline, kind)] = next_entry["event_id"]
                while futures:
                    ready, _ = wait(futures, return_when=FIRST_COMPLETED)
                    for future in ready:
                        event_id = futures.pop(future)
                        collected += future.result()
                        processed += 1
                        if progress:
                            progress(
                                {
                                    "phase": "collect",
                                    "event_id": event_id,
                                    "collected": collected,
                                    "processed": processed,
                                    "total": total,
                                    **self.store.event_status(week.id, event_id),
                                }
                            )
                        next_entry = next(pending, None) if not self.pm.work_expired else None
                        if next_entry is not None:
                            futures[pool.submit(self._collect_event, next_entry, week, deadline, kind)] = next_entry[
                                "event_id"
                            ]
        states = self.store.event_counts(week.id)
        return self.store.batch(week.id) | {
            "collected": states.get("collected", 0),
            "event_states": states,
            "source_coverage": self.store.source_coverage(week.id),
            "construction_complete": self.store.batch(week.id)["scan_complete"]
            and not any(states.get(s, 0) for s in ("candidate", "collecting", "failed")),
            "yielded": yielded or self.pm.work_expired,
        }

    def _discover_source(
        self, week: Week, deadline: datetime | None, refresh: bool, progress: Callable[[Record], None] | None
    ) -> None:
        """完整归档后筛选；枚举失败与Event排除分别保留。"""
        self.store.discovery_started(week.id)
        protected = self.store.known_answer_ids(week.id)
        coverage = self.pm.discovery_status()
        source_pass = coverage.get("boundary_receipt", {}).get("request_id")
        if refresh and coverage.get("complete"):
            source_pass = None
        replay = (
            self.store.source_replay(week.id, source_pass)
            if source_pass is not None
            else {"request_id": None, "source_count": 0}
        )
        source_count = replay["source_count"]
        for page in self.pm.discover(
            week, deadline=deadline, refresh=refresh, after_request_id=replay["request_id"]
        ):
            if page.rows:
                self.pm.require_budget()
            if source_pass is None or not coverage.get("complete"):
                coverage = self.pm.discovery_status()
            if source_pass is None:
                source_pass = coverage["boundary_receipt"]["request_id"]
            entries: list[Record] = []
            seen_ids = []
            for raw in page.rows:
                reason: str | None
                event_id = str(raw["id"])
                seen_ids.append(event_id)
                source_count += 1
                if event_id in protected:
                    continue
                try:
                    source = admit_event(raw, week, instant(page.receipt["observed_at"]))
                except (ValueError, TypeError, KeyError) as error:
                    source = source_metadata(raw, page.receipt, week)
                    status, reason = "rejected", str(error)
                else:
                    previous = self.store.event_status(week.id, event_id)
                    if previous["status"] not in {"missing", "rejected"}:
                        continue
                    if previous["status"] == "rejected":
                        self.store.reset_event(week.id, event_id)
                    status, reason = "candidate", None
                    source.update(source_metadata(raw, page.receipt, week))
                entries.append({"event_id": event_id, "payload": source, "status": status, "reason": reason})
            if entries or seen_ids:
                self.store.source_page(
                    week.id,
                    entries,
                    seen_ids=seen_ids,
                    source_pass=source_pass,
                    receipt_id=page.receipt["request_id"],
                )
            if progress:
                progress({"phase": "discover", "source_events": source_count, **coverage})
        coverage = self.pm.discovery_status()
        if source_count != coverage["events"]:
            raise ValueError("source_catalog_count_mismatch")
        self.store.discovery_complete(week.id, coverage, source_pass=source_pass)

    def _collect_event(self, entry: Record, week: Week, deadline: datetime | None, kind: str) -> bool:
        event_id = entry["event_id"]
        if str(entry.get("reason", "")).endswith("market_resolved_during_collection"):
            return False
        if deadline and self.clock() >= deadline:
            return False
        source = entry["payload"]
        try:
            self.pm.require_budget()
            raw, receipt = self.pm.event(event_id, deadline=deadline)
            try:
                current = admit_event(raw, week, instant(receipt["observed_at"]))
            except (ValueError, TypeError, KeyError) as error:
                self.store.event(week.id, event_id, source_metadata(raw, receipt, week), "rejected", str(error))
                return False
            current.update(source_metadata(raw, receipt, week))
            if (kind == "formal" and entry["status"] == "collected") or (
                source and source_binding(source) != source_binding(current)
            ):
                self.store.reset_event(week.id, event_id)
            source = current
            resolutions, tokens, _ = self.pm.evidence(source, deadline=deadline)
            require_unresolved(source, resolutions, tokens)
            self.store.event(week.id, event_id, source, "collecting")
            _, complete = self.store.position(week.id, event_id, "comments")
            if not complete:
                self.store.reset_event(week.id, event_id, "comments")
                for page in self.pm.comments(event_id, deadline=deadline):
                    with prepared_comment_rows(page, event_id, deadline, self.pm) as records:
                        self.store.page(
                            week.id,
                            event_id,
                            "comments",
                            page.receipt["query_hash"],
                            records,
                            page.next_cursor,
                            False,
                        )
                attempted: set[str] = set()
                while missing := set(self.store.missing_comment_parents(week.id, event_id)) - attempted:
                    for parent_id in sorted(missing):
                        attempted.add(parent_id)
                        try:
                            with self.pm.comment_thread(parent_id, deadline=deadline) as page:
                                if not any(str(row.get("id")) == parent_id for row in page.rows):
                                    raise ValueError("comment_parent_not_returned")
                                with prepared_comment_rows(page, event_id, deadline, self.pm) as records:
                                    self.store.page(
                                        week.id,
                                        event_id,
                                        "comments",
                                        page.receipt["query_hash"],
                                        records,
                                        None,
                                        False,
                                    )
                                if parent_id in self.store.missing_comment_parents(week.id, event_id):
                                    raise ValueError("comment_parent_unavailable_before_cutoff")
                        except httpx.HTTPStatusError as error:
                            if error.response.status_code != 404:
                                raise
                self.store.finish_comments(week.id, event_id)
            position, complete = self.store.position(week.id, event_id, "trades")
            if not complete:
                for page in self.pm.trades(event_id, cursor=position, deadline=deadline):
                    self.store.page(
                        week.id,
                        event_id,
                        "trades",
                        page.receipt["query_hash"],
                        trade_rows(page, source, deadline),
                        page.next_cursor,
                        page.next_cursor is None,
                    )
            resolutions, tokens, _ = self.pm.evidence(source, deadline=deadline)
            require_unresolved(source, resolutions, tokens)
            final_raw, final_receipt = self.pm.event(event_id, deadline=deadline)
            try:
                final_source = admit_event(final_raw, week, instant(final_receipt["observed_at"]))
            except (ValueError, TypeError, KeyError) as error:
                self.store.event(
                    week.id, event_id, source_metadata(final_raw, final_receipt, week), "rejected", str(error)
                )
                return False
            final_source.update(source_metadata(final_raw, final_receipt, week))
            if source_binding(source) != source_binding(final_source):
                self.store.reset_event(week.id, event_id)
                raise ValueError("source_changed_during_collection")
            final_source["native_unresolved_verified"] = True
            self.store.event(week.id, event_id, final_source, "collected")
            return True
        except WorkYield:
            return False
        except Exception as error:
            reason = f"{type(error).__name__}:{error}"
            known = isinstance(error, EvidenceError) and has_known_answer(source, *error.partial[:2])
            if isinstance(error, EvidenceError) and isinstance(error.cause, WorkYield) and not known:
                return False
            self.store.failure(week.id, event_id, "V1", self.clock(), reason)
            if known:
                reason = "market_resolved_during_collection"
            status = "rejected" if known or str(error) == "market_resolved_during_collection" else "failed"
            self.store.event(week.id, event_id, source, status, reason)
            return False

    def refresh(
        self, week_id: str, *, progress: Callable[[Record], None] | None = None, resume: bool = False
    ) -> Record:
        week = self.store.week(week_id)
        if week.policy.read_only:
            raise ValueError("legacy_batch_read_only")
        base = read_manifest(self.store.root / "batches" / week_id / "V1" / "manifest.json")
        self.store.verify(base)
        release_path = self.store.root / "batches" / week_id / "benchmark" / "release.json"
        if release_path.exists():
            week = Week.restore(read_manifest(release_path)["protocol"])
        updated = 0
        failed: list[Record] = []
        cursor_path = self.store.root / "batches" / week_id / "refresh-cursor.json"
        cursor = read_manifest(cursor_path) if resume and cursor_path.exists() else {}
        after = cursor.get("after_event_id")
        ready = after is None

        def advance(event_id: str | None) -> None:
            if resume:
                atomic_json(cursor_path, {"after_event_id": event_id})

        for source in read_records(self.store.root, base, "events"):
            if not ready:
                ready = source["event_id"] == after
                continue
            if getattr(self.pm, "work_expired", False):
                return {"batch_id": week_id, "updated": updated, "failures": failed, "yielded": True}
            previous = self.store.latest_resolution(week_id, source["event_id"])
            if previous and previous["status"] == "verified" and previous.get("validity") in {"valid", "invalid"}:
                advance(source["event_id"])
                continue
            observed = self.clock()
            fetch_error: Exception | None = None
            resolutions: list[Record] = []
            tokens: list[Record] = []
            receipts: list[Record] = []
            try:
                if observed < week.evidence_cutoff:
                    resolutions, tokens, receipts = self.pm.evidence(source, deadline=week.evidence_cutoff)
            except Exception as error:
                fetch_error = error
                if isinstance(error, EvidenceError):
                    resolutions, tokens, receipts = error.partial
            try:
                observed = (
                    self.clock() if fetch_error or not receipts else max(instant(r["observed_at"]) for r in receipts)
                )
                observations = evidence_observations(source, resolutions, tokens, receipts, observed)
                result = settle_event(
                    source, resolutions, tokens, week, observed, previous=previous, observations=observations
                )
                result["receipt_ids"] = [r["request_id"] for r in receipts]
            except Exception as error:
                fetch_error = error
                observed = self.clock()
                result = settle_event(source, [], [], week, observed, previous=previous)
            yielded = isinstance(fetch_error, WorkYield) or (
                isinstance(fetch_error, EvidenceError) and isinstance(fetch_error.cause, WorkYield)
            )
            if fetch_error is not None and not yielded:
                message = f"{type(fetch_error).__name__}:{fetch_error}"
                self.store.failure(week_id, source["event_id"], "V3", self.clock(), message)
                failed.append({"event_id": source["event_id"], "error": message})
            if "protocol" not in base:
                result.update(validity="not_evaluated", eligibility="legacy_source_only", time=source["time"])
            changed = not previous or (
                instant(previous["observed_at"]) <= observed and fact_content(previous) != fact_content(result)
            )
            if changed:
                self.store.add_resolution(week_id, result)
                updated += 1
                if progress:
                    progress({"phase": "resolve", "updated": updated, "failed": len(failed)})
            if yielded:
                return {"batch_id": week_id, "updated": updated, "failures": failed, "yielded": True}
            advance(source["event_id"])
        if not ready:
            raise ValueError("refresh_cursor_not_in_frozen_snapshot")
        advance(None)
        return {"batch_id": week_id, "updated": updated, "failures": failed, "yielded": False}


def stage_task(store: Store, stage: str, manifest_path: str, *, v2_task_id: str | None = None) -> Record:
    if stage not in {"V2", "V4"}:
        raise ValueError("invalid_extension_stage")
    path = (store.root / manifest_path).resolve()
    if not path.is_relative_to(store.root):
        raise ValueError("input_manifest_outside_store")
    manifest = read_manifest(path)
    if manifest["stage"] != {"V2": "V1", "V4": "V3"}[stage]:
        raise ValueError("stage_input_mismatch")
    store.verify(manifest)
    upstream: Record = {}
    if stage == "V2" and v2_task_id:
        raise ValueError("v2_cannot_depend_on_v2")
    if stage == "V4":
        upstream["V2"] = None
        if v2_task_id:
            row = store.db.execute("SELECT stage,payload FROM tasks WHERE task_id=?", [v2_task_id]).fetchone()
            if row is None or row[0] != "V2":
                raise ValueError("v2_task_missing")
            previous = simplejson.loads(row[1])
            if previous["input_manifest"] != manifest["parent_manifest"] or not previous.get("output"):
                raise ValueError("v2_output_missing_or_wrong_batch")
            output = previous["output"]
            if file_hash(Path(output["manifest"])) != output["sha256"]:
                raise ValueError("v2_output_changed")
            upstream["V2"] = {"task_id": v2_task_id, **output}
    input_hash = digest(manifest) if stage == "V2" else digest([manifest, upstream])
    task_id = digest([stage, input_hash])
    task = {
        "task_id": task_id,
        "stage": stage,
        "input_manifest": manifest_path,
        "input_hash": input_hash,
        "handler_version": None,
        "status": "not_configured",
        "output": None,
        "attempts": 0,
        "upstream_artifacts": upstream,
    }
    store.db.execute(
        "INSERT INTO tasks VALUES (?,?,?,?,?) ON CONFLICT DO NOTHING",
        [task_id, stage, input_hash, task["status"], dumps(task)],
    )
    saved = store.db.execute("SELECT payload FROM tasks WHERE task_id=?", [task_id]).fetchone()
    assert saved is not None
    return simplejson.loads(saved[0])
