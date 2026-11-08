"""按官方契约流式读取；完整响应集中归档，分页期间保持固定过滤参数。"""

from __future__ import annotations

import hashlib
import shutil
import sqlite3
import tempfile
import threading
import time
import zlib
from collections import OrderedDict
from collections.abc import Callable, Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing, contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from queue import Empty, Full, Queue
from typing import Any
from urllib.parse import parse_qs, urlsplit
from uuid import uuid4

import httpx
import ijson
import simplejson

from .contract import Record, Week, digest, dumps, instant, stamp

ARCHIVE_SCHEMA = """
    CREATE TABLE IF NOT EXISTS bodies (
        sha256 TEXT PRIMARY KEY, raw_size INTEGER NOT NULL, content BLOB NOT NULL);
    CREATE TABLE IF NOT EXISTS receipts (
        request_id TEXT PRIMARY KEY, payload TEXT NOT NULL);
"""


class EvidenceError(RuntimeError):
    """部分请求失败时，仍交付此前完整取得的原生证据及回执。"""

    def __init__(self, cause: Exception, resolutions: list[Record], tokens: list[Record], receipts: list[Record]):
        super().__init__(f"{type(cause).__name__}:{cause}")
        self.partial = (resolutions, tokens, receipts)


def evidence_observations(
    source: Record, resolutions: list[Record], tokens: list[Record], receipts: list[Record], observed: datetime
) -> dict[str, Record]:
    """把每份原生响应映射回冻结 condition/token，保留各自的实际时间。"""
    observations = {}
    for market in source["markets"]:
        token_ids = {o["token_id"] for o in market["outcomes"]}
        matched = []
        lifecycle_receipts = []
        token_receipts: dict[str, list[Record]] = {token: [] for token in token_ids}
        for receipt in receipts:
            query = parse_qs(urlsplit(receipt["url"]).query)
            conditions = set(query.get("condition", [""])[0].split(","))
            assets = set(query.get("token_id", [""])[0].split(","))
            if market["condition_id"] in conditions or token_ids & assets:
                matched.append(receipt)
            if market["condition_id"] in conditions:
                lifecycle_receipts.append(receipt)
            for token in token_ids & assets:
                token_receipts[token].append(receipt)
        if any(row.get("condition_id") == market["condition_id"] for row in resolutions) and not lifecycle_receipts:
            raise ValueError("evidence_receipt_binding_missing")
        if any(row.get("token_id") in token_ids and not token_receipts[row["token_id"]] for row in tokens):
            raise ValueError("evidence_receipt_binding_missing")
        observations[market["condition_id"]] = {
            "observed_at": stamp(max((instant(r["observed_at"]) for r in matched), default=observed)),
            "unresolved_observed_at": stamp(
                min((instant(r.get("requested_at") or r["observed_at"]) for r in matched), default=observed)
            ),
            "receipt_ids": [r["request_id"] for r in matched],
            "lifecycle_observed_at": stamp(
                max((instant(r["observed_at"]) for r in lifecycle_receipts), default=observed)
            ),
            "lifecycle_receipt_ids": [r["request_id"] for r in lifecycle_receipts],
            "token_observations": {
                token: {
                    "observed_at": stamp(max(instant(r["observed_at"]) for r in entries)),
                    "receipt_ids": [r["request_id"] for r in entries],
                }
                for token, entries in token_receipts.items()
                if entries
            },
        }
    return observations


class AuditArchive:
    """SQLite耐久事务和去重压缩BLOB；每次提交先于数据页检查点。"""

    def __init__(self, path: Path):
        self.lock = threading.RLock()
        self.db = sqlite3.connect(path, check_same_thread=False)
        self.db.execute("PRAGMA journal_mode=DELETE")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.executescript(
            ARCHIVE_SCHEMA
            + """
            CREATE TABLE IF NOT EXISTS source_scan (
                id INTEGER PRIMARY KEY CHECK(id=1), fingerprint TEXT, upper_id INTEGER, boundary TEXT);
            CREATE TABLE IF NOT EXISTS source_ranges (
                lower_id INTEGER PRIMARY KEY, upper_id INTEGER, events INTEGER, missing INTEGER, proof TEXT);
            CREATE TABLE IF NOT EXISTS source_events (
                event_id INTEGER PRIMARY KEY, request_id TEXT NOT NULL, prefix TEXT NOT NULL);
            CREATE INDEX IF NOT EXISTS source_event_request ON source_events(request_id);
        """
        )

    def append(self, spool: tempfile.SpooledTemporaryFile[bytes], receipt: Record) -> None:
        with self.lock, self.db:
            exists = self.db.execute("SELECT 1 FROM bodies WHERE sha256=?", [receipt["body_sha256"]]).fetchone()
            if not exists:
                with tempfile.SpooledTemporaryFile(max_size=4 * 1024 * 1024) as compressed:
                    encoder = zlib.compressobj(level=6)
                    spool.seek(0)
                    for chunk in iter(lambda: spool.read(1024 * 1024), b""):
                        compressed.write(encoder.compress(chunk))
                    compressed.write(encoder.flush())
                    cursor = self.db.execute(
                        "INSERT INTO bodies VALUES (?,?,zeroblob(?))",
                        [receipt["body_sha256"], receipt["bytes"], compressed.tell()],
                    )
                    assert cursor.lastrowid is not None
                    with self.db.blobopen("bodies", "content", cursor.lastrowid, readonly=False) as blob:
                        compressed.seek(0)
                        shutil.copyfileobj(compressed, blob, 1024 * 1024)
            self.db.execute("INSERT INTO receipts VALUES (?,?)", [receipt["request_id"], dumps(receipt)])

    def error(self, receipt: Record) -> None:
        with self.lock, self.db:
            self.db.execute("INSERT INTO receipts VALUES (?,?)", [receipt["request_id"], dumps(receipt)])

    def source_scan(self) -> Record | None:
        with self.lock:
            row = self.db.execute("SELECT fingerprint,upper_id,boundary FROM source_scan WHERE id=1").fetchone()
        return {"fingerprint": row[0], "upper_id": row[1], "boundary": simplejson.loads(row[2])} if row else None

    def begin_source_scan(self, fingerprint: str, upper_id: int, boundary: Record) -> None:
        with self.lock, self.db:
            self.db.execute("DELETE FROM source_events")
            self.db.execute("DELETE FROM source_ranges")
            self.db.execute("DELETE FROM source_scan")
            self.db.execute("INSERT INTO source_scan VALUES (1,?,?,?)", [fingerprint, upper_id, dumps(boundary)])

    def source_ranges(self) -> set[int]:
        with self.lock:
            return {row[0] for row in self.db.execute("SELECT lower_id FROM source_ranges")}

    def source_range(
        self, lower: int, upper: int, found: dict[int, tuple[Record, str]], missing: dict[int, Record]
    ) -> None:
        """完整响应先归档，再原子提交一个ID区间；失败区间不产生完成标记。"""
        if set(found) & set(missing) or set(found) | set(missing) != set(range(lower, upper + 1)):
            raise ValueError("source_range_incomplete")
        proof = {"confirmed_404": {str(key): value for key, value in missing.items()}}
        with self.lock, self.db:
            self.db.executemany(
                "INSERT OR REPLACE INTO source_events VALUES (?,?,?)",
                [(key, receipt["request_id"], prefix) for key, (receipt, prefix) in found.items()],
            )
            self.db.execute(
                "INSERT OR REPLACE INTO source_ranges VALUES (?,?,?,?,?)",
                [lower, upper, len(found), len(missing), dumps(proof)],
            )

    def source_coverage(self) -> Record:
        scan = self.source_scan()
        if scan is None:
            return {"complete": False, "id_upper_bound": None, "events": 0, "confirmed_404": 0}
        with self.lock:
            scanned, found, missing = self.db.execute(
                "SELECT coalesce(sum(upper_id-lower_id+1),0),coalesce(sum(events),0),coalesce(sum(missing),0) "
                "FROM source_ranges"
            ).fetchone()
            complete = scanned == scan["upper_id"] and found + missing == scanned
            if complete and self.db.execute("SELECT count(*) FROM source_events").fetchone()[0] != found:
                raise ValueError("source_catalog_count_mismatch")
        return {
            "method": "public_positive_event_id_census_v1",
            "versions": ["v1", "v2"],
            "closed": "any",
            "id_lower_bound": 1,
            "id_upper_bound": scan["upper_id"],
            "classified_ids": scanned,
            "events": found,
            "confirmed_404": missing,
            "complete": complete,
            "boundary_receipt": scan["boundary"],
            "scope": "冻结最大可见ID以内的公开正整数Event ID；分次观察，不是原子快照。",
        }

    @contextmanager
    def archived_page(self, request_id: str, prefix: str) -> Iterator[Page]:
        """从原始HTTP响应恢复有界页面，不另存一份完整Event JSON。"""
        with tempfile.SpooledTemporaryFile(max_size=4 * 1024 * 1024) as spool:
            with self.lock:
                row = self.db.execute("SELECT payload FROM receipts WHERE request_id=?", [request_id]).fetchone()
                if row is None:
                    raise ValueError("source_receipt_missing")
                receipt = simplejson.loads(row[0])
                body = self.db.execute(
                    "SELECT rowid,raw_size FROM bodies WHERE sha256=?", [receipt["body_sha256"]]
                ).fetchone()
                if body is None:
                    raise ValueError("source_body_missing")
                decoder = zlib.decompressobj()
                hasher = hashlib.sha256()
                with self.db.blobopen("bodies", "content", body[0], readonly=True) as blob:
                    for chunk in iter(lambda: blob.read(1024 * 1024), b""):
                        data = decoder.decompress(chunk)
                        spool.write(data)
                        hasher.update(data)
                tail = decoder.flush()
                spool.write(tail)
                hasher.update(tail)
                if not decoder.eof or spool.tell() != body[1] or hasher.hexdigest() != receipt["body_sha256"]:
                    raise ValueError("source_body_checksum_mismatch")
            yield Page(spool, prefix, receipt)

    def source_pages(self) -> Iterator[DiscoveryPage]:
        with self.lock:
            pages = self.db.execute(
                "SELECT DISTINCT request_id,prefix FROM source_events ORDER BY request_id"
            ).fetchall()
        for request_id, prefix in pages:
            with self.lock:
                expected = {
                    str(row[0])
                    for row in self.db.execute("SELECT event_id FROM source_events WHERE request_id=?", [request_id])
                }
            with self.archived_page(request_id, prefix) as page:
                rows = list(page.rows)
                if len(rows) != len(expected) or {str(row.get("id")) for row in rows} != expected:
                    raise ValueError("source_catalog_identity_mismatch")
                yield DiscoveryPage(rows, page.receipt)

    def close(self) -> None:
        self.db.close()


def evidence_snapshot(source: Path, destination: Path, previous: Record | None = None) -> Record | None:
    """冻结新增回执及其完整响应；检查上一个边界，拒绝截断或替换后的归档。"""
    after = (previous or {}).get("source_request_rowid_end", 0)
    with closing(sqlite3.connect(source.resolve().as_uri() + "?mode=ro", uri=True)) as db:
        if after:
            assert previous is not None
            boundary = db.execute("SELECT request_id FROM receipts WHERE rowid=?", [after]).fetchone()
            if boundary is None or boundary[0] != previous["source_request_id_end"]:
                raise ValueError("evidence_archive_regressed")
        last = db.execute("SELECT rowid,request_id FROM receipts ORDER BY rowid DESC LIMIT 1").fetchone()
    if last is None or last[0] == after:
        return None
    if last[0] < after:
        raise ValueError("evidence_archive_regressed")
    # 只覆盖尚无manifest的本次临时生成目录；既有快照由调用方直接复用。
    destination.unlink(missing_ok=True)
    with closing(sqlite3.connect(destination, uri=True)) as target:
        target.execute("PRAGMA journal_mode=DELETE")
        target.execute("PRAGMA synchronous=FULL")
        target.executescript(ARCHIVE_SCHEMA)
        target.execute("ATTACH DATABASE ? AS upstream", [source.resolve().as_uri() + "?mode=ro"])
        with target:
            target.execute(
                "INSERT INTO receipts SELECT request_id,payload FROM upstream.receipts WHERE rowid>? AND rowid<=?",
                [after, last[0]],
            )
            target.execute("""INSERT INTO bodies SELECT sha256,raw_size,content FROM upstream.bodies
                WHERE sha256 IN (SELECT json_extract(payload,'$.body_sha256') FROM receipts)""")
            count = target.execute("SELECT count(*) FROM receipts").fetchone()[0]
            if count != last[0] - after:
                raise ValueError("evidence_archive_receipts_missing")
    return {
        "source_request_rowid_start": after + 1,
        "source_request_rowid_end": last[0],
        "source_request_id_end": last[1],
        "requests": count,
    }


@dataclass
class Page:
    file: tempfile.SpooledTemporaryFile[bytes]
    prefix: str
    receipt: Record
    next_cursor: str | None = None

    @property
    def rows(self) -> Iterator[Record]:
        """记录只能在当前分页迭代期间消费，不跨页持有。"""
        self.file.seek(0)
        yield from ijson.items(self.file, self.prefix, use_float=False)

    def field(self, prefix: str) -> Any:
        self.file.seek(0)
        return next(ijson.items(self.file, prefix, use_float=False), None)

    def require_array(self, prefix: str) -> None:
        self.file.seek(0)
        for current, event, _ in ijson.parse(self.file):
            if current == prefix:
                if event != "start_array":
                    raise ValueError(f"invalid_api_array:{prefix}")
                return
        raise ValueError(f"missing_api_array:{prefix}")


@dataclass
class DiscoveryPage:
    rows: list[Record]
    receipt: Record


class Polymarket:
    def __init__(
        self,
        archive: Path,
        *,
        gamma_url: str = "https://gamma-api.polymarket.com",
        data_url: str = "https://data-api.polymarket.com",
        timeout: float = 60,
        attempts: int = 5,
        rate: float = 5,
        workers: int = 1,
        transport_factory: Callable[[], httpx.BaseTransport] | None = None,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ):
        self.gamma_url, self.data_url = gamma_url.rstrip("/"), data_url.rstrip("/")
        self.clock, self.attempts, self.rate = clock, attempts, rate
        if not 1 <= workers <= 16 or rate <= 0 or attempts < 1:
            raise ValueError("invalid_network_limits")
        self.workers = workers
        self.rate_lock = threading.Lock()
        archive.parent.mkdir(parents=True, exist_ok=True)
        self.archive = AuditArchive(archive)
        self.timeout, self.transport_factory = timeout, transport_factory
        self.client_lock = threading.Lock()
        self.clients: dict[int, httpx.Client] = {}
        self.closed = False
        self.last_request = 0.0
        self.profile_lock = threading.Lock()
        self.profiles: OrderedDict[str, tuple[Record, Record]] = OrderedDict()

    def __enter__(self) -> Polymarket:
        return self

    def __exit__(self, *_: Any) -> None:
        with self.client_lock:
            self.closed = True
            clients = list(self.clients.values())
            self.clients.clear()
        try:
            for client in clients:
                client.close()
        finally:
            self.archive.close()

    def _client(self) -> httpx.Client:
        """每个线程独占连接池，失败回收不会中断其他 worker。"""
        identifier = threading.get_ident()
        with self.client_lock:
            if self.closed:
                raise RuntimeError("source_client_closed")
            if identifier not in self.clients:
                self.clients[identifier] = httpx.Client(
                    timeout=self.timeout,
                    transport=self.transport_factory() if self.transport_factory else None,
                    headers={"User-Agent": "ForeseeBench/0.1 (read-only research)"},
                )
            return self.clients[identifier]

    def _retire_client(self, client: httpx.Client) -> None:
        with self.client_lock:
            identifier = threading.get_ident()
            if self.clients.get(identifier) is client:
                del self.clients[identifier]
        client.close()

    @contextmanager
    def page(
        self,
        path: str,
        params: Record,
        prefix: str,
        *,
        data: bool = False,
        deadline: datetime | None = None,
    ) -> Iterator[Page]:
        url = (self.data_url if data else self.gamma_url) + path
        for attempt in range(self.attempts):
            delivered = False
            if deadline and self.clock() >= deadline:
                raise TimeoutError("freeze_deadline")
            with self.rate_lock:
                delay = 1 / self.rate - (time.monotonic() - self.last_request)
                if delay > 0:
                    time.sleep(delay)
                self.last_request = time.monotonic()
            requested = self.clock()
            if deadline and requested >= deadline:
                raise TimeoutError("freeze_deadline")
            try:
                client = self._client()
                with tempfile.SpooledTemporaryFile(max_size=4 * 1024 * 1024) as spool:
                    with client.stream("GET", url, params=params) as response:
                        hasher = hashlib.sha256()
                        size = 0
                        for chunk in response.iter_bytes():
                            spool.write(chunk)
                            hasher.update(chunk)
                            size += len(chunk)
                        observed = self.clock()
                        receipt = {
                            "request_id": str(uuid4()),
                            "url": str(response.url),
                            "query_hash": digest([url, params]),
                            "status": response.status_code,
                            "requested_at": stamp(requested),
                            "observed_at": stamp(observed),
                            "body_sha256": hasher.hexdigest(),
                            "bytes": size,
                            "trace_id": response.headers.get("x-trace-id"),
                        }
                        self.archive.append(spool, receipt)
                        if response.status_code in {429, 500, 502, 503, 504} and attempt + 1 < self.attempts:
                            retry = response.headers.get("Retry-After", "")
                            time.sleep(float(retry) if retry.isdecimal() else min(60, 2**attempt))
                            continue
                        response.extensions["foreseebench_receipt"] = receipt
                        response.raise_for_status()
                        if deadline and observed >= deadline:
                            raise TimeoutError("freeze_deadline")
                    delivered = True
                    yield Page(spool, prefix, receipt)
                    return
            except (httpx.TimeoutException, httpx.NetworkError, httpx.RemoteProtocolError, httpx.ProxyError) as error:
                if delivered:
                    raise
                self.archive.error(
                    {
                        "request_id": str(uuid4()),
                        "url": url,
                        "params": params,
                        "query_hash": digest([url, params]),
                        "observed_at": stamp(self.clock()),
                        "status": None,
                        "body_sha256": None,
                        "bytes": None,
                        "error": f"{type(error).__name__}:{error}",
                        "attempt": attempt + 1,
                    }
                )
                self._retire_client(client)
                if attempt + 1 == self.attempts:
                    raise
                time.sleep(min(60, 2**attempt))
        raise RuntimeError("request_attempts_exhausted")

    def discovery_status(self) -> Record:
        return self.archive.source_coverage()

    def discover(
        self, week: Week, *, deadline: datetime | None = None, refresh: bool = False
    ) -> Iterator[DiscoveryPage]:
        """先完整保存双版本、全状态来源；所有日期与领域筛选留给下游。"""
        fingerprint = digest([self.gamma_url, "positive_event_id_census_v1", ["v1", "v2"], "any"])
        scan = self.archive.source_scan()
        if scan and scan["fingerprint"] != fingerprint:
            raise ValueError("source_scan_configuration_changed")
        if scan is None or refresh and self.discovery_status()["complete"]:
            params = {"limit": 1, "closed": "any", "version": ["v1", "v2"], "order": "id", "ascending": "false"}
            with self.page("/events/keyset", params, "events.item", deadline=deadline) as page:
                page.require_array("events")
                rows = list(page.rows)
                if not rows:
                    raise ValueError("source_upper_boundary_missing")
                if len(rows) > 1 or rows and (not str(rows[0].get("id", "")).isdecimal() or int(rows[0]["id"]) < 1):
                    raise ValueError("source_upper_boundary_invalid")
                visible_upper = int(rows[0]["id"])
                upper = max(visible_upper, scan["upper_id"] if scan else 0)
                boundary = dict(page.receipt)
                if scan and upper > visible_upper:
                    boundary["retained_upper_bound_from"] = scan["boundary"]["request_id"]
                self.archive.begin_source_scan(fingerprint, upper, boundary)
            scan = self.archive.source_scan()
        assert scan is not None
        completed = self.archive.source_ranges()
        queue: Queue[Any] = Queue(maxsize=self.workers * 2)
        stop = threading.Event()

        def put(value: Any) -> None:
            while not stop.is_set():
                try:
                    queue.put(value, timeout=0.2)
                    return
                except Full:
                    continue

        def acquire(index: int) -> None:
            try:
                for lower in range(1 + index * 100, scan["upper_id"] + 1, self.workers * 100):
                    if stop.is_set():
                        return
                    if lower in completed:
                        continue
                    self._discover_id_range(lower, min(lower + 99, scan["upper_id"]), deadline)
                    put(DiscoveryPage([], scan["boundary"]))
            except Exception as error:
                put(error)
            finally:
                put(None)

        with ThreadPoolExecutor(max_workers=self.workers) as pool:
            for i in range(self.workers):
                pool.submit(acquire, i)
            finished = 0
            try:
                while finished < self.workers:
                    try:
                        value = queue.get(timeout=0.5)
                    except Empty:
                        continue
                    if value is None:
                        finished += 1
                    elif isinstance(value, Exception):
                        raise value
                    else:
                        yield value
            finally:
                stop.set()
        if not self.discovery_status()["complete"]:
            raise ValueError("source_discovery_incomplete")
        yield from self.archive.source_pages()

    def _discover_id_range(self, lower: int, upper: int, deadline: datetime | None) -> None:
        identifiers = set(range(lower, upper + 1))
        params: Record = {
            "id": list(range(lower, upper + 1)),
            "limit": 100,
            "closed": "any",
            "version": ["v1", "v2"],
            "order": "id",
            "ascending": "true",
        }
        cursors: set[str] = set()
        found: dict[int, tuple[Record, str]] = {}
        missing: dict[int, Record] = {}
        while True:
            with self.page("/events/keyset", params, "events.item", deadline=deadline) as page:
                page.require_array("events")
                for row in page.rows:
                    value = str(row.get("id", ""))
                    if not value.isdecimal() or int(value) not in identifiers or int(value) in found:
                        raise ValueError("source_event_identity_outside_query_or_duplicate")
                    found[int(value)] = (page.receipt, "events.item")
                cursor = page.field("next_cursor")
            if cursor is None:
                break
            if not isinstance(cursor, str) or not cursor or cursor in cursors:
                raise ValueError("repeated_discovery_cursor")
            cursors.add(cursor)
            params["after_cursor"] = cursor
        for identifier in sorted(identifiers - set(found)):
            try:
                _, receipt = self.event(str(identifier), deadline=deadline)
                found[identifier] = (receipt, "")
            except httpx.HTTPStatusError as error:
                if error.response.status_code != 404:
                    raise
                missing[identifier] = error.response.extensions["foreseebench_receipt"]
        self.archive.source_range(lower, upper, found, missing)

    def event(self, event_id: str, *, deadline: datetime | None = None) -> tuple[Record, Record]:
        with self.page(f"/events/{event_id}", {}, "", deadline=deadline) as page:
            value = page.field("")
            if not isinstance(value, dict) or str(value.get("id")) != event_id:
                raise ValueError("event_identity_mismatch")
            return value, page.receipt

    def comments(self, event_id: str, *, deadline: datetime | None = None) -> Iterator[Page]:
        """按官方游标读取当前 Event；末页省略 next_cursor。"""
        params: Record = {
            "parent_entity_type": "Event",
            "parent_entity_id": event_id,
            "limit": 100,
            "order": "createdAt",
            "ascending": "true",
        }
        seen: set[str] = set()
        while True:
            with self.page("/comments/keyset", params, "comments.item", deadline=deadline) as page:
                page.require_array("comments")
                cursor = page.field("next_cursor")
                if cursor is not None:
                    if not isinstance(cursor, str) or not cursor or cursor in seen:
                        raise ValueError("invalid_or_repeated_comment_cursor")
                    seen.add(cursor)
                page.next_cursor = cursor
                yield page
            if cursor is None:
                return
            params["after_cursor"] = cursor

    @contextmanager
    def comment_thread(self, comment_id: str, *, deadline: datetime | None = None) -> Iterator[Page]:
        with self.page(f"/comments/{comment_id}", {"get_positions": "false"}, "item", deadline=deadline) as page:
            page.require_array("")
            yield page

    def public_profile(self, address: str, *, deadline: datetime | None = None) -> tuple[Record, Record]:
        """按地址共享有限缓存；404 保留为来源缺失，其他错误继续抛出。"""
        key = address.lower()
        with self.profile_lock:
            if key in self.profiles:
                self.profiles.move_to_end(key)
                return self.profiles[key]
            try:
                with self.page("/public-profile", {"address": address}, "", deadline=deadline) as page:
                    value = page.field("")
                    if not isinstance(value, dict):
                        raise ValueError("invalid_public_profile")
                    result = (value, page.receipt)
            except httpx.HTTPStatusError as error:
                if error.response.status_code != 404:
                    raise
                result = ({}, error.response.extensions["foreseebench_receipt"])
            self.profiles[key] = result
            if len(self.profiles) > 4096:
                self.profiles.popitem(last=False)
            return result

    def trades(self, event_id: str, *, cursor: str | None = None, deadline: datetime | None = None) -> Iterator[Page]:
        params: Record = {
            "event_id": event_id,
            "limit": 1000,
            "taker_only": "true",
            "filter_type": "TOKENS",
            "filter_amount": "0.000001",
        }
        cursors: set[str] = set()
        while True:
            if cursor:
                params["cursor"] = cursor
            with self.page("/v2/trades", params, "data.item", data=True, deadline=deadline) as page:
                page.require_array("data")
                pagination = page.field("pagination")
                if not isinstance(pagination, dict) or not {"has_more", "next_cursor"} <= pagination.keys():
                    raise ValueError("trade_pagination_missing")
                page.next_cursor = pagination["next_cursor"]
                if not isinstance(pagination["has_more"], bool) or (
                    page.next_cursor is not None and (not isinstance(page.next_cursor, str) or not page.next_cursor)
                ):
                    raise ValueError("trade_pagination_invalid_type")
                if bool(page.next_cursor) != bool(pagination["has_more"]):
                    raise ValueError("trade_pagination_inconsistent")
                yield page
                cursor = page.next_cursor
            if not cursor:
                break
            if cursor in cursors:
                raise ValueError("repeated_trade_cursor")
            cursors.add(cursor)

    def evidence(
        self, source: Record, *, deadline: datetime | None = None
    ) -> tuple[list[Record], list[Record], list[Record]]:
        conditions = [m["condition_id"] for m in source["markets"]]
        tokens = [o["token_id"] for m in source["markets"] for o in m["outcomes"]]
        resolutions: list[Record] = []
        token_rows: list[Record] = []
        receipts: list[Record] = []
        try:
            for path, key, values, cap, output in [
                ("/v2/resolutions", "condition", conditions, 20, resolutions),
                ("/v2/tokens", "token_id", tokens, 50, token_rows),
            ]:
                for start in range(0, len(values), cap):
                    with self.page(
                        path,
                        {key: ",".join(values[start : start + cap])},
                        "data.item",
                        data=True,
                        deadline=deadline,
                    ) as page:
                        page.require_array("data")
                        rows = list(page.rows)
                        output.extend(rows)
                        receipts.append(page.receipt)
        except Exception as error:
            raise EvidenceError(error, resolutions, token_rows, receipts) from error
        return resolutions, token_rows, receipts
