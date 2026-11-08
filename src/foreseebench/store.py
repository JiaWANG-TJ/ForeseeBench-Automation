"""单写入者事务状态、流式 Parquet 快照和可核验发布清单。"""

from __future__ import annotations

import fcntl
import hashlib
import importlib.metadata
import os
import threading
from collections import Counter, defaultdict
from collections.abc import Iterable, Iterator
from datetime import datetime
from functools import wraps
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import simplejson

from .contract import Record, Week, digest, dumps, gt_deadline, instant, settle_event, stamp

TEXT = pa.string()
PARQUET_SCHEMA_VERSION = "3.0"
COMMENT_RELATIONS_SQL = """SELECT event_id, payload->>'comment_id' AS id,
    nullif(payload->>'parent_comment_id','') AS parent FROM rows
    WHERE batch=? AND kind='comments'
    QUALIFY row_number() OVER (PARTITION BY event_id,payload->>'comment_id'
        ORDER BY payload->>'observed_at' DESC,page DESC,ordinal DESC)=1"""
TIME = pa.struct([("start_at", TEXT), ("end_at", TEXT)])
OUTCOME = pa.struct([("label", TEXT), ("index", pa.int32()), ("token_id", TEXT)])
MARKET = pa.struct(
    [
        (key, TEXT)
        for key in (
            "market_id",
            "condition_id",
            "question",
            "description",
            "resolution_source",
            "group_item_title",
            "version",
            "end_at",
            "game_start_at",
        )
    ]
    + [("neg_risk", pa.bool_()), ("outcomes", pa.list_(OUTCOME))]
)
SCHEMAS = {
    "events": pa.schema(
        [
            (key, TEXT)
            for key in (
                "source_id",
                "event_id",
                "batch_id",
                "question",
                "description",
                "observed_at",
                "frozen_at",
                "source_hash",
                "primary_domain",
            )
        ]
        + [
            ("time", TIME),
            ("domain", pa.list_(TEXT)),
            ("tag_ids", pa.list_(TEXT)),
            ("series_ids", pa.list_(TEXT)),
            ("markets", pa.list_(MARKET)),
        ]
    ),
    "comments": pa.schema(
        [
            (key, TEXT)
            for key in (
                "event_id",
                "comment_id",
                "parent_comment_id",
                "profile_observed_at",
                "user_id",
                "userAddress",
                "body",
                "createdAt",
                "updatedAt",
                "observed_at",
            )
        ]
        + [("profile", pa.struct([("name", TEXT), ("bio", TEXT)]))]
    ),
    "trades": pa.schema(
        [
            (key, TEXT)
            for key in (
                "event_id",
                "row_id",
                "transaction_hash",
                "condition_id",
                "token_id",
                "option_label",
                "outcome",
                "side",
                "price_per_share",
                "shares",
                "traded_at",
                "observed_at",
            )
        ]
    ),
    "resolutions": pa.schema(
        [
            (key, TEXT)
            for key in (
                "source_id",
                "event_id",
                "batch_id",
                "observed_at",
                "status",
                "eligibility",
                "validity",
                "gt_deadline_at",
                "gt_verified_at",
                "qualification_verified_at",
                "markets_json",
            )
        ]
        + [("time", TIME)]
    ),
}


def serialized(method: Any) -> Any:
    """多个网络worker共用一个串行事务边界，读取使用线程本地cursor。"""

    @wraps(method)
    def call(self: Any, *args: Any, **kwargs: Any) -> Any:
        with self._write_lock:
            return method(self, *args, **kwargs)

    return call


def file_hash(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def implementation_provenance() -> Record:
    package = Path(__file__).parent
    files = {p.name: file_hash(p) for p in sorted(package.glob("*.py"))}
    versions = {
        name: importlib.metadata.version(name)
        for name in ("foreseebench", "pyarrow", "numpy", "duckdb", "httpx", "ijson", "huggingface-hub", "hf-xet")
    }
    lock = package.parent.parent / "uv.lock"
    value = {"source_files": files, "dependencies": versions, "lock_sha256": file_hash(lock) if lock.exists() else None}
    return value | {"implementation_hash": digest(value)}


def atomic_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".partial")
    with temporary.open("w", encoding="utf-8") as stream:
        stream.write(dumps(value) + "\n")
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)
    directory = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def read_manifest(path: Path) -> Record:
    with path.open(encoding="utf-8") as stream:
        return simplejson.load(stream, use_decimal=True)


def read_records(root: Path, manifest: Record, table: str, event_id: str | None = None) -> Iterator[Record]:
    batch_size = 64 if table in {"events", "resolutions"} else 4096
    if event_id is not None:
        paths = [str(root / e["path"]) for e in manifest["files"] if e["table"] == table]
        if not paths:
            return
        dataset = ds.dataset(paths, format="parquet")
        scanner = dataset.scanner(filter=ds.field("event_id") == event_id, batch_size=batch_size)
        for batch in scanner.to_batches():
            for row in batch.to_pylist():
                if table == "resolutions":
                    row["markets"] = simplejson.loads(row.pop("markets_json"), use_decimal=True)
                yield row
        return
    for entry in manifest["files"]:
        if entry["table"] != table:
            continue
        parquet = pq.ParquetFile(root / entry["path"], page_checksum_verification=True)
        for batch in parquet.iter_batches(batch_size=batch_size):
            for row in batch.to_pylist():
                if event_id is None or row.get("event_id") == event_id:
                    if table == "resolutions":
                        row["markets"] = simplejson.loads(row.pop("markets_json"), use_decimal=True)
                    yield row


class Store:
    def __init__(
        self,
        root: Path,
        *,
        row_group_rows: int = 65536,
        row_group_bytes: int = 64 * 1024**2,
        shard_bytes: int = 512 * 1024**2,
        memory_limit: str = "1GB",
    ):
        self.root = root.resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self.lock = (self.root / "writer.lock").open("a+")
        try:
            fcntl.flock(self.lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            self.lock.close()
            raise RuntimeError("another_foreseebench_writer_is_active") from None
        self._write_lock = threading.RLock()
        self._local = threading.local()
        self._cursors: list[Any] = []
        self._publications_loaded = False
        self._root_db = duckdb.connect(str(self.root / "state.duckdb"))
        self.db.execute("SET memory_limit = ?", [memory_limit])
        self.db.execute("SET threads = 2")
        self.row_group_rows, self.row_group_bytes, self.shard_bytes = (
            row_group_rows,
            row_group_bytes,
            shard_bytes,
        )
        self.db.execute("""
            CREATE TABLE IF NOT EXISTS batches (
              id VARCHAR PRIMARY KEY, kind VARCHAR, config_hash VARCHAR,
              scan_complete BOOLEAN DEFAULT false, frozen BOOLEAN DEFAULT false);
            CREATE TABLE IF NOT EXISTS events (
              batch VARCHAR, event_id VARCHAR, payload JSON, status VARCHAR, reason VARCHAR,
              PRIMARY KEY(batch, event_id));
            CREATE TABLE IF NOT EXISTS streams (
              batch VARCHAR, event_id VARCHAR, kind VARCHAR, position JSON, complete BOOLEAN,
              PRIMARY KEY(batch, event_id, kind));
            CREATE TABLE IF NOT EXISTS rows (
              batch VARCHAR, event_id VARCHAR, kind VARCHAR, page VARCHAR, ordinal BIGINT, payload JSON);
            CREATE TABLE IF NOT EXISTS resolutions (
              batch VARCHAR, event_id VARCHAR, observed_at VARCHAR, payload JSON,
              PRIMARY KEY(batch, event_id, observed_at));
            CREATE TABLE IF NOT EXISTS tasks (
              task_id VARCHAR PRIMARY KEY, stage VARCHAR, input_hash VARCHAR, status VARCHAR,
              payload JSON);
            CREATE TABLE IF NOT EXISTS failures (
              batch VARCHAR, event_id VARCHAR, phase VARCHAR, observed_at VARCHAR, error VARCHAR);
            CREATE TABLE IF NOT EXISTS published_events (event_id VARCHAR PRIMARY KEY, batch VARCHAR);
            CREATE TABLE IF NOT EXISTS published_batches (batch VARCHAR PRIMARY KEY, release_sha256 VARCHAR);
        """)
        self.db.execute("ALTER TABLE events ADD COLUMN IF NOT EXISTS source_pass VARCHAR")

    def __enter__(self) -> Store:
        return self

    @property
    def db(self) -> Any:
        with self._write_lock:
            if not hasattr(self._local, "cursor"):
                self._local.cursor = self._root_db.cursor()
                self._cursors.append(self._local.cursor)
            return self._local.cursor

    def __exit__(self, *_: Any) -> None:
        for cursor in self._cursors:
            cursor.close()
        self._root_db.close()
        fcntl.flock(self.lock, fcntl.LOCK_UN)
        self.lock.close()

    @serialized
    def begin(self, week: Week, kind: str, config_hash: str) -> None:
        if week.policy.read_only:
            raise ValueError("legacy_batch_read_only")
        if kind not in {"formal", "bootstrap", "qualification"}:
            raise ValueError("invalid_batch_kind")
        existing = self.db.execute("SELECT kind, config_hash FROM batches WHERE id=?", [week.id]).fetchone()
        if existing and existing != (kind, config_hash):
            raise ValueError("batch_configuration_conflict")
        protocol_path = self.root / "batches" / week.id / "protocol.json"
        if protocol_path.exists():
            if read_manifest(protocol_path) != week.record():
                raise ValueError("batch_protocol_conflict")
        elif existing:
            raise ValueError("legacy_batch_is_immutable")
        else:
            atomic_json(protocol_path, week.record())
        self.db.execute(
            "INSERT INTO batches(id,kind,config_hash) VALUES (?,?,?) ON CONFLICT DO NOTHING",
            [week.id, kind, config_hash],
        )
        provenance_path = self.root / "batches" / week.id / "run.json"
        current = implementation_provenance()
        if provenance_path.exists():
            previous = read_manifest(provenance_path)
            if kind == "formal" and previous["implementation_hash"] != current["implementation_hash"]:
                raise ValueError("formal_batch_implementation_changed")
        else:
            atomic_json(provenance_path, current)

    def week(self, batch_id: str) -> Week:
        if "/" in batch_id or "\\" in batch_id or batch_id in {"", ".", ".."}:
            raise ValueError("invalid_batch_id")
        path = self.root / "batches" / batch_id / "protocol.json"
        if path.exists():
            return Week.restore(read_manifest(path))
        return Week.parse(batch_id)

    @serialized
    def batch(self, batch_id: str) -> Record:
        row = self.db.execute("SELECT * FROM batches WHERE id=?", [batch_id]).fetchone()
        if row is None:
            raise ValueError("batch_missing")
        return dict(zip(("id", "kind", "config_hash", "scan_complete", "frozen"), row, strict=True))

    def mutable(self, batch_id: str) -> None:
        if self.batch(batch_id)["frozen"]:
            raise ValueError("batch_already_frozen")

    @serialized
    def discovery_started(self, batch_id: str) -> None:
        self.mutable(batch_id)
        self.db.execute("UPDATE batches SET scan_complete=false WHERE id=?", [batch_id])

    @serialized
    def discovery_complete(
        self, batch_id: str, coverage: Record | None = None, *, source_pass: str | None = None
    ) -> None:
        self.mutable(batch_id)
        if coverage is not None:
            if not coverage.get("complete"):
                raise ValueError("source_discovery_incomplete")
            if source_pass is not None:
                self.db.execute(
                    """UPDATE events SET status='rejected', reason=CASE
                    WHEN reason LIKE '%market_resolved_during_collection' THEN reason
                    ELSE 'source_unavailable_in_latest_scan' END
                    WHERE batch=? AND source_pass IS DISTINCT FROM ?""",
                    [batch_id, source_pass],
                )
                unavailable = self.db.execute(
                    "SELECT count(*) FROM events WHERE batch=? AND source_pass IS DISTINCT FROM ?",
                    [batch_id, source_pass],
                ).fetchone()[0]
                retained = sum(self.event_counts(batch_id).values())
                if retained - unavailable != coverage["events"]:
                    raise ValueError("source_inventory_count_mismatch")
                coverage = dict(coverage, retained_events=retained, previously_visible_now_unavailable=unavailable)
            atomic_json(self.root / "batches" / batch_id / "source-coverage.json", coverage)
        self.db.execute("UPDATE batches SET scan_complete=true WHERE id=?", [batch_id])

    def source_coverage(self, batch_id: str) -> Record | None:
        path = self.root / "batches" / batch_id / "source-coverage.json"
        return read_manifest(path) if path.exists() else None

    @serialized
    def previously_published(self, event_id: str, batch_id: str) -> bool:
        if not self._publications_loaded:
            for path in sorted((self.root / "batches").glob("*/benchmark/release.json")):
                self.index_publication(path.parent.parent.name)
            self._publications_loaded = True
        return bool(
            self.db.execute(
                "SELECT 1 FROM published_events WHERE event_id=? AND batch<>?",
                [event_id, batch_id],
            ).fetchone()
        )

    @serialized
    def index_publication(self, batch_id: str) -> None:
        """索引不可变正式发布清单；崩溃后由清单修复，不把V1预选当作发布。"""
        self.week(batch_id)
        path = self.root / "batches" / batch_id / "benchmark" / "release.json"
        checksum = file_hash(path)
        indexed = self.db.execute("SELECT release_sha256 FROM published_batches WHERE batch=?", [batch_id]).fetchone()
        if indexed:
            if indexed[0] != checksum:
                raise ValueError("benchmark_release_changed")
            return
        release = read_manifest(path)
        identifiers = [member["event_id"] for member in release["selection"]["members"]]
        if release["batch_id"] != batch_id or len(set(identifiers)) != len(identifiers):
            raise ValueError("invalid_benchmark_release_identity")
        self.db.execute("BEGIN TRANSACTION")
        try:
            for identifier in identifiers:
                previous = self.db.execute(
                    "SELECT batch FROM published_events WHERE event_id=?", [identifier]
                ).fetchone()
                if previous and previous[0] != batch_id:
                    raise ValueError("event_already_published")
                self.db.execute("INSERT OR IGNORE INTO published_events VALUES (?,?)", [identifier, batch_id])
            self.db.execute("INSERT INTO published_batches VALUES (?,?)", [batch_id, checksum])
            self.db.execute("COMMIT")
        except Exception:
            self.db.execute("ROLLBACK")
            raise

    @serialized
    def event(self, batch_id: str, event_id: str, payload: Record, status: str, reason: str | None = None) -> None:
        self.mutable(batch_id)
        self.db.execute(
            """INSERT INTO events(batch,event_id,payload,status,reason) VALUES (?,?,?,?,?)
            ON CONFLICT(batch,event_id) DO UPDATE SET
            payload=excluded.payload,status=excluded.status,reason=excluded.reason""",
            [batch_id, event_id, dumps(payload), status, reason],
        )

    @serialized
    def source_page(
        self, batch_id: str, entries: list[Record], *, seen_ids: list[str] | None = None, source_pass: str | None = None
    ) -> None:
        self.mutable(batch_id)
        self.db.execute("BEGIN TRANSACTION")
        try:
            if entries:
                self.db.executemany(
                    """INSERT INTO events(batch,event_id,payload,status,reason) VALUES (?,?,?,?,?)
                    ON CONFLICT(batch,event_id) DO UPDATE SET
                    payload=excluded.payload,status=excluded.status,reason=excluded.reason""",
                    [
                        (batch_id, row["event_id"], dumps(row["payload"]), row["status"], row["reason"])
                        for row in entries
                    ],
                )
            if seen_ids:
                self.db.execute(
                    "UPDATE events SET source_pass=? WHERE batch=? AND event_id IN ("
                    + ",".join("?" for _ in seen_ids)
                    + ")",
                    [source_pass, batch_id, *seen_ids],
                )
            self.db.execute("COMMIT")
        except Exception:
            self.db.execute("ROLLBACK")
            raise

    def iter_events(self, batch_id: str, statuses: tuple[str, ...] | None = None) -> Iterator[Record]:
        query = "SELECT event_id,payload,status,reason FROM events WHERE batch=?"
        parameters = [batch_id]
        if statuses is not None:
            query += " AND status IN (" + ",".join("?" for _ in statuses) + ")"
            parameters.extend(statuses)
        cursor = self._root_db.cursor()
        try:
            cursor.execute(query + " ORDER BY event_id", parameters)
            while rows := cursor.fetchmany(256):
                for row in rows:
                    yield {
                        "event_id": row[0],
                        "payload": simplejson.loads(row[1], use_decimal=True),
                        "status": row[2],
                        "reason": row[3],
                    }
        finally:
            cursor.close()

    @serialized
    def event_counts(self, batch_id: str) -> dict[str, int]:
        return dict(
            self.db.execute(
                "SELECT status,count(*) FROM events WHERE batch=? GROUP BY status ORDER BY status", [batch_id]
            ).fetchall()
        )

    @serialized
    def known_answer_ids(self, batch_id: str) -> set[str]:
        return {
            row[0]
            for row in self.db.execute(
                "SELECT event_id FROM events WHERE batch=? AND reason LIKE '%market_resolved_during_collection'",
                [batch_id],
            ).fetchall()
        }

    @serialized
    def events(self, batch_id: str, status: str | None = None) -> list[Record]:
        query = "SELECT event_id,payload,status,reason FROM events WHERE batch=?"
        parameters = [batch_id]
        if status:
            query += " AND status=?"
            parameters.append(status)
        rows = self.db.execute(query + " ORDER BY event_id", parameters).fetchall()
        return [
            {
                "event_id": r[0],
                "payload": simplejson.loads(r[1], use_decimal=True),
                "status": r[2],
                "reason": r[3],
            }
            for r in rows
        ]

    @serialized
    def event_status(self, batch_id: str, event_id: str) -> Record:
        row = self.db.execute(
            "SELECT status,reason FROM events WHERE batch=? AND event_id=?", [batch_id, event_id]
        ).fetchone()
        return {"status": row[0], "reason": row[1]} if row else {"status": "missing", "reason": None}

    @serialized
    def position(self, batch_id: str, event_id: str, kind: str) -> tuple[Any, bool]:
        row = self.db.execute(
            "SELECT position,complete FROM streams WHERE batch=? AND event_id=? AND kind=?",
            [batch_id, event_id, kind],
        ).fetchone()
        return (simplejson.loads(row[0]), row[1]) if row else (None, False)

    @serialized
    def missing_comment_parents(self, batch_id: str, event_id: str) -> list[str]:
        return [
            row[0]
            for row in self.db.execute(
                f"""WITH comments AS ({COMMENT_RELATIONS_SQL})
            SELECT DISTINCT c.parent FROM comments c WHERE c.event_id=? AND c.parent IS NOT NULL
            AND NOT EXISTS (SELECT 1 FROM comments p WHERE p.event_id=c.event_id AND p.id=c.parent)""",
                [batch_id, event_id],
            ).fetchall()
        ]

    @serialized
    def finish_comments(self, batch_id: str, event_id: str) -> None:
        self.mutable(batch_id)
        row = self.db.execute(
            """UPDATE streams SET position='null',complete=true
            WHERE batch=? AND event_id=? AND kind='comments' RETURNING event_id""",
            [batch_id, event_id],
        ).fetchone()
        if row is None:
            raise ValueError("comment_stream_missing")

    @serialized
    def comment_coverage(self, batch_id: str) -> list[Record]:
        rows = self.db.execute(
            f"""WITH comments AS ({COMMENT_RELATIONS_SQL})
            SELECT e.event_id,coalesce(s.complete,false),count(c.id),
                count(c.id) FILTER (WHERE c.parent IS NULL),
                count(c.id) FILTER (WHERE c.parent IS NOT NULL),
                count(c.id) FILTER (WHERE c.parent IS NOT NULL AND p.id IS NULL)
            FROM events e LEFT JOIN streams s ON s.batch=e.batch AND s.event_id=e.event_id AND s.kind='comments'
            LEFT JOIN comments c ON c.event_id=e.event_id
            LEFT JOIN comments p ON p.event_id=c.event_id AND p.id=c.parent
            WHERE e.batch=? AND e.status='collected'
            GROUP BY e.event_id,s.complete ORDER BY e.event_id""",
            [batch_id, batch_id],
        ).fetchall()
        return [
            dict(zip(("event_id", "complete", "total", "roots", "replies", "orphans"), row, strict=True))
            for row in rows
        ]

    @serialized
    def reset_event(self, batch_id: str, event_id: str, kind: str | None = None) -> None:
        self.mutable(batch_id)
        suffix = " AND kind=?" if kind else ""
        params = [batch_id, event_id] + ([kind] if kind else [])
        self.db.execute("BEGIN TRANSACTION")
        try:
            self.db.execute("DELETE FROM rows WHERE batch=? AND event_id=?" + suffix, params)
            self.db.execute("DELETE FROM streams WHERE batch=? AND event_id=?" + suffix, params)
            self.db.execute("COMMIT")
        except BaseException:
            self.db.execute("ROLLBACK")
            raise

    @serialized
    def page(
        self,
        batch_id: str,
        event_id: str,
        kind: str,
        page_key: str,
        records: Iterable[Record],
        position: Any,
        complete: bool,
    ) -> None:
        self.mutable(batch_id)
        self.db.execute("BEGIN TRANSACTION")
        try:
            self.db.execute(
                "DELETE FROM rows WHERE batch=? AND event_id=? AND kind=? AND page=?",
                [batch_id, event_id, kind, page_key],
            )
            buffer = []
            for ordinal, record in enumerate(records):
                buffer.append(
                    {
                        "batch": batch_id,
                        "event_id": event_id,
                        "kind": kind,
                        "page": page_key,
                        "ordinal": ordinal,
                        "payload": dumps(record),
                    }
                )
                if len(buffer) >= 1000:
                    self._insert(buffer)
                    buffer.clear()
            if buffer:
                self._insert(buffer)
            if kind == "comments":
                conflict = self.db.execute(
                    """SELECT payload->>'comment_id' FROM rows
                    WHERE batch=? AND event_id=? AND kind='comments' AND page=?
                    GROUP BY payload->>'comment_id' HAVING count(DISTINCT payload)>1 LIMIT 1""",
                    [batch_id, event_id, page_key],
                ).fetchone()
                if conflict:
                    raise ValueError("conflicting_comment_duplicate")
            self.db.execute(
                "INSERT OR REPLACE INTO streams VALUES (?,?,?,?,?)",
                [batch_id, event_id, kind, dumps(position), complete],
            )
            self.db.execute("COMMIT")
        except BaseException:
            self.db.execute("ROLLBACK")
            raise

    def _insert(self, buffer: list[Record]) -> None:
        self.db.register("incoming_rows", pa.Table.from_pylist(buffer))
        try:
            self.db.execute("INSERT INTO rows SELECT * FROM incoming_rows")
        finally:
            self.db.unregister("incoming_rows")

    @serialized
    def add_resolution(self, batch_id: str, result: Record) -> None:
        if not self.batch(batch_id)["frozen"]:
            raise ValueError("resolution_requires_frozen_input")
        self.db.execute(
            "INSERT OR REPLACE INTO resolutions VALUES (?,?,?,?)",
            [batch_id, result["event_id"], result["observed_at"], dumps(result)],
        )

    @serialized
    def failure(self, batch_id: str, event_id: str | None, phase: str, observed_at: datetime, error: str) -> None:
        self.db.execute(
            "INSERT INTO failures VALUES (?,?,?,?,?)", [batch_id, event_id, phase, stamp(observed_at), error]
        )

    @serialized
    def failures(self, batch_id: str) -> list[Record]:
        return [
            {"event_id": r[0], "phase": r[1], "observed_at": r[2], "error": r[3]}
            for r in self.db.execute(
                "SELECT event_id,phase,observed_at,error FROM failures WHERE batch=? ORDER BY observed_at", [batch_id]
            ).fetchall()
        ]

    @serialized
    def latest_resolution(self, batch_id: str, event_id: str) -> Record | None:
        row = self.db.execute(
            "SELECT payload FROM resolutions WHERE batch=? AND event_id=? ORDER BY observed_at DESC LIMIT 1",
            [batch_id, event_id],
        ).fetchone()
        return simplejson.loads(row[0], use_decimal=True) if row else None

    def _stage_records(self, batch_id: str, kind: str) -> Iterator[Record]:
        query = """SELECT r.payload FROM rows r JOIN events e USING(batch,event_id)
                   WHERE r.batch=? AND r.kind=? AND e.status='collected'"""
        if kind == "comments":
            query += """ QUALIFY row_number() OVER (
                PARTITION BY r.event_id, r.payload->>'comment_id'
                ORDER BY r.payload->>'observed_at' DESC, r.page DESC, r.ordinal DESC)=1"""
        query += " ORDER BY r.event_id, r.payload->>'createdAt', r.payload->>'traded_at', r.page, r.ordinal"
        cursor = self.db.cursor()
        try:
            cursor.execute(query, [batch_id, kind])
            while rows := cursor.fetchmany(1000):
                for row in rows:
                    yield simplejson.loads(row[0], use_decimal=True)
        finally:
            cursor.close()

    def _write_table(self, destination: Path, table: str, records: Iterable[Record]) -> list[Record]:
        entries: list[Record] = []
        buffer: list[Record] = []
        buffered_bytes = 0
        writer: pq.ParquetWriter | None = None
        path: Path | None = None
        count = 0

        def flush() -> None:
            nonlocal writer, path, count, buffered_bytes
            if writer is None:
                path = destination / f"{table}-{len(entries):05d}.parquet"
                writer = pq.ParquetWriter(
                    path.with_suffix(".partial"),
                    SCHEMAS[table],
                    compression="zstd",
                    compression_level=3,
                    use_content_defined_chunking=True,
                    write_page_index=True,
                    write_page_checksum=True,
                )
            if buffer:
                writer.write_table(
                    pa.Table.from_pylist(buffer, schema=SCHEMAS[table]),
                    row_group_size=self.row_group_rows,
                )
                count += len(buffer)
                buffer.clear()
                buffered_bytes = 0

        def finish() -> None:
            nonlocal writer, count
            if writer is not None and path is not None:
                writer.close()
                temporary = path.with_suffix(".partial")
                with temporary.open("rb") as stream:
                    os.fsync(stream.fileno())
                os.replace(temporary, path)
                entries.append(
                    {
                        "table": table,
                        "path": str(path.relative_to(self.root)),
                        "sha256": file_hash(path),
                        "bytes": path.stat().st_size,
                        "rows": count,
                    }
                )
                writer, count = None, 0

        try:
            for row in records:
                if table == "resolutions":
                    row = dict(row)
                    row["markets_json"] = dumps(row.pop("markets"))
                buffer.append(row)
                buffered_bytes += len(dumps(row).encode())
                if len(buffer) >= self.row_group_rows or buffered_bytes >= self.row_group_bytes:
                    flush()
                    if path is not None and path.with_suffix(".partial").stat().st_size >= self.shard_bytes:
                        finish()
            if buffer or not entries:
                flush()
            finish()
        finally:
            if writer is not None:
                writer.close()
        return entries

    def freeze(self, week: Week, now: datetime) -> Record:
        manifest_path = self.root / "batches" / week.id / "V1" / "manifest.json"
        if manifest_path.exists():
            manifest = read_manifest(manifest_path)
            self.verify(manifest)
            self.db.execute("UPDATE batches SET frozen=true WHERE id=?", [week.id])
            self.activate(manifest_path, manifest)
            return manifest
        week = self.week(week.id)
        if week.policy.read_only:
            raise ValueError("legacy_batch_read_only")
        batch = self.batch(week.id)
        if not batch["scan_complete"]:
            raise ValueError("discovery_incomplete")
        states = self.event_counts(week.id)
        incomplete = sum(count for status, count in states.items() if status not in {"collected", "rejected"})
        if batch["kind"] != "qualification" and incomplete:
            raise ValueError(f"construction_incomplete:{incomplete}")
        if batch["kind"] == "formal" and now < week.freeze_at:
            raise ValueError("freeze_not_due")
        provenance_path = self.root / "batches" / week.id / "run.json"
        provenance = read_manifest(provenance_path) if provenance_path.exists() else None
        if batch["kind"] == "formal":
            if (
                provenance is None
                or provenance["implementation_hash"] != implementation_provenance()["implementation_hash"]
            ):
                raise ValueError("formal_batch_implementation_changed")
            late = self.db.execute(
                """SELECT count(*) FROM rows r JOIN events e USING(batch,event_id)
                WHERE r.batch=? AND e.status='collected' AND (
                try_cast(r.payload->>'observed_at' AS TIMESTAMPTZ) IS NULL OR
                try_cast(r.payload->>'observed_at' AS TIMESTAMPTZ)>=? OR
                (r.payload->>'profile_observed_at' IS NOT NULL AND (
                try_cast(r.payload->>'profile_observed_at' AS TIMESTAMPTZ) IS NULL OR
                try_cast(r.payload->>'profile_observed_at' AS TIMESTAMPTZ)>=?)))""",
                [week.id, week.freeze_at, week.freeze_at],
            ).fetchone()
            if late and late[0]:
                raise ValueError("late_feature_observation")
        for entry in self.iter_events(week.id, ("collected",)):
            if not all(self.position(week.id, entry["event_id"], kind)[1] for kind in ("comments", "trades")):
                raise ValueError("event_stream_incomplete")
            if batch["kind"] == "formal" and instant(entry["payload"]["observed_at"]) >= week.freeze_at:
                raise ValueError("late_source_observation")
        destination = manifest_path.parent
        destination.mkdir(parents=True, exist_ok=True)
        actual_freeze = week.freeze_at if batch["kind"] == "formal" else now
        records = (
            dict(e["payload"], frozen_at=stamp(actual_freeze)) for e in self.iter_events(week.id, ("collected",))
        )
        files = self._write_table(destination, "events", records)
        for table in ("comments", "trades"):
            files.extend(self._write_table(destination, table, self._stage_records(week.id, table)))
        files.extend(self._write_table(destination, "resolutions", []))
        archive = self.root / "batches" / week.id / "raw-v1.sqlite"
        if archive.exists():
            files.append(
                {
                    "table": "evidence",
                    "path": str(archive.relative_to(self.root)),
                    "sha256": file_hash(archive),
                    "bytes": archive.stat().st_size,
                    "rows": 0,
                }
            )
        from .benchmark import select_events

        inventory, exclusions = self.write_source_inventory(week.id, destination)
        files.append(inventory)
        coverage = self.source_coverage(week.id)
        if coverage is not None and inventory["rows"] != coverage.get("retained_events", coverage["events"]):
            raise ValueError("source_inventory_count_mismatch")

        manifest = {
            "schema_version": PARQUET_SCHEMA_VERSION,
            "manifest_version": "3.0",
            "batch_id": week.id,
            "kind": batch["kind"],
            "stage": "V1",
            "config_hash": batch["config_hash"],
            "protocol": week.record(),
            "source_coverage": coverage,
            "source_inventory": inventory,
            "source_event_count": inventory["rows"],
            "exclusions": exclusions,
            "selection": select_events((e["payload"] for e in self.iter_events(week.id, ("collected",))), week),
            "benchmark_status": "awaiting_v2",
            "collection_implementation": provenance,
            "freeze_implementation": implementation_provenance(),
            "writer": {
                "compression": "zstd",
                "level": 3,
                "content_defined_chunking": True,
                "row_group_rows": self.row_group_rows,
                "row_group_bytes": self.row_group_bytes,
                "shard_bytes": self.shard_bytes,
            },
            "frozen_at": stamp(actual_freeze),
            "scheduled_freeze_at": stamp(week.freeze_at),
            "schedule_compliant": batch["kind"] == "formal",
            "construction_complete": not incomplete,
            "created_at": stamp(now),
            "files": files,
            "counts": {t: sum(f["rows"] for f in files if f["table"] == t) for t in SCHEMAS},
            "discovery_complete": True,
            "candidate_count": sum(states.values()),
            "comment_coverage": self.comment_coverage(week.id),
            "execution_failures": self.failures(week.id),
            "stages": {"V2": "not_configured", "V4": "not_configured"},
        }
        self.verify(manifest)
        atomic_json(manifest_path, manifest)
        self.db.execute("UPDATE batches SET frozen=true WHERE id=?", [week.id])
        self.activate(manifest_path, manifest)
        return manifest

    def write_source_inventory(self, batch_id: str, destination: Path) -> tuple[Record, Record]:
        """逐行保存完整来源分母，避免百万条排除记录挤入清单和内存。"""
        from .domains import PRIMARY_DOMAINS, event_group, grouping_record

        target = destination / "source-inventory.jsonl"
        temporary = target.with_suffix(".partial")
        total = excluded = 0
        reasons: Counter[str] = Counter()
        domains: Counter[str] = Counter()
        by_domain: dict[str, Counter[str]] = defaultdict(Counter)
        screening: dict[str, Counter[str]] = defaultdict(Counter)
        with temporary.open("w", encoding="utf-8") as output:
            for entry in self.iter_events(batch_id):
                source = entry["payload"]
                group = event_group(source)
                total += 1
                domains[group] += 1
                date_status = source.get("date_window_status")
                screening[group][
                    "date_passed"
                    if date_status == "passed"
                    else "date_unknown"
                    if date_status is None
                    else "date_excluded"
                ] += 1
                screening[group][
                    "native_unresolved_verified"
                    if source.get("native_unresolved_verified") is True
                    else "native_unresolved_unverified"
                ] += 1
                reason = entry["reason"]
                if entry["status"] != "collected":
                    excluded += 1
                    reasons[reason or entry["status"]] += 1
                    by_domain[group][reason or entry["status"]] += 1
                output.write(
                    dumps(
                        {
                            "event_id": entry["event_id"],
                            "status": entry["status"],
                            "reason": reason,
                            "primary_domain": group,
                            "raw_tag_ids": source.get("tag_ids"),
                            "source_hash": source.get("source_hash"),
                            "source_receipt_id": source.get("source_receipt_id"),
                            "observed_at": source.get("observed_at"),
                            "date_window_status": date_status,
                            "native_unresolved_verified": source.get("native_unresolved_verified"),
                        }
                    )
                    + "\n"
                )
            output.flush()
            os.fsync(output.fileno())
        temporary.replace(target)
        inventory = {
            "table": "source_inventory",
            "path": str(target.relative_to(self.root)),
            "rows": total,
            "bytes": target.stat().st_size,
            "sha256": file_hash(target),
            "by_primary_domain": {group: domains[group] for group in (*PRIMARY_DOMAINS, "Unclassified")},
            "grouping": grouping_record(),
            "screening": {
                group: {
                    key: screening[group][key]
                    for key in (
                        "date_passed",
                        "date_excluded",
                        "date_unknown",
                        "native_unresolved_verified",
                        "native_unresolved_unverified",
                    )
                }
                for group in (*PRIMARY_DOMAINS, "Unclassified")
            },
        }
        return inventory, {
            "count": excluded,
            "by_reason": dict(sorted(reasons.items())),
            "by_primary_domain": {group: dict(sorted(counts.items())) for group, counts in sorted(by_domain.items())},
            "inventory_path": inventory["path"],
        }

    def activate(self, path: Path, manifest: Record) -> None:
        pointer = self.root / "batches" / manifest["batch_id"] / "latest.json"
        if pointer.exists():
            previous = read_manifest(self.root / read_manifest(pointer)["manifest"])
            if previous["stage"] == "V3" and manifest["stage"] == "V1":
                return
            if previous["stage"] == manifest["stage"] and instant(previous["created_at"]) > instant(
                manifest["created_at"]
            ):
                return
        atomic_json(pointer, {"manifest": str(path.relative_to(self.root))})

    def release_results(self, week: Week, now: datetime) -> Record:
        week = self.week(week.id)
        if week.policy.read_only:
            raise ValueError("legacy_batch_read_only")
        base = read_manifest(self.root / "batches" / week.id / "V1" / "manifest.json")
        self.verify(base)
        release_path = self.root / "batches" / week.id / "benchmark" / "release.json"
        release = read_manifest(release_path) if release_path.exists() else None
        if release:
            week = Week.restore(release["protocol"])
        results = []
        for event in read_records(self.root, base, "events"):
            result = self.latest_resolution(week.id, event["event_id"])
            changed = result is None or (result.get("validity") == "pending" and now >= gt_deadline(event, week))
            if changed:
                result = settle_event(event, [], [], week, now, previous=result)
            assert result is not None
            if "protocol" not in base:
                result.update(validity="not_evaluated", eligibility="legacy_source_only", time=event["time"])
            if changed:
                self.add_resolution(week.id, result)
            results.append(result)
        failures = self.failures(week.id)
        from .benchmark import benchmark_artifacts

        benchmark_files = benchmark_artifacts(self, week.id)
        selected_ids = {member["event_id"] for member in release["selection"]["members"]} if release else set()
        selected_results = {row["event_id"]: row["validity"] for row in results if row["event_id"] in selected_ids}
        final_path = release_path.with_name("final.json")
        if final_path.exists():
            final = read_manifest(final_path)
            selected_results = {row["event_id"]: row["validity"] for row in final["questions"]}
            benchmark_status = "completed"
        elif not release:
            benchmark_status = "awaiting_v2"
        elif any(status not in {"valid", "invalid"} for status in selected_results.values()):
            benchmark_status = "awaiting_gt"
        else:
            benchmark_status = "awaiting_v4"
        collection_closed = now >= week.evidence_cutoff
        generation = digest([results, failures, benchmark_files, benchmark_status, collection_closed])
        destination = self.root / "batches" / week.id / f"V3-{generation}"
        if (destination / "manifest.json").exists():
            manifest = read_manifest(destination / "manifest.json")
            self.verify(manifest)
            self.activate(destination / "manifest.json", manifest)
            return manifest
        destination.mkdir(parents=True, exist_ok=True)
        files = [f for f in base["files"] if f["table"] != "resolutions"]
        files += self._write_table(destination, "resolutions", results)
        files += benchmark_files
        previous_fragments = []
        pointer_path = self.root / "batches" / week.id / "latest.json"
        if pointer_path.exists():
            previous_manifest = read_manifest(self.root / read_manifest(pointer_path)["manifest"])
            previous_fragments = [
                f for f in previous_manifest["files"] if f["table"] == "evidence" and f.get("phase") == "V3"
            ]
            files += previous_fragments
        archive = self.root / "batches" / week.id / "raw-v3.sqlite"
        if archive.exists():
            from .source import evidence_snapshot

            target = destination / "evidence.sqlite"
            previous_fragment = max(previous_fragments, key=lambda f: f["source_request_rowid_end"], default=None)
            fragment = evidence_snapshot(archive, target, previous_fragment)
            if fragment:
                files.append(
                    {
                        "table": "evidence",
                        "phase": "V3",
                        "path": str(target.relative_to(self.root)),
                        "sha256": file_hash(target),
                        "bytes": target.stat().st_size,
                        "rows": 0,
                        **fragment,
                    }
                )
        unique_files: dict[str, Record] = {}
        for entry in files:
            previous_entry = unique_files.get(entry["path"])
            if previous_entry and previous_entry["sha256"] != entry["sha256"]:
                raise ValueError("conflicting_manifest_file")
            if previous_entry and previous_entry["table"] in SCHEMAS and entry["table"] not in SCHEMAS:
                continue
            unique_files[entry["path"]] = entry
        files = list(unique_files.values())
        manifest = dict(
            base,
            stage="V3",
            created_at=stamp(now),
            files=files,
            parent_manifest=str((self.root / "batches" / week.id / "V1" / "manifest.json").relative_to(self.root)),
            parent_manifest_sha256=file_hash(self.root / "batches" / week.id / "V1" / "manifest.json"),
            counts=dict(base["counts"], resolutions=len(results)),
            execution_failures=failures,
            benchmark_status=benchmark_status,
            gt_collection_closed=collection_closed,
            benchmark_selection=release["selection"] if release else None,
            result_status={
                "valid_events": sum(r.get("validity") == "valid" for r in results),
                "invalid_events": sum(r.get("validity") == "invalid" for r in results),
                "pending_events": sum(r.get("validity") not in {"valid", "invalid"} for r in results),
                "all_events_terminal": all(r.get("validity") in {"valid", "invalid"} for r in results),
                "benchmark_status": benchmark_status,
                "selected_valid_events": sum(status == "valid" for status in selected_results.values()),
                "selected_invalid_events": sum(status == "invalid" for status in selected_results.values()),
                "selected_pending_events": sum(
                    status not in {"valid", "invalid"} for status in selected_results.values()
                ),
            },
        )
        self.verify(manifest)
        atomic_json(destination / "manifest.json", manifest)
        self.activate(destination / "manifest.json", manifest)
        return manifest

    def verify(self, manifest: Record) -> Record:
        counts = {t: 0 for t in SCHEMAS}
        for entry in manifest["files"]:
            path = (self.root / entry["path"]).resolve()
            if not path.is_relative_to(self.root) or not path.is_file():
                raise ValueError("manifest_path_invalid")
            if file_hash(path) != entry["sha256"]:
                raise ValueError(f"checksum_mismatch:{entry['path']}")
            if entry["table"] in SCHEMAS:
                metadata = pq.ParquetFile(path).metadata
                if metadata.num_rows != entry["rows"]:
                    raise ValueError("row_count_mismatch")
                counts[entry["table"]] += metadata.num_rows
        if manifest.get("counts") != counts:
            raise ValueError("manifest_counts_mismatch")
        return {"valid": True, "counts": counts}

    def status(self) -> list[Record]:
        return [
            self.batch(row[0])
            | {
                "event_states": self.db.execute(
                    "SELECT status,count(*) FROM events WHERE batch=? GROUP BY status ORDER BY status",
                    [row[0]],
                ).fetchall()
            }
            for row in self.db.execute("SELECT id FROM batches ORDER BY id").fetchall()
        ]
