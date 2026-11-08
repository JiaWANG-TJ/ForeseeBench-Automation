from datetime import UTC, datetime

import httpx
import pytest

from foreseebench.source import Polymarket


def test_comment_cursor_collects_beyond_offset_limit_and_keeps_replies(tmp_path):
    cursors = []

    def handler(request):
        assert request.url.path == "/comments/keyset"
        assert request.url.params["parent_entity_type"] == "Event"
        assert request.url.params["parent_entity_id"] == "42"
        assert "offset" not in request.url.params
        cursor = request.url.params.get("after_cursor")
        cursors.append(cursor)
        number = {None: 0, "page-one": 1, "page-two": 2, "page-three": 3}[cursor]
        rows = [{"id": str(number * 100 + i)} for i in range(1 if number == 3 else 100)]
        rows.append({"id": f"reply-{number}", "parentCommentID": rows[0]["id"]})
        data = {"comments": rows}
        if number < 3:
            data["next_cursor"] = ["page-one", "page-two", "page-three"][number]
        return httpx.Response(200, json=data)

    with Polymarket(tmp_path / "evidence.zip", transport_factory=lambda: httpx.MockTransport(handler), rate=1e9) as pm:
        pages = [list(p.rows) for p in pm.comments("42")]
    assert cursors == [None, "page-one", "page-two", "page-three"]
    assert len({r["id"] for page in pages for r in page}) == 305
    assert sum(bool(r.get("parentCommentID")) for page in pages for r in page) == 4


def test_short_trade_page_with_cursor_is_not_eof(tmp_path):
    seen = []

    def handler(request):
        seen.append(dict(request.url.params))
        cursor = request.url.params.get("cursor")
        return httpx.Response(
            200,
            json={
                "data": [{"transaction_hash": "same"}],
                "pagination": {
                    "has_more": cursor is None,
                    "next_cursor": "opaque" if cursor is None else None,
                },
            },
        )

    with Polymarket(tmp_path / "evidence.zip", transport_factory=lambda: httpx.MockTransport(handler), rate=1e9) as pm:
        pages = [list(p.rows) for p in pm.trades("42")]
    assert len(pages) == 2
    assert len([r for p in pages for r in p]) == 2
    assert seen[1]["filter_amount"] == "0.000001"
    assert seen[1]["event_id"] == "42"
    assert "start" not in seen[1]


def test_response_received_after_cutoff_is_rejected(tmp_path):
    clock = iter(
        [
            datetime(2026, 11, 2, 5, 59, tzinfo=UTC),
            datetime(2026, 11, 2, 5, 59, tzinfo=UTC),
            datetime(2026, 11, 2, 6, 0, tzinfo=UTC),
        ]
    )
    with Polymarket(
        tmp_path / "evidence.zip",
        rate=1e9,
        clock=lambda: next(clock),
        transport_factory=lambda: httpx.MockTransport(lambda _: httpx.Response(200, json=[])),
    ) as pm:
        import pytest

        with pytest.raises(TimeoutError, match="freeze_deadline"):
            list(pm.comments("42", deadline=datetime(2026, 11, 2, 6, tzinfo=UTC)))


def test_completed_response_is_durable_before_process_exit(tmp_path):
    import sqlite3
    import subprocess
    import sys
    import textwrap

    archive = tmp_path / "evidence.sqlite"
    script = textwrap.dedent("""
        import os, sys, httpx
        from pathlib import Path
        from foreseebench.source import Polymarket
        pm = Polymarket(Path(sys.argv[1]), rate=1e9,
            transport_factory=lambda: httpx.MockTransport(lambda _: httpx.Response(200, json={'comments':[]})))
        with pm.page('/comments/keyset', {}, 'comments.item') as page:
            list(page.rows)
        os._exit(0)
    """)
    subprocess.run([sys.executable, "-c", script, str(archive)], check=True)
    with sqlite3.connect(archive) as db:
        assert db.execute("SELECT count(*) FROM receipts").fetchone()[0] == 1
        assert db.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
    with Polymarket(
        archive,
        rate=1e9,
        transport_factory=lambda: httpx.MockTransport(lambda _: httpx.Response(200, json={"comments": []})),
    ) as pm:
        for page in pm.comments("42"):
            list(page.rows)
    with sqlite3.connect(archive) as db:
        assert db.execute("SELECT count(*) FROM receipts").fetchone()[0] == 2
        assert db.execute("SELECT count(*) FROM bodies").fetchone()[0] == 1


def test_missing_pagination_cursor_is_not_an_empty_completed_history(tmp_path):
    import pytest

    with Polymarket(
        tmp_path / "evidence.sqlite",
        rate=1e9,
        transport_factory=lambda: httpx.MockTransport(
            lambda _: httpx.Response(200, json={"data": [], "pagination": {"has_more": False}})
        ),
    ) as pm:
        with pytest.raises(ValueError, match="trade_pagination_missing"):
            list(pm.trades("42"))


@pytest.mark.parametrize("error_type", [httpx.RemoteProtocolError, httpx.ProxyError])
def test_remote_disconnect_is_retried_and_kept_in_audit_history(tmp_path, monkeypatch, error_type):
    import json
    import sqlite3

    import foreseebench.source as module

    monkeypatch.setattr(module.time, "sleep", lambda _: None)
    requests = []

    def handler(request):
        requests.append(request)
        if len(requests) == 1:
            raise error_type("disconnected", request=request)
        return httpx.Response(200, json={"comments": []})

    path = tmp_path / "audit.sqlite"
    with Polymarket(path, rate=1e9, transport_factory=lambda: httpx.MockTransport(handler)) as pm:
        for page in pm.comments("42"):
            assert list(page.rows) == []
    with sqlite3.connect(path) as db:
        receipts = [json.loads(row[0]) for row in db.execute("SELECT payload FROM receipts ORDER BY rowid")]
    assert [r["status"] for r in receipts] == [None, 200]
    assert receipts[0]["body_sha256"] is None
    assert receipts[0]["error"].startswith(error_type.__name__ + ":")


def test_parallel_discovery_preserves_the_inclusive_id_upper_boundary(tmp_path):
    from foreseebench.contract import Week

    week = Week.parse("2026-11-02")

    def handler(request):
        if request.url.path.startswith("/events/") and request.url.path != "/events/keyset":
            return httpx.Response(404, json={"error": "not found"})
        ids = request.url.params.get_list("id")
        rows = [{"id": "203"}] if not ids or "203" in ids else []
        return httpx.Response(200, json={"events": rows, "next_cursor": None})

    for workers in (1, 8, 13, 14, 16):
        with Polymarket(
            tmp_path / f"{workers}.sqlite",
            workers=workers,
            rate=1e9,
            transport_factory=lambda: httpx.MockTransport(handler),
        ) as pm:
            assert sum(len(list(page.rows)) for page in pm.discover(week)) == 1
            assert pm.discovery_status()["id_upper_bound"] == 203
            assert pm.discovery_status()["classified_ids"] == 203


@pytest.mark.parametrize(
    "response",
    [
        {},
        {"comments": None},
        {"comments": [], "next_cursor": ""},
        {"comments": [], "next_cursor": 7},
        {"comments": [], "next_cursor": "repeated"},
    ],
)
def test_invalid_comment_page_never_becomes_completed_empty_history(tmp_path, response):
    with Polymarket(
        tmp_path / "audit.sqlite",
        rate=1e9,
        transport_factory=lambda: httpx.MockTransport(lambda _: httpx.Response(200, json=response)),
    ) as pm:
        with pytest.raises(ValueError):
            list(pm.comments("42"))


@pytest.mark.parametrize("parent_type,parent_id", [("Series", 42), ("Event", 43)])
def test_comment_normalization_rejects_foreign_discussions(tmp_path, parent_type, parent_id):
    from foreseebench.pipeline import comment_rows

    with Polymarket(
        tmp_path / "audit.sqlite",
        rate=1e9,
        transport_factory=lambda: httpx.MockTransport(
            lambda _: httpx.Response(
                200,
                json={
                    "comments": [
                        {
                            "id": "1",
                            "parentEntityType": parent_type,
                            "parentEntityID": parent_id,
                        }
                    ]
                },
            )
        ),
    ) as pm:
        with pytest.raises(ValueError, match="comment_event_mismatch"):
            for page in pm.comments("42"):
                list(comment_rows(page, "42", None, pm))


@pytest.mark.parametrize("status,users", [(404, []), (200, [{"id": "first"}, {"id": "second"}])])
def test_missing_or_ambiguous_profile_does_not_invent_user_id(tmp_path, status, users):
    from foreseebench.pipeline import comment_rows

    calls = []

    def handler(request):
        if request.url.path == "/public-profile":
            calls.append(request.url.params["address"])
            return httpx.Response(status, json={"users": users})
        return httpx.Response(
            200,
            json={
                "comments": [
                    {"id": str(i), "userAddress": "0x" + "1" * 40, "profile": {"name": "原名"}} for i in (1, 2)
                ]
            },
        )

    with Polymarket(tmp_path / "audit.sqlite", rate=1e9, transport_factory=lambda: httpx.MockTransport(handler)) as pm:
        result = [row for page in pm.comments("42") for row in comment_rows(page, "42", None, pm)]
    assert len(calls) == 1
    assert all(row["user_id"] is None and row["profile"] == {"name": "原名", "bio": None} for row in result)


def test_nested_profile_failure_does_not_retry_the_parent_thread(tmp_path, monkeypatch):
    import foreseebench.source as module
    from foreseebench.pipeline import comment_rows

    monkeypatch.setattr(module.time, "sleep", lambda _: None)
    requests = []

    def handler(request):
        requests.append(request.url.path)
        if request.url.path == "/public-profile":
            raise httpx.ConnectError("profile unavailable", request=request)
        return httpx.Response(200, json=[{"id": "1", "userAddress": "0x" + "1" * 40}])

    with Polymarket(
        tmp_path / "audit.sqlite", rate=1e9, attempts=2, transport_factory=lambda: httpx.MockTransport(handler)
    ) as pm:
        with pytest.raises(httpx.ConnectError, match="profile unavailable"):
            with pm.comment_thread("1") as page:
                list(comment_rows(page, "42", None, pm))
    assert requests == ["/comments/1", "/public-profile", "/public-profile"]


def test_conflicting_duplicate_comments_fail_before_storage(tmp_path):
    from foreseebench.pipeline import prepared_comment_rows

    with Polymarket(
        tmp_path / "audit.sqlite",
        rate=1e9,
        transport_factory=lambda: httpx.MockTransport(
            lambda _: httpx.Response(
                200, json={"comments": [{"id": "1", "body": "first"}, {"id": "1", "body": "other"}]}
            )
        ),
    ) as pm:
        with pytest.raises(ValueError, match="conflicting_comment_duplicate"):
            for page in pm.comments("42"):
                with prepared_comment_rows(page, "42", None, pm) as records:
                    list(records)


def test_failed_proxy_tls_does_not_exhaust_later_retry_pool(tmp_path, monkeypatch):
    import socketserver
    import threading

    import foreseebench.source as module

    class ClosingProxy(socketserver.BaseRequestHandler):
        def handle(self):
            self.request.recv(8192)
            self.request.sendall(b"HTTP/1.1 200 Connection Established\r\n\r\n")

    original_client = httpx.Client
    monkeypatch.setattr(module.time, "sleep", lambda _: None)
    with socketserver.ThreadingTCPServer(("127.0.0.1", 0), ClosingProxy) as server:
        worker = threading.Thread(target=server.serve_forever, daemon=True)
        worker.start()

        def client(**kwargs):
            return original_client(
                **kwargs,
                proxy=f"http://127.0.0.1:{server.server_address[1]}",
                limits=httpx.Limits(max_connections=2),
            )

        monkeypatch.setattr(module.httpx, "Client", client)
        try:
            with Polymarket(
                tmp_path / "audit.sqlite", gamma_url="https://example.invalid", timeout=0.2, attempts=3, rate=1e9
            ) as pm:
                with pytest.raises(httpx.ConnectError):
                    list(pm.comments("42"))
        finally:
            server.shutdown()
            worker.join()


def test_retry_owns_a_fresh_injected_transport(tmp_path, monkeypatch):
    import foreseebench.source as module

    monkeypatch.setattr(module.time, "sleep", lambda _: None)
    instances = []
    calls = []

    class OwnedTransport(httpx.BaseTransport):
        closed = False

        def handle_request(self, request):
            if self.closed:
                raise RuntimeError("transport_already_closed")
            calls.append(request)
            if len(calls) == 1:
                raise httpx.ConnectError("retry", request=request)
            return httpx.Response(200, json={"comments": []})

        def close(self):
            self.closed = True

    def factory():
        transport = OwnedTransport()
        instances.append(transport)
        return transport

    with Polymarket(tmp_path / "audit.sqlite", attempts=2, rate=1e9, transport_factory=factory) as pm:
        assert [list(page.rows) for page in pm.comments("42")] == [[]]
    assert len(instances) == 2
    assert all(transport.closed for transport in instances)
