import httpx
import pytest
from test_contract import raw_event

from foreseebench.contract import Week, admit_event, digest, instant
from foreseebench.pipeline import Pipeline
from foreseebench.source import Polymarket
from foreseebench.store import Store, atomic_json, read_manifest


def test_source_catalog_keeps_all_states_versions_and_detail_only_events(tmp_path):
    archive = tmp_path / "source.sqlite"
    calls = []
    closed = {"id": "1", "closed": True, "markets": [{"version": "v1"}], "unknown_field": {"kept": True}}
    hidden = {"id": "2", "closed": False, "markets": [{"version": "v1"}], "tradingEligible": False}
    v2 = {"id": "4", "closed": False, "markets": [{"version": "v2", "outcomes": ["A", "B", "C"]}]}

    def handler(request):
        calls.append(str(request.url))
        if request.url.path == "/events/keyset":
            assert request.url.params.get_list("version") == ["v1", "v2"]
            assert request.url.params["closed"] == "any"
            assert not {"end_date_min", "end_date_max", "active", "archived", "tag_id"} & set(request.url.params)
            if not request.url.params.get_list("id"):
                return httpx.Response(200, json={"events": [v2]})
            assert request.url.params.get_list("id") == ["1", "2", "3", "4"]
            if request.url.params.get("after_cursor") is None:
                return httpx.Response(200, json={"events": [closed], "next_cursor": "opaque-next"})
            assert request.url.params["after_cursor"] == "opaque-next"
            return httpx.Response(200, json={"events": [v2]})
        if request.url.path == "/events/2":
            return httpx.Response(200, json=hidden)
        assert request.url.path == "/events/3"
        return httpx.Response(404, json={"error": "not found"})

    with Polymarket(archive, workers=2, rate=1e9, transport_factory=lambda: httpx.MockTransport(handler)) as pm:
        events = {str(row["id"]): row for page in pm.discover(Week.parse("2026-11-09")) for row in page.rows}
        coverage = pm.discovery_status()
    assert events == {"1": closed, "2": hidden, "4": v2}
    assert coverage["id_upper_bound"] == 4
    assert coverage["events"] == 3
    assert coverage["confirmed_404"] == 1
    assert coverage["complete"] is True
    request_count = len(calls)
    with Polymarket(archive, rate=1e9, transport_factory=lambda: httpx.MockTransport(handler)) as pm:
        replayed = {str(row["id"]): row for page in pm.discover(Week.parse("2026-11-09")) for row in page.rows}
    assert replayed == events
    assert len(calls) == request_count


def test_pipeline_archives_the_full_source_before_collecting_date_candidates(tmp_path):
    first = raw_event()
    first["id"] = "1"
    closed = dict(first, id="2", closed=True)
    future = dict(first, id="3", markets=[dict(first["markets"][0], endDate="2027-01-01T00:00:00Z")])
    condition = first["markets"][0]["conditionId"]

    def handler(request):
        if request.url.path == "/events/keyset":
            events = [first, closed, future] if request.url.params.get_list("id") else [future]
            return httpx.Response(200, json={"events": events})
        if request.url.path == "/events/1":
            return httpx.Response(200, json=first)
        if request.url.path == "/v2/resolutions":
            return httpx.Response(200, json={"data": [{"condition_id": condition, "status": "posed"}]})
        if request.url.path == "/v2/tokens":
            return httpx.Response(
                200,
                json={
                    "data": [
                        {
                            "token_id": str(100 + i),
                            "condition_id": condition,
                            "outcome_index": i,
                            "module": "v1_ctf",
                            "opposite_token_id": str(101 - i),
                            "resolved": False,
                        }
                        for i in range(2)
                    ]
                },
            )
        if request.url.path == "/comments/keyset":
            return httpx.Response(200, json={"comments": []})
        assert request.url.path == "/v2/trades"
        return httpx.Response(200, json={"data": [], "pagination": {"has_more": False, "next_cursor": None}})

    week = Week.parse("2026-11-02")
    with (
        Store(tmp_path) as store,
        Polymarket(
            tmp_path / "batches" / week.id / "raw-v1.sqlite",
            rate=1e9,
            workers=2,
            clock=lambda: instant("2026-11-02T18:00:00Z"),
            transport_factory=lambda: httpx.MockTransport(handler),
        ) as pm,
    ):
        result = Pipeline(store, pm, clock=lambda: instant("2026-11-02T18:00:00Z")).collect(week, "formal", "config")
        assert result["source_coverage"]["events"] == 3
        assert result["event_states"] == {"collected": 1, "rejected": 2}
        assert result["construction_complete"] is True
        assert store.event_status(week.id, "2")["reason"] == "event_already_closed"
        assert store.event_status(week.id, "3")["reason"] == "market_end_outside_window"
        manifest = store.freeze(week, week.freeze_at)
        assert manifest["source_coverage"]["complete"] is True
        assert manifest["source_coverage"]["events"] == 3
        assert manifest["counts"]["events"] == 1
        assert manifest["source_inventory"]["screening"]["Unclassified"] == {
            "date_passed": 2,
            "date_excluded": 1,
            "date_unknown": 0,
            "native_unresolved_verified": 1,
            "native_unresolved_unverified": 2,
        }
        assert {str(row["id"]) for page in pm.discover(week) for row in page.rows} == {"1", "2", "3"}


def test_freeze_keeps_every_excluded_source_in_a_separate_streamed_inventory(tmp_path):
    import json

    week = Week.parse("2026-11-02")
    with Store(tmp_path) as store:
        store.begin(week, "bootstrap", "config")
        store.source_page(
            week.id,
            [
                {
                    "event_id": str(i),
                    "payload": {"tag_ids": ["21"]},
                    "status": "rejected",
                    "reason": "market_end_outside_window",
                }
                for i in range(1, 2001)
            ],
        )
        store.discovery_complete(week.id, {"complete": True, "events": 2000, "confirmed_404": 0})
        manifest = store.freeze(week, week.freeze_at)
        assert "excluded" not in manifest
        assert manifest["source_event_count"] == 2000
        assert manifest["exclusions"]["count"] == 2000
        assert manifest["exclusions"]["by_reason"] == {"market_end_outside_window": 2000}
        assert manifest["source_inventory"]["rows"] == 2000
        path = tmp_path / manifest["source_inventory"]["path"]
        rows = [json.loads(line) for line in path.open()]
        assert {row["event_id"] for row in rows} == {str(i) for i in range(1, 2001)}
        assert {row["primary_domain"] for row in rows} == {"Crypto"}
        assert store.verify(manifest)["valid"] is True
        assert read_manifest(tmp_path / "batches" / week.id / "V1" / "manifest.json") == manifest


@pytest.mark.parametrize("rejection_at", ["discovery", "published", "detail", "final"])
def test_source_metadata_survives_validation_and_previous_publication(tmp_path, rejection_at):
    import json
    import sqlite3

    from test_weekly_protocol import token_pair

    week = Week.parse("2026-11-02")
    now = instant("2026-11-02T18:00:00Z")
    raw = dict(raw_event(), id="1", tags=[{"id": "21", "label": "Crypto"}])
    closed = dict(raw, closed=True, tags=[{"id": "120", "label": "Finance"}])
    market = admit_event(raw, week, now)["markets"][0]
    detail_calls = []
    archive = tmp_path / "batches" / week.id / "raw-v1.sqlite"
    if rejection_at == "published":
        atomic_json(
            tmp_path / "batches" / "2026-10-26" / "benchmark" / "release.json",
            {"batch_id": "2026-10-26", "selection": {"members": [{"event_id": "1"}]}},
        )

    def handler(request):
        path = request.url.path
        if path == "/events/keyset":
            data = {"events": [closed if rejection_at == "discovery" else raw]}
        elif path == "/events/1":
            detail_calls.append(path)
            data = closed if rejection_at == "detail" or rejection_at == "final" and len(detail_calls) == 2 else raw
        elif path == "/v2/resolutions":
            data = {"data": [{"condition_id": market["condition_id"], "status": "posed"}]}
        elif path == "/v2/tokens":
            data = {"data": token_pair(market)}
        elif path == "/comments/keyset":
            data = {"comments": []}
        else:
            assert path == "/v2/trades"
            data = {"data": [], "pagination": {"has_more": False, "next_cursor": None}}
        return httpx.Response(200, json=data)

    with (
        Store(tmp_path) as store,
        Polymarket(archive, rate=1e9, clock=lambda: now, transport_factory=lambda: httpx.MockTransport(handler)) as pm,
    ):
        result = Pipeline(store, pm, clock=lambda: now).collect(week, "formal", "config")
        published = rejection_at == "published"
        assert result["event_states"] == {"collected" if published else "rejected": 1}
        manifest = store.freeze(week, week.freeze_at)
        inventory = [json.loads(line) for line in (tmp_path / manifest["source_inventory"]["path"]).open()]
        assert len(inventory) == 1
        row = inventory[0]
        group = "Crypto" if published else "FinanceMarkets"
        reason = "event_already_closed"
        assert row["primary_domain"] == group
        assert row["raw_tag_ids"] == ["21" if published else "120"]
        assert row["source_hash"] == digest(raw if published else closed)
        assert row["observed_at"] == "2026-11-02T18:00:00.000000Z"
        assert manifest["exclusions"]["by_primary_domain"] == ({} if published else {group: {reason: 1}})
        with sqlite3.connect(archive) as db:
            receipt = db.execute(
                "SELECT payload FROM receipts WHERE request_id=?", [row["source_receipt_id"]]
            ).fetchone()
        assert receipt is not None
        expected_path = "/events/keyset" if rejection_at == "discovery" else "/events/1"
        assert expected_path in json.loads(receipt[0])["url"]
    assert len(detail_calls) == {"discovery": 0, "published": 2, "detail": 1, "final": 2}[rejection_at]


def test_rate_limit_wait_cannot_start_a_request_after_the_deadline(tmp_path, monkeypatch):
    import foreseebench.source as module

    now = [instant("2026-11-11T16:59:59Z")]
    cutoff = instant("2026-11-11T17:00:00Z")
    paths = []

    def handler(request):
        paths.append(request.url.path)
        return httpx.Response(200, json={"data": []})

    monkeypatch.setattr(module.time, "sleep", lambda _: now.__setitem__(0, cutoff))
    with Polymarket(
        tmp_path / "audit.sqlite", rate=1, clock=lambda: now[0], transport_factory=lambda: httpx.MockTransport(handler)
    ) as pm:
        with pm.page("/first", {}, "data.item") as page:
            assert list(page.rows) == []
        with pytest.raises(TimeoutError, match="freeze_deadline"):
            with pm.page("/second", {}, "data.item", deadline=cutoff):
                pass
    assert paths == ["/first"]


def test_discovery_failure_is_retained_as_a_batch_failure_and_blocks_freeze(tmp_path):
    week = Week.parse("2026-11-02")
    with (
        Store(tmp_path) as store,
        Polymarket(
            tmp_path / "audit.sqlite",
            rate=1e9,
            attempts=1,
            clock=lambda: instant("2026-11-02T18:00:00Z"),
            transport_factory=lambda: httpx.MockTransport(lambda _: httpx.Response(503, json={"error": "unavailable"})),
        ) as pm,
    ):
        with pytest.raises(httpx.HTTPStatusError):
            Pipeline(store, pm, clock=lambda: instant("2026-11-02T18:00:00Z")).collect(week, "formal", "config")
        failures = store.failures(week.id)
        assert len(failures) == 1
        assert failures[0]["event_id"] is None
        assert failures[0]["phase"] == "discovery"
        with pytest.raises(ValueError, match="discovery_incomplete"):
            store.freeze(week, week.freeze_at)


def test_empty_boundary_does_not_certify_an_empty_platform(tmp_path):
    with Polymarket(
        tmp_path / "audit.sqlite",
        rate=1e9,
        transport_factory=lambda: httpx.MockTransport(lambda _: httpx.Response(200, json={"events": []})),
    ) as pm:
        with pytest.raises(ValueError, match="source_upper_boundary_missing"):
            list(pm.discover(Week.parse("2026-11-09")))
        assert pm.discovery_status()["complete"] is False


def test_failed_source_range_resumes_without_refetching_completed_ranges(tmp_path):
    broken = [True]
    ranges = []

    def handler(request):
        if request.url.path != "/events/keyset":
            return httpx.Response(404, json={"error": "not found"})
        ids = request.url.params.get_list("id")
        if not ids:
            return httpx.Response(200, json={"events": [{"id": "101"}]})
        ranges.append(ids[0])
        if ids[0] == "101" and broken[0]:
            return httpx.Response(503, json={"error": "unavailable"})
        return httpx.Response(200, json={"events": [{"id": ids[0]}]})

    path = tmp_path / "audit.sqlite"
    with Polymarket(path, attempts=1, rate=1e9, transport_factory=lambda: httpx.MockTransport(handler)) as pm:
        with pytest.raises(httpx.HTTPStatusError):
            list(pm.discover(Week.parse("2026-11-09")))
        assert pm.discovery_status()["complete"] is False
        assert pm.discovery_status()["classified_ids"] == 100
    broken[0] = False
    with Polymarket(path, attempts=1, rate=1e9, transport_factory=lambda: httpx.MockTransport(handler)) as pm:
        assert {row["id"] for page in pm.discover(Week.parse("2026-11-09")) for row in page.rows} == {"1", "101"}
        assert pm.discovery_status()["complete"] is True
    assert ranges.count("1") == 1
    assert ranges.count("101") == 2


def test_result_evidence_is_archived_in_append_only_fragments(tmp_path):
    import sqlite3

    from test_weekly_protocol import token_pair

    week = Week.parse("2026-11-02")
    source = admit_event(raw_event(), week, week.collection_start)
    market = source["markets"][0]
    now = [instant("2026-11-04T10:00:00Z")]

    def handler(request):
        data = (
            [{"condition_id": market["condition_id"], "status": "posed"}]
            if request.url.path == "/v2/resolutions"
            else token_pair(market)
        )
        return httpx.Response(200, json={"data": data})

    with (
        Store(tmp_path) as store,
        Polymarket(
            tmp_path / "batches" / week.id / "raw-v3.sqlite",
            rate=1e9,
            clock=lambda: now[0],
            transport_factory=lambda: httpx.MockTransport(handler),
        ) as pm,
    ):
        store.begin(week, "bootstrap", "config")
        store.discovery_complete(week.id)
        store.event(week.id, "42", source, "collected")
        for table in ("comments", "trades"):
            store.page(week.id, "42", table, "empty", [], None, True)
        store.freeze(week, week.freeze_at)
        pipeline = Pipeline(store, pm, clock=lambda: now[0])
        pipeline.refresh(week.id)
        first = store.release_results(week, now[0])
        now[0] = instant("2026-11-06T17:01:00Z")
        pipeline.refresh(week.id)
        second = store.release_results(week, now[0])
        fragments = [f for f in second["files"] if f.get("phase") == "V3"]
        assert len(fragments) == 2
        assert [f["requests"] for f in fragments] == [2, 2]
        now[0] = instant("2026-11-06T18:00:00Z")
        assert pipeline.refresh(week.id)["updated"] == 0
        assert store.release_results(week, now[0]) == second
        now[0] = week.evidence_cutoff
        pipeline.refresh(week.id)
        closed = store.release_results(week, now[0])
        assert closed["gt_collection_closed"] is True
        assert len({f["path"] for f in closed["files"]}) == len(closed["files"])
        identifiers = []
        for file in closed["files"]:
            if file.get("phase") != "V3":
                continue
            with sqlite3.connect(tmp_path / file["path"]) as db:
                identifiers.extend(row[0] for row in db.execute("SELECT request_id FROM receipts"))
        assert len(identifiers) == len(set(identifiers)) == 6
        assert store.verify(first)["valid"] is True
        assert store.verify(second)["valid"] is True
