from datetime import timedelta

import httpx
import pytest

from foreseebench.contract import Week, instant
from foreseebench.pipeline import Pipeline
from foreseebench.source import Polymarket
from foreseebench.store import Store


def test_catalog_replay_resumes_after_committed_pages_without_downloading_the_source_again(tmp_path):
    now = [instant("2026-11-10T00:00:00Z")]
    week = Week.bootstrap(now[0])
    calls = []

    def source(request):
        calls.append(str(request.url))
        ids = request.url.params.get_list("id")
        return httpx.Response(200, json={"events": [{"id": value} for value in ids or ["201"]]})

    def elapsed_after_commit(progress):
        if progress.get("source_events", 0):
            now[0] += timedelta(seconds=2)

    reports = []
    for _ in range(4):
        with Store(tmp_path) as store, Polymarket(
            tmp_path / "raw.sqlite",
            clock=lambda: now[0],
            stop_at=now[0] + timedelta(seconds=1),
            rate=1e9,
            attempts=1,
            transport_factory=lambda: httpx.MockTransport(source),
        ) as pm:
            report = Pipeline(store, pm, clock=lambda: now[0]).collect(
                week, "bootstrap", "config", refresh_completed=False, progress=elapsed_after_commit
            )
            reports.append(report)
        if report["construction_complete"]:
            break

    assert reports[0]["yielded"]
    assert reports[-1]["construction_complete"]
    assert len(calls) == 4
    with Store(tmp_path) as store:
        assert store.batch(week.id)["scan_complete"]
        assert sum(store.event_counts(week.id).values()) == 201
        assert store.source_coverage(week.id)["events"] == 201


def test_failed_source_page_does_not_commit_rows_or_advance_the_replay_cursor(tmp_path):
    week = Week.bootstrap(instant("2026-11-10T00:00:00Z"))
    original = {"event_id": "1", "payload": {"question": "before"}, "status": "candidate", "reason": None}
    with Store(tmp_path) as store:
        store.begin(week, "bootstrap", "config")
        store.source_page(week.id, [original], seen_ids=["1"], source_pass="pass", receipt_id="b")
        with pytest.raises(ValueError, match="source_replay_out_of_order"):
            store.source_page(
                week.id,
                [dict(original, payload={"question": "after"}), dict(original, event_id="2")],
                seen_ids=["1", "2"],
                source_pass="pass",
                receipt_id="a",
            )
        assert store.source_replay(week.id, "pass") == {"request_id": "b", "source_count": 1}
        assert store.events(week.id) == [original]


def test_explicit_source_refresh_replays_every_event_under_its_new_pass(tmp_path):
    now = instant("2026-11-09T18:00:00Z")
    week = Week.parse("2026-11-09")
    title = ["before"]
    calls = []

    def source(request):
        calls.append(str(request.url))
        return httpx.Response(200, json={"events": [{"id": "1", "tags": [{"id": "1", "label": title[0]}]}]})

    with Store(tmp_path) as store, Polymarket(
        tmp_path / "raw.sqlite",
        clock=lambda: now,
        rate=1e9,
        attempts=1,
        transport_factory=lambda: httpx.MockTransport(source),
    ) as pm:
        pipeline = Pipeline(store, pm, clock=lambda: now)
        assert pipeline.collect(week, "formal", "config")["construction_complete"]
        title[0] = "after"
        assert pipeline.collect(week, "formal", "config")["construction_complete"]
        assert store.events(week.id)[0]["payload"]["domain"] == ["after"]
        assert store.source_coverage(week.id)["events"] == 1
    assert len(calls) == 4
