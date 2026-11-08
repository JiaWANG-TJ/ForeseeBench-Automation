from datetime import timedelta

import pytest
from test_contract import raw_event

from foreseebench.contract import Week, admit_event, instant, settle_event
from foreseebench.pipeline import Pipeline
from foreseebench.store import Store, atomic_json, read_manifest


def test_weekly_cycle_reserves_three_days_to_answer_and_five_days_for_gt():
    week = Week.parse("2026-11-09")
    record = week.record()
    assert record["protocol_version"] == "3.0"
    assert week.collection_start == instant("2026-11-09T17:00:00Z")
    assert week.freeze_at == instant("2026-11-10T11:00:00Z")
    assert week.publish_at == instant("2026-11-10T17:00:00Z")
    assert week.prediction_cutoff == instant("2026-11-13T17:00:00Z")
    assert week.end_max == instant("2026-11-18T17:00:00Z")
    assert week.evidence_cutoff == instant("2026-11-18T17:00:00Z")
    assert week.finalize_at == instant("2026-11-19T17:00:00Z")
    assert week.finalize_at - week.collection_start == timedelta(days=10)
    assert record["gt_deadline_at"] == "2026-11-18T17:00:00.000000Z"
    assert record["policy"]["domain_event_cap"] == 200
    assert not {"target_events", "minimum_events"} & record["policy"].keys()
    assert Week.restore(record).record() == record


@pytest.mark.parametrize("end", ["2026-11-06T20:00:00Z", "2026-11-11T16:00:00Z"])
def test_all_events_share_the_batch_gt_deadline(end):
    week = Week.parse("2026-11-02")
    raw = raw_event()
    raw["markets"][0]["endDate"] = end
    source = admit_event(raw, week, week.freeze_at)
    waiting = settle_event(source, [], [], week, instant("2026-11-09T12:00:00Z"))
    assert waiting["validity"] == "pending"
    assert waiting["gt_deadline_at"] == "2026-11-11T17:00:00.000000Z"
    expired = settle_event(source, [], [], week, instant("2026-11-11T17:00:00Z"), previous=waiting)
    assert expired["eligibility"] == "gt_missing_at_deadline"
    assert len(expired["markets"]) == 1


@pytest.mark.parametrize("start", [None, "not-a-date"])
def test_start_metadata_never_excludes_an_event_from_end_date_screening(start):
    raw = raw_event()
    raw["startDate"] = start
    raw["markets"][0]["gameStartTime"] = "2020-01-01T00:00:00Z"
    source = admit_event(raw, Week.parse("2026-11-02"), instant("2026-11-03T10:00:00Z"))
    assert source["time"]["start_at"] is None
    assert source["markets"][0]["game_start_at"] == "2020-01-01T00:00:00.000000Z"
    assert source["time"]["end_at"] == "2026-11-06T20:00:00.000000Z"


def test_gt_deadline_closes_missing_results_without_more_upstream_requests(tmp_path):
    class Upstream:
        def evidence(self, source, *, deadline=None):
            raise AssertionError("截止后禁止再次请求原生GT")

    week = Week.parse("2026-11-02")
    source = admit_event(raw_event(), week, week.collection_start)
    with Store(tmp_path) as store:
        store.begin(week, "bootstrap", "config")
        store.discovery_complete(week.id)
        store.event(week.id, "42", source, "collected")
        for table in ("comments", "trades"):
            store.page(week.id, "42", table, "empty", [], None, True)
        frozen = store.freeze(week, week.freeze_at)
        pipeline = Pipeline(store, Upstream(), clock=lambda: instant("2026-11-11T17:00:00Z"))
        result = pipeline.refresh(week.id)
        assert result["failures"] == []
        assert store.latest_resolution(week.id, "42")["eligibility"] == "gt_missing_at_deadline"
        assert pipeline.refresh(week.id)["updated"] == 0
        assert store.verify(frozen)["valid"] is True


def test_formal_collection_waits_until_monday_17_utc(tmp_path):
    class Upstream:
        def discover(self, *args, **kwargs):
            raise AssertionError("正式采集窗口尚未开始")

    week = Week.parse("2026-11-09")
    with Store(tmp_path) as store:
        pipeline = Pipeline(store, Upstream(), clock=lambda: instant("2026-11-09T16:59:59Z"))
        with pytest.raises(ValueError, match="outside_formal_collection_window"):
            pipeline.collect(week, "formal", "config")
        assert store.status() == []


def test_primary_domains_count_each_event_once_and_report_all_domain_shortfalls():
    from foreseebench.benchmark import select_events

    cases = {
        "TechScience": ["22", "74", "107"],
        "WeatherEnvironment": ["84", "87"],
        "Crypto": ["21", "120"],
        "BusinessEarnings": ["1013"],
        "MacroEconomy": ["100328", "105174"],
        "FinanceMarkets": ["120"],
        "GeopoliticsSecurity": ["2", "100265"],
        "PoliticsGovernance": ["2"],
        "Culture": ["315"],
        "Sports": ["1"],
        "Unclassified": ["999999999"],
    }
    events = [
        {"event_id": group, "source_id": group, "tag_ids": tags, "domain": ["原始标签"]}
        for group, tags in cases.items()
    ]
    result = select_events(events, Week.parse("2026-11-09"))
    assert result["available_groups"] == dict.fromkeys(cases, 1)
    assert {row["event_id"]: row["group"] for row in result["members"]} == {group: group for group in cases}
    assert result["grouping"]["version"] == "native_tags_10_v1"
    assert "domain_coverage" not in result
    assert len(result["available_groups"]) == 11
    assert all(event["domain"] == ["原始标签"] for event in events)


def test_raw_source_publication_does_not_consume_a_formal_event_and_release_index_recovers(tmp_path):
    week = Week.parse("2026-11-02")
    source = admit_event(raw_event(), week, week.collection_start)
    with Store(tmp_path) as store:
        store.begin(week, "bootstrap", "config")
        store.discovery_complete(week.id)
        store.event(week.id, "42", source, "collected")
        for table in ("comments", "trades"):
            store.page(week.id, "42", table, "empty", [], None, True)
        store.freeze(week, week.freeze_at)
        assert store.previously_published("42", "2026-11-09") is False
    atomic_json(
        tmp_path / "batches" / week.id / "benchmark" / "release.json",
        {
            "batch_id": week.id,
            "selection": {"members": [{"event_id": "42"}]},
        },
    )
    with Store(tmp_path) as store:
        assert store.previously_published("42", "2026-11-09") is True
        assert store.previously_published("42", week.id) is False
        assert store.previously_published("43", "2026-11-09") is False


def test_scheduler_closes_the_gt_window_without_needing_a_network_client(tmp_path, monkeypatch):
    from datetime import UTC, datetime

    from foreseebench import cli

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return cls(2026, 11, 12, 18, tzinfo=UTC)

    week = Week.parse("2026-11-02")
    with Store(tmp_path) as store:
        store.begin(week, "bootstrap", "config")
        store.discovery_complete(week.id)
        store.event(week.id, "42", admit_event(raw_event(), week, week.collection_start), "collected")
        for table in ("comments", "trades"):
            store.page(week.id, "42", table, "empty", [], None, True)
        store.freeze(week, week.freeze_at)
    monkeypatch.setattr(cli, "datetime", Clock)
    config = {
        "storage": {"root": tmp_path},
        "scheduler": {"publish": False, "poll_seconds": 300},
        "publication": {},
        "config_hash": "config",
    }
    cli.tick(config)
    state = read_manifest(tmp_path / "scheduler.json")
    assert state["last_error"] is None
    assert state["gt_closed"][week.id]["deadline"] == "2026-11-11T17:00:00.000000Z"
    with Store(tmp_path) as store:
        assert store.latest_resolution(week.id, "42")["eligibility"] == "gt_missing_at_deadline"


def test_legacy_protocol_is_read_without_changing_frozen_dates():
    original = {
        "protocol_version": "2.0",
        "batch_id": "2026-11-02",
        "open_at": "2026-11-02T00:00:00.000000Z",
        "freeze_at": "2026-11-02T06:00:00.000000Z",
        "publication_at": "2026-11-03T00:00:00.000000Z",
        "submission_deadline": "2026-11-06T00:00:00.000000Z",
        "end_date_min": "2026-11-06T00:00:00.000000Z",
        "end_date_max": "2026-12-06T00:00:00.000000Z",
        "policy": {
            "freeze_hour": 6,
            "publish_offset_hours": 24,
            "submission_hours": 72,
            "ending_window_days": 30,
            "gt_grace_hours": 48,
            "target_events": 600,
            "minimum_events": 500,
            "sampling_seed": 20261105,
        },
    }
    restored = Week.restore(original)
    assert restored.record() == original
    assert restored.evidence_cutoff == instant("2026-12-08T00:00:00Z")


def test_previous_sampling_policy_is_read_only_without_rewriting_its_record(tmp_path):
    original = Week.parse("2026-11-02").record()
    original["policy"].pop("domain_event_cap")
    original["policy"].update(target_events=1000, minimum_events=500)
    restored = Week.restore(original)
    assert restored.record() == original
    with Store(tmp_path) as store, pytest.raises(ValueError, match="legacy_batch_read_only"):
        store.begin(restored, "bootstrap", "old-config")


def test_a_request_started_before_submission_cannot_prove_a_later_unresolved_state():
    from foreseebench.source import evidence_observations

    source = admit_event(raw_event(), Week.parse("2026-11-02"), instant("2026-11-03T10:00:00Z"))
    condition = source["markets"][0]["condition_id"]
    receipts = [
        {
            "request_id": "cross-boundary",
            "url": f"https://data-api.polymarket.com/v2/resolutions?condition={condition}",
            "requested_at": "2026-11-06T16:59:59Z",
            "observed_at": "2026-11-06T17:00:01Z",
        }
    ]
    observations = evidence_observations(
        source, [{"condition_id": condition, "status": "posed"}], [], receipts, instant("2026-11-06T17:00:01Z")
    )
    assert observations[condition]["unresolved_observed_at"] == "2026-11-06T16:59:59.000000Z"


def test_start_metadata_changes_do_not_change_the_question_structure_binding():
    from foreseebench.contract import source_binding

    week = Week.parse("2026-11-02")
    raw = raw_event()
    first = admit_event(raw, week, week.collection_start)
    raw["startDate"] = None
    raw["markets"][0]["gameStartTime"] = "2020-01-01T00:00:00Z"
    second = admit_event(raw, week, week.collection_start)
    assert first["source_hash"] != second["source_hash"]
    assert source_binding(first) == source_binding(second)


def test_unknown_lifecycle_and_inconsistent_payout_are_not_certified_unresolved():
    from test_weekly_protocol import token_pair

    from foreseebench.contract import require_unresolved

    week = Week.parse("2026-11-02")
    source = admit_event(raw_event(), week, week.collection_start)
    market = source["markets"][0]
    with pytest.raises(ValueError, match="unresolved_lifecycle_evidence_missing_or_invalid"):
        require_unresolved(
            source, [{"condition_id": market["condition_id"], "status": "unknown_future_state"}], token_pair(market)
        )
    tokens = token_pair(market)
    tokens[0]["final_price"] = 0
    life = [{"condition_id": market["condition_id"], "status": "posed"}]
    with pytest.raises(ValueError, match="unresolved_token_evidence_missing_or_invalid"):
        require_unresolved(source, life, tokens)
    state = settle_event(source, life, tokens, week, instant("2026-11-06T18:00:00Z"))
    assert state["markets"][0]["last_unresolved_at"] is None


def test_repeated_unresolved_polling_only_materializes_the_submission_boundary_proof(tmp_path):
    from test_weekly_protocol import token_pair

    now = [instant("2026-11-04T10:00:00Z")]
    week = Week.parse("2026-11-02")
    source = admit_event(raw_event(), week, week.collection_start)
    market = source["markets"][0]

    class Native:
        def evidence(self, event, *, deadline=None):
            receipts = [
                {
                    "request_id": str(now[0]),
                    "observed_at": now[0].isoformat(),
                    "requested_at": now[0].isoformat(),
                    "url": f"https://data-api.polymarket.com/v2/resolutions?condition={market['condition_id']}",
                },
                {
                    "request_id": "tokens-" + str(now[0]),
                    "observed_at": now[0].isoformat(),
                    "requested_at": now[0].isoformat(),
                    "url": "https://data-api.polymarket.com/v2/tokens?token_id=100,101",
                },
            ]
            return [{"condition_id": market["condition_id"], "status": "posed"}], token_pair(market), receipts

    with Store(tmp_path) as store:
        store.begin(week, "bootstrap", "config")
        store.discovery_complete(week.id)
        store.event(week.id, "42", source, "collected")
        for table in ("comments", "trades"):
            store.page(week.id, "42", table, "empty", [], None, True)
        store.freeze(week, week.freeze_at)
        pipeline = Pipeline(store, Native(), clock=lambda: now[0])
        assert pipeline.refresh(week.id)["updated"] == 1
        first = store.release_results(week, now[0])
        now[0] = instant("2026-11-05T10:00:00Z")
        assert pipeline.refresh(week.id)["updated"] == 0
        assert store.release_results(week, now[0]) == first
        now[0] = instant("2026-11-06T17:01:00Z")
        assert pipeline.refresh(week.id)["updated"] == 1
        boundary = store.release_results(week, now[0])
        now[0] = instant("2026-11-06T18:00:00Z")
        assert pipeline.refresh(week.id)["updated"] == 0
        assert store.release_results(week, now[0]) == boundary
        assert (
            store.latest_resolution(week.id, "42")["markets"][0]["last_unresolved_at"] == "2026-11-06T17:01:00.000000Z"
        )
