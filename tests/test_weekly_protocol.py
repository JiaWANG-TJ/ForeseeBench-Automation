from datetime import UTC, datetime

import pytest
from test_contract import raw_event

from foreseebench.contract import Week, admit_event, instant, settle_event


def test_weekly_calendar_accepts_old_open_events_and_preserves_every_market():
    week = Week.parse("2026-11-09")
    assert week.publish_at == datetime(2026, 11, 10, 17, tzinfo=UTC)
    assert week.prediction_cutoff == datetime(2026, 11, 13, 17, tzinfo=UTC)
    assert week.end_max == datetime(2026, 11, 18, 17, tzinfo=UTC)
    event = raw_event()
    event["startDate"] = "2018-01-01T08:00:00+08:00"
    event["markets"][0]["endDate"] = "2026-11-14T20:00:00-05:00"
    event["markets"].append(
        dict(
            event["markets"][0],
            id="10",
            conditionId="0x" + "b" * 64,
            clobTokenIds='["200","201"]',
            endDate="2026-11-18T04:59:00Z",
        )
    )
    source = admit_event(event, week, week.open_at)
    assert source["time"] == {
        "start_at": "2018-01-01T00:00:00.000000Z",
        "end_at": "2026-11-18T04:59:00.000000Z",
    }
    assert len(source["markets"]) == 2
    event["markets"][1]["endDate"] = "2026-11-18T17:00:01Z"
    with pytest.raises(ValueError, match="market_end_outside_window"):
        admit_event(event, week, week.open_at)
    event["markets"][0].update(endDate="2026-11-14T00:00:00Z", gameStartTime="2026-11-12 23:00:00+00")
    event["markets"][1]["endDate"] = "2026-11-18T04:59:00Z"
    assert admit_event(event, week, week.open_at)["markets"][0]["game_start_at"] == "2026-11-12T23:00:00.000000Z"


def test_submission_boundary_uses_the_equivalent_utc_time():
    week = Week.parse("2026-11-09")
    event = raw_event()
    event["markets"][0]["endDate"] = "2026-11-14T01:00:00+08:00"
    with pytest.raises(ValueError, match="market_end_outside_window"):
        admit_event(event, week, week.open_at)


def token_pair(market, yes=None):
    return [
        {
            "token_id": o["token_id"],
            "condition_id": market["condition_id"],
            "module": "v1_ctf",
            "outcome_index": i,
            "opposite_token_id": market["outcomes"][1 - i]["token_id"],
            "resolved": yes is not None,
            "final_price": (yes if i == 0 else 1 - yes) if yes is not None else None,
        }
        for i, o in enumerate(market["outcomes"])
    ]


def test_missing_optional_resolution_time_does_not_destroy_verified_payouts():
    week = Week.parse("2026-11-02")
    source = admit_event(raw_event(), week, week.open_at)
    m = source["markets"][0]
    pending = settle_event(
        source,
        [{"condition_id": m["condition_id"], "status": "posed"}],
        token_pair(m),
        week,
        instant("2026-11-06T18:00:00Z"),
    )
    result = settle_event(
        source,
        [{"condition_id": m["condition_id"], "status": "resolved"}],
        token_pair(m, 1),
        week,
        instant("2026-11-06T23:00:00Z"),
        previous=pending,
    )
    assert result["status"] == "verified"
    assert result["validity"] == "valid"
    assert result["markets"][0]["winner"] == "Yes"
    assert result["markets"][0]["resolved_at"] is None
    assert result["time"]["end_at"] == "2026-11-06T20:00:00.000000Z"
    assert result["gt_deadline_at"] == "2026-11-11T17:00:00.000000Z"


def test_unknown_winning_option_is_never_inferred_and_late_gt_cannot_revive_event():
    week = Week.parse("2026-11-02")
    raw = raw_event()
    raw["markets"] = [
        dict(
            raw["markets"][0],
            id=str(i),
            conditionId="0x" + str(i) * 64,
            groupItemTitle=name,
            clobTokenIds=f'["{i * 10}","{i * 10 + 1}"]',
        )
        for i, name in enumerate(("A", "B", "C"), 1)
    ]
    source = admit_event(raw, week, week.open_at)
    lifecycle = [
        {
            "condition_id": m["condition_id"],
            "status": "resolved" if i < 2 else "posed",
            **({"resolved_at": "2026-11-07T00:00:00Z"} if i < 2 else {}),
        }
        for i, m in enumerate(source["markets"])
    ]
    tokens = [row for i, m in enumerate(source["markets"]) for row in token_pair(m, 0 if i < 2 else None)]
    pending = settle_event(source, lifecycle, tokens, week, instant("2026-11-07T01:00:00Z"))
    assert [market["winner"] for market in pending["markets"]] == ["No", "No", None]
    assert pending["validity"] == "pending"
    expired = settle_event(source, lifecycle, tokens, week, instant("2026-11-11T17:00:01Z"), previous=pending)
    assert expired["validity"] == "invalid"
    assert expired["eligibility"] == "gt_missing_at_deadline"
    assert [market["winner"] for market in expired["markets"]] == ["No", "No", None]
    lifecycle[2].update(status="resolved", resolved_at="2026-11-12T01:00:00Z")
    late = settle_event(
        source,
        lifecycle,
        tokens[:4] + token_pair(source["markets"][2], 1),
        week,
        instant("2026-11-12T02:00:00Z"),
        previous=expired,
    )
    assert [market["winner"] for market in late["markets"]] == ["No", "No", "Yes"]
    assert late["validity"] == "invalid"


def test_equivalent_utc_offsets_are_the_same_terminal_evidence():
    week = Week.parse("2026-11-02")
    source = admit_event(raw_event(), week, week.open_at)
    m = source["markets"][0]
    life = {"condition_id": m["condition_id"], "status": "resolved", "resolved_at": "2026-11-06T20:00:00Z"}
    first = settle_event(source, [life], token_pair(m, 1), week, instant("2026-11-06T22:00:00Z"))
    second = settle_event(
        source,
        [dict(life, resolved_at="2026-11-07 04:00:00+08:00")],
        token_pair(m, 1),
        week,
        instant("2026-11-07T01:00:00Z"),
        previous=first,
    )
    assert second["status"] == "verified"
    assert second["markets"][0]["observed_at"] == "2026-11-06T22:00:00.000000Z"


def test_balanced_selection_keeps_scarce_domains_and_the_complete_source_pool():
    from collections import Counter

    from foreseebench.benchmark import select_events

    capacities = {"1": 2065, "2": 163, "120": 92, "22": 56, "315": 33, "21": 12, "84": 11}
    events = [
        {"event_id": f"{tag}-{i}", "source_id": f"source-{tag}-{i}", "tag_ids": [tag], "domain": [tag]}
        for tag, count in capacities.items()
        for i in range(count)
    ]
    selected = select_events(events, Week.parse("2026-11-09"))
    assert len(events) == 2432
    assert len({row["event_id"] for row in selected["members"]}) == 567
    assert Counter(row["group"] for row in selected["members"]) == {
        "Sports": 200,
        "PoliticsGovernance": 163,
        "FinanceMarkets": 92,
        "TechScience": 56,
        "Culture": 33,
        "Crypto": 12,
        "WeatherEnvironment": 11,
    }
    assert select_events(list(reversed(events)), Week.parse("2026-11-09")) == selected
    assert all(event["domain"] == event["tag_ids"] for event in events)
    assert selected["domain_event_cap"] == 200
    assert not {"target_events", "minimum_events", "size_status"} & selected.keys()


def test_balanced_selection_rotates_series_and_end_days_before_taking_more_repeated_events():
    from collections import Counter

    from foreseebench.benchmark import select_events
    from foreseebench.contract import Policy

    events = [
        {
            "event_id": f"{series}-{i}",
            "source_id": f"source-{series}-{i}",
            "tag_ids": ["1"],
            "series_ids": [series],
            "time": {"end_at": f"2026-11-{8 + i % 2:02d}T18:00:00Z"},
        }
        for series, count in (("league-a", 1000), ("league-b", 2))
        for i in range(count)
    ]
    week = Week.parse("2026-11-02", Policy(domain_event_cap=4))
    selected = select_events(events, week)
    by_id = {event["event_id"]: event for event in events}
    chosen = [by_id[row["event_id"]] for row in selected["members"]]
    assert Counter(row["series_ids"][0] for row in chosen) == {"league-a": 2, "league-b": 2}
    assert Counter(row["time"]["end_at"] for row in chosen) == {
        "2026-11-08T18:00:00Z": 2,
        "2026-11-09T18:00:00Z": 2,
    }
    assert select_events(list(reversed(events)), week) == selected


def test_balanced_selection_date_order_uses_seed_when_cap_truncates():
    from foreseebench.benchmark import select_events
    from foreseebench.contract import Policy

    events = [
        {
            "event_id": str(day),
            "source_id": f"source-{day}",
            "tag_ids": ["1"],
            "series_ids": ["league-a"],
            "time": {"end_at": f"2026-11-{day:02d}T18:00:00Z"},
        }
        for day in (8, 9, 10)
    ]
    selected_days = set()
    for seed in range(20261105, 20261125):
        week = Week.parse("2026-11-02", Policy(domain_event_cap=1, sampling_seed=seed))
        selected = select_events(events, week)
        assert select_events(list(reversed(events)), week) == selected
        assert selected["selected_count"] == 1
        selected_days.add(selected["members"][0]["end_day"])
    assert selected_days == {"2026-11-08", "2026-11-09", "2026-11-10"}


def test_batch_protocol_and_selected_members_survive_restart_without_dropping_raw_events(tmp_path):
    from foreseebench.store import Store, read_records

    week = Week.bootstrap(instant("2026-11-05T14:00:00Z"))
    raw = raw_event()
    raw["startDate"] = "2018-01-01T00:00:00Z"
    raw["tags"] = [{"id": 22, "label": "Tech"}]
    raw["series"] = [{"id": "series-7"}]
    raw["markets"][0]["resolutionSource"] = "https://example.org/official-results"
    raw["markets"][0]["endDate"] = "2026-11-13T00:00:00Z"
    source = admit_event(raw, week, instant("2026-11-05T15:00:00Z"))
    with Store(tmp_path) as store:
        store.begin(week, "bootstrap", "config")
        store.discovery_complete(week.id)
        store.event(week.id, "42", source, "collected")
        for kind in ("comments", "trades"):
            store.page(week.id, "42", kind, "empty", [], None, True)
    with Store(tmp_path) as store:
        assert store.week(week.id) == week
        frozen = store.freeze(store.week(week.id), instant("2026-11-05T16:00:00Z"))
        assert frozen["protocol"] == week.record()
        assert frozen["selection"]["selected_count"] == 1
        assert "size_status" not in frozen["selection"]
        event = next(read_records(tmp_path, frozen, "events"))
        assert event["tag_ids"] == ["22"]
        assert event["series_ids"] == ["series-7"]
        assert event["markets"][0]["resolution_source"] == "https://example.org/official-results"
        assert event["time"]["end_at"] == "2026-11-13T00:00:00.000000Z"


def test_api_outage_at_event_deadline_invalidates_but_keeps_the_full_event(tmp_path):
    from foreseebench.pipeline import Pipeline
    from foreseebench.store import Store, read_records

    class Outage:
        def evidence(self, source, *, deadline=None):
            raise OSError("upstream_unavailable")

    week = Week.parse("2026-11-02")
    with Store(tmp_path) as store:
        store.begin(week, "bootstrap", "config")
        store.discovery_complete(week.id)
        source = admit_event(raw_event(), week, week.open_at)
        store.event(week.id, "42", source, "collected")
        for table in ("comments", "trades"):
            store.page(week.id, "42", table, "empty", [], None, True)
        frozen = store.freeze(week, instant("2026-11-02T07:00:00Z"))
        outage = Pipeline(store, Outage(), clock=lambda: instant("2026-11-11T16:59:59Z")).refresh(week.id)
        assert len(outage["failures"]) == 1
        report = Pipeline(store, Outage(), clock=lambda: instant("2026-11-11T17:00:00Z")).refresh(week.id)
        assert report["failures"] == []
        assert len(store.failures(week.id)) == 1
        assert report["updated"] == 1
        result = store.latest_resolution(week.id, "42")
        assert result["validity"] == "invalid"
        assert result["eligibility"] == "gt_missing_at_deadline"
        assert len(result["markets"]) == 1
        released = store.release_results(week, instant("2026-11-11T17:00:00Z"))
        assert released["counts"]["events"] == 1
        assert released["counts"]["resolutions"] == 1
        assert released["result_status"]["invalid_events"] == 1
        assert next(read_records(tmp_path, released, "resolutions"))["validity"] == "invalid"
        assert store.verify(frozen)["valid"] is True


def test_release_persists_expiry_and_reuses_snapshots_without_new_evidence(tmp_path):
    from foreseebench.store import Store

    week = Week.parse("2026-11-02")
    source = admit_event(raw_event(), week, week.open_at)
    market = source["markets"][0]
    life = {"condition_id": market["condition_id"], "status": "resolved"}
    with Store(tmp_path) as store:
        store.begin(week, "bootstrap", "config")
        store.discovery_complete(week.id)
        store.event(week.id, "42", source, "collected")
        for table in ("comments", "trades"):
            store.page(week.id, "42", table, "empty", [], None, True)
        store.freeze(week, week.freeze_at)
        pending = settle_event(source, [life], token_pair(market, 1), week, instant("2026-11-06T22:00:00Z"))
        assert pending["validity"] == "pending"
        store.add_resolution(week.id, pending)
        first = store.release_results(week, instant("2026-11-07T00:00:00Z"))
        second = store.release_results(week, instant("2026-11-07T01:00:00Z"))
        assert first == second
        store.release_results(week, instant("2026-11-11T17:00:00Z"))
        expired = store.latest_resolution(week.id, "42")
        assert expired["validity"] == "invalid"
        assert expired["eligibility"] == "timing_unverified_at_deadline"
        late = settle_event(
            source,
            [dict(life, resolved_at="2026-11-06T21:00:00Z")],
            token_pair(market, 1),
            week,
            instant("2026-11-12T00:00:00Z"),
            previous=expired,
        )
        assert late["validity"] == "invalid"
        assert late["markets"][0]["winner"] == "Yes"


def test_late_time_evidence_keeps_its_own_receipt_and_cannot_borrow_payout_time():
    week = Week.parse("2026-11-02")
    source = admit_event(raw_event(), week, week.open_at)
    market = source["markets"][0]
    condition = market["condition_id"]
    life = {"condition_id": condition, "status": "resolved"}
    first = settle_event(
        source,
        [life],
        token_pair(market, 1),
        week,
        instant("2026-11-07T00:00:00Z"),
        observations={condition: {"receipt_ids": ["payout-receipt"]}},
    )
    late = settle_event(
        source,
        [dict(life, resolved_at="2026-11-06T21:00:00Z")],
        token_pair(market, 1),
        week,
        instant("2026-11-12T00:00:00Z"),
        previous=first,
        observations={condition: {"receipt_ids": ["new-receipt"], "lifecycle_receipt_ids": ["time-receipt"]}},
    )
    assert late["validity"] == "invalid"
    assert late["eligibility"] == "late_evidence"
    assert late["gt_verified_at"] == "2026-11-07T00:00:00.000000Z"
    assert late["qualification_verified_at"] == "2026-11-12T00:00:00.000000Z"
    assert late["markets"][0]["receipt_ids"] == ["payout-receipt"]
    assert late["markets"][0]["resolved_at_receipt_ids"] == ["time-receipt"]


@pytest.mark.parametrize("known_from", ["lifecycle", "token", "partial_token"])
def test_known_native_results_before_submission_survive_split_response_times(known_from):
    week = Week.parse("2026-11-02")
    source = admit_event(raw_event(), week, week.open_at)
    market = source["markets"][0]
    condition = market["condition_id"]
    before = "2026-11-06T16:59:59Z"
    after = "2026-11-06T17:00:01Z"
    observations = {
        condition: {
            "observed_at": after,
            "lifecycle_observed_at": before if known_from == "lifecycle" else after,
            "token_observations": {"100": {"observed_at": before}, "101": {"observed_at": after}},
        }
    }
    early = settle_event(
        source,
        [{"condition_id": condition, "status": "proposed" if known_from == "lifecycle" else "posed"}],
        token_pair(market, None if known_from == "lifecycle" else 1)[: 1 if known_from == "partial_token" else 2],
        week,
        instant(after),
        observations=observations,
    )
    assert early["eligibility"] == "early_resolution"
    late = settle_event(
        source,
        [{"condition_id": condition, "status": "resolved", "resolved_at": "2026-11-06T21:00:00Z"}],
        token_pair(market, 1),
        week,
        instant("2026-11-06T22:00:00Z"),
        previous=early,
    )
    assert late["validity"] == "invalid"


def test_refresh_preserves_individual_response_times_across_the_submission_boundary(tmp_path):
    from foreseebench.pipeline import Pipeline
    from foreseebench.store import Store

    week = Week.parse("2026-11-02")
    source = admit_event(raw_event(), week, week.open_at)
    market = source["markets"][0]
    condition = market["condition_id"]

    class Native:
        def evidence(self, event, *, deadline=None):
            return (
                [{"condition_id": condition, "status": "proposed"}],
                token_pair(market),
                [
                    {
                        "request_id": "before",
                        "observed_at": "2026-11-06T16:59:59Z",
                        "url": f"https://data-api.polymarket.com/v2/resolutions?condition={condition}",
                    },
                    {
                        "request_id": "after",
                        "observed_at": "2026-11-06T17:00:01Z",
                        "url": "https://data-api.polymarket.com/v2/tokens?token_id=100,101",
                    },
                ],
            )

    with Store(tmp_path) as store:
        store.begin(week, "bootstrap", "config")
        store.discovery_complete(week.id)
        store.event(week.id, "42", source, "collected")
        for table in ("comments", "trades"):
            store.page(week.id, "42", table, "empty", [], None, True)
        store.freeze(week, week.freeze_at)
        Pipeline(store, Native(), clock=lambda: instant("2026-11-06T17:00:01Z")).refresh(week.id)
        result = store.latest_resolution(week.id, "42")
        assert result["eligibility"] == "early_resolution"
        assert result["markets"][0]["lifecycle_receipt_ids"] == ["before"]


def test_new_lifecycle_time_can_join_preserved_payouts_without_refetching_the_tokens():
    week = Week.parse("2026-11-02")
    source = admit_event(raw_event(), week, week.open_at)
    market = source["markets"][0]
    life = {"condition_id": market["condition_id"], "status": "resolved"}
    first = settle_event(source, [life], token_pair(market, 1), week, instant("2026-11-07T00:00:00Z"))
    result = settle_event(
        source,
        [dict(life, resolved_at="2026-11-06T21:00:00Z")],
        [],
        week,
        instant("2026-11-07T02:00:00Z"),
        previous=first,
    )
    assert result["validity"] == "valid"
    assert result["gt_verified_at"] == "2026-11-07T00:00:00.000000Z"
    assert result["qualification_verified_at"] == "2026-11-07T02:00:00.000000Z"


@pytest.mark.parametrize("first_token_failure", [True, False])
def test_partial_fetch_failure_and_repeated_receipts_preserve_known_early_information(tmp_path, first_token_failure):
    import httpx

    from foreseebench.pipeline import Pipeline
    from foreseebench.source import Polymarket
    from foreseebench.store import Store

    week = Week.parse("2026-11-02")
    source = admit_event(raw_event(), week, week.open_at)
    market = source["markets"][0]
    turn = {"second": False, "now": instant("2026-11-06T16:59:59Z")}

    def handler(request):
        final = turn["second"] and first_token_failure
        if request.url.path == "/v2/resolutions":
            lifecycle = {"condition_id": market["condition_id"], "status": "resolved" if final else "proposed"}
            if final:
                lifecycle["resolved_at"] = "2026-11-06T21:00:00Z"
            return httpx.Response(200, json={"data": [lifecycle]})
        if not turn["second"]:
            turn["now"] = instant("2026-11-06T17:00:01Z")
            if first_token_failure:
                return httpx.Response(503, json={"error": "temporary"})
        return httpx.Response(200, json={"data": token_pair(market, 1 if final else None)})

    with Store(tmp_path) as store:
        store.begin(week, "bootstrap", "config")
        store.discovery_complete(week.id)
        store.event(week.id, "42", source, "collected")
        for table in ("comments", "trades"):
            store.page(week.id, "42", table, "empty", [], None, True)
        store.freeze(week, week.freeze_at)
        with Polymarket(
            tmp_path / "batches" / week.id / "raw-v3.sqlite",
            attempts=1,
            rate=1e9,
            clock=lambda: turn["now"],
            transport_factory=lambda: httpx.MockTransport(handler),
        ) as pm:
            pipeline = Pipeline(store, pm, clock=lambda: turn["now"])
            pipeline.refresh(week.id)
            assert store.latest_resolution(week.id, "42")["eligibility"] == "early_resolution"
            first = store.release_results(week, turn["now"])
            turn.update(second=True, now=instant("2026-11-06T22:00:00Z"))
            second = pipeline.refresh(week.id)
            assert store.latest_resolution(week.id, "42")["validity"] == "invalid"
            if first_token_failure:
                assert store.latest_resolution(week.id, "42")["markets"][0]["winner"] == "Yes"
            else:
                assert second["updated"] == 0
                assert store.release_results(week, turn["now"]) == first
