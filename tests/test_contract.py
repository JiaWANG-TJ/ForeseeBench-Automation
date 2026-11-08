from datetime import UTC, datetime, timedelta

import pytest

from foreseebench.contract import Week, admit_event, settle_event


def raw_event():
    return {
        "id": "42",
        "title": "原始事件",
        "description": "原始说明",
        "startDate": "2026-11-01T00:00:00Z",
        "tags": [{"label": "A"}, {"label": "B"}],
        "markets": [
            {
                "id": "9",
                "conditionId": "0x" + "a" * 64,
                "question": "原始条件",
                "description": "原始规则",
                "version": "v1",
                "outcomes": '["Yes","No"]',
                "clobTokenIds": '["100","101"]',
                "groupItemTitle": "",
                "endDate": "2026-11-06T20:00:00Z",
                "closed": False,
            }
        ],
    }


def test_entire_event_time_window_and_single_market_are_preserved():
    week = Week.parse("2026-11-02")
    source = admit_event(raw_event(), week, datetime(2026, 11, 2, 1, tzinfo=UTC))
    assert source["question"] == "原始事件"
    assert source["markets"][0]["group_item_title"] == ""
    assert source["domain"] == ["A", "B"]
    changed = raw_event()
    changed["markets"].append(
        dict(
            changed["markets"][0],
            id="10",
            conditionId="0x" + "b" * 64,
            endDate="2026-12-06T00:00:01Z",
        )
    )
    try:
        admit_event(changed, week, datetime(2026, 11, 2, 1, tzinfo=UTC))
    except ValueError as error:
        assert str(error) == "market_end_outside_window"
    else:
        raise AssertionError("越界的任一Market必须导致整个Event排除")


def test_missing_resolution_time_preserves_payout_but_does_not_invent_timing():
    week = Week.parse("2026-11-02")
    source = admit_event(raw_event(), week, week.open_at)
    condition = source["markets"][0]["condition_id"]
    lifecycle = [{"condition_id": condition, "status": "resolved", "last_update_timestamp": "1794100000"}]
    tokens = [
        {
            "token_id": "100",
            "condition_id": condition,
            "module": "v1_ctf",
            "outcome_index": 0,
            "opposite_token_id": "101",
            "resolved": True,
            "final_price": 1,
        },
        {
            "token_id": "101",
            "condition_id": condition,
            "module": "v1_ctf",
            "outcome_index": 1,
            "opposite_token_id": "100",
            "resolved": True,
            "final_price": 0,
        },
    ]
    result = settle_event(source, lifecycle, tokens, week, week.evidence_cutoff)
    assert "groundtruth" not in result
    assert result["markets"][0]["winner"] == "Yes"
    assert result["time"]["end_at"] == "2026-11-06T20:00:00.000000Z"
    assert result["status"] == "verified"
    assert result["validity"] == "invalid"
    assert result["markets"][0]["winner"] == "Yes"


@pytest.mark.parametrize(
    "start",
    [
        "2018-10-19T05:59:59Z",
        "2026-10-19T06:00:00Z",
        "2026-11-02T05:59:59Z",
        "2026-11-02T06:00:00Z",
    ],
)
def test_event_start_is_metadata_without_an_age_filter(start):
    raw = raw_event()
    raw["startDate"] = start
    assert admit_event(raw, Week.parse("2026-11-02"), datetime.now(UTC))


@pytest.mark.parametrize(
    "end,accepted",
    [
        ("2026-11-06T17:00:00Z", False),
        ("2026-11-06T17:00:01Z", True),
        ("2026-11-11T17:00:00Z", True),
        ("2026-11-11T17:00:01Z", False),
    ],
)
def test_all_market_end_boundaries(end, accepted):
    raw = raw_event()
    raw["markets"][0]["endDate"] = end
    if accepted:
        assert admit_event(raw, Week.parse("2026-11-02"), datetime.now(UTC))
    else:
        with pytest.raises(ValueError, match="market_end_outside_window"):
            admit_event(raw, Week.parse("2026-11-02"), datetime.now(UTC))


def test_single_market_native_labels_and_multiple_topics_are_not_semantically_filtered():
    raw = raw_event()
    raw["startDate"] = None
    raw["createdAt"] = "2026-11-01T08:00:00+08:00"
    raw["markets"][0]["outcomes"] = '["Up","Down"]'
    raw["markets"].append(
        dict(
            raw["markets"][0],
            id="10",
            conditionId="0x" + "b" * 64,
            question="另一个主题",
            groupItemTitle=None,
            negRisk=False,
        )
    )
    source = admit_event(raw, Week.parse("2026-11-02"), datetime.now(UTC))
    assert source["time"]["start_at"] == "2026-11-01T00:00:00.000000Z"
    assert len(source["markets"]) == 2
    assert source["markets"][0]["outcomes"][1]["label"] == "Down"


@pytest.mark.parametrize(
    "resolved,expected",
    [
        ("2026-11-06T17:00:00Z", "early_resolution"),
        ("2026-11-06T17:00:01Z", "in_window"),
        ("2026-11-11T17:00:01Z", "late_resolution"),
    ],
)
def test_resolution_time_boundaries(resolved, expected):
    week = Week.parse("2026-11-02")
    source = admit_event(raw_event(), week, week.open_at)
    condition = source["markets"][0]["condition_id"]
    lifecycle = [{"condition_id": condition, "status": "resolved", "resolved_at": resolved}]
    tokens = [
        {
            "token_id": str(100 + i),
            "condition_id": condition,
            "module": "v1_ctf",
            "outcome_index": i,
            "opposite_token_id": str(101 - i),
            "resolved": True,
            "final_price": 1 - i,
        }
        for i in range(2)
    ]
    observed = (
        week.evidence_cutoff + timedelta(seconds=2)
        if expected == "late_resolution"
        else datetime(2026, 11, 7, tzinfo=UTC)
    )
    result = settle_event(source, lifecycle, tokens, week, observed)
    assert result["eligibility"] == expected
    tokens[0]["final_price"] = tokens[1]["final_price"] = 0.5
    result = settle_event(source, lifecycle, tokens, week, observed)
    assert result["status"] == "fractional_or_invalid_payout"
    assert result["markets"][0]["winner"] is None


@pytest.mark.parametrize(
    "resolved,expected",
    [
        ("2026-11-06T17:00:00Z", "early_resolution"),
        ("2026-11-11T17:00:01Z", "late_resolution"),
    ],
)
def test_early_resolution_is_identified_even_when_token_evidence_is_missing(resolved, expected):
    week = Week.parse("2026-11-02")
    source = admit_event(raw_event(), week, week.open_at)
    result = settle_event(
        source,
        [
            {
                "condition_id": source["markets"][0]["condition_id"],
                "status": "resolved",
                "resolved_at": resolved,
            }
        ],
        [],
        week,
        week.evidence_cutoff + timedelta(seconds=2),
    )
    assert result["eligibility"] == expected
    assert result["markets"][0]["winner"] is None


@pytest.mark.parametrize(
    "version,selected,module",
    [
        ("v1", ["100", "101"], "v1_ctf"),
        ("v2", ["200", "201"], "binary"),
    ],
)
def test_protocol_version_selects_exact_assets_and_preserves_outcome_order(version, selected, module):
    week = Week.parse("2026-11-02")
    raw = raw_event()
    raw["markets"][0].update(version=version, positionIds=["200", "201"], outcomes='["No","Yes"]')
    source = admit_event(raw, week, week.open_at)
    market = source["markets"][0]
    assert [o["token_id"] for o in market["outcomes"]] == selected
    lifecycle = [{"condition_id": market["condition_id"], "status": "resolved", "resolved_at": "2026-11-07T00:00:00Z"}]
    tokens = [
        {
            "token_id": selected[i],
            "condition_id": market["condition_id"],
            "module": module,
            "outcome_index": i,
            "opposite_token_id": selected[1 - i],
            "resolved": True,
            "final_price": i,
        }
        for i in range(2)
    ]
    other = ["200", "201"] if version == "v1" else ["100", "101"]
    tokens.extend(dict(tokens[i], token_id=other[i], final_price=1 - i) for i in range(2))
    result = settle_event(source, lifecycle, tokens, week, datetime(2026, 11, 7, 1, tzinfo=UTC))
    assert result["markets"][0]["winner"] == "Yes"
    assert result["eligibility"] == "in_window"
    tokens.append(dict(tokens[0]))
    assert settle_event(source, lifecycle, tokens, week, week.evidence_cutoff)["status"] == "ambiguous_evidence"
    raw["markets"][0]["positionIds" if version == "v2" else "clobTokenIds"] = None
    with pytest.raises(ValueError, match="array_missing"):
        admit_event(raw, week, week.open_at)


@pytest.mark.parametrize("tags,expected", [(None, None), ([], []), ([{"label": "A"}, {"label": "A"}], ["A", "A"])])
def test_optional_domain_missingness_never_becomes_an_exclusion(tags, expected):
    raw = raw_event()
    raw["tags"] = tags
    source = admit_event(raw, Week.parse("2026-11-02"), datetime.now(UTC))
    assert source["domain"] == expected


@pytest.mark.parametrize(
    "second_observed,eligibility", [("2026-11-08T05:00:00Z", "in_window"), ("2026-11-11T18:00:00Z", "late_evidence")]
)
def test_partial_terminal_market_survives_missing_later_token_rows(second_observed, eligibility):
    from foreseebench.contract import instant

    week = Week.parse("2026-11-02")
    raw = raw_event()
    raw["markets"][0]["groupItemTitle"] = "A"
    raw["markets"].append(
        dict(raw["markets"][0], id="10", conditionId="0x" + "b" * 64, groupItemTitle="B", clobTokenIds='["200","201"]')
    )
    source = admit_event(raw, week, week.open_at)
    a, b = [m["condition_id"] for m in source["markets"]]

    def token_rows(condition, base, yes):
        return [
            {
                "condition_id": condition,
                "token_id": str(base + i),
                "outcome_index": i,
                "module": "v1_ctf",
                "opposite_token_id": str(base + 1 - i),
                "resolved": True,
                "final_price": yes if i == 0 else 1 - yes,
            }
            for i in range(2)
        ]

    first = settle_event(
        source,
        [{"condition_id": a, "status": "resolved", "resolved_at": "2026-11-06T20:00:00Z"}],
        token_rows(a, 100, 1),
        week,
        instant("2026-11-06T21:00:00Z"),
    )
    second = settle_event(
        source,
        [
            {"condition_id": a, "status": "resolved", "resolved_at": "2026-11-06T20:00:00Z"},
            {"condition_id": b, "status": "resolved", "resolved_at": "2026-11-07T20:00:00Z"},
        ],
        token_rows(b, 200, 0),
        week,
        instant(second_observed),
        previous=first,
    )
    assert second["status"] == "verified"
    assert [market["winner"] for market in second["markets"]] == ["Yes", "No"]
    assert second["eligibility"] == eligibility
    assert second["markets"][0]["observed_at"] == "2026-11-06T21:00:00.000000Z"


def test_repeated_conflicting_reply_cannot_erase_a_verified_terminal_fact():
    week = Week.parse("2026-11-02")
    raw = raw_event()
    raw["markets"][0]["groupItemTitle"] = "A"
    raw["markets"].append(
        dict(raw["markets"][0], id="10", conditionId="0x" + "b" * 64, groupItemTitle="B", clobTokenIds='["200","201"]')
    )
    source = admit_event(raw, week, week.open_at)
    a, b = [m["condition_id"] for m in source["markets"]]

    def tokens(c, base, yes):
        return [
            {
                "condition_id": c,
                "token_id": str(base + i),
                "outcome_index": i,
                "module": "v1_ctf",
                "opposite_token_id": str(base + 1 - i),
                "resolved": True,
                "final_price": yes if i == 0 else 1 - yes,
            }
            for i in range(2)
        ]

    ra = {"condition_id": a, "status": "resolved", "resolved_at": "2026-11-06T20:00:00Z"}
    rb = {"condition_id": b, "status": "resolved", "resolved_at": "2026-11-07T01:00:00Z"}
    first = settle_event(source, [ra], tokens(a, 100, 1), week, week.evidence_cutoff)
    second = settle_event(source, [ra], tokens(a, 100, 0), week, week.evidence_cutoff, previous=first)
    third = settle_event(
        source, [ra, rb], tokens(a, 100, 0) + tokens(b, 200, 1), week, week.evidence_cutoff, previous=second
    )
    assert third["status"] == "conflicting_evidence"
    assert third["markets"][0]["winner"] is None
    assert third["markets"][0]["previous_verified"]["winner"] == "Yes"


def test_fractional_payout_survives_missing_followup_and_conflicting_payout_is_retained():
    from test_weekly_protocol import token_pair

    from foreseebench.delivery import native_groundtruth

    week = Week.parse("2026-11-02")
    source = admit_event(raw_event(), week, week.open_at)
    market = source["markets"][0]
    lifecycle = [{"condition_id": market["condition_id"], "status": "resolved"}]
    now = datetime(2026, 11, 7, tzinfo=UTC)
    first = settle_event(source, lifecycle, token_pair(market, 0.5), week, now)
    second = settle_event(source, [], [], week, now + timedelta(hours=1), previous=first)
    result = native_groundtruth(source, second)
    assert result["status"] == "complete"
    assert result["markets"][0]["payouts"] == [0.5, 0.5]
    assert second["markets"][0]["observed_at"] == first["markets"][0]["observed_at"]
    changed = settle_event(source, lifecycle, token_pair(market, 1), week, now + timedelta(hours=2), previous=second)
    assert native_groundtruth(source, changed)["status"] == "conflict"
    assert [t["final_price"] for t in changed["markets"][0]["previous_verified"]["tokens"]] == [0.5, 0.5]
