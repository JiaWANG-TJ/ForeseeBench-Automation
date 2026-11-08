from datetime import UTC, datetime

import httpx
from test_contract import raw_event

from foreseebench.contract import Week
from foreseebench.pipeline import Pipeline
from foreseebench.source import Polymarket
from foreseebench.store import Store, read_records


def test_concurrent_bootstrap_constructs_every_candidate_and_preserves_fill_counts(tmp_path):
    count = 12
    now = datetime(2026, 11, 4, 18, tzinfo=UTC)
    week = Week.bootstrap(now)

    def event(i):
        data = raw_event()
        data["id"] = str(i)
        data["markets"][0].update(
            id=str(i),
            conditionId="0x" + f"{i:064x}",
            clobTokenIds=f'["{1000 + 2 * i}","{1001 + 2 * i}"]',
            endDate="2026-11-10T20:00:00Z",
        )
        return data

    def handler(request):
        path = request.url.path
        if path == "/events/keyset":
            ids = request.url.params.get_list("id")
            data = {"events": [event(i) for i in range(1, count + 1)] if ids else [event(count)]}
        elif path.startswith("/events/"):
            i = int(path.rsplit("/", 1)[1])
            data = event(i)
        elif path == "/comments/keyset":
            i = request.url.params["parent_entity_id"]
            data = {"comments": [{"id": "c" + i, "body": "原文", "createdAt": "2026-11-02T01:00:00Z"}]}
        elif path == "/v2/trades":
            i = int(request.url.params["event_id"])
            trade = {
                "condition_id": "0x" + f"{i:064x}",
                "token_id": str(1000 + 2 * i),
                "outcome": "Yes",
                "side": "BUY",
                "transaction_hash": "same",
                "timestamp": 1793577600,
                "price": 0.5,
                "size": 2,
            }
            data = {"data": [trade, trade], "pagination": {"has_more": False, "next_cursor": None}}
        elif path == "/v2/resolutions":
            data = {
                "data": [{"condition_id": c, "status": "posed"} for c in request.url.params["condition"].split(",")]
            }
        elif path == "/v2/tokens":
            data = {
                "data": [
                    {
                        "token_id": token,
                        "condition_id": "0x" + f"{(int(token) - 1000) // 2:064x}",
                        "resolved": False,
                        "module": "v1_ctf",
                        "outcome_index": int(token) % 2,
                        "opposite_token_id": str(int(token) ^ 1),
                    }
                    for token in request.url.params["token_id"].split(",")
                ]
            }
        else:
            raise AssertionError(path)
        return httpx.Response(200, json=data)

    with Store(tmp_path) as store:
        with Polymarket(
            tmp_path / "raw.sqlite",
            workers=4,
            rate=1e9,
            clock=lambda: now,
            transport_factory=lambda: httpx.MockTransport(handler),
        ) as pm:
            result = Pipeline(store, pm, clock=lambda: now).collect(week, "bootstrap", "config")
            assert result["construction_complete"] is True
            assert result["collected"] == count
        manifest = store.freeze(week, now)
        assert manifest["candidate_count"] == count
        assert manifest["counts"]["trades"] == 2 * count
        assert manifest["counts"]["comments"] == count
        assert len(list(read_records(tmp_path, manifest, "events"))) == count


def test_completed_formal_streams_are_replaced_by_a_fresh_complete_scan(tmp_path):
    turn = {"value": 1}
    now = datetime(2026, 11, 2, 18, tzinfo=UTC)
    raw = raw_event()
    condition = raw["markets"][0]["conditionId"]

    def handler(request):
        path = request.url.path
        if path == "/events/keyset":
            data = {"events": [raw], "next_cursor": None}
        elif path == "/events/42":
            data = raw
        elif path.startswith("/events/"):
            return httpx.Response(404, json={"error": "not found"})
        elif path == "/comments/keyset":
            comments = [
                {
                    "id": "1",
                    "body": "旧文" if turn["value"] == 1 else "编辑后的原文",
                    "createdAt": "2026-11-02T01:00:00Z",
                }
            ]
            if turn["value"] == 2:
                comments.append(
                    {"id": "2", "parentCommentID": "1", "body": "新回复", "createdAt": "2026-11-02T01:30:00Z"}
                )
            data = {"comments": comments}
        elif path == "/v2/trades":
            row = {
                "condition_id": condition,
                "token_id": "100",
                "outcome": "Yes",
                "side": "BUY",
                "price": 0.5,
                "size": 2,
                "timestamp": 1793577600,
                "transaction_hash": "same",
            }
            data = {"data": [row] * turn["value"], "pagination": {"has_more": False, "next_cursor": None}}
        elif path == "/v2/resolutions":
            data = {"data": [{"condition_id": condition, "status": "posed"}]}
        else:
            data = {
                "data": [
                    {
                        "token_id": str(i),
                        "condition_id": condition,
                        "resolved": False,
                        "module": "v1_ctf",
                        "outcome_index": i - 100,
                        "opposite_token_id": str(201 - i),
                    }
                    for i in (100, 101)
                ]
            }
        return httpx.Response(200, json=data)

    with Store(tmp_path) as store:
        with Polymarket(
            tmp_path / "raw.sqlite", rate=1e9, clock=lambda: now, transport_factory=lambda: httpx.MockTransport(handler)
        ) as pm:
            pipeline = Pipeline(store, pm, clock=lambda: now)
            pipeline.collect("2026-11-02", "formal", "config")
            turn["value"] = 2
            pipeline.collect("2026-11-02", "formal", "config")
        manifest = store.freeze(Week.parse("2026-11-02"), Week.parse("2026-11-02").freeze_at)
        assert manifest["counts"]["trades"] == 2
        assert [r["body"] for r in read_records(tmp_path, manifest, "comments")] == ["编辑后的原文", "新回复"]
