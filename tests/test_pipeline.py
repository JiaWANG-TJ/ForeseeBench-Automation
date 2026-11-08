from datetime import UTC, datetime

import httpx
import pytest
from test_contract import raw_event

from foreseebench.pipeline import Pipeline
from foreseebench.source import Polymarket
from foreseebench.store import Store, read_records


def test_end_to_end_raw_source_freeze_and_settlement(tmp_path):
    raw = raw_event()
    condition = raw["markets"][0]["conditionId"]
    phase = {"settled": False, "profile_failure": True, "bad_parent": True}
    profile_calls = []
    address = "0x" + "1" * 40

    def comment(identifier, parent=None):
        return {
            "id": identifier,
            "parentEntityType": "Event",
            "parentEntityID": 42,
            "parentCommentID": parent,
            "body": identifier,
            "createdAt": "2026-11-02T08:30:00+08:00",
            "userAddress": address,
            "profile": {"name": "评论中的名字"} if identifier == "root" else {},
        }

    def handler(request):
        path = request.url.path
        if path == "/events/keyset":
            data = {"events": [raw], "next_cursor": None}
        elif path == "/events/42":
            data = raw
        elif path.startswith("/events/"):
            return httpx.Response(404, json={"error": "event not found"})
        elif path == "/comments/keyset":
            data = {
                "comments": [
                    comment("root"),
                    comment("reply", "root"),
                    comment("deep", "reply"),
                    comment("recoverable", "parent"),
                    comment("orphan", "deleted"),
                ]
            }
        elif path == "/comments/parent":
            data = [] if phase["bad_parent"] else [comment("parent")]
        elif path == "/comments/deleted":
            return httpx.Response(404, json={"error": "comment not found"})
        elif path == "/public-profile":
            profile_calls.append(request.url.params["address"])
            if phase["profile_failure"]:
                return httpx.Response(503, json={"error": "temporary failure"})
            import threading

            finished = threading.Event()
            worker = threading.Thread(
                target=lambda: (store.position("2026-11-02", "other", "comments"), finished.set())
            )
            worker.start()
            assert finished.wait(1), "profile_request_holds_store_lock"
            worker.join()
            data = {"proxyWallet": address, "users": [{"id": "user-42"}], "name": "账户名", "bio": "公开简介"}
        elif path == "/v2/trades":
            data = {
                "data": [
                    {
                        "transaction_hash": "same",
                        "condition_id": condition,
                        "token_id": "100",
                        "outcome": "Yes",
                        "side": "BUY",
                        "price": 0.5,
                        "size": 2,
                        "timestamp": 1793577600,
                    }
                ]
                * 2,
                "pagination": {"has_more": False, "next_cursor": None},
            }
        elif path == "/v2/resolutions":
            data = {
                "data": [
                    {
                        "condition_id": condition,
                        "status": "resolved" if phase["settled"] else "active",
                        "resolved_at": "2026-11-06T20:30:00Z" if phase["settled"] else None,
                    }
                ]
            }
        elif path == "/v2/tokens":
            data = {
                "data": [
                    {
                        "token_id": str(100 + i),
                        "condition_id": condition,
                        "module": "v1_ctf",
                        "outcome_index": i,
                        "opposite_token_id": str(101 - i),
                        "resolved": phase["settled"],
                        "final_price": 1 - i if phase["settled"] else None,
                    }
                    for i in range(2)
                ]
            }
        else:
            raise AssertionError(path)
        return httpx.Response(200, json=data)

    now = datetime(2026, 11, 2, 18, tzinfo=UTC)
    with Store(tmp_path) as store:
        with Polymarket(
            tmp_path / "batches/2026-11-02/raw-v1.sqlite",
            clock=lambda: now,
            rate=1e9,
            attempts=1,
            transport_factory=lambda: httpx.MockTransport(handler),
        ) as pm:
            pipeline = Pipeline(store, pm, clock=lambda: now)
            failed = pipeline.collect("2026-11-02", "formal", "config")
            assert failed["construction_complete"] is False
            assert store.event_status("2026-11-02", "42")["status"] == "failed"
            assert store.position("2026-11-02", "42", "comments")[1] is False
            phase["profile_failure"] = False
            incomplete_parent = pipeline.collect("2026-11-02", "formal", "config")
            assert incomplete_parent["construction_complete"] is False
            assert "comment_parent_not_returned" in store.event_status("2026-11-02", "42")["reason"]
            phase["bad_parent"] = False
            pipeline.collect("2026-11-02", "formal", "config")
        manifest = store.freeze(pipeline.week, datetime(2026, 11, 3, 11, tzinfo=UTC))
        assert manifest["counts"]["events"] == 1
        assert manifest["counts"]["trades"] == 2
        comments = {row["comment_id"]: row for row in read_records(tmp_path, manifest, "comments")}
        assert len(comments) == 6
        assert profile_calls == [address, address]
        assert len(manifest["execution_failures"]) == 2
        assert all(row["user_id"] == "user-42" for row in comments.values())
        assert comments["root"]["profile"]["name"] == "评论中的名字"
        assert comments["reply"]["profile"] == {"name": "账户名", "bio": "公开简介"}
        assert comments["reply"]["createdAt"] == "2026-11-02T00:30:00.000000Z"
        assert comments["reply"]["profile_observed_at"] == "2026-11-02T18:00:00.000000Z"
        assert manifest["comment_coverage"] == [
            {
                "event_id": "42",
                "complete": True,
                "total": 6,
                "roots": 2,
                "replies": 4,
                "orphans": 1,
            }
        ]
        phase["settled"] = True
        now = datetime(2026, 11, 7, 1, tzinfo=UTC)
        with Polymarket(
            tmp_path / "batches/2026-11-02/raw-v3.sqlite",
            clock=lambda: now,
            rate=1e9,
            transport_factory=lambda: httpx.MockTransport(handler),
        ) as pm:
            Pipeline(store, pm, clock=lambda: now).refresh("2026-11-02")
        result = store.latest_resolution("2026-11-02", "42")
        assert result["markets"][0]["winner"] == "Yes"
        assert result["eligibility"] == "in_window"
        v3 = store.release_results(pipeline.week, datetime(2026, 11, 8, 7, tzinfo=UTC))
        assert [f for f in manifest["files"] if f["table"] in {"trades", "comments"}] == [
            f for f in v3["files"] if f["table"] in {"trades", "comments"}
        ]
        with pytest.raises(ValueError, match="batch_already_frozen"):
            store.reset_event("2026-11-02", "42")


def test_later_collection_pass_discovers_new_events_and_rechecks_closed_events(tmp_path):
    turn = {"number": 1}
    now = datetime(2026, 11, 2, 18, tzinfo=UTC)

    def handler(request):
        path = request.url.path
        if path == "/events/keyset":
            ids = ["42"] if turn["number"] == 1 else ["43"] if turn["number"] == 3 else ["42", "43"]
            if not request.url.params.get_list("id"):
                ids = ids[-1:]
            data = {"events": [dict(raw_event(), id=i, closed=i == "42" and turn["number"] == 2) for i in ids]}
        elif path.startswith("/events/"):
            data = raw_event()
            data["id"] = path.rsplit("/", 1)[1]
            if data["id"] not in {"42", "43"} or data["id"] == "42" and turn["number"] == 3:
                return httpx.Response(404, json={"error": "event not found"})
            data["closed"] = data["id"] == "42" and turn["number"] == 2
        elif path == "/comments/keyset":
            event_id = request.url.params["parent_entity_id"]
            data = {
                "comments": [
                    {
                        "id": f"comment-{event_id}",
                        "body": f"pass-{turn['number']}",
                        "parentEntityType": "Event",
                        "parentEntityID": event_id,
                    }
                ]
            }
        elif path == "/v2/trades":
            data = {"data": [], "pagination": {"has_more": False, "next_cursor": None}}
        elif path == "/v2/resolutions":
            data = {"data": [{"condition_id": "0x" + "a" * 64, "status": "posed"}]}
        elif path == "/v2/tokens":
            data = {
                "data": [
                    {
                        "token_id": str(i),
                        "condition_id": "0x" + "a" * 64,
                        "resolved": False,
                        "module": "v1_ctf",
                        "outcome_index": i - 100,
                        "opposite_token_id": str(201 - i),
                    }
                    for i in (100, 101)
                ]
            }
        else:
            raise AssertionError(path)
        return httpx.Response(200, json=data)

    with Store(tmp_path) as store:
        with Polymarket(
            tmp_path / "raw.zip", clock=lambda: now, rate=1e9, transport_factory=lambda: httpx.MockTransport(handler)
        ) as pm:
            pipeline = Pipeline(store, pm, clock=lambda: now)
            pipeline.collect("2026-11-02", "formal", "config")
            turn["number"] = 2
            pipeline.collect("2026-11-02", "formal", "config")
            turn["number"] = 3
            pipeline.collect("2026-11-02", "formal", "config")
            assert store.event_status("2026-11-02", "42")["reason"] == "source_unavailable_in_latest_scan"
            turn["number"] = 4
            pipeline.collect("2026-11-02", "formal", "config")
        assert [e["event_id"] for e in store.events("2026-11-02", "collected")] == ["42", "43"]
        assert store.source_coverage("2026-11-02")["retained_events"] == 2
        manifest = store.freeze(pipeline.week, pipeline.week.freeze_at)
        assert {r["body"] for r in read_records(tmp_path, manifest, "comments")} == {"pass-4"}


def test_reserved_stages_preserve_received_state_and_bind_v2_output(tmp_path):
    from foreseebench.cli import receive_stage_result
    from foreseebench.contract import Week
    from foreseebench.pipeline import stage_task
    from foreseebench.store import atomic_json, file_hash, read_manifest

    week = Week.parse("2026-11-02")
    with Store(tmp_path) as store:
        store.begin(week, "qualification", "config")
        store.discovery_complete(week.id)
        store.freeze(week, week.freeze_at)
        task = stage_task(store, "V2", "batches/2026-11-02/V1/manifest.json")
        output = tmp_path / "fixture.json"
        atomic_json(output, {"test_fixture": True})
        artifact = tmp_path / "artifact.json"
        atomic_json(
            artifact,
            {
                "input_hash": task["input_hash"],
                "handler_version": "test-fixture",
                "files": [{"path": "fixture.json", "sha256": file_hash(output)}],
            },
        )
        received = receive_stage_result(store, task["task_id"], artifact)
        assert received["status"] == "awaiting_handler_validation"
        assert stage_task(store, "V2", task["input_manifest"])["status"] == "awaiting_handler_validation"
        store.release_results(week, week.evidence_cutoff)
        latest = read_manifest(tmp_path / "batches/2026-11-02/latest.json")["manifest"]
        v4 = stage_task(store, "V4", latest, v2_task_id=task["task_id"])
        assert v4["upstream_artifacts"]["V2"]["sha256"] == file_hash(artifact)
        assert v4["status"] == "not_configured"
