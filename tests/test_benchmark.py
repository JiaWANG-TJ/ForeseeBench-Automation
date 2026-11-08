from datetime import timedelta

import pytest
from test_contract import raw_event
from test_weekly_protocol import token_pair

from foreseebench.cli import receive_stage_result
from foreseebench.contract import Week, admit_event, dumps, instant
from foreseebench.pipeline import stage_task
from foreseebench.store import Store, atomic_json, file_hash


def prepare_v2(store, root):
    week = Week.parse("2026-11-02")
    raw = raw_event()
    raw["tags"] = [{"id": "2", "label": "Politics"}]
    raw["markets"][0]["endDate"] = "2026-11-11T00:00:00Z"
    source = admit_event(raw, week, week.open_at)
    store.begin(week, "bootstrap", "config")
    store.discovery_complete(week.id)
    store.event(week.id, "42", source, "collected")
    for table in ("comments", "trades"):
        store.page(week.id, "42", table, "empty", [], None, True)
    store.freeze(week, week.freeze_at)
    task = stage_task(store, "V2", "batches/2026-11-02/V1/manifest.json")
    output = root / "v2.jsonl"
    output.write_text(
        dumps(
            {
                "source_id": source["source_id"],
                "event_id": "42",
                "source_hash": source["source_hash"],
                "eligible": True,
                "reason": None,
                "review": {
                    "answer_available_after": "2026-11-11T00:00:00Z",
                    "evidence_urls": ["https://polymarket.com/event/test-fixture"],
                },
                "questions": [
                    {
                        "qa_id": "qa-42",
                        "question": "测试题干",
                        "answer_spec": {"type": "binary"},
                        "condition_ids": [source["markets"][0]["condition_id"]],
                    }
                ],
            }
        )
        + "\n"
    )
    artifact = root / "v2-manifest.json"
    atomic_json(
        artifact,
        {
            "stage": "V2",
            "input_hash": task["input_hash"],
            "handler_version": "test-fixture-v1",
            "files": [{"path": "v2.jsonl", "sha256": file_hash(output), "table": "questions"}],
        },
    )
    receive_stage_result(store, task["task_id"], artifact)
    return week, raw, source, task


def test_release_rejects_a_publication_that_would_exceed_the_whole_cycle_budget(tmp_path):
    from foreseebench.benchmark import release_benchmark, validate_stage_result

    class Native:
        def event(self, event_id):
            raise AssertionError("超过整批预算后不应开始发布核验请求")

    with Store(tmp_path) as store:
        _, _, _, task = prepare_v2(store, tmp_path)
        validate_stage_result(
            store,
            task["task_id"],
            validator=lambda *args: {
                "passed": True,
                "validator_version": "fixture",
                "evidence": ["fixture-only"],
            },
        )
        with pytest.raises(ValueError, match="publication_misses_cycle_budget"):
            release_benchmark(store, Native(), task["task_id"], clock=lambda: instant("2026-11-06T17:00:00Z"))


def test_delayed_release_gets_72_hours_and_submissions_bind_the_exact_manifest(tmp_path):
    from foreseebench.benchmark import accept_submission, release_benchmark, validate_stage_result

    with Store(tmp_path) as store:
        week, raw, source, task = prepare_v2(store, tmp_path)
        now = instant("2026-11-04T10:00:00Z")

        class Native:
            def event(self, event_id):
                return raw, {"observed_at": now.isoformat()}

            def evidence(self, event):
                return (
                    [{"condition_id": source["markets"][0]["condition_id"], "status": "posed"}],
                    token_pair(source["markets"][0]),
                    [{"request_id": "fixture-native"}],
                )

        with pytest.raises(ValueError, match="stage_handler_not_configured"):
            validate_stage_result(store, task["task_id"])
        with pytest.raises(ValueError, match="v2_not_validated"):
            release_benchmark(store, Native(), task["task_id"], clock=lambda: now)
        validate_stage_result(
            store,
            task["task_id"],
            validator=lambda *args: {"passed": True, "validator_version": "test-fixture", "evidence": ["fixture-only"]},
        )
        release = release_benchmark(store, Native(), task["task_id"], clock=lambda: now)
        assert release["protocol"]["publication_at"] == "2026-11-04T10:00:00.000000Z"
        assert release["protocol"]["submission_deadline"] == "2026-11-07T10:00:00.000000Z"
        path = tmp_path / "batches" / week.id / "benchmark/release.json"
        release_hash = file_hash(path)
        payload = {
            "release_sha256": release_hash,
            "v2_manifest_sha256": release["v2_manifest_sha256"],
            "predictions": {"qa-42": {"answer": "Yes", "confidence": 0.8}},
        }
        for confidence in (None, "0.8", True, -0.1, 1.1, float("nan"), float("inf")):
            invalid = dict(payload, predictions={"qa-42": {"answer": "Yes", "confidence": confidence}})
            with pytest.raises(ValueError, match="submission_confidence_invalid"):
                accept_submission(store, week.id, "participant", invalid, clock=lambda: now)
        with pytest.raises(ValueError, match="prediction_requires_answer_and_confidence"):
            accept_submission(
                store, week.id, "participant", dict(payload, predictions={"qa-42": "Yes"}), clock=lambda: now
            )
        received = accept_submission(store, week.id, "participant", payload, clock=lambda: now)
        assert received["received_at"] == "2026-11-04T10:00:00.000000Z"
        assert received["sequence"] == 1
        assert accept_submission(store, week.id, "participant", payload, clock=lambda: now)["sequence"] == 2
        import json
        import subprocess
        import sys

        config = tmp_path / "submission.toml"
        config.write_text(f'[storage]\nroot = "{tmp_path}"\n')
        request = tmp_path / "submission.json"
        atomic_json(request, payload)
        child = subprocess.run(
            [
                sys.executable,
                "-c",
                """
import sys
from datetime import UTC, datetime
from foreseebench import benchmark, cli
class Clock(datetime):
    @classmethod
    def now(cls, tz=None):
        return cls(2026, 11, 4, 10, tzinfo=UTC)
benchmark.datetime = Clock
sys.argv = ["foreseebench", "--config", sys.argv[1], "submit", "--week", "2026-11-02",
            "--participant", "concurrent-participant", "--file", sys.argv[2]]
cli.main()
""",
                str(config),
                str(request),
            ],
            capture_output=True,
            text=True,
            timeout=15,
        )
        assert child.returncode == 0, child.stderr
        assert json.loads(child.stdout)["sequence"] == 3
        for answer in ("开放文本答案", 23.5, ["甲", "乙"], False):
            value = {"answer": answer, "confidence": 0.7}
            receipt = accept_submission(
                store, week.id, "participant", dict(payload, predictions={"qa-42": value}), clock=lambda: now
            )
            assert receipt["predictions"]["qa-42"] == value
        with pytest.raises(ValueError, match="submission_closed"):
            accept_submission(store, week.id, "participant", payload, clock=lambda: now + timedelta(hours=72))
        with pytest.raises(ValueError, match="submission_version_mismatch"):
            accept_submission(store, week.id, "participant", dict(payload, release_sha256="wrong"), clock=lambda: now)
        with pytest.raises(ValueError, match="unknown_question_id"):
            accept_submission(
                store, week.id, "participant", dict(payload, predictions={"unknown": 1}), clock=lambda: now
            )
        with pytest.raises(ValueError, match="benchmark_already_released"):
            release_benchmark(store, Native(), task["task_id"], clock=lambda: now)


def test_final_result_keeps_invalid_questions_without_population_acceptance_gates(tmp_path):
    from foreseebench.benchmark import finalize_benchmark, release_benchmark, validate_stage_result
    from foreseebench.contract import settle_event
    from foreseebench.store import read_manifest

    def validate(*args):
        return {"passed": True, "validator_version": "fixture", "evidence": ["fixture-only"]}

    with Store(tmp_path) as store:
        week, raw, source, task = prepare_v2(store, tmp_path)
        now = instant("2026-11-03T17:00:00Z")

        class Native:
            def event(self, event_id):
                return raw, {"observed_at": now.isoformat()}

            def evidence(self, event):
                return (
                    [{"condition_id": source["markets"][0]["condition_id"], "status": "posed"}],
                    token_pair(source["markets"][0]),
                    [{"request_id": "fixture"}],
                )

        validate_stage_result(store, task["task_id"], validator=validate)
        release_benchmark(store, Native(), task["task_id"], clock=lambda: now)
        deadline = instant("2026-11-17T00:00:00Z")
        expired = settle_event(source, [], [], week, deadline)
        store.add_resolution(week.id, expired)
        v3 = store.release_results(week, deadline)
        assert v3["result_status"]["all_events_terminal"] is True
        latest = read_manifest(tmp_path / "batches" / week.id / "latest.json")["manifest"]
        task4 = stage_task(store, "V4", latest, v2_task_id=task["task_id"])
        output = tmp_path / "v4.jsonl"
        output.write_text(
            dumps(
                {
                    "qa_id": "qa-42",
                    "event_id": "42",
                    "source_id": source["source_id"],
                    "groundtruth": None,
                    "validity": "invalid",
                }
            )
            + "\n"
        )
        artifact = tmp_path / "v4-manifest.json"
        atomic_json(
            artifact,
            {
                "stage": "V4",
                "input_hash": task4["input_hash"],
                "handler_version": "fixture",
                "release_sha256": file_hash(tmp_path / "batches" / week.id / "benchmark/release.json"),
                "files": [{"path": "v4.jsonl", "sha256": file_hash(output)}],
            },
        )
        receive_stage_result(store, task4["task_id"], artifact)
        validate_stage_result(store, task4["task_id"], validator=validate)
        final = finalize_benchmark(store, task4["task_id"], clock=lambda: deadline + timedelta(hours=1))
        assert final["event_count"] == 1
        assert final["question_count"] == 1
        assert final["invalid_events"] == 1
        assert final["valid_events"] == 0
        assert final["status"] == "completed"
        assert not {"acceptance", "size_acceptance", "acceptance_failures", "domain_coverage"} & final.keys()
        assert final["by_domain"]["invalid"]["PoliticsGovernance"] == 1
        assert final["by_domain"]["valid"]["PoliticsGovernance"] == 0
        assert "finalization_deadline_at" in final
        assert final["questions"][0]["reason"] == "gt_missing_at_deadline"
        snapshot = store.release_results(week, deadline)
        assert snapshot["benchmark_status"] == "completed"
        assert snapshot["result_status"]["selected_invalid_events"] == 1


def test_hf_viewer_keeps_legacy_parquet_schema_out_of_the_new_protocol_default():
    from foreseebench.delivery import dataset_card

    card = dataset_card(
        {
            "batches": {
                "old": {
                    "schema_version": "1.1",
                    "files": [{"table": "events", "path": "old/events.parquet", "rows": 1}],
                },
                "new": {
                    "schema_version": "2.0",
                    "files": [{"table": "events", "path": "new/events.parquet", "rows": 1}],
                },
            }
        }
    )
    primary = card.split("- config_name: events\n", 1)[1].split("- config_name:", 1)[0]
    assert "new/events.parquet" in primary
    assert "old/events.parquet" not in primary
    legacy = card.split("- config_name: events_legacy\n", 1)[1].split("- config_name:", 1)[0]
    assert "old/events.parquet" in legacy


def test_previously_observed_answers_cannot_be_hidden_by_regressed_native_states(tmp_path):
    from foreseebench.benchmark import release_benchmark, validate_stage_result
    from foreseebench.contract import settle_event

    with Store(tmp_path) as store:
        _, raw, source, task = prepare_v2(store, tmp_path)
        now = instant("2026-11-04T10:00:00Z")
        market = source["markets"][0]
        result = settle_event(
            source,
            [{"condition_id": market["condition_id"], "status": "resolved"}],
            token_pair(market, 1),
            store.week("2026-11-02"),
            instant("2026-11-04T00:00:00Z"),
        )
        store.add_resolution("2026-11-02", result)
        validate_stage_result(
            store,
            task["task_id"],
            validator=lambda *args: {"passed": True, "validator_version": "fixture", "evidence": ["fixture-only"]},
        )

        class Native:
            def event(self, event_id):
                return raw, {"observed_at": now.isoformat()}

            def evidence(self, event):
                return ([{"condition_id": market["condition_id"], "status": "posed"}], token_pair(market), [])

        with pytest.raises(ValueError, match="no_eligible_benchmark_events"):
            release_benchmark(store, Native(), task["task_id"], clock=lambda: now)


def test_failed_prepublication_fetch_retains_known_answers_for_future_retries(tmp_path):
    from foreseebench.benchmark import release_benchmark, validate_stage_result
    from foreseebench.source import EvidenceError

    with Store(tmp_path) as store:
        _, raw, source, task = prepare_v2(store, tmp_path)
        now = instant("2026-11-04T10:00:00Z")
        market = source["markets"][0]
        turn = {"failed": True}
        validate_stage_result(
            store,
            task["task_id"],
            validator=lambda *args: {"passed": True, "validator_version": "fixture", "evidence": ["fixture-only"]},
        )

        class Native:
            def event(self, event_id):
                return raw, {"observed_at": now.isoformat()}

            def evidence(self, event):
                if turn["failed"]:
                    raise EvidenceError(
                        OSError("token_request_failed"),
                        [{"condition_id": market["condition_id"], "status": "proposed"}],
                        [],
                        [
                            {
                                "request_id": "life",
                                "observed_at": now.isoformat(),
                                "url": f"https://data-api.polymarket.com/v2/resolutions?condition={market['condition_id']}",
                            }
                        ],
                    )
                return ([{"condition_id": market["condition_id"], "status": "posed"}], token_pair(market), [])

        with pytest.raises(ValueError, match="no_eligible_benchmark_events"):
            release_benchmark(store, Native(), task["task_id"], clock=lambda: now)
        turn["failed"] = False
        with pytest.raises(ValueError, match="no_eligible_benchmark_events"):
            release_benchmark(store, Native(), task["task_id"], clock=lambda: now)
