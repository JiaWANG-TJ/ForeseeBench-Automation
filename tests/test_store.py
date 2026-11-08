from datetime import UTC, datetime

from test_contract import raw_event

from foreseebench.contract import Week, admit_event
from foreseebench.store import Store, read_records


def test_page_retry_preserves_fill_multiplicity_and_frozen_hashes(tmp_path):
    week = Week.parse("2026-11-02")
    source = admit_event(raw_event(), week, week.open_at)
    with Store(tmp_path) as store:
        store.begin(week, "qualification", "config")
        store.event(week.id, "42", source, "collected")
        fill = {
            "event_id": "42",
            "row_id": "first",
            "transaction_hash": "same",
            "condition_id": source["markets"][0]["condition_id"],
            "token_id": "100",
            "outcome": "Yes",
            "side": "BUY",
            "price_per_share": "0.5",
            "shares": "1",
            "traded_at": "2026-11-02T01:00:00Z",
            "observed_at": "2026-11-02T02:00:00Z",
        }
        for _ in range(2):
            store.page(week.id, "42", "trades", "page", [fill, dict(fill, row_id="second")], None, True)
        store.page(week.id, "42", "comments", "empty", [], None, True)
        store.discovery_complete(week.id)
        manifest = store.freeze(week, datetime(2026, 11, 2, 6, tzinfo=UTC))
        assert manifest["counts"]["trades"] == 2
        assert len(list(read_records(tmp_path, manifest, "trades"))) == 2
        assert store.freeze(week, week.freeze_at) == manifest


def test_failed_page_is_atomic_and_does_not_move_checkpoint(tmp_path):
    import pytest

    week = Week.parse("2026-11-02")
    with Store(tmp_path) as store:
        store.begin(week, "qualification", "config")
        store.event(week.id, "42", admit_event(raw_event(), week, week.open_at), "collected")
        store.page(week.id, "42", "trades", "initial", [{"event_id": "42", "row_id": "old"}], "cursor1", False)

        def broken():
            for _ in range(1001):
                yield {"event_id": "42", "row_id": "new"}
            raise OSError("interrupted")

        with pytest.raises(OSError):
            store.page(week.id, "42", "trades", "next", broken(), "cursor2", False)
        assert store.position(week.id, "42", "trades") == ("cursor1", False)
        store.page(week.id, "42", "trades", "next", [], None, True)
        store.page(week.id, "42", "comments", "empty", [], None, True)
        store.discovery_complete(week.id)
        manifest = store.freeze(week, week.freeze_at)
        assert manifest["counts"]["trades"] == 1


def test_manifest_commit_crash_repairs_pointer_without_regression(tmp_path, monkeypatch):
    import pytest

    import foreseebench.store as module
    from foreseebench.store import read_manifest

    week = Week.parse("2026-11-02")
    real_write = module.atomic_json

    def fail_pointer(path, value):
        if path.name == "latest.json":
            raise OSError("power_loss")
        real_write(path, value)

    with Store(tmp_path) as store:
        store.begin(week, "qualification", "config")
        store.discovery_complete(week.id)
        monkeypatch.setattr(module, "atomic_json", fail_pointer)
        with pytest.raises(OSError):
            store.freeze(week, week.freeze_at)
        monkeypatch.setattr(module, "atomic_json", real_write)
        store.freeze(week, week.freeze_at)
        pointer = tmp_path / "batches" / week.id / "latest.json"
        assert read_manifest(pointer)["manifest"].endswith("V1/manifest.json")
        monkeypatch.setattr(module, "atomic_json", fail_pointer)
        with pytest.raises(OSError):
            store.release_results(week, week.evidence_cutoff)
        monkeypatch.setattr(module, "atomic_json", real_write)
        store.release_results(week, week.evidence_cutoff)
        current = read_manifest(pointer)
        assert "/V3-" in current["manifest"]
        store.freeze(week, week.freeze_at)
        assert read_manifest(pointer) == current


def test_new_execution_failure_produces_a_new_result_snapshot(tmp_path):
    week = Week.parse("2026-11-02")
    with Store(tmp_path) as store:
        store.begin(week, "qualification", "config")
        store.discovery_complete(week.id)
        store.freeze(week, week.freeze_at)
        old = store.release_results(week, week.evidence_cutoff)
        store.failure(week.id, "42", "V3", week.evidence_cutoff, "NetworkError")
        new = store.release_results(week, week.evidence_cutoff)
        assert len(new["execution_failures"]) == 1
        assert old != new


def test_bootstrap_requires_all_candidates_and_uses_actual_freeze_time(tmp_path):
    import pytest

    week = Week.parse("2026-11-02")
    actual = datetime(2026, 11, 4, 18, tzinfo=UTC)
    with Store(tmp_path) as store:
        store.begin(week, "bootstrap", "config")
        store.discovery_complete(week.id)
        source = admit_event(raw_event(), week, actual)
        store.event(week.id, "42", source, "failed", "NetworkError")
        with pytest.raises(ValueError, match="construction_incomplete"):
            store.freeze(week, actual)
        store.event(week.id, "42", source, "collected")
        for kind in ("comments", "trades"):
            store.page(week.id, "42", kind, "empty", [], None, True)
        manifest = store.freeze(week, actual)
        assert manifest["frozen_at"] == "2026-11-04T18:00:00.000000Z"
        assert manifest["scheduled_freeze_at"] == "2026-11-03T11:00:00.000000Z"
        assert manifest["construction_complete"] is True
        assert manifest["schedule_compliant"] is False
        assert store.previously_published("42", "2026-11-09") is False


def test_formal_freeze_rejects_profile_observed_after_cutoff(tmp_path):
    import pytest
    from test_contract import raw_event

    from foreseebench.contract import Week, admit_event, stamp
    from foreseebench.store import Store

    week = Week.parse("2026-11-02")
    with Store(tmp_path) as store:
        store.begin(week, "formal", "config")
        store.event(week.id, "42", admit_event(raw_event(), week, week.open_at), "collected")
        store.page(
            week.id,
            "42",
            "comments",
            "page",
            [
                {
                    "event_id": "42",
                    "comment_id": "1",
                    "observed_at": stamp(week.open_at),
                    "profile_observed_at": stamp(week.freeze_at),
                }
            ],
            None,
            True,
        )
        store.page(week.id, "42", "trades", "empty", [], None, True)
        store.discovery_complete(week.id)
        with pytest.raises(ValueError, match="late_feature_observation"):
            store.freeze(week, week.freeze_at)


def test_conflicting_comment_duplicates_roll_back_the_page(tmp_path):
    import pytest

    week = Week.parse("2026-11-02")
    with Store(tmp_path) as store:
        store.begin(week, "qualification", "config")
        store.event(week.id, "42", admit_event(raw_event(), week, week.open_at), "collecting")
        with pytest.raises(ValueError, match="conflicting_comment_duplicate"):
            store.page(
                week.id,
                "42",
                "comments",
                "page",
                [
                    {"event_id": "42", "comment_id": "1", "body": "first"},
                    {"event_id": "42", "comment_id": "1", "body": "different"},
                ],
                None,
                True,
            )
        assert store.position(week.id, "42", "comments") == (None, False)
