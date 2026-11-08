import io
import json

import pytest

from foreseebench.delivery import export_jsonl, publication_index


def test_dataset_cards_preserve_configs_and_link_both_languages():
    from huggingface_hub import DatasetCard

    from foreseebench.delivery import dataset_card

    index = {
        "batches": {
            "2026-11-02": {
                "files": [
                    {"table": table, "path": f"batches/2026-11-02/V1/{table}-00000.parquet", "rows": 1}
                    for table in ("events", "comments", "trades", "resolutions")
                ]
            }
        }
    }
    index["exports"] = [
        {"path": f"{view}/{name}.jsonl", "view": view, "format": name}
        for name in ("basic", "extended")
        for view in ("full", "balanced")
    ]
    chinese = dataset_card(index)
    english = dataset_card(index, language="en")
    expected = [
        {
            "config_name": name,
            **({"default": True} if name == "basic" else {}),
            "data_files": [{"split": view, "path": [f"{view}/{name}.jsonl"]} for view in ("full", "balanced")],
        }
        for name in ("basic", "extended")
    ] + [
        {
            "config_name": table,
            "data_files": [{"split": "data", "path": [f"batches/2026-11-02/V1/{table}-00000.parquet"]}],
        }
        for table in ("events", "comments", "trades", "resolutions")
    ]
    assert DatasetCard(chinese).data.configs == expected
    assert DatasetCard(english).data.configs == expected
    infos = DatasetCard(chinese).data.to_dict().get("dataset_info", [])
    assert {info["config_name"] for info in infos} == {"basic", "extended"}
    for info in infos:
        fields = {field["name"]: field["dtype"] for field in info["features"]}
        assert fields["native_gt"] == "json"
        assert fields["markets"] == "json"
        assert fields["source_id"] == "string"
    assert "**简体中文**" in chinese
    assert "[English](https://huggingface.co/datasets/SII-JiaWANG/ForeseeBench/blob/main/README.en.md)" in chinese
    assert "**English**" in english
    assert "[简体中文](https://huggingface.co/datasets/SII-JiaWANG/ForeseeBench)" in english


def test_publication_does_not_replace_newer_result_with_v1():
    import pytest

    old = {"batches": {"2026-11-02": {"stage": "V3", "created_at": "2026-11-08T08:00:00Z"}}}
    with pytest.raises(ValueError, match="publication_regression"):
        publication_index(
            old,
            {"batch_id": "2026-11-02", "stage": "V1", "created_at": "2026-11-02T06:00:00Z"},
            "manifest.json",
        )


def test_published_v1_input_cannot_be_replaced_from_another_data_directory():
    import pytest

    original = {
        "batch_id": "2026-11-02",
        "stage": "V1",
        "kind": "formal",
        "files": [],
        "created_at": "2026-11-02T06:00:00Z",
        "config_hash": "original",
    }
    index = publication_index({}, original, "batches/2026-11-02/V1/manifest.json")
    changed = dict(original, created_at="2026-11-02T07:00:00Z", config_hash="different")
    with pytest.raises(ValueError, match="frozen_input_conflict"):
        publication_index(index, changed, "batches/2026-11-02/V1/manifest.json")


def archive_event_source(root, raw, *, batch_id="2026-11-02"):
    import hashlib

    from foreseebench.contract import dumps
    from foreseebench.source import AuditArchive

    path = root / "batches" / batch_id / "raw-v1.sqlite"
    path.parent.mkdir(parents=True, exist_ok=True)
    archive = AuditArchive(path)
    try:
        for index, payload in enumerate((raw, dict(raw, slug="later-event-slug"))):
            body = dumps(payload).encode()
            archive.append(
                io.BytesIO(body),
                {
                    "request_id": f"{raw['id']}-{index}",
                    "url": f"https://gamma-api.polymarket.com/events/{raw['id']}",
                    "status": 200,
                    "body_sha256": hashlib.sha256(body).hexdigest(),
                    "bytes": len(body),
                },
            )
    finally:
        archive.close()


def freeze_event_slug_snapshot(root, slug, *, raw=None, stored_changes=None, week_id="2026-11-02"):
    from test_contract import raw_event

    from foreseebench.contract import Week, admit_event
    from foreseebench.store import Store

    week = Week.parse(week_id)
    raw = raw_event() if raw is None else raw
    if slug is not None:
        raw["slug"] = slug
    raw["markets"][0]["slug"] = "market-slug-must-not-be-exported"
    archive_event_source(root, raw, batch_id=week.id)
    with Store(root) as store:
        store.begin(week, "qualification", "config")
        source = admit_event(raw, week, week.open_at)
        source.update(stored_changes or {})
        store.event(week.id, "42", source, "collected")
        for kind in ("comments", "trades"):
            store.page(week.id, "42", kind, "empty", [], None, True)
        store.discovery_complete(week.id)
        return store.freeze(week, week.freeze_at)


def test_clean_export_keeps_every_market_and_exact_native_payouts(tmp_path):
    from test_contract import raw_event
    from test_weekly_protocol import token_pair

    from foreseebench.contract import Week, instant, settle_event
    from foreseebench.store import Store, read_records

    raw = raw_event()
    raw["markets"] = [
        dict(
            raw["markets"][0],
            id=str(9 + i),
            conditionId="0x" + character * 64,
            clobTokenIds=json.dumps([str(100 + i * 2), str(101 + i * 2)]),
            description=f"原始结算规则{i}",
            resolutionSource=f"https://example.org/results/{i}",
        )
        for i, character in enumerate("abc")
    ]
    frozen = freeze_event_slug_snapshot(tmp_path, "native-results", raw=raw)
    source = next(read_records(tmp_path, frozen, "events"))
    week = Week.parse("2026-11-02")
    lifecycle = [
        {"condition_id": market["condition_id"], "status": "resolved" if i < 2 else "posed"}
        for i, market in enumerate(source["markets"])
    ]
    tokens = (
        token_pair(source["markets"][0], 0) + token_pair(source["markets"][1], 0) + token_pair(source["markets"][2])
    )
    for token in tokens[2:4]:
        token["final_price"] = 0.5
    now = instant("2026-11-07T00:00:00Z")
    with Store(tmp_path) as store:
        store.add_resolution(week.id, settle_event(source, lifecycle, tokens, week, now))
        result = store.release_results(week, now)
    output = io.StringIO()
    export_jsonl(tmp_path, result, output, extended=True)
    row = json.loads(output.getvalue())
    assert row["source_id"] == source["source_id"]
    assert row["title"] == "原始事件"
    assert not {"qa_id", "options", "market_resolutions", "groundtruth", "gt_confidence", "sampling"} & row.keys()
    assert [m["description"] for m in row["markets"]] == ["原始结算规则0", "原始结算规则1", "原始结算规则2"]
    assert row["markets"][2]["resolution_source"] == "https://example.org/results/2"
    assert row["native_gt"]["status"] == "partial"
    assert [m["condition_id"] for m in row["native_gt"]["markets"]] == [m["condition_id"] for m in source["markets"]]
    assert [m["payouts"] for m in row["native_gt"]["markets"]] == [[0, 1], [0.5, 0.5], [None, None]]
    assert [m["status"] for m in row["native_gt"]["markets"]] == ["resolved", "fractional", "pending"]
    assert all(m["resolved_at"] is None for m in row["native_gt"]["markets"])
    assert not any("receipt_ids" in m for m in row["native_gt"]["markets"])
    v1_output = io.StringIO()
    export_jsonl(tmp_path, frozen, v1_output)
    assert json.loads(v1_output.getvalue())["native_gt"] is None


def test_empty_full_and_balanced_views_preserve_zero_counts_and_stream_without_placeholder_rows(tmp_path):
    from datasets import load_dataset

    from foreseebench.contract import Week, instant
    from foreseebench.delivery import build_exports, dataset_card
    from foreseebench.store import Store, read_manifest

    week = Week.bootstrap(instant("2026-11-05T14:00:00Z"))
    with Store(tmp_path) as store:
        store.begin(week, "bootstrap", "config")
        store.discovery_complete(week.id)
        frozen = store.freeze(week, instant("2026-11-05T16:00:00Z"))
        index = publication_index({}, frozen, f"batches/{week.id}/V1/manifest.json")
        index["exports"] = build_exports(store, index, "unused", "unused")
        assert len(index["exports"]) == 4
        for entry in index["exports"]:
            assert entry["counts"]["events"] == 0
            assert (tmp_path / entry["local_path"]).read_bytes() == b""
        stats = read_manifest(tmp_path / index["statistics"]["local_path"])
        assert stats["cumulative"]["full"]["events"] == 0
        assert stats["cumulative"]["balanced"]["events"] == 0
        export_root = tmp_path / "exports"
        (export_root / "README.md").write_text(dataset_card(index), encoding="utf-8")
        (export_root / "batches").symlink_to(tmp_path / "batches", target_is_directory=True)
        for name in ("basic", "extended"):
            loaded = load_dataset(str(export_root), name=name, streaming=True)
            assert {split: list(data) for split, data in loaded.items()} == {"full": [], "balanced": []}
        assert store.verify(frozen)["valid"] is True


def test_full_and_balanced_exports_keep_frozen_members_when_gt_changes(tmp_path):
    from datetime import timedelta

    from test_contract import raw_event
    from test_weekly_protocol import token_pair

    from foreseebench.contract import Policy, Week, admit_event, settle_event
    from foreseebench.delivery import build_exports
    from foreseebench.store import Store, read_manifest

    week = Week.parse("2026-11-02", Policy(domain_event_cap=2))
    sources = []
    for i in range(1, 6):
        raw = raw_event()
        raw.update(id=str(i), tags=[{"id": "1" if i <= 3 else "84", "label": "原始标签"}])
        raw["markets"][0].update(
            id=str(i), conditionId="0x" + f"{i:064x}", clobTokenIds=json.dumps([str(i * 2), str(i * 2 + 1)])
        )
        archive_event_source(tmp_path, raw)
        sources.append(admit_event(raw, week, week.collection_start))
    with Store(tmp_path) as store:
        store.begin(week, "qualification", "config")
        for source in sources:
            store.event(week.id, source["event_id"], source, "collected")
            for kind in ("comments", "trades"):
                store.page(week.id, source["event_id"], kind, "empty", [], None, True)
        store.discovery_complete(week.id)
        frozen = store.freeze(week, week.freeze_at)
        path = f"batches/{week.id}/V1/manifest.json"
        index = publication_index({}, frozen, path)
        exports = index["exports"] = build_exports(store, index, "unused", "unused")
        assert {e["path"] for e in exports} == {
            "full/basic.jsonl",
            "full/extended.jsonl",
            "balanced/basic.jsonl",
            "balanced/extended.jsonl",
        }

        def rows(view):
            return {
                row["event_id"]: row
                for row in (json.loads(line) for line in (tmp_path / "exports" / view / "basic.jsonl").open())
            }

        full, balanced = rows("full"), rows("balanced")
        assert len(full) == 5
        assert len(balanced) == 4
        expected = {m["event_id"] for m in frozen["selection"]["members"]}
        assert set(balanced) == expected
        assert all(row == full[identifier] for identifier, row in balanced.items())
        direct = io.StringIO()
        export_jsonl(tmp_path, frozen, direct, view="balanced")
        assert {json.loads(line)["event_id"] for line in direct.getvalue().splitlines()} == expected
        source = next(source for source in sources if source["event_id"] in expected)
        now = week.prediction_cutoff + timedelta(hours=1)
        result = settle_event(
            source,
            [
                {
                    "condition_id": source["markets"][0]["condition_id"],
                    "status": "resolved",
                    "resolved_at": (week.prediction_cutoff - timedelta(hours=1)).isoformat(),
                }
            ],
            token_pair(source["markets"][0], 1),
            week,
            now,
        )
        store.add_resolution(week.id, result)
        updated = store.release_results(week, now)
        latest = read_manifest(tmp_path / "batches" / week.id / "latest.json")["manifest"]
        index = publication_index(index, updated, latest)
        index["exports"] = build_exports(store, index, "unused", "unused")
        assert set(rows("balanced")) == expected
        assert rows("balanced")[source["event_id"]]["native_gt"]["qualification"]["validity"] == "invalid"
        stats = read_manifest(tmp_path / index["statistics"]["local_path"])
        assert stats["batches"][week.id]["domains"]["Sports"]["full"] == 3
        assert stats["batches"][week.id]["domains"]["Sports"]["balanced"] == 2
        assert stats["batches"][week.id]["domains"]["WeatherEnvironment"]["full"] == 2
        assert store.verify(frozen)["valid"] is True
        from datasets import load_dataset

        from foreseebench.delivery import dataset_card

        export_root = tmp_path / "exports"
        (export_root / "README.md").write_text(dataset_card(index), encoding="utf-8")
        (export_root / "batches").symlink_to(tmp_path / "batches", target_is_directory=True)
        loaded = load_dataset(str(export_root), name="extended", chunksize=512, cache_dir=str(tmp_path / "hf-cache"))
        assert {name: len(data) for name, data in loaded.items()} == {"full": 5, "balanced": 4}
        assert isinstance(loaded["full"][0]["markets"], list)
        assert isinstance(loaded["full"][0]["native_gt"], dict)
        assert loaded["full"][0]["comments"] == []


@pytest.mark.parametrize("first_is_formally_published", [False, True])
def test_cumulative_exports_select_one_whole_snapshot_and_keep_all_original_files(
    tmp_path, first_is_formally_published
):
    from test_contract import raw_event

    from foreseebench.delivery import build_exports
    from foreseebench.store import Store, atomic_json, file_hash

    first = freeze_event_slug_snapshot(tmp_path, "first-snapshot")
    raw = raw_event()
    raw["title"] = "新版原始事件"
    raw["markets"][0]["endDate"] = "2026-11-13T20:00:00Z"
    second = freeze_event_slug_snapshot(tmp_path, "second-snapshot", raw=raw, week_id="2026-11-09")
    first_path = "batches/2026-11-02/V1/manifest.json"
    if first_is_formally_published:
        first = dict(first, stage="V3", benchmark_selection={"members": [{"event_id": "42"}]})
        first_path = "batches/2026-11-02/V3-fixture/manifest.json"
        atomic_json(tmp_path / first_path, first)
    index = publication_index({}, first, first_path)
    index = publication_index(index, second, "batches/2026-11-09/V1/manifest.json")
    with Store(tmp_path) as store:
        artifacts = build_exports(store, index, "fixture/repo", "main")
    for artifact in artifacts:
        assert artifact["counts"]["events"] == 1
        rows = [json.loads(line) for line in (tmp_path / artifact["local_path"]).open()]
        assert len(rows) == 1
        assert rows[0]["event_id"] == "42"
        assert rows[0]["slug"] == ("first-snapshot" if first_is_formally_published else "second-snapshot")
        assert rows[0]["title"] == ("原始事件" if first_is_formally_published else "新版原始事件")
        assert rows[0]["batch_id"] == ("2026-11-02" if first_is_formally_published else "2026-11-09")
    assert all(
        file_hash(tmp_path / f["path"]) == f["sha256"] for manifest in (first, second) for f in manifest["files"]
    )


@pytest.mark.parametrize("slug", ["den-sif-lyn-2026-11-06-exact-score", None])
def test_both_exports_preserve_frozen_event_slug_and_missingness(tmp_path, slug):
    from foreseebench.store import file_hash

    manifest = freeze_event_slug_snapshot(tmp_path, slug)
    for extended in (False, True):
        output = io.StringIO()
        export_jsonl(tmp_path, manifest, output, extended=extended)
        result = json.loads(output.getvalue())
        assert result["slug"] == slug
        assert result["event_id"] == "42"
        assert all("slug" not in market for market in result["markets"])
    assert all(file_hash(tmp_path / entry["path"]) == entry["sha256"] for entry in manifest["files"])


def test_nonempty_export_requires_frozen_source_evidence(tmp_path):
    manifest = freeze_event_slug_snapshot(tmp_path, "original-event-slug")
    manifest["files"] = [entry for entry in manifest["files"] if entry["table"] != "evidence"]
    for extended in (False, True):
        with pytest.raises(ValueError, match="v1_evidence_missing"):
            export_jsonl(tmp_path, manifest, io.StringIO(), extended=extended)


@pytest.mark.parametrize("event_description", [None, "原始规则", "事件描述，与市场规则不同"])
def test_exports_keep_one_event_description_and_preserve_native_market_questions(tmp_path, event_description):
    from test_contract import raw_event

    from foreseebench.store import file_hash, read_records

    raw = raw_event()
    raw["description"] = event_description
    raw["markets"][0]["groupItemTitle"] = None
    for index, condition in [(1, "b"), (2, "c")]:
        raw["markets"].append(
            dict(
                raw["markets"][0],
                id=str(9 + index),
                conditionId="0x" + condition * 64,
                groupItemTitle="重复标签",
                clobTokenIds=json.dumps([str(100 + 2 * index), str(101 + 2 * index)]),
            )
        )
    manifest = freeze_event_slug_snapshot(tmp_path, "event-slug", raw=raw)
    for extended in (False, True):
        output = io.StringIO()
        export_jsonl(tmp_path, manifest, output, extended=extended)
        result = json.loads(output.getvalue())
        assert result["title"] == "原始事件"
        assert result["slug"] == "event-slug"
        assert [market["label"] for market in result["markets"]] == [None, "重复标签", "重复标签"]
        assert "options" not in result
        if extended:
            assert result["description"] == event_description
            assert [market["description"] for market in result["markets"]] == ["原始规则"] * 3
            assert [market["question"] for market in result["markets"]] == ["原始条件"] * 3
            assert output.getvalue().count('"description":') == 4
        else:
            assert "description" not in result
    stored = next(read_records(tmp_path, manifest, "events"))
    assert [market["description"] for market in stored["markets"]] == ["原始规则"] * 3
    assert all(file_hash(tmp_path / entry["path"]) == entry["sha256"] for entry in manifest["files"])


@pytest.mark.parametrize("field", ["question", "description"])
def test_exports_reject_event_text_not_matching_frozen_source(tmp_path, field):
    manifest = freeze_event_slug_snapshot(tmp_path, "event-slug", stored_changes={field: "错误文本"})
    for extended in (False, True):
        with pytest.raises(ValueError, match="frozen_event_metadata_mismatch"):
            export_jsonl(tmp_path, manifest, io.StringIO(), extended=extended)


def test_export_validation_uses_event_ids_instead_of_question_text(tmp_path):
    from foreseebench.delivery import validate_export

    path = tmp_path / "basic.jsonl"
    rows = [
        {"source_id": "source-1", "event_id": "42", "title": "同名但不同的事件"},
        {"source_id": "source-2", "event_id": "43", "title": "同名但不同的事件"},
    ]
    expected = {"events": 2, "comments": 0, "trades": 0}
    path.write_text("".join(json.dumps(row) + "\n" for row in rows))
    assert validate_export(path, expected) == expected
    rows[1]["event_id"] = "42"
    path.write_text("".join(json.dumps(row) + "\n" for row in rows))
    with pytest.raises(ValueError, match="duplicate_event_in_export"):
        validate_export(path, expected)


def test_streaming_export_preserves_duplicates_unicode_and_decimal_values(tmp_path):
    from datetime import UTC, datetime

    from test_contract import raw_event

    from foreseebench.contract import Week, admit_event
    from foreseebench.store import Store

    week = Week.parse("2026-11-02")
    raw = raw_event()
    archive_event_source(tmp_path, raw)
    with Store(tmp_path) as store:
        store.begin(week, "qualification", "config")
        store.event(week.id, "42", admit_event(raw, week, week.open_at), "collected")
        store.page(
            week.id,
            "42",
            "comments",
            "a",
            [
                {
                    "event_id": "42",
                    "comment_id": "1",
                    "body": "中文\n评论",
                    "createdAt": "2026-11-02T01:00:00Z",
                }
            ],
            None,
            True,
        )
        row = {
            "event_id": "42",
            "row_id": "1",
            "price_per_share": "0.1234567890123456789",
            "shares": "2",
        }
        store.page(week.id, "42", "trades", "b", [row, dict(row, row_id="2")], None, True)
        store.discovery_complete(week.id)
        manifest = store.freeze(week, datetime(2026, 11, 2, 6, tzinfo=UTC))
    output = io.StringIO()
    export_jsonl(tmp_path, manifest, output, extended=True)
    result = json.loads(output.getvalue())
    assert result["title"] == "原始事件"
    assert result["comments"][0]["body"] == "中文\n评论"
    assert len(result["token_trade_history"]) == 2
    assert "0.1234567890123456789" in output.getvalue()


def test_export_builds_reply_tree_and_keeps_orphans_without_losing_counts(tmp_path):
    from test_contract import raw_event

    from foreseebench.contract import Week, admit_event
    from foreseebench.delivery import validate_export
    from foreseebench.store import Store

    week = Week.parse("2026-11-02")
    raw = raw_event()
    archive_event_source(tmp_path, raw)
    with Store(tmp_path) as store:
        store.begin(week, "qualification", "config")
        store.event(week.id, "42", admit_event(raw, week, week.open_at), "collected")
        rows = [
            {"event_id": "42", "comment_id": identifier, "parent_comment_id": parent, "body": identifier}
            for identifier, parent in [("child", "root"), ("deep", "child"), ("root", None), ("orphan", "deleted")]
        ]
        store.page(week.id, "42", "comments", "page", rows, None, True)
        store.page(week.id, "42", "trades", "empty", [], None, True)
        store.discovery_complete(week.id)
        manifest = store.freeze(week, week.freeze_at)
    output = io.StringIO()
    export_jsonl(tmp_path, manifest, output, extended=True)
    event = json.loads(output.getvalue())
    assert [row["comment_id"] for row in event["comments"]] == ["root"]
    child = event["comments"][0]["replies"][0]
    assert child["comment_id"] == "child"
    assert child["replies"][0]["comment_id"] == "deep"
    assert child["replies"][0]["replies"] == []
    assert event["orphan_replies"][0]["parent_comment_id"] == "deleted"
    path = tmp_path / "extended.jsonl"
    path.write_text(output.getvalue(), encoding="utf-8")
    assert validate_export(path, {"events": 1, "comments": 4, "trades": 0})["comments"] == 4


def test_first_v3_publish_contains_parent_manifest_and_its_files(tmp_path, monkeypatch):
    from pathlib import Path
    from types import SimpleNamespace

    import foreseebench.delivery as module
    from foreseebench.contract import Week
    from foreseebench.store import Store, atomic_json

    operations = []
    remote_root = tmp_path / "remote"
    downloads = []

    class Hub:
        def repo_info(self, *args, **kwargs):
            return SimpleNamespace(sha="parent")

        def file_exists(self, *args, **kwargs):
            return False

        def preupload_lfs_files(self, *args, **kwargs):
            pass

        def create_commit(self, *args, **kwargs):
            operations.extend(kwargs["operations"])
            for operation in kwargs["operations"]:
                target = remote_root / operation.path_in_repo
                target.parent.mkdir(parents=True, exist_ok=True)
                value = operation.path_or_fileobj
                target.write_bytes(value if isinstance(value, bytes) else Path(value).read_bytes())
            return SimpleNamespace(oid="commit")

    def download(repo_id, filename, **kwargs):
        downloads.append(filename)
        return str(remote_root / filename)

    monkeypatch.setattr(module, "HfApi", Hub)
    monkeypatch.setattr(module, "hf_hub_download", download)
    week = Week.parse("2026-11-02")
    with Store(tmp_path) as store:
        store.begin(week, "qualification", "config")
        store.discovery_complete(week.id)
        v1 = store.freeze(week, week.freeze_at)
        v3 = store.release_results(week, week.evidence_cutoff)
        path = tmp_path / "current-manifest.json"
        atomic_json(path, v3)
        module.publish(store, path, "owner/repo", "qualification")
        uploaded = {o.path_in_repo for o in operations}
        assert v3["parent_manifest"] in uploaded
        assert {f["path"] for f in v1["files"]} <= uploaded
        assert module.EXPORT_PATHS | {"statistics.json"} <= uploaded
        assert module.EXPORT_PATHS | {"statistics.json"} <= set(downloads)
        assert {"README.md", "README.en.md"} <= uploaded


def test_export_rejects_comment_cycles_instead_of_dropping_them(tmp_path):
    import pytest
    from test_contract import raw_event

    from foreseebench.contract import Week, admit_event
    from foreseebench.store import Store

    week = Week.parse("2026-11-02")
    raw = raw_event()
    archive_event_source(tmp_path, raw)
    with Store(tmp_path) as store:
        store.begin(week, "qualification", "config")
        store.event(week.id, "42", admit_event(raw, week, week.open_at), "collected")
        store.page(
            week.id,
            "42",
            "comments",
            "page",
            [
                {"event_id": "42", "comment_id": "a", "parent_comment_id": "b"},
                {"event_id": "42", "comment_id": "b", "parent_comment_id": "a"},
            ],
            None,
            True,
        )
        store.page(week.id, "42", "trades", "empty", [], None, True)
        store.discovery_complete(week.id)
        manifest = store.freeze(week, week.freeze_at)
    with pytest.raises(ValueError, match="comment_reply_cycle"):
        export_jsonl(tmp_path, manifest, io.StringIO(), extended=True)


def test_republish_updates_only_changed_cards_and_then_becomes_unchanged(tmp_path, monkeypatch):
    import shutil
    from types import SimpleNamespace

    import foreseebench.delivery as module
    from foreseebench.contract import Week, dumps
    from foreseebench.store import Store

    remote = tmp_path / "remote"
    remote.mkdir()
    commits = []

    class Hub:
        head = "before"

        def repo_info(self, *args, **kwargs):
            return SimpleNamespace(sha=self.head)

        def file_exists(self, repo_id, path, **kwargs):
            return (remote / path).is_file()

        def preupload_lfs_files(self, *args, **kwargs):
            raise AssertionError("文档更新不应上传数据文件")

        def create_commit(self, *args, **kwargs):
            assert kwargs["parent_commit"] == self.head
            operations = kwargs["operations"]
            commits.append({o.path_in_repo for o in operations})
            for operation in operations:
                (remote / operation.path_in_repo).write_bytes(operation.path_or_fileobj)
            self.head = "after"
            return SimpleNamespace(oid=self.head)

    hub = Hub()
    monkeypatch.setattr(module, "HfApi", lambda: hub)
    monkeypatch.setattr(module, "hf_hub_download", lambda repo, filename, **kw: str(remote / filename))
    week = Week.parse("2026-11-02")
    with Store(tmp_path / "local") as store:
        store.begin(week, "qualification", "config")
        store.discovery_complete(week.id)
        manifest = store.freeze(week, week.freeze_at)
        path = store.root / "batches" / week.id / "V1" / "manifest.json"
        index = publication_index({}, manifest, str(path.relative_to(store.root)))
        index["exports"] = module.build_exports(store, index, "unused", "unused")
        for entry in [*index["exports"], index["statistics"]]:
            target = remote / entry["path"]
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(store.root / entry["local_path"], target)
        shutil.rmtree(store.root / "exports")
        original_index = (dumps(index) + "\n").encode()
        (remote / "manifest.json").write_bytes(original_index)
        (remote / "README.md").write_text("Old English card\n")

        receipt = module.publish(store, path, "owner/repo", "qualification")
        assert commits == [{"README.md", "README.en.md"}]
        assert receipt["revision"] == "after"
        assert receipt["unchanged"] is False
        assert (remote / "manifest.json").read_bytes() == original_index
        assert "**简体中文**" in (remote / "README.md").read_text()
        assert "**English**" in (remote / "README.en.md").read_text()
        assert not (store.root / "exports").exists()

        repeated = module.publish(store, path, "owner/repo", "qualification")
        assert repeated["unchanged"] is True
        assert repeated["revision"] == "after"
        assert len(commits) == 1

        (remote / "README.en.md").write_text("Outdated English card\n")
        module.publish(store, path, "owner/repo", "qualification")
        assert commits[-1] == {"README.en.md"}
        assert len(commits) == 2
        assert (remote / "manifest.json").read_bytes() == original_index


def test_export_reads_hub_snapshot_symlinks_for_remote_historical_batches(tmp_path, monkeypatch):
    import foreseebench.delivery as module
    from foreseebench.store import Store, file_hash

    original = tmp_path / "original"
    manifest = freeze_event_slug_snapshot(original, "frozen-event-slug")
    name = "batches/2026-11-02/V1/manifest.json"
    snapshot = tmp_path / "cache/snapshots/revision"
    blobs = tmp_path / "cache/blobs"
    blobs.mkdir(parents=True)
    for relative in [name] + [f["path"] for f in manifest["files"]]:
        src = original / relative
        blob = blobs / file_hash(src)
        blob.write_bytes(src.read_bytes())
        target = snapshot / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.symlink_to(blob)
    downloaded = []

    def download(repo, filename, **kwargs):
        downloaded.append(filename)
        return str(snapshot / filename)

    monkeypatch.setattr(module, "hf_hub_download", download)
    with Store(tmp_path / "consumer") as consumer:
        entries = module.build_exports(consumer, publication_index({}, manifest, name), "owner/repo", "revision")
    assert {e["path"] for e in entries} == module.EXPORT_PATHS
    assert all(e["counts"]["events"] == 1 for e in entries)
    assert "batches/2026-11-02/raw-v1.sqlite" in downloaded
    for entry in entries:
        row = json.loads((tmp_path / "consumer" / entry["local_path"]).read_text())
        assert row["slug"] == "frozen-event-slug"
