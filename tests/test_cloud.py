import hashlib
from pathlib import Path
from types import SimpleNamespace

import pytest

from foreseebench.cloud import BucketCheckpoint
from foreseebench.store import Store


class BucketAPI:
    """与官方 SDK 同形的隔离存储边界，禁止测试访问真实 Bucket。"""

    def __init__(self):
        self.files = {}
        self.fail_path = None
        self.downloads = []
        self.repositories = {}
        self.bucket_sizes = {"owner/state": 0}

    def whoami(self, *, cache=False):
        return {"name": "owner"}

    def list_models(self, *, author):
        return [
            SimpleNamespace(id=name)
            for kind, name in self.repositories
            if kind == "model" and name.startswith(author + "/")
        ]

    def list_datasets(self, *, author):
        return [
            SimpleNamespace(id=name)
            for kind, name in self.repositories
            if kind == "dataset" and name.startswith(author + "/")
        ]

    def list_spaces(self, *, author):
        return [
            SimpleNamespace(id=name)
            for kind, name in self.repositories
            if kind == "space" and name.startswith(author + "/")
        ]

    def repo_info(self, repo_id, *, repo_type, **kwargs):
        return self.repositories[repo_type, repo_id]

    def list_buckets(self, namespace):
        return [SimpleNamespace(id=name) for name in self.bucket_sizes if name.startswith(namespace + "/")]

    def bucket_info(self, bucket_id):
        size = self.bucket_sizes.get(bucket_id)
        if size is not None and bucket_id == "owner/state":
            size += sum(map(len, self.files.values()))
        return SimpleNamespace(id=bucket_id, private=True, size=size)

    def list_bucket_tree(self, bucket_id, prefix=None, *, recursive=None):
        return [
            SimpleNamespace(path=p, size=len(data), xet_hash=hashlib.sha256(data).hexdigest())
            for p, data in self.files.items()
            if p.startswith(prefix or "")
        ]

    def get_bucket_paths_info(self, bucket_id, paths):
        return [entry for entry in self.list_bucket_tree(bucket_id) if entry.path in paths]

    def batch_bucket_files(self, bucket_id, *, add=None, delete=None):
        for source, target in add or []:
            if target == self.fail_path:
                raise OSError("upload_interrupted")
            self.files[target] = source if isinstance(source, bytes) else Path(source).read_bytes()
        for target in delete or []:
            self.files.pop(target, None)

    def download_bucket_files(self, bucket_id, files, *, raise_on_missing_files=False):
        for remote, local in files:
            if remote not in self.files:
                if raise_on_missing_files:
                    raise FileNotFoundError(remote)
                continue
            self.downloads.append(remote)
            Path(local).parent.mkdir(parents=True, exist_ok=True)
            Path(local).write_bytes(self.files[remote])


def test_completed_checkpoint_restores_closed_database_on_a_new_runner(tmp_path):
    from foreseebench.contract import Week

    original = tmp_path / "first"
    with Store(original) as store:
        store.begin(Week.parse("2026-11-09"), "formal", "config")
    api = BucketAPI()
    checkpoint = BucketCheckpoint("owner/state", api=api)
    receipt = checkpoint.save(original)
    restored = tmp_path / "second"
    resumed = BucketCheckpoint("owner/state", api=api)
    assert resumed.restore(restored)["generation"] == receipt["generation"]
    with Store(restored) as store:
        assert store.batch("2026-11-09")["config_hash"] == "config"
    assert "completed.json" in api.files


def test_interrupted_upload_keeps_previous_complete_generation_and_never_restores_corrupt_data(tmp_path):
    from foreseebench.store import atomic_json, read_manifest

    api = BucketAPI()
    first = tmp_path / "first"
    atomic_json(first / "scheduler.json", {"step": 1})
    checkpoint = BucketCheckpoint("owner/state", api=api)
    checkpoint.save(first)
    atomic_json(first / "scheduler.json", {"step": 2})
    api.fail_path = "completed.json"
    with pytest.raises(OSError, match="upload_interrupted"):
        checkpoint.save(first)
    resumed = BucketCheckpoint("owner/state", api=api)
    resumed.restore(tmp_path / "second")
    assert read_manifest(tmp_path / "second/scheduler.json") == {"step": 1}
    entry = resumed.manifest["files"]["scheduler.json"]
    api.files[entry["object"]] = b"corrupt"
    with pytest.raises(ValueError, match="checksum_mismatch"):
        resumed.restore(tmp_path / "third")
    assert not (tmp_path / "third/scheduler.json").exists()


def test_checkpoint_retains_two_generations_and_permanent_evidence_but_removes_orphans(tmp_path):
    from foreseebench.store import atomic_json

    api = BucketAPI()
    checkpoint = BucketCheckpoint("owner/state", api=api)
    evidence = tmp_path / "batches/week/V1/evidence.sqlite"
    evidence.parent.mkdir(parents=True)
    evidence.write_bytes(b"permanent evidence")
    hashes = []
    for step in (1, 2, 3):
        atomic_json(tmp_path / "scheduler.json", {"step": step})
        checkpoint.save(tmp_path)
        hashes.append(checkpoint.manifest["files"]["scheduler.json"]["object"])
    assert hashes[0] not in api.files
    assert hashes[1] in api.files and hashes[2] in api.files
    assert checkpoint.manifest["files"]["batches/week/V1/evidence.sqlite"]["object"] in api.files
    assert len([p for p in api.files if p.startswith("generations/")]) == 2


def test_source_budget_yields_without_completing_scan_and_resumes_the_saved_ranges(tmp_path):
    import httpx

    from foreseebench.contract import Week, instant
    from foreseebench.pipeline import Pipeline
    from foreseebench.source import Polymarket

    clock = [instant("2026-11-09T18:00:00Z")]
    boundary = instant("2026-11-09T18:00:10Z")
    ranges = []

    def handler(request):
        ids = request.url.params.get_list("id")
        if not ids:
            return httpx.Response(200, json={"events": [{"id": "101"}]})
        ranges.append(ids)
        if ids[0] == "1":
            clock[0] = boundary
        return httpx.Response(200, json={"events": [{"id": value} for value in ids]})

    week = Week.parse("2026-11-09")
    archive = tmp_path / "batches" / week.id / "raw-v1.sqlite"
    with (
        Store(tmp_path) as store,
        Polymarket(
            archive,
            rate=1e9,
            stop_at=boundary,
            clock=lambda: clock[0],
            transport_factory=lambda: httpx.MockTransport(handler),
        ) as pm,
    ):
        result = Pipeline(store, pm, clock=lambda: clock[0]).collect(week, "formal", "config")
        assert result["yielded"] and not result["scan_complete"]
        assert not result["construction_complete"]
        assert store.failures(week.id) == []
    with (
        Store(tmp_path) as store,
        Polymarket(
            archive, rate=1e9, clock=lambda: clock[0], transport_factory=lambda: httpx.MockTransport(handler)
        ) as pm,
    ):
        result = Pipeline(store, pm, clock=lambda: clock[0]).collect(week, "formal", "config", refresh_completed=False)
        assert result["construction_complete"]
        assert result["source_coverage"]["events"] == 101
    assert [ids[0] for ids in ranges] == ["1", "101"]


def test_restore_is_sparse_and_hydrates_only_the_requested_active_batch(tmp_path):
    from foreseebench.store import atomic_json

    api = BucketAPI()
    original = tmp_path / "first"
    for week in ("old", "active"):
        atomic_json(original / "batches" / week / "protocol.json", {"batch_id": week})
        (original / "batches" / week / "raw-v1.sqlite").write_bytes(week.encode())
    checkpoint = BucketCheckpoint("owner/state", api=api)
    checkpoint.save(original)
    restored = tmp_path / "second"
    resumed = BucketCheckpoint("owner/state", api=api)
    resumed.restore(restored)
    assert (restored / "batches/old/protocol.json").exists()
    assert not (restored / "batches/old/raw-v1.sqlite").exists()
    resumed.hydrate_batch(restored, "active", frozen=False)
    assert (restored / "batches/active/raw-v1.sqlite").read_bytes() == b"active"
    assert not (restored / "batches/old/raw-v1.sqlite").exists()
    resumed.save(restored)
    assert "batches/old/raw-v1.sqlite" in resumed.manifest["files"]


def test_gt_refresh_resumes_after_finished_event_and_keeps_partial_evidence(tmp_path):
    import httpx
    from test_contract import raw_event

    from foreseebench.contract import Week, admit_event, instant
    from foreseebench.pipeline import Pipeline
    from foreseebench.source import Polymarket
    from foreseebench.store import read_manifest

    week = Week.parse("2026-11-02")
    clock = [instant("2026-11-05T12:00:00Z")]
    stop_at = instant("2026-11-05T12:00:10Z")
    calls = []

    def handler(request):
        calls.append(request.url.path)
        if request.url.path == "/v2/tokens":
            clock[0] = stop_at
        return httpx.Response(200, json={"data": []})

    with Store(tmp_path) as store:
        store.begin(week, "bootstrap", "config")
        store.discovery_complete(week.id)
        for event_id in ("41", "42"):
            source = admit_event(dict(raw_event(), id=event_id), week, week.collection_start)
            store.event(week.id, event_id, source, "collected")
            for table in ("comments", "trades"):
                store.page(week.id, event_id, table, "empty", [], None, True)
        store.freeze(week, week.freeze_at)
        with Polymarket(
            tmp_path / "batches" / week.id / "raw-v3.sqlite",
            rate=1e9,
            stop_at=stop_at,
            clock=lambda: clock[0],
            transport_factory=lambda: httpx.MockTransport(handler),
        ) as pm:
            result = Pipeline(store, pm, clock=lambda: clock[0]).refresh(week.id, resume=True)
        assert result["yielded"]
        assert read_manifest(tmp_path / "batches" / week.id / "refresh-cursor.json")["after_event_id"] == "41"
        with Polymarket(
            tmp_path / "batches" / week.id / "raw-v3.sqlite",
            rate=1e9,
            clock=lambda: clock[0],
            transport_factory=lambda: httpx.MockTransport(handler),
        ) as pm:
            result = Pipeline(store, pm, clock=lambda: clock[0]).refresh(week.id, resume=True)
        assert not result["yielded"]
        assert store.latest_resolution(week.id, "42") is not None
        assert store.failures(week.id) == []
    assert calls == ["/v2/resolutions", "/v2/tokens", "/v2/resolutions", "/v2/tokens"]


def test_run_once_restores_progress_and_freezes_only_after_complete_source_census(tmp_path):
    import httpx

    from foreseebench.cloud import run_once
    from foreseebench.contract import instant

    now = [instant("2026-11-08T10:00:00Z")]
    ranges = []

    def handler(request):
        ids = request.url.params.get_list("id")
        if not ids:
            return httpx.Response(200, json={"events": [{"id": "101"}]})
        ranges.append(ids[0])
        if ids[0] == "1":
            now[0] = instant("2026-11-08T10:00:10Z")
        return httpx.Response(200, json={"events": [{"id": value} for value in ids]})

    api = BucketAPI()
    config = {
        "storage": {"root": tmp_path / "first"},
        "scheduler": {"publish": False, "poll_seconds": 300},
        "publication": {},
        "config_hash": "config",
        "network": {
            "gamma_url": "https://source.invalid",
            "data_url": "https://source.invalid",
            "timeout_seconds": 1,
            "attempts": 1,
            "requests_per_second": 1e9,
            "workers": 1,
            "transport_factory": lambda: httpx.MockTransport(handler),
        },
    }
    first = run_once(
        config, BucketCheckpoint("owner/state", api=api), bootstrap=True, work_seconds=10, clock=lambda: now[0]
    )
    assert first["yielded"] and first["checkpoint"]["generation"]
    config["storage"]["root"] = tmp_path / "second"
    second = run_once(
        config, BucketCheckpoint("owner/state", api=api), bootstrap=True, work_seconds=10, clock=lambda: now[0]
    )
    assert not second["yielded"] and second["errors"] == {}
    with Store(tmp_path / "second") as store:
        batches = store.status()
        assert len(batches) == 1 and batches[0]["frozen"]
    assert ranges == ["1", "101"]


def test_run_once_recovers_committed_first_publication_from_a_cold_runner(tmp_path, monkeypatch):
    from datetime import timedelta

    from test_contract import raw_event
    from test_delivery import archive_event_source, fake_dataset_hub

    from foreseebench import delivery
    from foreseebench.cloud import CheckpointDeadline, run_once
    from foreseebench.contract import Week, admit_event, digest, instant
    from foreseebench.store import read_manifest

    now = instant("2026-11-08T12:00:00Z")
    week = Week.bootstrap(now)
    initial = tmp_path / "initial"
    raw = raw_event()
    raw["markets"][0]["endDate"] = "2026-11-13T20:00:00Z"
    archive_event_source(initial, raw, batch_id=week.id)
    with Store(initial) as store:
        store.begin(week, "bootstrap", "config")
        source = admit_event(raw, week, now)
        store.event(week.id, source["event_id"], source, "collected")
        for kind in ("comments", "trades"):
            store.page(week.id, source["event_id"], kind, "empty", [], None, True)
        store.discovery_complete(week.id)
    api = BucketAPI()
    api.repositories["dataset", "owner/private"] = SimpleNamespace(private=True, used_storage=0)
    BucketCheckpoint("owner/state", api=api).save(initial)
    remote = tmp_path / "hub"
    hub = fake_dataset_hub(remote, monkeypatch)

    def interrupted(repo_id, filename, **kwargs):
        if filename.startswith("balanced/basic/"):
            raise CheckpointDeadline("runner_lost_after_commit")
        return hub.download(repo_id, filename, **kwargs)

    monkeypatch.setattr(delivery, "hf_hub_download", interrupted)
    config = {
        "storage": {"root": tmp_path / "first"},
        "scheduler": {"publish": True, "poll_seconds": 300},
        "publication": {"repo_id": "owner/private", "revision": "main"},
        "config_hash": "config",
    }
    with pytest.raises(CheckpointDeadline, match="runner_lost_after_commit"):
        run_once(config, BucketCheckpoint("owner/state", api=api), clock=lambda: now)
    committed = read_manifest(remote / hub.head / "manifest.json")
    binding = committed["batches"][week.id]
    frozen = read_manifest(remote / hub.head / binding["manifest"])
    hub.downloads.clear()
    monkeypatch.setattr(delivery, "hf_hub_download", hub.download)
    fresh = tmp_path / "cold-runner"
    resumed = run_once(
        dict(config, storage={"root": fresh}),
        BucketCheckpoint("owner/state", api=api),
        clock=lambda: now + timedelta(hours=1),
    )
    assert resumed["errors"] == {}
    assert len(hub.commits) == 1
    assert read_manifest(fresh / binding["manifest"]) == frozen
    receipt = read_manifest((fresh / binding["manifest"]).parent / "publication.json")
    assert receipt["manifest_hash"] == digest(frozen) == binding["manifest_hash"]
    assert set(committed["updated_artifacts"]) <= {name for _, name in hub.downloads}
    final = tmp_path / "receipt-check"
    BucketCheckpoint("owner/state", api=api).restore(final)
    assert read_manifest((final / binding["manifest"]).parent / "publication.json") == receipt


def test_run_once_does_not_upload_when_export_exhausts_the_work_budget(tmp_path, monkeypatch):
    import os
    from datetime import timedelta

    from test_delivery import fake_dataset_hub

    from foreseebench.cloud import run_once
    from foreseebench.contract import Week, instant

    now = instant("2026-11-08T12:00:00Z")
    week = Week.bootstrap(now)
    with Store(tmp_path / "initial") as store:
        store.begin(week, "bootstrap", "config")
        store.discovery_complete(week.id)
        store.freeze(week, now)
    api = BucketAPI()
    api.repositories["dataset", "owner/private"] = SimpleNamespace(private=True, used_storage=0)
    BucketCheckpoint("owner/state", api=api).save(tmp_path / "initial")
    hub = fake_dataset_hub(tmp_path / "hub", monkeypatch)
    current = {"now": now, "exporting": False}
    repo_info, fsync = hub.repo_info, os.fsync

    def lookup(*args, **kwargs):
        current["exporting"] = True
        return repo_info(*args, **kwargs)

    def slow_disk(fd):
        fsync(fd)
        if current["exporting"]:
            current["now"] = now + timedelta(seconds=11)

    monkeypatch.setattr(hub, "repo_info", lookup)
    monkeypatch.setattr(os, "fsync", slow_disk)
    result = run_once(
        {
            "storage": {"root": tmp_path / "runner"},
            "scheduler": {"publish": True, "poll_seconds": 300},
            "publication": {"repo_id": "owner/private", "revision": "main"},
            "config_hash": "config",
        },
        BucketCheckpoint("owner/state", api=api),
        work_seconds=10,
        clock=lambda: current["now"],
    )
    assert current["now"] == now + timedelta(seconds=11)
    assert hub.preuploads == []
    assert hub.commits == []
    assert result["yielded"] and result["errors"] == {}
    assert result["checkpoint"]["generation"]
    restored = tmp_path / "next-runner"
    BucketCheckpoint("owner/state", api=api).restore(restored)
    with Store(restored) as store:
        assert store.batch(week.id)["frozen"]


@pytest.mark.parametrize("legacy", [False, True])
def test_run_once_publishes_a_closed_batch_from_sparse_checkpoint_state(tmp_path, monkeypatch, legacy):
    from test_delivery import fake_dataset_hub

    from foreseebench.cloud import run_once
    from foreseebench.contract import Week, instant, stamp
    from foreseebench.store import atomic_json, read_manifest

    week = Week.bootstrap(instant("2026-11-08T12:00:00Z"))
    initial = tmp_path / "initial"
    with Store(initial) as store:
        store.begin(week, "bootstrap", "config")
        store.discovery_complete(week.id)
        store.freeze(week, week.collection_start)
        store.release_results(week, week.evidence_cutoff)
    if legacy:
        protocol = week.record()
        protocol["policy"].pop("domain_event_cap")
        protocol["policy"].update(target_events=1000, minimum_events=500)
        atomic_json(initial / "batches" / week.id / "protocol.json", protocol)
    atomic_json(
        initial / "scheduler.json",
        {"last_refresh": {}, "gt_closed": {week.id: {"deadline": stamp(week.evidence_cutoff)}}},
    )
    api = BucketAPI()
    api.repositories["dataset", "owner/private"] = SimpleNamespace(private=True, used_storage=0)
    BucketCheckpoint("owner/state", api=api).save(initial)
    hub = fake_dataset_hub(tmp_path / "hub", monkeypatch)
    result = run_once(
        {
            "storage": {"root": tmp_path / "runner"},
            "scheduler": {"publish": True, "poll_seconds": 300},
            "publication": {"repo_id": "owner/private", "revision": "main"},
            "config_hash": "config",
        },
        BucketCheckpoint("owner/state", api=api),
        clock=lambda: week.finalize_at,
    )
    assert result["errors"] == {}
    if legacy:
        assert hub.commits == []
        return
    assert len(hub.commits) == 1
    remote = read_manifest(tmp_path / "hub" / hub.head / "manifest.json")
    assert remote["batches"][week.id]["stage"] == "V3"


def test_run_once_finishes_durable_publication_before_a_new_census_exhausts_the_budget(tmp_path, monkeypatch):
    from datetime import timedelta

    import httpx
    from test_delivery import fake_dataset_hub

    from foreseebench.cloud import run_once
    from foreseebench.contract import Week, instant, stamp
    from foreseebench.store import atomic_json, read_manifest

    week = Week.parse("2026-10-26")
    initial = tmp_path / "initial"
    with Store(initial) as store:
        store.begin(week, "formal", "config")
        store.discovery_complete(week.id)
        store.freeze(week, week.freeze_at)
        store.release_results(week, week.evidence_cutoff)
    atomic_json(
        initial / "scheduler.json",
        {"last_refresh": {}, "gt_closed": {week.id: {"deadline": stamp(week.evidence_cutoff)}}},
    )
    api = BucketAPI()
    api.repositories["dataset", "owner/private"] = SimpleNamespace(private=True, used_storage=0)
    BucketCheckpoint("owner/state", api=api).save(initial)
    hub = fake_dataset_hub(tmp_path / "hub", monkeypatch)
    now = instant("2026-11-09T17:00:00Z")
    current = {"now": now}
    requests = []

    def source(request):
        requests.append(request.url.path)
        current["now"] = now + timedelta(seconds=11)
        return httpx.Response(200, json={"events": [{"id": "101"}]})

    result = run_once(
        {
            "storage": {"root": tmp_path / "runner"},
            "scheduler": {"publish": True, "poll_seconds": 300},
            "publication": {"repo_id": "owner/private", "revision": "main"},
            "config_hash": "config",
            "network": {
                "gamma_url": "https://source.invalid",
                "data_url": "https://source.invalid",
                "timeout_seconds": 1,
                "attempts": 1,
                "requests_per_second": 1e9,
                "workers": 1,
                "transport_factory": lambda: httpx.MockTransport(source),
            },
        },
        BucketCheckpoint("owner/state", api=api),
        work_seconds=10,
        clock=lambda: current["now"],
    )
    assert requests
    assert result["yielded"] and result["errors"] == {}
    assert len(hub.commits) == 1
    remote = read_manifest(tmp_path / "hub" / hub.head / "manifest.json")
    assert remote["batches"][week.id]["stage"] == "V3"
    restored = tmp_path / "next-runner"
    BucketCheckpoint("owner/state", api=api).restore(restored)
    assert (restored / Path(remote["batches"][week.id]["manifest"]).parent / "publication.json").is_file()


def test_qualification_namespace_cannot_read_or_gc_production_objects(tmp_path):
    from foreseebench.store import atomic_json

    api = BucketAPI()
    production = BucketCheckpoint("owner/state", api=api)
    atomic_json(tmp_path / "prod/scheduler.json", {"production": True})
    production.save(tmp_path / "prod")
    production_files = dict(api.files)
    qualification = BucketCheckpoint("owner/state", api=api, namespace="qualification/check-1")
    assert qualification.restore(tmp_path / "qual") is None
    for step in (1, 2, 3):
        atomic_json(tmp_path / "qual/scheduler.json", {"step": step})
        qualification.save(tmp_path / "qual")
    assert all(api.files[path] == data for path, data in production_files.items())
    assert "qualification/check-1/completed.json" in api.files


def test_checkpoint_refuses_live_database_or_a_stale_writer(tmp_path):
    from foreseebench.store import atomic_json

    api = BucketAPI()
    first = BucketCheckpoint("owner/state", api=api)
    with Store(tmp_path / "first"):
        with pytest.raises(RuntimeError, match="checkpoint_requires_closed_store"):
            first.save(tmp_path / "first")
    first.save(tmp_path / "first")
    stale = BucketCheckpoint("owner/state", api=api)
    stale.restore(tmp_path / "stale")
    atomic_json(tmp_path / "first/scheduler.json", {"step": 2})
    first.save(tmp_path / "first")
    with pytest.raises(RuntimeError, match="checkpoint_parent_changed"):
        stale.save(tmp_path / "stale")


def test_sparse_restore_rejects_a_generation_with_missing_unhydrated_evidence(tmp_path):
    from foreseebench.store import atomic_json

    original = tmp_path / "first"
    atomic_json(original / "scheduler.json", {"closed": True})
    archive = original / "batches/old/raw-v1.sqlite"
    archive.parent.mkdir(parents=True)
    archive.write_bytes(b"frozen evidence")
    api = BucketAPI()
    checkpoint = BucketCheckpoint("owner/state", api=api)
    checkpoint.save(original)
    api.files.pop(checkpoint.manifest["files"]["batches/old/raw-v1.sqlite"]["object"])
    with pytest.raises(ValueError, match="checkpoint_generation_incomplete"):
        BucketCheckpoint("owner/state", api=api).restore(tmp_path / "second")
    assert not (tmp_path / "second/scheduler.json").exists()


def test_storage_limit_counts_visible_private_repositories_and_buckets_before_publication():
    from huggingface_hub import CommitOperationAdd

    from foreseebench.cloud import StorageGuard, StorageLimit

    api = BucketAPI()
    api.repositories = {
        ("dataset", "owner/private"): SimpleNamespace(private=True, used_storage=20),
        ("model", "owner/model"): SimpleNamespace(private=True, used_storage=30),
        ("space", "owner/public"): SimpleNamespace(private=False, used_storage=None),
    }
    api.bucket_sizes = {"owner/state": 10, "owner/other": 10}
    guard = StorageGuard(api, "owner/state", limit_bytes=100)
    first = CommitOperationAdd(path_in_repo="data/first.jsonl", path_or_fileobj=b"x" * 20)
    assert guard.before_publish("owner/private", [first])["projected_bytes"] == 90
    assert guard.before_publish("owner/private", [first])["projected_bytes"] == 90
    with pytest.raises(StorageLimit) as error:
        guard.before_publish(
            "owner/private", [CommitOperationAdd(path_in_repo="data/second.jsonl", path_or_fileobj=b"y" * 10)]
        )
    assert error.value.report["status"] == "storage_limit"
    assert error.value.report["projected_bytes"] == 100
    assert error.value.report["reason"] == "limit_reached"


@pytest.mark.parametrize("unknown", ["repository", "bucket", "listing"])
def test_unknown_storage_usage_never_authorizes_checkpoint_upload(tmp_path, unknown):
    from foreseebench.cloud import StorageLimit
    from foreseebench.store import atomic_json

    api = BucketAPI()
    if unknown == "repository":
        api.repositories["model", "owner/unknown"] = SimpleNamespace(private=True, used_storage=None)
    elif unknown == "bucket":
        api.bucket_sizes["owner/other"] = None
    else:

        def unavailable(*, author):
            raise OSError("listing_unavailable")

        api.list_models = unavailable
    atomic_json(tmp_path / "scheduler.json", {"step": 1})
    with pytest.raises(StorageLimit) as error:
        BucketCheckpoint("owner/state", api=api).save(tmp_path)
    assert error.value.report["reason"] == "usage_unknown"
    assert error.value.report["visible_used_bytes"] is None
    assert api.files == {}


def test_checkpoint_reserves_new_bytes_and_keeps_previous_generation_when_capacity_is_exhausted(tmp_path):
    from foreseebench.cloud import DEFAULT_STORAGE_LIMIT_BYTES, StorageLimit
    from foreseebench.store import atomic_json

    api = BucketAPI()
    original = tmp_path / "first"
    atomic_json(original / "scheduler.json", {"step": 1})
    BucketCheckpoint("owner/state", api=api).save(original)
    retained = dict(api.files)
    api.bucket_sizes["owner/state"] = DEFAULT_STORAGE_LIMIT_BYTES - sum(map(len, api.files.values())) - 1000
    resumed = BucketCheckpoint("owner/state", api=api)
    restored = tmp_path / "second"
    resumed.restore(restored)
    atomic_json(restored / "scheduler.json", {"step": 2, "note": "x" * 6000})
    with pytest.raises(StorageLimit) as error:
        resumed.save(restored)
    assert error.value.report["planned_new_bytes"] > 6000
    assert error.value.report["reason"] == "limit_reached"
    assert api.files == retained


def test_run_once_reports_storage_limit_without_writing_or_losing_the_completed_pointer(tmp_path):
    from foreseebench.cloud import DEFAULT_STORAGE_LIMIT_BYTES, run_once
    from foreseebench.contract import instant
    from foreseebench.store import atomic_json

    api = BucketAPI()
    atomic_json(tmp_path / "first/scheduler.json", {"last_refresh": {}})
    BucketCheckpoint("owner/state", api=api).save(tmp_path / "first")
    retained = dict(api.files)
    api.bucket_sizes["owner/state"] = DEFAULT_STORAGE_LIMIT_BYTES
    result = run_once(
        {"storage": {"root": tmp_path / "second"}, "scheduler": {"publish": False}, "publication": {}},
        BucketCheckpoint("owner/state", api=api),
        clock=lambda: instant("2026-11-08T12:00:00Z"),
    )
    assert result["storage"]["status"] == "storage_limit"
    assert result["status"]["phases"] == {"storage_limit": 1}
    assert result["checkpoint"]["status"] == "not_written"
    assert api.files == retained


def test_run_once_checks_publication_additions_before_any_dataset_upload(tmp_path, monkeypatch):
    from foreseebench import delivery
    from foreseebench.cloud import DEFAULT_STORAGE_LIMIT_BYTES, run_once
    from foreseebench.contract import Week, instant

    class PublicationAPI(BucketAPI):
        def file_exists(self, *args, **kwargs):
            return False

        def preupload_lfs_files(self, *args, **kwargs):
            raise AssertionError("storage_limit_must_precede_preupload")

        def create_commit(self, *args, **kwargs):
            raise AssertionError("storage_limit_must_precede_commit")

    now = instant("2026-11-08T12:00:00Z")
    api = PublicationAPI()
    info = SimpleNamespace(private=True, used_storage=0, sha="head")
    api.repositories["dataset", "owner/private"] = info
    week = Week.bootstrap(now)
    with Store(tmp_path / "first") as store:
        store.begin(week, "bootstrap", "config")
        store.discovery_complete(week.id)
        store.freeze(week, now)
    BucketCheckpoint("owner/state", api=api).save(tmp_path / "first")
    retained = dict(api.files)
    info.used_storage = DEFAULT_STORAGE_LIMIT_BYTES - sum(map(len, api.files.values())) - 1
    monkeypatch.setattr(delivery, "HfApi", lambda: api)
    result = run_once(
        {
            "storage": {"root": tmp_path / "second"},
            "scheduler": {"publish": True, "poll_seconds": 300},
            "publication": {"repo_id": "owner/private", "revision": "main"},
            "config_hash": "config",
        },
        BucketCheckpoint("owner/state", api=api),
        clock=lambda: now,
    )
    assert result["storage"]["status"] == "storage_limit"
    assert result["storage"]["reason"] == "limit_reached"
    assert result["storage"]["planned_new_bytes"] > 0
    assert api.files == retained


def test_checkpoint_budget_includes_publication_bytes_reserved_in_the_same_run(tmp_path):
    from huggingface_hub import CommitOperationAdd

    from foreseebench.cloud import StorageLimit
    from foreseebench.store import atomic_json

    api = BucketAPI()
    api.repositories["dataset", "owner/private"] = SimpleNamespace(private=True, used_storage=0)
    checkpoint = BucketCheckpoint("owner/state", api=api, storage_limit_bytes=6000)
    checkpoint.storage_guard.before_publish(
        "owner/private", [CommitOperationAdd(path_in_repo="data/new.jsonl", path_or_fileobj=b"x" * 2000)]
    )
    atomic_json(tmp_path / "scheduler.json", {"step": 1})
    with pytest.raises(StorageLimit) as error:
        checkpoint.save(tmp_path)
    assert error.value.report["reserved_bytes"] == 2000
    assert error.value.report["projected_bytes"] >= 6000
    assert api.files == {}
