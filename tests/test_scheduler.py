from datetime import UTC, datetime

from foreseebench import cli
from foreseebench.contract import Week
from foreseebench.store import Store


def test_incomplete_old_batch_cannot_block_later_weeks(tmp_path, monkeypatch):
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return cls(2026, 11, 10, 12, tzinfo=UTC)

    monkeypatch.setattr(cli, "datetime", Clock)
    with Store(tmp_path) as store:
        store.begin(Week.parse("2026-11-02"), "formal", "config")
        store.begin(Week.parse("2026-11-09"), "formal", "config")
        store.discovery_complete("2026-11-09")
    config = {
        "storage": {"root": tmp_path},
        "scheduler": {"publish": False, "poll_seconds": 300},
        "publication": {},
        "config_hash": "config",
    }
    cli.tick(config)
    with Store(tmp_path) as store:
        assert store.batch("2026-11-09")["frozen"] is True
        assert store.batch("2026-11-02")["frozen"] is False


def test_scheduler_repairs_pointer_after_database_already_frozen(tmp_path, monkeypatch):
    import foreseebench.store as module

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return cls(2026, 11, 3, 12, tzinfo=UTC)

    monkeypatch.setattr(cli, "datetime", Clock)
    calls = []
    monkeypatch.setattr(cli, "publish", lambda *a, **k: calls.append(True))
    with Store(tmp_path) as store:
        store.begin(Week.parse("2026-11-02"), "formal", "config")
        store.discovery_complete("2026-11-02")
    real_write = module.atomic_json

    def crash(path, value):
        if path.name == "latest.json":
            raise OSError("power_loss")
        real_write(path, value)

    config = {
        "storage": {"root": tmp_path},
        "scheduler": {"publish": True, "poll_seconds": 300},
        "publication": {},
        "config_hash": "config",
    }
    monkeypatch.setattr(module, "atomic_json", crash)
    cli.tick(config)
    monkeypatch.setattr(module, "atomic_json", real_write)
    cli.tick(config)
    assert (tmp_path / "batches/2026-11-02/latest.json").exists()
    assert calls == [True]


def test_scheduler_waits_for_freeze_after_full_pass_instead_of_starting_unfinishable_rescans(tmp_path, monkeypatch):
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return cls(2026, 11, 2, 5, 59, tzinfo=UTC)

    monkeypatch.setattr(cli, "datetime", Clock)
    monkeypatch.setattr(
        cli.Pipeline, "collect", lambda *a, **k: (_ for _ in ()).throw(AssertionError("unexpected rescan"))
    )
    with Store(tmp_path) as store:
        store.begin(Week.parse("2026-11-02"), "formal", "config")
        store.discovery_complete("2026-11-02")
    cli.tick(
        {
            "storage": {"root": tmp_path},
            "scheduler": {"publish": False, "poll_seconds": 300},
            "publication": {},
            "config_hash": "config",
        }
    )
    from foreseebench.store import read_manifest

    assert read_manifest(tmp_path / "scheduler.json")["last_error"] is None


def test_bounded_tick_does_not_open_sources_after_its_work_deadline(tmp_path):
    from foreseebench.contract import instant
    from foreseebench.store import read_manifest

    now = instant("2026-11-09T18:00:00Z")
    config = {
        "storage": {"root": tmp_path},
        "scheduler": {"publish": False, "poll_seconds": 300},
        "publication": {},
        "config_hash": "config",
    }
    result = cli.tick(config, clock=lambda: now, stop_at=now, bootstrap=True)
    assert result["yielded"]
    assert result["errors"] == {}
    assert read_manifest(tmp_path / "scheduler.json")["last_tick"] == "2026-11-09T18:00:00.000000Z"


def test_bootstrap_freezes_completed_construction_at_the_actual_early_time(tmp_path):
    from foreseebench.contract import instant
    from foreseebench.store import read_manifest

    now = instant("2026-11-08T18:00:00Z")
    week = Week.bootstrap(now)
    with Store(tmp_path) as store:
        store.begin(week, "bootstrap", "config")
        store.discovery_complete(week.id)
    result = cli.tick(
        {
            "storage": {"root": tmp_path},
            "scheduler": {"publish": False, "poll_seconds": 300},
            "publication": {},
            "config_hash": "config",
        },
        clock=lambda: now,
        bootstrap=True,
    )
    assert result["errors"] == {}
    manifest = read_manifest(tmp_path / "batches" / week.id / "V1/manifest.json")
    assert manifest["frozen_at"] == "2026-11-08T18:00:00.000000Z"
    assert manifest["scheduled_freeze_at"] == "2026-11-09T12:00:00.000000Z"


def test_expired_incomplete_bootstrap_stops_requests_and_records_failure_once(tmp_path):
    from foreseebench.contract import instant
    from foreseebench.store import read_manifest

    now = instant("2026-11-08T18:00:00Z")
    week = Week.bootstrap(instant("2026-11-07T00:00:00Z"))
    with Store(tmp_path) as store:
        store.begin(week, "bootstrap", "config")
    config = {"storage": {"root": tmp_path}, "scheduler": {"publish": False}, "config_hash": "config"}
    for _ in range(2):
        assert cli.tick(config, clock=lambda: now)["errors"] == {}
    with Store(tmp_path) as store:
        assert len(store.failures(week.id)) == 1
        assert not store.batch(week.id)["frozen"]
    assert read_manifest(tmp_path / "scheduler.json")["collection_closed"][week.id]["status"] == "expired_incomplete"


def test_existing_gt_work_has_priority_over_a_new_full_domain_census(tmp_path):
    import httpx
    from test_contract import raw_event

    from foreseebench.contract import admit_event, instant

    clock = [instant("2026-11-09T18:00:00Z")]
    stop_at = instant("2026-11-09T18:00:10Z")
    calls = []

    def handler(request):
        calls.append(request.url.path)
        clock[0] = stop_at
        return httpx.Response(200, json={"data": []})

    week = Week.parse("2026-11-02")
    with Store(tmp_path) as store:
        store.begin(week, "bootstrap", "config")
        store.discovery_complete(week.id)
        store.event(week.id, "42", admit_event(raw_event(), week, week.collection_start), "collected")
        for table in ("comments", "trades"):
            store.page(week.id, "42", table, "empty", [], None, True)
        store.freeze(week, week.freeze_at)
    config = {
        "storage": {"root": tmp_path},
        "config_hash": "config",
        "scheduler": {"publish": False, "poll_seconds": 300},
        "network": {
            "gamma_url": "https://source.invalid",
            "data_url": "https://source.invalid",
            "timeout_seconds": 1,
            "attempts": 1,
            "requests_per_second": 1e9,
            "transport_factory": lambda: httpx.MockTransport(handler),
        },
    }
    result = cli.tick(config, clock=lambda: clock[0], stop_at=stop_at)
    assert result["yielded"] and result["errors"] == {}
    assert calls == ["/v2/resolutions"]
    assert result["status"] == {
        "date": "2026-11-09",
        "phases": {"awaiting_gt": 1},
        "source_event_counts": {"collected": 1},
        "stages": {"V2": "not_configured", "V4": "not_configured"},
    }
