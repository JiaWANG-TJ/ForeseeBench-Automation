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
