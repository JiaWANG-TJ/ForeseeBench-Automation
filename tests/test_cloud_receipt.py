import sys

import httpx
from test_cloud import BucketAPI

from foreseebench import cli, cloud
from foreseebench.store import read_manifest


def test_cloud_cli_writes_verified_receipt_and_restores_it_on_the_next_run(tmp_path, monkeypatch):
    api = BucketAPI()
    monkeypatch.setattr(cloud, "HfApi", lambda: api)

    def source(request):
        return httpx.Response(200, json={"events": [{"id": "1"}]})

    monkeypatch.setattr(
        cli,
        "settings",
        lambda path: {
            "storage": {"root": tmp_path / "unused"},
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
                "transport_factory": lambda: httpx.MockTransport(source),
            },
        },
    )
    output = tmp_path / "receipt.json"
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "cloud", "--bucket", "owner/state", "--qualification",
            "--namespace", "qualification/receipt", "--work-seconds", "30",
            "--receipt-output", str(output),
        ],
    )
    cloud.main()
    first = read_manifest(output)
    assert first["errors"] == {}
    assert first["checkpoint"]["generation"]
    assert first["restored"] is None
    cloud.main()
    second = read_manifest(output)
    assert second["restored"]["generation"] == first["checkpoint"]["generation"]
    assert second["checkpoint"]["generation"] != first["checkpoint"]["generation"]
