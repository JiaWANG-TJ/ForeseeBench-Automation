import hashlib
import json

import httpx

from deploy.hf_space.monitor import request_activation


def test_monitor_submission_requires_owner_confirmation_and_is_not_repeated(tmp_path):
    calls = []
    url = "https://sii-jiawang-foreseebench-automation.hf.space/health"

    def endpoint(request):
        calls.append(request)
        if request.method == "GET":
            assert request.url.params["email"] == "owner@example.invalid"
            return httpx.Response(
                200, json={"nonce": "fixture", "timestamp": 100, "difficulty": 4, "signature": "signed"}
            )
        payload = json.loads(request.content)
        assert payload["url"] == url
        proof = hashlib.sha256(f"fixture|{payload['counter']}".encode()).digest()
        assert int.from_bytes(proof, "big") < 1 << 252
        return httpx.Response(200, json={"ok": True})

    with httpx.Client(transport=httpx.MockTransport(endpoint)) as client:
        receipt = tmp_path / "monitor.json"
        result = request_activation("owner@example.invalid", url, receipt, client=client)
        assert result["state"] == "pending_owner_confirmation"
        assert request_activation("owner@example.invalid", url, receipt, client=client) == result
    assert len(calls) == 2
