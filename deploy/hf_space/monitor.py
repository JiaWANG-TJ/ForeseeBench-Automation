"""通过UptimeRobot官方邮箱确认流程申请本项目的免费监控。"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import time
from datetime import UTC, datetime
from pathlib import Path

import httpx

API = "https://api.uptimerobot.com/agentic/agent-monitor"
HEALTH_URL = "https://sii-jiawang-foreseebench-automation.hf.space/health"


def request_activation(email: str, url: str, receipt: Path, *, client: httpx.Client) -> dict:
    if url != HEALTH_URL or "@" not in email or any(character.isspace() for character in email):
        raise ValueError("monitor_target_invalid")
    identity = hashlib.sha256(json.dumps([email, url]).encode()).hexdigest()
    if receipt.exists():
        previous = json.loads(receipt.read_text())
        if previous["identity"] != identity:
            raise ValueError("monitor_receipt_identity_mismatch")
        return previous
    response = client.get(API + "/challenge", params={"email": email, "url": url})
    response.raise_for_status()
    challenge = response.json()
    difficulty = challenge["difficulty"]
    if isinstance(difficulty, bool) or not isinstance(difficulty, int) or not 0 <= difficulty <= 30:
        raise ValueError("monitor_challenge_invalid")
    if not all(isinstance(challenge.get(key), str) and challenge[key] for key in ("nonce", "signature")):
        raise ValueError("monitor_challenge_invalid")
    target, counter = 1 << (256 - difficulty), 0
    deadline = time.monotonic() + 60
    while int.from_bytes(hashlib.sha256(f"{challenge['nonce']}|{counter}".encode()).digest(), "big") >= target:
        counter += 1
        if counter % 1024 == 0 and time.monotonic() >= deadline:
            raise TimeoutError("monitor_challenge_budget")
    record = {
        "identity": identity, "url": url, "requested_at": datetime.now(UTC).isoformat(),
        "state": "submission_unconfirmed",
    }
    receipt.parent.mkdir(parents=True, exist_ok=True)
    receipt.write_text(json.dumps(record, indent=2) + "\n")
    response = client.post(
        API,
        json={
            "email": email, "url": url, "nonce": challenge["nonce"], "timestamp": challenge["timestamp"],
            "counter": counter, "signature": challenge["signature"],
        },
    )
    response.raise_for_status()
    record["state"] = "pending_owner_confirmation"
    receipt.write_text(json.dumps(record, indent=2) + "\n")
    return record


def main() -> None:
    parser = argparse.ArgumentParser(description="申请UptimeRobot监控；收到邮件确认后才会生效")
    parser.add_argument("--receipt", type=Path, required=True)
    args = parser.parse_args()
    email = os.environ.get("UPTIMEROBOT_EMAIL", "")
    with httpx.Client(timeout=20) as client:
        health = client.get(HEALTH_URL)
        health.raise_for_status()
        if health.json().get("healthy") is not True:
            raise ValueError("space_not_ready_for_monitoring")
        print(json.dumps(request_activation(email, HEALTH_URL, args.receipt, client=client)))


if __name__ == "__main__":
    main()
