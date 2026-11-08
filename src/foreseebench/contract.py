"""唯一的时间、来源绑定和正式支付核验规则。"""

from __future__ import annotations

import dataclasses
import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any
from uuid import NAMESPACE_URL, uuid5

import simplejson

Record = dict[str, Any]
UNRESOLVED_STATES = {"initialized", "posed", "active"}
ANSWER_STATES = {"resolved", "proposed", "challenged", "reproposed", "disputed", "arbitration"}


@dataclass(frozen=True)
class Policy:
    """唯一生产协议；实际参数随批次冻结，禁止事后改变期限。"""

    collection_start_offset_hours: int = 17
    freeze_offset_hours: int = 35
    publish_offset_hours: int = 41
    submission_hours: int = 72
    ending_window_days: int = 5
    finalization_hours: int = 24
    domain_event_cap: int = 200
    sampling_seed: int = 20261105
    legacy_gt_grace_hours: int | None = None
    historical_policy: Record | None = dataclasses.field(default=None, repr=False)

    def __post_init__(self) -> None:
        if not 1 <= self.domain_event_cap <= 200:
            raise ValueError("invalid_domain_event_cap")
        if not 0 <= self.collection_start_offset_hours <= self.freeze_offset_hours <= self.publish_offset_hours:
            raise ValueError("invalid_publication_schedule")
        if min(self.submission_hours, self.ending_window_days) <= 0 or self.finalization_hours < 0:
            raise ValueError("invalid_protocol_duration")
        if self.legacy_gt_grace_hours is not None and self.legacy_gt_grace_hours <= 0:
            raise ValueError("invalid_legacy_grace_period")

    @property
    def read_only(self) -> bool:
        return self.historical_policy is not None or self.legacy_gt_grace_hours is not None

    def record(self) -> Record:
        if self.historical_policy is not None:
            return dict(self.historical_policy)
        return {
            key: value
            for key, value in dataclasses.asdict(self).items()
            if key not in {"legacy_gt_grace_hours", "historical_policy"}
        }

    @classmethod
    def load(cls, values: Record | None = None) -> Policy:
        parameters = dict(values or {})
        historical = bool({"freeze_hour", "gt_grace_hours", "target_events", "minimum_events"} & parameters.keys())
        if "freeze_hour" in parameters or "gt_grace_hours" in parameters:
            parameters = {
                "collection_start_offset_hours": 0,
                "freeze_offset_hours": parameters.pop("freeze_hour", 6),
                "publish_offset_hours": 24,
                "ending_window_days": 30,
                "finalization_hours": 0,
                "legacy_gt_grace_hours": parameters.pop("gt_grace_hours", 48),
                **parameters,
            }
        parameters.pop("target_events", None)
        parameters.pop("minimum_events", None)
        if historical:
            parameters["historical_policy"] = dict(values or {})
        return cls(**parameters)


def dumps(value: Any) -> str:
    return simplejson.dumps(
        value,
        ensure_ascii=False,
        use_decimal=True,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def digest(value: Any) -> str:
    return hashlib.sha256(dumps(value).encode()).hexdigest()


def instant(value: str | datetime) -> datetime:
    result = value if isinstance(value, datetime) else datetime.fromisoformat(value.replace("Z", "+00:00"))
    if result.tzinfo is None:
        raise ValueError("timezone_missing")
    return result.astimezone(UTC)


def stamp(value: str | datetime) -> str:
    return instant(value).isoformat(timespec="microseconds").replace("+00:00", "Z")


def metadata_time(value: object) -> str | None:
    """非准入时间允许缺失；原始值保留在来源归档，不替它编造时间。"""
    if not isinstance(value, (str, datetime)):
        return None
    try:
        return stamp(value)
    except ValueError:
        return None


def array(value: Any) -> list[Any]:
    result = json.loads(value) if isinstance(value, str) else value
    if not isinstance(result, list):
        raise ValueError("array_missing")
    return result


@dataclass(frozen=True)
class Week:
    open_at: datetime
    policy: Policy = dataclasses.field(default_factory=Policy)
    publication_at: datetime | None = None
    collection_cutoff: datetime | None = None
    batch_key: str | None = None
    collection_started_at: datetime | None = None

    @classmethod
    def parse(cls, value: str, policy: Policy | None = None) -> Week:
        day = datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=UTC)
        if day.weekday() != 0:
            raise ValueError("week_must_start_on_monday")
        return cls(day, policy or Policy())

    @classmethod
    def current(cls, now: datetime, policy: Policy | None = None) -> Week:
        now = instant(now)
        return cls(
            (now - timedelta(days=now.weekday())).replace(hour=0, minute=0, second=0, microsecond=0), policy or Policy()
        )

    @classmethod
    def bootstrap(cls, now: datetime, policy: Policy | None = None) -> Week:
        now = instant(now)
        week = cls.current(now, policy)
        return dataclasses.replace(
            week,
            publication_at=now
            + timedelta(hours=week.policy.publish_offset_hours - week.policy.collection_start_offset_hours),
            collection_cutoff=now
            + timedelta(hours=week.policy.freeze_offset_hours - week.policy.collection_start_offset_hours),
            batch_key=f"{week.id}-bootstrap-{now:%Y%m%dT%H%M%SZ}",
            collection_started_at=now,
        )

    def record(self) -> Record:
        record = {
            "protocol_version": "2.0" if self.policy.legacy_gt_grace_hours is not None else "3.0",
            "batch_id": self.id,
            "open_at": stamp(self.open_at),
            "freeze_at": stamp(self.freeze_at),
            "publication_at": stamp(self.publish_at),
            "submission_deadline": stamp(self.prediction_cutoff),
            "end_date_min": stamp(self.end_min),
            "end_date_max": stamp(self.end_max),
            "policy": self.policy.record(),
        }
        if self.policy.legacy_gt_grace_hours is None:
            record.update(
                collection_start_at=stamp(self.collection_start),
                gt_deadline_at=stamp(self.evidence_cutoff),
                finalize_at=stamp(self.finalize_at),
            )
        return record

    @classmethod
    def restore(cls, value: Record) -> Week:
        return cls(
            instant(value["open_at"]),
            Policy.load(value["policy"]),
            instant(value["publication_at"]),
            instant(value["freeze_at"]),
            value["batch_id"],
            instant(value["collection_start_at"]) if value.get("collection_start_at") else None,
        )

    @property
    def id(self) -> str:
        return self.batch_key or self.open_at.date().isoformat()

    @property
    def freeze_at(self) -> datetime:
        return self.collection_cutoff or self.open_at + timedelta(hours=self.policy.freeze_offset_hours)

    @property
    def collection_start(self) -> datetime:
        return self.collection_started_at or self.open_at + timedelta(hours=self.policy.collection_start_offset_hours)

    @property
    def publish_at(self) -> datetime:
        return self.publication_at or self.open_at + timedelta(hours=self.policy.publish_offset_hours)

    @property
    def end_min(self) -> datetime:
        return self.prediction_cutoff

    @property
    def end_max(self) -> datetime:
        return self.prediction_cutoff + timedelta(days=self.policy.ending_window_days)

    @property
    def prediction_cutoff(self) -> datetime:
        return self.publish_at + timedelta(hours=self.policy.submission_hours)

    @property
    def evidence_cutoff(self) -> datetime:
        return self.end_max + timedelta(hours=self.policy.legacy_gt_grace_hours or 0)

    @property
    def finalize_at(self) -> datetime:
        return self.evidence_cutoff + timedelta(hours=self.policy.finalization_hours)


def scheduled_end(source: Record) -> datetime:
    """从完整冻结 Market 集合取得 PM 原定日期，不推测实际结算时间。"""
    if not source.get("markets"):
        raise ValueError("markets_missing")
    return max(instant(m["end_at"]) for m in source["markets"])


def gt_deadline(source: Record, week: Week) -> datetime:
    if week.policy.legacy_gt_grace_hours is not None:
        return scheduled_end(source) + timedelta(hours=week.policy.legacy_gt_grace_hours)
    return week.evidence_cutoff


def source_binding(source: Record) -> str:
    binding = {key: source[key] for key in ("event_id", "question", "description", "domain")}
    binding["end_at"] = source["time"]["end_at"]
    binding["markets"] = [
        {key: value for key, value in market.items() if key != "game_start_at"} for market in source["markets"]
    ]
    return digest(binding)


def market_end_date(market: Record, week: Week) -> datetime:
    if not isinstance(market, dict) or not market.get("endDate"):
        raise ValueError("market_end_missing")
    value = market["endDate"]
    if not isinstance(value, (str, datetime)):
        raise ValueError("market_end_invalid")
    end = instant(value)
    if not week.end_min < end <= week.end_max:
        raise ValueError("market_end_outside_window")
    return end


def admit_event(raw: Record, week: Week, observed_at: datetime) -> Record:
    """整 Event 时间筛选；保留空标签、原生 outcome 和混合语义。"""
    start = metadata_time(raw.get("startDate") or raw.get("createdAt"))
    if not raw.get("id") or not raw.get("title"):
        raise ValueError("event_identity_missing")
    if raw.get("closed") is True:
        raise ValueError("event_already_closed")
    if raw.get("archived") is True:
        raise ValueError("event_archived")
    markets = []
    seen: set[str] = set()
    for market in raw.get("markets", []):
        end = market_end_date(market, week)
        if market.get("archived") is True:
            raise ValueError("market_archived")
        if market.get("closed") is True or any(
            market.get(key) == "resolved" for key in ("resolutionStatus", "umaResolutionStatus")
        ):
            raise ValueError("market_already_resolved_or_closed")
        condition = market.get("conditionId")
        if not condition or not market.get("id") or condition in seen:
            raise ValueError("market_identity_missing_or_duplicate")
        seen.add(condition)
        version = market.get("version")
        if version not in ("v1", "v2"):
            raise ValueError("unsupported_market_version")
        labels = array(market.get("outcomes"))
        tokens = array(market.get("positionIds") if version == "v2" else market.get("clobTokenIds"))
        if len(labels) != 2 or len(tokens) != 2 or len(set(tokens)) != len(tokens):
            raise ValueError("outcome_token_mismatch")
        if any(not isinstance(token, str) or not token.isdecimal() for token in tokens):
            raise ValueError("invalid_token_id")
        markets.append(
            {
                "market_id": str(market["id"]),
                "condition_id": condition,
                "question": market.get("question"),
                "description": market.get("description"),
                "resolution_source": market.get("resolutionSource"),
                "group_item_title": market.get("groupItemTitle"),
                "version": version,
                "end_at": stamp(end),
                "game_start_at": metadata_time(market.get("gameStartTime")),
                "neg_risk": market.get("negRisk"),
                "outcomes": [
                    {"label": label, "index": i, "token_id": token}
                    for i, (label, token) in enumerate(zip(labels, tokens, strict=True))
                ],
            }
        )
    if not markets:
        raise ValueError("markets_missing")
    event_id = str(raw["id"])
    source = {
        "source_id": str(uuid5(NAMESPACE_URL, f"https://github.com/JiaWANG-TJ/ForeseeBench/event/{event_id}")),
        "event_id": event_id,
        "batch_id": week.id,
        "question": raw["title"],
        "description": raw.get("description"),
        "domain": (
            [t.get("label") if isinstance(t, dict) else None for t in raw["tags"]]
            if isinstance(raw.get("tags"), list)
            else None
        ),
        "time": {"start_at": start, "end_at": stamp(max(instant(m["end_at"]) for m in markets))},
        "tag_ids": [str(t["id"]) for t in raw.get("tags", []) if isinstance(t, dict) and t.get("id") is not None]
        if isinstance(raw.get("tags"), list)
        else None,
        "series_ids": sorted(
            {str(s["id"]) for s in raw.get("series", []) if isinstance(s, dict) and s.get("id") is not None}
        )
        if isinstance(raw.get("series"), list)
        else None,
        "markets": markets,
        "observed_at": stamp(observed_at),
        "frozen_at": stamp(week.freeze_at),
        "source_hash": digest(raw),
        "date_window_status": "passed",
    }
    from .domains import event_group

    source["primary_domain"] = event_group(source)
    return source


def token_matches(market: Record, index: int, row: Record) -> bool:
    outcome = market["outcomes"][index]
    return (
        row.get("token_id") == outcome["token_id"]
        and row.get("condition_id") == market["condition_id"]
        and row.get("outcome_index") == outcome["index"]
        and row.get("module") in ({"v1_ctf"} if market["version"] == "v1" else {"binary", "neg_risk", "directional"})
        and row.get("opposite_token_id") == market["outcomes"][1 - index]["token_id"]
    )


def bound_tokens(market: Record, by_token: dict[str, list[Record]]) -> tuple[list[Record], bool]:
    """按冻结资产、版本和 outcome 下标核对证据，不按价格猜对应关系。"""
    selected = []
    for outcome in market["outcomes"]:
        matches = by_token.get(outcome["token_id"], [])
        selected.append(matches[0] if len(matches) == 1 else {})
    valid = len(selected) == 2 and all(token_matches(market, i, row) for i, row in enumerate(selected))
    return selected, valid


def settled_payouts(market: Record, fact: Record) -> list[Decimal] | None:
    """仅接受绑定完整、已结算且总支付为1的原生资产结果，包含真实分数支付。"""
    if fact.get("lifecycle", {}).get("status") != "resolved":
        return None
    by_token: dict[str, list[Record]] = {}
    for token in fact.get("tokens", []):
        by_token.setdefault(token.get("token_id", ""), []).append(token)
    tokens, binding = bound_tokens(market, by_token)
    if not binding or any(token.get("resolved") is not True or token.get("final_price") is None for token in tokens):
        return None
    try:
        values = [Decimal(str(token["final_price"])) for token in tokens]
        if any(not value.is_finite() or not 0 <= value <= 1 for value in values) or sum(values) != 1:
            return None
    except ArithmeticError:
        return None
    return values


def has_known_answer(source: Record, resolutions: list[Record], tokens: list[Record]) -> bool:
    conditions = {market["condition_id"] for market in source["markets"]}
    if any(row.get("condition_id") in conditions and row.get("status") in ANSWER_STATES for row in resolutions):
        return True
    by_token: dict[str, list[Record]] = {}
    for row in tokens:
        if row.get("resolved") is True:
            by_token.setdefault(row.get("token_id", ""), []).append(row)
    return any(
        token_matches(market, i, row)
        for market in source["markets"]
        for i, outcome in enumerate(market["outcomes"])
        for row in by_token.get(outcome["token_id"], [])
    )


def require_unresolved(source: Record, resolutions: list[Record], tokens: list[Record]) -> None:
    if has_known_answer(source, resolutions, tokens):
        raise ValueError("market_resolved_during_collection")
    by_condition: dict[str, list[Record]] = {}
    by_token: dict[str, list[Record]] = {}
    for row in resolutions:
        by_condition.setdefault(row.get("condition_id", ""), []).append(row)
    for row in tokens:
        by_token.setdefault(row.get("token_id", ""), []).append(row)
    for market in source["markets"]:
        life = by_condition.get(market["condition_id"], [])
        if len(life) != 1:
            raise ValueError("unresolved_lifecycle_evidence_missing_or_invalid")
        if life[0].get("status") not in UNRESOLVED_STATES:
            raise ValueError("unresolved_lifecycle_evidence_missing_or_invalid")
        rows, valid = bound_tokens(market, by_token)
        if not valid:
            raise ValueError("unresolved_token_evidence_missing_or_invalid")
        if any(row.get("resolved") is True for row in rows):
            raise ValueError("market_resolved_during_collection")
        if not all(row.get("resolved") is False and row.get("final_price") is None for row in rows):
            raise ValueError("unresolved_token_evidence_missing_or_invalid")


def settle_event(
    source: Record,
    resolutions: list[Record],
    tokens: list[Record],
    week: Week,
    observed_at: datetime,
    *,
    previous: Record | None = None,
    observations: dict[str, Record] | None = None,
) -> Record:
    """真值事实与期限资格分别记录；所有 Market 必须取得可核验的 0/1 支付。"""
    by_condition: dict[str, list[Record]] = {}
    by_token: dict[str, list[Record]] = {}
    for row in resolutions:
        by_condition.setdefault(row.get("condition_id", ""), []).append(row)
    for row in tokens:
        by_token.setdefault(row.get("token_id", ""), []).append(row)
    evidence = []
    for market in source["markets"]:
        condition = market["condition_id"]
        matches = by_condition.get(condition, [])
        lifecycle = dict(matches[0]) if len(matches) == 1 else {}
        selected, valid_binding = bound_tokens(market, by_token)
        observation = (observations or {}).get(condition, {})
        seen_at = stamp(observation.get("observed_at", observed_at))
        lifecycle_seen_at = stamp(observation.get("lifecycle_observed_at", seen_at))
        lifecycle_receipts = observation.get("lifecycle_receipt_ids", observation.get("receipt_ids", []))
        token_observations = observation.get("token_observations", {})
        state, winner, resolved_at, time_error = "pending", None, None, None
        raw_time = lifecycle.get("resolved_at")
        if raw_time:
            try:
                resolved_at = stamp(raw_time)
                lifecycle["resolved_at"] = resolved_at
            except (ValueError, TypeError):
                time_error = str(raw_time)
        if len(matches) > 1 or any(len(by_token.get(o["token_id"], [])) > 1 for o in market["outcomes"]):
            state = "ambiguous_evidence"
        elif lifecycle.get("status") == "resolved":
            if not valid_binding:
                state = "token_binding_missing_or_invalid"
            elif not all(row.get("resolved") is True for row in selected):
                state = "token_not_resolved"
            elif any(row.get("final_price") is None for row in selected):
                state = "payout_missing"
            else:
                try:
                    amounts = [Decimal(str(row["final_price"])) for row in selected]
                    binary = sorted(amounts) == [Decimal(0), Decimal(1)]
                except ArithmeticError:
                    amounts, binary = [], False
                if binary:
                    winner = market["outcomes"][amounts.index(Decimal(1))]["label"]
                    state = "verified"
                else:
                    state = "fractional_or_invalid_payout"
        current = {
            "condition_id": condition,
            "status": state,
            "winner": winner,
            "resolved_at": resolved_at,
            "lifecycle": lifecycle,
            "tokens": selected,
            "observed_at": seen_at,
            "receipt_ids": observation.get("receipt_ids", []),
            "time_error": time_error,
            "lifecycle_observed_at": lifecycle_seen_at,
            "lifecycle_receipt_ids": lifecycle_receipts,
            "token_observations": token_observations,
            "resolved_at_observed_at": lifecycle_seen_at if resolved_at else None,
            "resolved_at_receipt_ids": lifecycle_receipts if resolved_at else [],
        }
        old = next((dict(e) for e in (previous or {}).get("markets", []) if e["condition_id"] == condition), None)
        if old and old.get("resolved_at"):
            try:
                old["resolved_at"] = stamp(old["resolved_at"])
            except (ValueError, TypeError):
                old["resolved_at"] = None
        last_unresolved = (old or {}).get("last_unresolved_at")
        last_unresolved_verified = (old or {}).get("last_unresolved_verified_at", last_unresolved)
        known_at = [(old or {}).get("first_answer_observed_at")]
        if lifecycle.get("status") in ANSWER_STATES:
            known_at.append(lifecycle_seen_at)
        for i, token in enumerate(selected):
            if token_matches(market, i, token) and token.get("resolved") is True:
                known_at.append(token_observations.get(token["token_id"], {}).get("observed_at", seen_at))
        known_times = [instant(value) for value in known_at if value]
        first_answer = stamp(min(known_times)) if known_times else None
        if (
            valid_binding
            and lifecycle.get("status") in UNRESOLVED_STATES
            and all(row.get("resolved") is False and row.get("final_price") is None for row in selected)
            and first_answer is None
        ):
            unresolved_at = stamp(observation.get("unresolved_observed_at", seen_at))
            if not last_unresolved or instant(unresolved_at) > instant(last_unresolved):
                last_unresolved = unresolved_at
                last_unresolved_verified = seen_at
        current["last_unresolved_at"] = last_unresolved
        current["last_unresolved_verified_at"] = last_unresolved_verified
        old_payouts = settled_payouts(market, old) if old else None
        if old and old["status"] == "conflicting_evidence":
            current = old
        elif old and old_payouts is not None:
            old.setdefault("observed_at", (previous or {})["observed_at"])
            current_payouts = settled_payouts(market, current)
            if (
                current_payouts is not None
                and current_payouts != old_payouts
                or state == "fractional_or_invalid_payout"
                and current_payouts is None
                or (
                    lifecycle.get("status") == "resolved"
                    and resolved_at
                    and old.get("resolved_at")
                    and resolved_at != old["resolved_at"]
                )
            ):
                current.update(status="conflicting_evidence", winner=None, previous_verified=old)
            else:
                current = old
                if lifecycle.get("status") == "resolved" and resolved_at and not old.get("resolved_at"):
                    current.update(
                        resolved_at=resolved_at,
                        lifecycle=lifecycle,
                        time_error=None,
                        lifecycle_observed_at=lifecycle_seen_at,
                        lifecycle_receipt_ids=lifecycle_receipts,
                        resolved_at_observed_at=lifecycle_seen_at,
                        resolved_at_receipt_ids=lifecycle_receipts,
                    )
        current["first_answer_observed_at"] = first_answer
        current["submission_unresolved_verified"] = bool(
            last_unresolved and instant(last_unresolved) >= week.prediction_cutoff
        )
        evidence.append(current)
    complete = bool(evidence) and all(e["status"] == "verified" for e in evidence)
    deadline = gt_deadline(source, week)
    earliest_result = []
    for item in evidence:
        for fact in (item, item.get("previous_verified", {})):
            if fact.get("resolved_at"):
                earliest_result.append(instant(fact["resolved_at"]))
            if fact.get("status") == "verified" and fact.get("observed_at"):
                earliest_result.append(instant(fact["observed_at"]))
        if item.get("first_answer_observed_at"):
            earliest_result.append(instant(item["first_answer_observed_at"]))
    verified_at = max(instant(e["observed_at"]) for e in evidence) if complete else None
    timing = []
    for item in evidence:
        proofs = []
        if item.get("resolved_at") and instant(item["resolved_at"]) > week.prediction_cutoff:
            proofs.append(instant(item.get("resolved_at_observed_at") or item["observed_at"]))
        if item.get("last_unresolved_at") and instant(item["last_unresolved_at"]) >= week.prediction_cutoff:
            proofs.append(instant(item.get("last_unresolved_verified_at") or item["last_unresolved_at"]))
        if proofs:
            timing.append(min(proofs))
    qualified_at = max([verified_at, *timing]) if verified_at and len(timing) == len(evidence) else None
    invalid_reasons = {
        "early_resolution",
        "late_resolution",
        "late_evidence",
        "gt_missing_at_deadline",
        "timing_unverified_at_deadline",
        "conflicting_evidence",
        "invalid_payout",
        "source_structure_changed",
    }
    eligibility = "unverified"
    if (previous or {}).get("eligibility") in invalid_reasons:
        eligibility = previous["eligibility"]  # type: ignore[index]
    elif any(t <= week.prediction_cutoff for t in earliest_result):
        eligibility = "early_resolution"
    elif any(e.get("resolved_at") and instant(e["resolved_at"]) > deadline for e in evidence):
        eligibility = "late_resolution"
    elif any(e["status"] == "conflicting_evidence" for e in evidence):
        eligibility = "conflicting_evidence"
    elif any(e["status"] == "fractional_or_invalid_payout" for e in evidence):
        eligibility = "invalid_payout"
    elif complete:
        assert verified_at is not None
        if verified_at > deadline or (qualified_at and qualified_at > deadline):
            eligibility = "late_evidence"
        elif any(e.get("resolved_at") and instant(e["resolved_at"]) > deadline for e in evidence):
            eligibility = "late_resolution"
        elif qualified_at is not None:
            eligibility = "in_window"
        else:
            eligibility = "timing_unverified_at_deadline" if observed_at >= deadline else "unverified_timing"
    elif observed_at >= deadline:
        eligibility = "gt_missing_at_deadline"
    return {
        "source_id": source["source_id"],
        "event_id": source["event_id"],
        "batch_id": week.id,
        "observed_at": stamp(observed_at),
        "status": "verified"
        if complete
        else next((e["status"] for e in evidence if e["status"] != "verified"), "pending"),
        "eligibility": eligibility,
        "validity": "valid"
        if eligibility == "in_window"
        else "invalid"
        if eligibility in invalid_reasons
        else "pending",
        "markets": evidence,
        "gt_deadline_at": stamp(deadline),
        "gt_verified_at": stamp(verified_at) if verified_at else None,
        "qualification_verified_at": stamp(qualified_at) if qualified_at else None,
        "time": {"start_at": source["time"]["start_at"], "end_at": stamp(scheduled_end(source))},
    }
