"""按冻结的PM标签规则派生唯一主领域；不替代语义审核。"""

from .contract import Record

DOMAIN_RULE_VERSION = "native_tags_10_v1"
PRIMARY_DOMAINS = (
    "TechScience",
    "WeatherEnvironment",
    "Crypto",
    "BusinessEarnings",
    "MacroEconomy",
    "FinanceMarkets",
    "GeopoliticsSecurity",
    "PoliticsGovernance",
    "Culture",
    "Sports",
)
# 保留规划分析的七类优先顺序，再按明确标签细分金融和政治。
PRIMARY_TAGS = {
    "TechScience": {"1325", "1401", "22", "74"},
    "WeatherEnvironment": {"103037", "104180", "84", "87"},
    "Crypto": {"21"},
    "EconomyFinance": {"100328", "107", "120", "225"},
    "Politics": {"100265", "2"},
    "Culture": {"315", "596"},
    "Sports": {"1"},
}
FALLBACK_TAGS = {
    "EconomyFinance": {"100196", "101701", "102000", "103176", "129", "159", "702"},
    "Sports": {"64"},
    "Culture": {"18", "286"},
}
BUSINESS_TAGS = {"107", "1013", "102451", "104548"}
MACRO_TAGS = {"129", "131", "225", "370", "702", "1624", "100328", "100478", "101701", "105174"}
GEOPOLITICS_TAGS = {"79", "80", "193", "793", "100265", "101794"}
HEALTH_TAGS = {"414", "101422", "104623"}


def event_group(event: Record, *, legacy: bool = False) -> str:
    if not legacy and event.get("primary_domain") is not None:
        group = event["primary_domain"]
        if group not in (*PRIMARY_DOMAINS, "Unclassified"):
            raise ValueError("unknown_primary_domain")
        return str(group)
    tags = set(event.get("tag_ids") or [])
    group = next(
        (name for mapping in (PRIMARY_TAGS, FALLBACK_TAGS) for name, ids in mapping.items() if tags & ids),
        "Unclassified",
    )
    if legacy:
        return group
    if group == "EconomyFinance":
        return "BusinessEarnings" if tags & BUSINESS_TAGS else "MacroEconomy" if tags & MACRO_TAGS else "FinanceMarkets"
    if group == "Politics":
        return "GeopoliticsSecurity" if tags & GEOPOLITICS_TAGS else "PoliticsGovernance"
    if group == "Unclassified":
        return "BusinessEarnings" if tags & BUSINESS_TAGS else "TechScience" if tags & HEALTH_TAGS else group
    return group


def grouping_record() -> Record:
    return {
        "version": DOMAIN_RULE_VERSION,
        "basis": "derived_from_official_tags",
        "semantic_validation": "not_performed",
        "primary": {group: sorted(ids) for group, ids in PRIMARY_TAGS.items()},
        "fallback": {group: sorted(ids) for group, ids in FALLBACK_TAGS.items()},
        "splits": {
            "BusinessEarnings": sorted(BUSINESS_TAGS),
            "MacroEconomy": sorted(MACRO_TAGS),
            "GeopoliticsSecurity": sorted(GEOPOLITICS_TAGS),
            "HealthFallback": sorted(HEALTH_TAGS),
        },
        "domains": list(PRIMARY_DOMAINS),
        "unclassified": "Unclassified",
    }
