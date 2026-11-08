# ForeseeBench

[简体中文](https://huggingface.co/datasets/SII-JiaWANG/ForeseeBench) | **English**

Complete Polymarket source Events and native settlement evidence. Each row contains an Event and all its frozen Markets. Extended views retain individual rules, comment relationships, and every API-visible trade.

[Public source and automation](https://github.com/JiaWANG-TJ/ForeseeBench-Automation) · [Shard manifest](https://huggingface.co/datasets/SII-JiaWANG/ForeseeBench/resolve/main/manifest.json) · [Statistics](https://huggingface.co/datasets/SII-JiaWANG/ForeseeBench/resolve/main/statistics.json)

## Current batches

{{BATCH_SUMMARY}}

Each batch retains its frozen timing and membership. Resolution-row counts do not establish the same number of valid forecasting outcomes. Native GT and statistics retain completeness, missingness, and exclusion reasons separately.

Primary-domain distribution for the latest batch:

{{DOMAIN_SUMMARY}}

## Four views

| Configuration | Split | Content |
| --- | --- | --- |
| basic | full | Basic structure and native GT for every qualified Event |
| extended | full | The same full population, with rules, comments, and trades |
| basic | balanced | Basic view of at most 200 Events per primary domain per batch |
| extended | balanced | Complete context for those same selected Events |

Scarce domains are retained in full without filling a global target. Primary domains use versioned project rules; original tags remain available. V1 freezes selection membership. GT updates never replace members, discard invalid Events, or resample.

Cumulative views use one whole snapshot per Event: a formal release binding takes priority, otherwise the latest frozen snapshot is selected. The balanced view is a subset of those same snapshots. Canonical batch inputs and original evidence remain traceable.

## Read and download

Use `datasets==5.1.0` or a compatible version. Complex columns decode to Python dictionaries, lists, or None. Private access requires authorized HF credentials.

```python
from datasets import load_dataset

rows = load_dataset("SII-JiaWANG/ForeseeBench", "basic", split="balanced", streaming=True)
for event in rows.take(1):
    print(event["source_id"], event["title"], event["native_gt"])
```

Data is stored in shards that the HF client combines automatically. The exports list in manifest.json identifies the current files. Pin revision for reproducibility. Empty views contain no placeholder rows and support streaming; this client raises an error when materializing an empty split.

After installing the source repository, download and verify a cumulative view as a single JSONL:

```bash
uv run foreseebench download-view --repo SII-JiaWANG/ForeseeBench   --format extended --view full --output full-extended.jsonl
```

## Fields

| Field | Meaning |
| --- | --- |
| source_id / event_id / batch_id | Stable project Event identity / PM Event ID / source batch |
| title / slug | Original frozen title and slug |
| domain / primary_domain | Original ordered tags / project-derived primary domain |
| time | Optional start information, scheduled end, observation and freeze times |
| markets | All Market identities, questions, versions, scheduled ends, and outcome/token mappings |
| native_gt | Null in V1; V3 aligns payouts and states with every Market and outcome |

Extended rows add complete Event and Market descriptions, resolution sources, comment trees, orphan replies, and individual trades. Comments and trades are neither aggregated nor truncated; selected Events retain all their context. See the [data contract](https://github.com/JiaWANG-TJ/ForeseeBench-Automation/blob/main/docs/data_contract.md).

## Timing and native GT

Regular batches prepare Monday 17:00 UTC, freeze Tuesday 11:00, and plan question publication for Tuesday 17:00. Actual publication starts 72 hours for answers, followed by 120 hours for GT and 24 hours for postprocessing. Every Market satisfies `submission_deadline < endDate <= gt_deadline`. Start dates do not determine admission. Bootstrap batches retain actual start/freeze times and are not presented as on-time weekly batches.

Native GT combines lifecycle evidence and exact token binding. Payouts come from token final_price; 0/1, fractional, unresolved, missing, and conflicting outcomes remain distinct. Unknown values are not filled with zero. A 0.5/0.5 payout is not a 50% probability of an outcome being true. Optional official resolved_at is separate from observation time and scheduled endDate.

Only complete payouts for all frozen Markets produce native_gt.status=complete. Forecasting-time eligibility is separate in qualification. Completeness does not establish constructed-question answers or scoreability. The raw pipeline does not call an LLM; V2/V4 processors and semantic grading remain unconfigured, and this dataset does not provide model scores.

Collection and updates run in [GitHub Actions](https://github.com/JiaWANG-TJ/ForeseeBench-Automation/actions). HF stores and distributes published data. Working state lives in a separate private Bucket; temporary copies and working checkpoints are cleaned according to the protocol. Canonical Parquet keeps exact decimal strings; use simplejson with use_decimal=True for exact JSONL parsing. Source usage is subject to [Polymarket terms](https://polymarket.com/tos).
