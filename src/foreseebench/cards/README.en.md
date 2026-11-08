# ForeseeBench

[简体中文](https://huggingface.co/datasets/SII-JiaWANG/ForeseeBench) | **English**

Complete Polymarket source data and domain-capped views. Each row preserves an Event and all its frozen Markets. Extended records include full rules, comment relationships, and individual trades. V3 adds official native settlement evidence.

[Code](https://github.com/JiaWANG-TJ/ForeseeBench) · [Full basic](https://huggingface.co/datasets/SII-JiaWANG/ForeseeBench/resolve/main/full/basic.jsonl) · [Full extended](https://huggingface.co/datasets/SII-JiaWANG/ForeseeBench/resolve/main/full/extended.jsonl) · [Balanced basic](https://huggingface.co/datasets/SII-JiaWANG/ForeseeBench/resolve/main/balanced/basic.jsonl) · [Balanced extended](https://huggingface.co/datasets/SII-JiaWANG/ForeseeBench/resolve/main/balanced/extended.jsonl) · [Statistics](https://huggingface.co/datasets/SII-JiaWANG/ForeseeBench/resolve/main/statistics.json)

## Current data

{{BATCH_SUMMARY}}

Every batch retains its original timing protocol and freeze time. A historical format projection is marked `historical_projection`; it does not change old dates or establish a prospective three-plus-five-day run. Unknown historical source and screening counts remain null.

Complete domain distribution for the latest batch:

{{DOMAIN_SUMMARY}}

Native GT completeness means every frozen Market has complete payout evidence. Timing eligibility and failures are separate in `native_gt.qualification`. A complete payout is not sufficient to establish that a question is scoreable. Statistics for every batch, including denominators and sampling membership, are available in statistics.json.

## Two source views

| Split | Definition |
| --- | --- |
| `full` | Every admitted and fully collected Event; population size and domain counts do not remove records |
| `balanced` | At most 200 Events per primary domain per batch, rotating through series and end dates; keep scarce domains in full without filling a global target |

Primary domains use versioned rules derived from PM tags. Each Event counts once, all original tags remain intact, and Unclassified is separate. V1 freezes membership. GT updates do not resample; unresolved, invalid, and conflicting records remain present.

Cumulative views use one whole snapshot per Event: a formal release binding takes priority, otherwise the latest frozen snapshot is used. The balanced view is a subset of those same snapshots. Every original Parquet input, per-batch membership, and raw response remains available; inputs and GT are never mixed across snapshots.

## Load

Use `datasets==5.1.0` or a compatible newer version. Complex columns use the official Json feature and decode to Python lists, dictionaries, or None. No manual features or additional JSON-string decoding is required.

```python
from datasets import load_dataset

data = load_dataset("SII-JiaWANG/ForeseeBench", "basic", split="balanced", streaming=True)
for event in data.take(1):
    print(event["source_id"], event["title"])
    print(event["markets"], event["native_gt"])
```

Zero-row views remain empty files and stream as empty iterators. `datasets==5.1.0` raises an error when materializing an empty split. For nonempty views, omit `streaming=True` to use indexed access and `len(data)`.

Both `basic` and `extended` expose full/balanced splits. Pin `revision` to an HF commit for reproducibility. Private access uses authorized HF credentials.

| Main field | Meaning |
| --- | --- |
| `source_id` / `event_id` / `batch_id` | Stable project Event ID / PM ID / source batch |
| `title` / `slug` | Frozen Event title and slug |
| `domain` / `primary_domain` | Original ordered tags / project-derived primary domain |
| `time` | Optional start metadata, maximum scheduled Market end, observation and freeze times |
| `markets` | Complete identities, questions, labels, versions, scheduled ends, and outcome/token mappings |
| `native_gt` | Null in V1; V3 aligns payouts, states, observation and settlement times with every frozen Market and outcome |

Extended records add complete Event and Market descriptions, resolution sources, comment trees, orphan replies, and trades. Comments and trades are not sampled or aggregated: selected Events retain their complete context. Canonical Parquet events/comments/trades/resolutions remain accessible, with incompatible historical schemas isolated in legacy configurations.

New JSONL and Parquet physical schemas are 3.0. Raw Events use source_id, not qa_id. Market structures are no longer duplicated in options, and raw Market outcomes are not collapsed into a constructed QA answer. Detailed HTTP receipts remain in associated audit files.

## Timing and native results

New batches use UTC: preparation starts Monday 17:00, V1 freezes Tuesday 11:00, and planned question publication is Tuesday 17:00. Actual publication starts 72 hours for answers, followed by 120 hours for GT collection and 24 hours for postprocessing. Every Market must satisfy `submission_deadline < endDate <= gt_deadline`. Start dates do not determine admission.

Archive v1/v2 and all public states first, recovering detail-only Events within a recorded ID boundary before screening. Collect Event-specific comments by cursor. Trades keep `taker_only=true`, `filter_type=TOKENS`, and `filter_amount=0.000001`, preserving each API-visible fill and its multiplicity.

Settlement combines lifecycle evidence and exact token binding. Payout vectors come from token `final_price` and follow frozen outcome order. Preserve 0/1, fractional, missing, and conflicting results separately. A 0.5/0.5 payout is not a 50% probability of an outcome being true. Unknown values are not filled with zero, market quotes are not GT, and later empty responses cannot erase verified payouts.

Scheduled end dates differ from settlement. Optional `resolved_at` remains null when unavailable. Collection stops at the batch deadline while retaining missing or ineligible results and unchanged sampling membership. Population counts are statistics, not release gates.

The raw pipeline does not call an LLM. V2 question construction and V4 question-GT construction remain interfaces. Future participants submit an answer and confidence; confidence evaluation uses the answer's actual 0/1 correctness label, not a separate gt_confidence. This dataset does not yet provide constructed questions or model scores.

Canonical Parquet keeps trade prices and quantities as exact decimal strings. For exact JSONL arithmetic, use `simplejson` with `use_decimal=True`; ordinary HF numerical display follows its floating-point types. See [Polymarket's terms](https://polymarket.com/tos) for source usage conditions.
