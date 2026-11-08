# ForeseeBench

[简体中文](README.zh.md) | **English**

ForeseeBench collects complete Polymarket Events, Markets, comments, individual trades, and official settlement evidence. It preserves every qualified source Event and publishes both a **full dataset** and a **domain-capped dataset** to Hugging Face.

[Dataset](https://huggingface.co/datasets/SII-JiaWANG/ForeseeBench) · [Execution protocol](docs/protocol.md) · [Data contract](docs/data_contract.md) · [Verification](docs/acceptance.md)

## Workflow

**Archive public sources → Screen complete Events → Freeze V1 and sampling membership → Collect native GT in V3 → Publish both views**

- Discovery covers explicit v1/v2 and all public states within a recorded Event ID boundary, including detail-only Events. Filtering follows source acquisition.
- `full` retains every qualified Event and all its frozen Markets. Missing results, failures, and historical snapshots are preserved.
- `balanced` selects at most **200 Events per primary domain**, rotating through series and UTC end dates. Scarce domains are kept in full; other domains do not fill unused places. Original tags remain unchanged.
- Membership is frozen before result collection. A later invalid or incomplete result does not remove a source from either view.
- Population size and domain distribution are statistics. There is no minimum of 500, global target of 1,000, or whole-batch success gate.

The raw pipeline does not call an LLM. V2 question construction and V4 construction of those questions' answers remain unconfigured interfaces. Native Market results are not collapsed into an Event-level QA answer.

## Time and configuration

| Stage | Default UTC schedule |
| --- | --- |
| Preparation | Monday 17:00 to Tuesday 17:00 |
| Freeze V1 | Tuesday 11:00 |
| Planned question publication | Tuesday 17:00; requires a real V2 processor |
| Answer window | 72 hours after actual publication |
| GT collection | The following 120 hours, with one batch deadline |
| Postprocessing | 24 hours |

The nominal cycle is ten days. Every frozen Market must satisfy `submission_deadline < endDate <= gt_deadline`. Start dates are optional metadata. Scheduled end dates do not prove actual settlement; payouts and forecasting-time eligibility are recorded separately.

[`foreseebench.toml`](foreseebench.toml) is the unified configuration: `domain_event_cap=200`, `sampling_seed=20261105`, eight network workers, five requests per second, and a 300-second scheduler interval.

## Run

Use Python 3.12 and [uv](https://docs.astral.sh/uv/). Configure an existing HF dataset repository you can write to, or set `scheduler.publish=false` for local operation. Authentication uses the host's HF credentials or `HF_TOKEN`.

```bash
uv sync --locked
uv run foreseebench --help
uv run foreseebench run
```

Run one writer per data directory. `formal` follows the weekly window; `bootstrap` records actual collection and freeze times; `qualification` isolates engineering validation. Historical protocols remain readable but cannot start new collection or GT updates.

Export an existing batch using its Monday date or recorded bootstrap ID:

```bash
uv run foreseebench export --week 2026-11-09 --view full --output data/full-basic.jsonl
uv run foreseebench export --week 2026-11-09 --view balanced --stage latest --extended --output data/balanced-extended.jsonl
```

The default stage is V1, with no native GT. `--stage latest` reads the latest immutable result snapshot.

## Data access

The `basic` and `extended` HF configurations each expose `full` and `balanced` splits. Use `datasets==5.1.0` or a compatible newer version. The dataset card declares the official JSON feature for nested fields, which decode to Python lists and dictionaries.

```python
from datasets import load_dataset

data = load_dataset("SII-JiaWANG/ForeseeBench", "basic", split="balanced", streaming=True)
for event in data.take(1):
    print(event["source_id"], event["title"])
    print(event["native_gt"])
```

Zero-row views remain empty files. Streaming safely yields no rows; `datasets==5.1.0` raises an error when materializing an empty split. For nonempty views, omit `streaming=True` to use indexed access and `len(data)`.

Downloadable files are `full/basic.jsonl`, `full/extended.jsonl`, `balanced/basic.jsonl`, `balanced/extended.jsonl`, and `statistics.json`. Statistics show every primary domain, unclassified records, known source/screening counts, both view sizes, and native GT states. Historical unknown counts remain null.

Source Events use `source_id`; `qa_id` is reserved for constructed questions. `markets` contains the complete structure once. `native_gt` retains aligned Market payouts and statuses. Extended records include complete rule descriptions, comments/replies, and trades. Detailed HTTP receipts remain in associated audit archives. See the [field definitions](docs/data_contract.md).

Cumulative views select one whole snapshot per Event: a formal release binding takes priority, otherwise the latest frozen snapshot is used. The balanced view uses a subset of those same snapshots. Per-batch memberships and all original Parquet inputs remain available. New physical data and JSONL schemas are version 3.0; historical files retain their original schema and timing protocol. Historical format projections are marked `historical_projection`.

## Future question interfaces

V2 may reconstruct sources into choice, numeric, ranking, or open-text questions. V4 will receive those questions and actual native GT. This release implements neither LLM processor nor semantic answer grading.

A prediction contains only `answer` and `confidence`, with finite confidence in `[0,1]`. Once a future grader determines correctness, confidence Brier loss is `(confidence - correct)²`, where `correct` is 0 or 1. No separate PM or LLM-generated `gt_confidence` is required. Market quotes are not settlement labels.

## Development

```bash
uv run pytest
uv run ruff check src tests scripts
uv run mypy src
```

See the [execution protocol](docs/protocol.md), [acceptance records](docs/acceptance.md), and [historical source/timing audit](docs/polymarket_timing_audit.md). Historical population targets are analysis context, not current publication conditions.
