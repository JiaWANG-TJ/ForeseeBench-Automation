# ForeseeBench

[简体中文](README.zh.md) | **English**

ForeseeBench builds reproducible forecasting source data from Polymarket. It collects complete Events and Markets, comments, individual trades, and official settlement evidence, then publishes a full view and a domain-capped view to Hugging Face.

[Dataset](https://huggingface.co/datasets/SII-JiaWANG/ForeseeBench) · [Automation](https://github.com/JiaWANG-TJ/ForeseeBench-Automation/actions) · [Data contract](docs/data_contract.md) · [Execution protocol](docs/protocol.md)

## What is implemented

- Archive public v1/v2 sources before screening complete Events by their Market end dates and unresolved state.
- Preserve every eligible source Event. `balanced` selects at most 200 Events per primary domain, with fixed-seed rotation across series and end dates. Scarce domains remain complete; no global population target is filled.
- Freeze source membership before collecting results. Preserve every Market, missing value, failure, comment relationship, and trade multiplicity.
- Update native settlement evidence from official lifecycle and token responses. Prices are not ground truth; payout completeness and forecasting-time eligibility are separate.
- Publish both views as incremental JSONL shards and canonical Parquet snapshots.

LLM question construction, question review, constructed-question GT, and answer grading are future processors. The shared model client and its interface probe are available; a successful probe does not establish those processors or benchmark scores.

## Current execution

The pipeline runs on the owner's existing compute resource under Supervisor. On November 11, 2026, GitHub Actions was disabled in both the private development repository and the public automation repository, and the automatic schedule was commented out. Source code and manual workflow definitions are retained. See the measured status in [acceptance](docs/acceptance.md).

The dedicated HF Space adapter and UptimeRobot activation client are implemented. On November 10, the real startup rejected this CPU-only application on ZeroGPU, and switching the new Space to CPU Basic returned HTTP 402 requiring PRO. The Space is paused; monitoring and production migration have not started. See the [Space deployment guide](docs/hf_space.md). This is not a verified free hosting route for the current account.

### Retained cloud workflow (disabled)

```text
GitHub-hosted CPU runner -> Polymarket APIs
                        -> private HF Bucket: resumable working state
                        -> HF Dataset: verified source and result snapshots
```

The retained workflow targets standard GitHub-hosted Ubuntu runners and checks which batch stages are due. Its five-minute schedule is commented out; all three workflows expose only manual dispatch, and repository Actions remains disabled. One production concurrency group prevents simultaneous writers when execution is explicitly re-enabled.

Each run has 45 minutes of work, a 10-minute checkpoint allowance, and a 60-minute job limit. Long collection resumes across runs. Database transactions use local runner storage; closed, verified snapshots are uploaded explicitly to the private Bucket. Only complete generations can be restored, and the latest two working generations are retained.

GitHub schedules can be delayed or missed. Batch state and actual UTC time determine recovery; missing a deadline never permits fabricated timestamps or incomplete publication. The Actions page shows executions, and `automation/status.json` records daily or phase-change aggregate status. HF Dataset stores and distributes data; it does not execute this workflow.

Public standard runners are free under GitHub's current rules. HF storage is bounded by the account's allowance; the default deployment does not enable paid compute or storage upgrades. Unknown or insufficient storage headroom stops new writes and is reported. See [GitHub limits](https://docs.github.com/en/actions/reference/limits) and [HF storage plans](https://huggingface.co/docs/hub/storage-limits).

The storage allowances apply to different resources:

| Resource | Purpose and capacity |
| --- | --- |
| GitHub Actions artifacts | The Free plan includes 500 MB, shared with Packages. This workflow does not upload weekly datasets as artifacts; dependency caches have a separate allowance. |
| Runner temporary disk | Standard `ubuntu-24.04` runners provide 14 GB. Each job gets fresh storage; restore loads control files first and retrieves batch files as needed. Peak database, archive, export, and dependency usage must fit during that job. |
| HF Dataset and private Bucket | Persistent snapshots and resumable state. HF lists 100 GB of private storage for free accounts. The application pauses before its visible usage plus reserved writes reaches 90 GB; this is a conservative guard, not the platform's billing meter. |

Sources: [GitHub storage billing](https://docs.github.com/en/billing/concepts/product-billing/github-actions), [runner specifications](https://docs.github.com/en/actions/reference/runners/github-hosted-runners), and [HF storage plans](https://huggingface.co/docs/hub/storage-limits). Capacity planning must include raw responses, Parquet, JSONL, versioned snapshots, and working state. A 1 GB weekly download does not establish 1 GB of total weekly storage growth. The two-generation checkpoint policy removes unreferenced temporary objects; it preserves referenced historical data.

### Running on an existing host

Use a persistent project directory, working network access, and HF authentication. Configure `HF_BUCKET` for the storage guard. Host-specific environment settings may be placed in the ignored `data/service/environment.sh`; keep credentials in the host's authentication cache.

```bash
bash scripts/service.sh start
bash scripts/service.sh status
bash scripts/service.sh logs
bash scripts/service.sh stop
```

`start` installs locked dependencies and runs connection/storage preflight checks. Supervisor restarts a failed worker, and a file lock prevents a second host from running the same directory concurrently. The main thread advances work in bounded intervals, preserving transactions and source cursors between runs. Local files remain in `data/`; completed snapshots are published to HF. Logs rotate automatically.

After the resource is recycled, restore network access and run `start` again. Inspire's `notebook start --post-start 'bash /absolute/path/to/ForeseeBench/scripts/service.sh start'` can attach this command to a platform restart. Restarting through the web UI alone does not execute it. Offline time can cause missed deadlines; such batches remain explicitly incomplete or invalid. A weekly batch also needs later GT polling, so running the script only once per week is insufficient.

## Weekly timing

All times are UTC. The schedule is configured in [`foreseebench.toml`](foreseebench.toml).

| Stage | Schedule |
| --- | --- |
| Source preparation | Monday 17:00 onward |
| V1 feature cutoff and freeze | Tuesday 11:00 |
| Planned question publication | Tuesday 17:00; requires a real V2 processor |
| Answers | 72 hours after actual publication |
| GT collection | Following 120 hours, with one batch deadline |
| Postprocessing | 24 hours after the GT deadline |

Every frozen Market must satisfy `submission_deadline < endDate <= gt_deadline`. Start dates are optional metadata. The nominal schedule spans ten days; there is no additional twelve-day cap. Weekly batches may overlap.

An initial `bootstrap` starts at its real creation time, allows 18 hours for complete collection, and records its actual freeze time. An incomplete batch at its deadline remains explicitly incomplete. V2/V4 remain unconfigured, so a planned publication time is not a claim that questions have been released.

## Set up a deployment

The following cloud setup steps apply only after an explicit decision to restore hosted execution. Repository Actions must first be re-enabled; changing the production variable alone does not restore automatic scheduling. Existing-host startup is documented above.

1. Use this public source repository and a writable HF Dataset plus a private HF Bucket.
2. Set the Dataset in `foreseebench.toml` and the Bucket/Dataset identifiers in `.github/workflows/cloud.yml`.
3. Add the authorized `HF_TOKEN` as a GitHub Actions secret. Never commit credentials or working data.
4. Run `Hosted runner readiness` manually and confirm `FORESEEBENCH_RUNNER_OK` in its log. A billing-lock failure before any step runs requires account resolution through [GitHub's documented process](https://docs.github.com/en/billing/how-tos/troubleshooting/locked-account); buying storage does not establish runner readiness.
5. Run the cloud workflow manually in `qualification` mode. Re-run the same workflow execution to verify recovery from a fresh runner.
6. After qualification and the single-writer handoff pass, set `FORESEEBENCH_ENABLED=true` and select `production` for an authorized manual run. Restoring the commented schedule is a separate change requiring approval.

The workflow supplies `--bootstrap` when no batch exists, then follows the weekly schedule. Qualification uses an isolated Bucket prefix and never publishes production data.

For development with Python 3.12 and [uv](https://docs.astral.sh/uv/):

```bash
uv sync --locked
uv run python -m foreseebench.cloud --help
uv run pytest
uv run ruff check src tests scripts
uv run mypy src
```

## Read or download data

Install `datasets==5.1.0` or a compatible version. The `basic` and `extended` configurations each expose `full` and `balanced` splits. Private repositories require authorized HF credentials.

```python
from datasets import load_dataset

rows = load_dataset("SII-JiaWANG/ForeseeBench", "basic", split="balanced", streaming=True)
for event in rows.take(1):
    print(event["source_id"], event["title"], event["native_gt"])
```

Nested fields decode to dictionaries, lists, or `None`. Streaming also handles empty views. Nonempty views support normal indexed loading. `manifest.json` lists the current shards; `statistics.json` reports full denominators, domain counts, selection membership, and GT states. Pin an HF `revision` for reproducible reads.

Download a verified cumulative view as one JSONL without a local pipeline database:

```bash
uv run foreseebench download-view --repo SII-JiaWANG/ForeseeBench   --format extended --view full --output full-extended.jsonl
```

The command pins the remote revision, verifies each shard, concatenates its original bytes, and atomically writes the validated output. It preserves decimal text. Source Events use `source_id`; `qa_id` belongs to future constructed questions. See the [field definitions](docs/data_contract.md).

## Model interface

The designated service is DeepSeek-V4.1-Flash-1017 deployed with vLLM on the Qizhi platform. Submit the contents of [`scripts/qz-serving-command.sh`](scripts/qz-serving-command.sh) through a non-login `bash -c` command to preserve the image environment; the container does not need this repository mounted. The model path is explicit and independent of the platform's model selector.

The manual `LLM interface probe` workflow reads `LLM_BASE_URL` and `LLM_API_KEY` from GitHub Secrets. It checks model identity, plain output, JSON Schema output, and thinking-mode final content. The temporary GPU service must be taken offline after the probe, including failure paths. GPU time uses the separately authorized Qizhi allocation.

The client is also callable as `python -m foreseebench.llm`. Actual QA generation and semantic scoring remain disabled. Predictions require only `answer` and `confidence`; future confidence Brier loss uses the answer's 0/1 correctness label, not an invented GT confidence.

## Verification and maintenance

The [acceptance record](docs/acceptance.md) distinguishes software checks, remote execution, data completeness, and model-interface readiness. Keep permanent source/results and necessary evidence; discard verified temporary downloads and superseded working checkpoints. Code and configuration changes during an active formal batch must respect its frozen implementation identity.
