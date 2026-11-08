# ForeseeBench

**简体中文** | [English](README.md)

ForeseeBench采集完整Polymarket Event、Markets、评论、逐笔成交和官方结算证据，同时向HF提供**完整集**与**分域采样集**。

[数据集](https://huggingface.co/datasets/SII-JiaWANG/ForeseeBench) · [执行协议](docs/protocol.md) · [字段契约](docs/data_contract.md) · [验收记录](docs/acceptance.md)

## 流程

**归档公开来源 → 筛选完整Event → 冻结V1与采样名单 → V3回收原生GT → 同时发布两个数据范围**

- 先按记录的Event ID边界归档v1/v2、全部公开状态，并补查列表遗漏的详情，再筛选。
- `full`保存全部合规Event及其全部冻结Markets。后续的未决、失败、迟到和冲突结果保留。
- `balanced`每个主领域最多200个Event；域内轮询系列与UTC结束日期。稀缺域全部保留，不把剩余名额补给体育等大域。
- 名单在回收结果前冻结；后来失效或GT不全的Event仍保留在原名单中。
- 题量与领域分布只作统计，不再以500、1000或整批全部有效作为数据保存和发布条件。

原始管线不调用LLM。V2构题与V4构造题GT保持未配置接口；原生Market结果不再自动拼成一个Event级QA答案。

## 时间与配置

| 环节 | 默认UTC安排 |
| --- | --- |
| 准备 | 周一17:00—周二17:00 |
| 冻结V1 | 周二11:00 |
| 计划发题 | 周二17:00，仍需真实V2处理器 |
| 用户作答 | 实际发题后72小时 |
| GT回收 | 随后120小时，全批统一截止 |
| 后处理 | 24小时 |

名义周期十天。每个冻结Market都须满足`提交截止 < endDate <= GT截止`；开始时间只作可选信息。计划结束日期不能证明真实结算，支付事实和时间资格分别记录。

统一配置为[`foreseebench.toml`](foreseebench.toml)：`domain_event_cap=200`、固定种子`20261105`、8个网络worker、共享每秒5次请求、300秒调度间隔。

## 运行

环境为Python 3.12与[uv](https://docs.astral.sh/uv/)。配置有写入权限的HF仓库；只在本地运行时设置`scheduler.publish=false`。认证沿用宿主HF凭据或`HF_TOKEN`。

```bash
uv sync --locked
uv run foreseebench --help
uv run foreseebench run
```

每个数据目录只运行一个写入进程。`formal`遵循周批窗口；`bootstrap`记录真实采集、冻结时间；`qualification`隔离工程核验。历史协议只读，不再启动旧周期采集或回收。

导出已存在的批次，`--week`接受周一日期或实际bootstrap批次ID：

```bash
uv run foreseebench export --week 2026-11-09 --view full --output data/full-basic.jsonl
uv run foreseebench export --week 2026-11-09 --view balanced --stage latest --extended --output data/balanced-extended.jsonl
```

默认读取V1，不含原生GT；`--stage latest`读取最新不可变结果快照。

## 读取数据

HF的`basic`和`extended`配置均提供`full`、`balanced`两个split。使用`datasets==5.1.0`或兼容的新版本；数据卡采用官方JSON字段类型，嵌套内容自动解码为Python列表和字典。

```python
from datasets import load_dataset

data = load_dataset("SII-JiaWANG/ForeseeBench", "basic", split="balanced", streaming=True)
for event in data.take(1):
    print(event["source_id"], event["title"])
    print(event["native_gt"])
```

零条视图仍保存为空文件。流式读取正常返回空迭代；`datasets==5.1.0`默认物化空split会报错。非空视图可省略`streaming=True`，使用下标及`len(data)`。

下载文件为`full/basic.jsonl`、`full/extended.jsonl`、`balanced/basic.jsonl`、`balanced/extended.jsonl`及`statistics.json`。统计包含全部主领域与未分类项、已知来源和筛选分母、两个数据范围的数量与原生GT状态；历史未知计数保持null。

原始Event使用`source_id`；`qa_id`仅用于真正构造的题目。`markets`保存一次完整结构，`native_gt`逐Market保存对齐的支付和状态。扩展版另含完整规则描述、评论关系与成交；详细HTTP回执留在关联审计归档中。[字段说明](docs/data_contract.md)

累计视图每个Event选择一份完整快照：正式题集绑定优先，否则取最新冻结快照；采样视图使用同一快照的子集。每批名单、全部Parquet输入与历史归档仍保留。新数据物理格式及JSONL为3.0；旧文件保留原schema和时间协议，历史格式投影标记为`historical_projection`。

## 预留的题目接口

未来V2可将元数据重构为选择、数值、排序或开放文本题，V4接收构造题与真实原生GT。本轮不运行这两个LLM处理器或语义判分器。

用户预测只包含`answer`和`confidence`，置信度须为有限的0—1数值。未来判分器依据参考答案产生`correct=0/1`后，置信度Brier loss为`(confidence-correct)²`。不需要另一个PM或LLM生成的`gt_confidence`；行情报价不作为结算标签。

## 开发验证

```bash
uv run pytest
uv run ruff check src tests scripts
uv run mypy src
```

详见[执行协议](docs/protocol.md)、[验收记录](docs/acceptance.md)及[历史来源与时间窗审计](docs/polymarket_timing_audit.md)。历史审计中的数量目标只保留分析意义。
