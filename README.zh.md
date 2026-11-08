# ForeseeBench

**简体中文** | [English](README.md)

ForeseeBench从Polymarket构建可复现的预测题源数据，采集完整Event及Markets、评论、逐笔成交和官方结算证据，并向HF发布完整视图与分域采样视图。

[数据集](https://huggingface.co/datasets/SII-JiaWANG/ForeseeBench) · [自动化运行](https://github.com/JiaWANG-TJ/ForeseeBench-Automation/actions) · [字段契约](docs/data_contract.md) · [执行协议](docs/protocol.md)

## 已实现能力

- 先归档公开v1/v2来源，再按全部Market的计划结束日期和原生未决状态筛选完整Event。
- 完整保存全部合规题源。`balanced`每主领域最多200个Event，使用固定种子在系列与结束日期之间轮询；稀缺领域全部保留，不补满总量。
- 回收结果前冻结成员名单。保留全部Market、缺失值、失败记录、评论关系与成交重数。
- 联合官方生命周期和token接口整理原生结算。行情不作为GT，支付是否齐全与预测时间资格分别记录。
- 以增量JSONL分片和规范Parquet快照发布四种视图。

LLM构题、题目审核、构造题GT和答案判分仍是后续处理器。当前提供统一模型客户端与接口探测；接口测试成功不等于这些业务已实现或已取得评测成绩。

## 自动化为什么能运行

```text
GitHub托管CPU任务 -> Polymarket API
                 -> HF私有Bucket：可恢复工作状态
                 -> HF Dataset：验收通过的输入与结果快照
```

公开运行仓库使用GitHub标准Ubuntu runner，无需本地Notebook一直在线。GitHub每五分钟触发工作流，程序检查当前哪些批次阶段到期；同一生产并发组只允许一个写入任务。

每次最多处理45分钟，预留10分钟上传检查点，任务硬超时60分钟。长采集跨多次任务续跑。数据库在runner本地盘执行事务，关闭后显式上传并核验快照；恢复只接受完整代次，工作检查点保留最近两代。

GitHub定时触发可能延迟或漏跑，程序依据批次状态及真实UTC时刻恢复。错过截止不能伪造时间或发布未完整数据。Actions页面展示实际执行，`automation/status.json`按日或阶段变化记录汇总状态。HF Dataset负责保存和分发数据，不负责执行这个工作流。

按GitHub当前规则，公开仓库标准runner免费；HF存储受账户额度约束。本部署不启用付费计算或存储升级，用量未知或空间余量不足时停止新增写入并报告。参见[GitHub限制](https://docs.github.com/en/actions/reference/limits)与[HF存储额度](https://huggingface.co/docs/hub/storage-limits)。

三种空间分别用于不同环节：

| 空间 | 用途与容量 |
| --- | --- |
| GitHub Actions制品 | Free计划包含500MB，与Packages共享。本工作流不把每周数据上传为制品；依赖缓存另有额度。 |
| runner临时磁盘 | 标准`ubuntu-24.04`提供14GB。每次任务使用新磁盘，恢复时先下载控制文件，再按需取回批次文件；数据库、归档、导出及依赖的运行峰值须能装入当次磁盘。 |
| HF Dataset与私有Bucket | 长期保存快照及可恢复状态。HF公布的免费私有存储为100GB；程序在可见用量加预留写入达到90GB前暂停。这是保守保护阈值，不是平台计费读数。 |

依据：[GitHub存储计费](https://docs.github.com/en/billing/concepts/product-billing/github-actions)、[runner规格](https://docs.github.com/en/actions/reference/runners/github-hosted-runners)及[HF存储额度](https://huggingface.co/docs/hub/storage-limits)。容量估算必须包含原始响应、Parquet、JSONL、版本快照与工作状态；每周下载1GB不代表全部存储只增加1GB。两代检查点策略只删除不再被引用的临时对象，被引用的历史数据继续保留。

## 每周时间安排

全部使用UTC，参数集中在[`foreseebench.toml`](foreseebench.toml)。

| 环节 | 安排 |
| --- | --- |
| 来源准备 | 周一17:00开始 |
| V1特征截止与冻结 | 周二11:00 |
| 计划发题 | 周二17:00，仍需真实V2处理器 |
| 用户作答 | 实际发题后72小时 |
| GT回收 | 随后120小时，全批统一截止 |
| 后处理 | GT截止后24小时 |

每个冻结Market都必须满足`提交截止 < endDate <= GT截止`；开始时间只作元数据。名义日程跨十天，不再另设十二天总周期上限。每周继续启动新批次，批次可重叠。

初始`bootstrap`使用真实启动时刻，允许18小时完成采集，并记录实际冻结时间。截止仍未完成的批次明确保留为未完成。V2/V4未配置时，计划发题时间不表示已经发布正式问题。

## 部署配置

1. 使用公开源码仓库、有写入权限的HF Dataset和私有HF Bucket。
2. 在`foreseebench.toml`设置Dataset，并在`.github/workflows/cloud.yml`设置Bucket及Dataset标识。
3. 将授权的`HF_TOKEN`配置为GitHub Actions Secret，不提交凭据或工作数据。
4. 手动运行`Hosted runner readiness`，确认日志实际出现`FORESEEBENCH_RUNNER_OK`。若执行步骤为0并报告billing lock，须按[GitHub官方流程](https://docs.github.com/en/billing/how-tos/troubleshooting/locked-account)解除账户限制；购买存储不能证明runner已经可用。
5. 手动运行云端工作流的`qualification`模式，再重跑同一次执行，验证从新的runner恢复检查点。
6. 两项检查通过后，将仓库变量`FORESEEBENCH_ENABLED`设为`true`启用生产；设为`false`暂停后续定时运行。

首次没有批次时，工作流以`--bootstrap`启动，随后遵循每周日程。工程核验使用独立Bucket前缀，不发布正式数据。

本地开发使用Python 3.12与[uv](https://docs.astral.sh/uv/)：

```bash
uv sync --locked
uv run python -m foreseebench.cloud --help
uv run pytest
uv run ruff check src tests scripts
uv run mypy src
```

## 读取与下载

使用`datasets==5.1.0`或兼容版本。`basic`和`extended`配置各提供`full`、`balanced` split；私有数据集需要授权HF凭据。

```python
from datasets import load_dataset

rows = load_dataset("SII-JiaWANG/ForeseeBench", "basic", split="balanced", streaming=True)
for event in rows.take(1):
    print(event["source_id"], event["title"], event["native_gt"])
```

嵌套字段自动解码为字典、列表或None；空视图也可流式读取。非空视图支持普通下标读取。`manifest.json`列出当前分片，`statistics.json`提供完整分母、领域数量、采样名单及GT状态；复现时固定HF的`revision`。

无需本地管线数据库，也可把一个累计视图下载为完整JSONL：

```bash
uv run foreseebench download-view --repo SII-JiaWANG/ForeseeBench   --format extended --view full --output full-extended.jsonl
```

命令固定远端提交、逐片核验并串联原始字节，最后原子写入通过校验的文件，保留十进制文本。原始Event使用`source_id`，`qa_id`只用于未来真正构造的题目。详见[字段定义](docs/data_contract.md)。

## 模型接口

指定模型为启智平台vLLM部署的DeepSeek-V4.1-Flash-1017。将[`scripts/qz-serving-command.sh`](scripts/qz-serving-command.sh)的内容用非登录`bash -c`提交，保留镜像环境；容器无需挂载本代码仓库。真正权重由显式路径确定，不由平台占位模型选择项决定。

手动`LLM interface probe`工作流从GitHub Secrets读取`LLM_BASE_URL`、`LLM_API_KEY`，检查模型身份、普通回答、JSON Schema输出及思考模式最终内容。临时GPU服务在验收成功或失败后都必须下线；GPU消耗单独授权的启智卡时。

客户端也可通过`python -m foreseebench.llm`调用。具体QA与语义判分仍未启用。用户预测只需`answer`与`confidence`；未来置信度Brier使用答案正确性的0/1标签，不编造GT置信度。

## 验证与维护

[验收记录](docs/acceptance.md)分别报告软件检查、远端执行、数据完整性和模型接口就绪。永久保留规范输入、结果及必要证据；及时清理验证完毕的临时下载和失效工作检查点。正式批次期间更新代码或配置，必须遵守已冻结的实现身份约束。
