# ForeseeBench

**简体中文** | [English](https://huggingface.co/datasets/SII-JiaWANG/ForeseeBench/blob/main/README.en.md)

Polymarket完整题源与原生结算数据。每行对应一个完整Event及全部冻结Markets；扩展视图同时保留独立规则、评论关系和逐笔成交。

[公开源码与运行](https://github.com/JiaWANG-TJ/ForeseeBench-Automation) · [分片清单](https://huggingface.co/datasets/SII-JiaWANG/ForeseeBench/resolve/main/manifest.json) · [完整统计](https://huggingface.co/datasets/SII-JiaWANG/ForeseeBench/resolve/main/statistics.json)

## 当前批次

{{BATCH_SUMMARY}}

每个批次使用其冻结时确定的时间与成员。表中GT或结算记录数量不代表同等数量的有效预测成绩；完整状态、缺失和失效原因以原生GT及统计为准。

最新批次的主领域分布：

{{DOMAIN_SUMMARY}}

## 四种读取视图

| 配置 | Split | 内容 |
| --- | --- | --- |
| basic | full | 全部合规Event的基础结构与原生GT |
| extended | full | 同一完整集合，增加规则、评论与成交 |
| basic | balanced | 每批每主领域最多200个Event的基础视图 |
| extended | balanced | 同一采样集合的完整上下文 |

稀缺领域全部保留，不补满固定题量。主领域来自版本化规则，原始标签同时保留。采样名单在V1冻结，后续GT更新不会换题、删除失效成员或重新采样。

同一Event的累计视图使用一份完整快照：正式题集绑定优先，否则取最新冻结快照。采样视图为相同快照的子集；各批规范输入和原始证据仍可追溯。

## 读取与下载

使用`datasets==5.1.0`或兼容版本。复杂字段自动解码成Python字典、列表或None；私有访问需要授权HF凭据。

```python
from datasets import load_dataset

rows = load_dataset("SII-JiaWANG/ForeseeBench", "basic", split="balanced", streaming=True)
for event in rows.take(1):
    print(event["source_id"], event["title"], event["native_gt"])
```

数据以分片保存，HF客户端自动组合相应视图。`manifest.json`的exports列出当前分片；不是四个不断膨胀的累计文件。复现时固定revision。空视图不造占位行，可正常流式读取；该客户端默认物化空split会报错。

安装源码仓库后，可下载并核验一个完整累计JSONL：

```bash
uv run foreseebench download-view --repo SII-JiaWANG/ForeseeBench   --format extended --view full --output full-extended.jsonl
```

## 字段

| 字段 | 含义 |
| --- | --- |
| source_id / event_id / batch_id | 项目稳定Event身份／PM Event ID／来源批次 |
| title / slug | 冻结的原始标题与slug |
| domain / primary_domain | 原始有序标签／项目派生主领域 |
| time | 可选开始信息、计划结束时间、源观察与冻结时间 |
| markets | 全部Market身份、题干、版本、计划结束日期和outcome/token映射 |
| native_gt | V1为null；V3按Market与outcome顺序保存支付和状态 |

extended增加Event及Market各自的完整描述、结算来源、评论树、缺父回复及逐笔成交。评论和成交不聚合、不截断，采样Event保留其全部上下文。详细字段见[数据契约](https://github.com/JiaWANG-TJ/ForeseeBench-Automation/blob/main/docs/data_contract.md)。

## 时间与GT

正常周批周一17:00 UTC准备，周二11:00冻结、17:00计划发题；实际发题后72小时作答、随后120小时GT回收、24小时后处理。全部Market满足`提交截止 < endDate <= GT截止`。开始时间不作准入限制。bootstrap记录真实启动和冻结时间，不冒充准点周批。

原生GT联合生命周期与精确token绑定核验。payouts来自token的final_price，0/1、分数支付、未决、缺失和冲突分别保存；未知不补零。0.5/0.5支付不是“结果有50%真实概率”。resolved_at只保存实际可得的官方时间，与观察时间和计划endDate分开。

支付全部齐备才标native_gt.status=complete；预测时间资格另见qualification。该状态不表示已经有构造题答案或可评分。原始管线不调用LLM，V2/V4处理器及语义判分仍未配置，本数据集尚不提供模型成绩。

采集和更新由[GitHub Actions](https://github.com/JiaWANG-TJ/ForeseeBench-Automation/actions)执行；HF保存和分发发布数据。工作状态使用独立私有Bucket，临时副本与工作检查点按规则清理。规范Parquet保留精确十进制字符串；精确解析JSONL时使用simplejson的use_decimal=True。来源使用条件见[Polymarket条款](https://polymarket.com/tos)。
