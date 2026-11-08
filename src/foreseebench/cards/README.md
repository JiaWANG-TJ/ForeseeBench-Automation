# ForeseeBench

**简体中文** | [English](https://huggingface.co/datasets/SII-JiaWANG/ForeseeBench/blob/main/README.en.md)

Polymarket完整源数据与分域采样数据。每条记录包含一个完整Event及其全部冻结Markets；扩展版同时保留规则描述、评论关系与逐笔成交，官方原生结算结果在V3更新。

[代码仓库](https://github.com/JiaWANG-TJ/ForeseeBench) · [完整基础版](https://huggingface.co/datasets/SII-JiaWANG/ForeseeBench/resolve/main/full/basic.jsonl) · [完整扩展版](https://huggingface.co/datasets/SII-JiaWANG/ForeseeBench/resolve/main/full/extended.jsonl) · [采样基础版](https://huggingface.co/datasets/SII-JiaWANG/ForeseeBench/resolve/main/balanced/basic.jsonl) · [采样扩展版](https://huggingface.co/datasets/SII-JiaWANG/ForeseeBench/resolve/main/balanced/extended.jsonl) · [完整统计](https://huggingface.co/datasets/SII-JiaWANG/ForeseeBench/resolve/main/statistics.json)

## 当前数据

{{BATCH_SUMMARY}}

表中每批保留自己的时间协议与冻结时刻。历史输入的格式投影标记为`historical_projection`，不改变旧批次日期，也不代表新3＋5周期的前瞻数据。未知的历史来源和筛选数量保持null。

最新批次的完整领域分布：

{{DOMAIN_SUMMARY}}

GT齐备表示全部冻结Market的原生支付证据完整；时间资格及异常原因另见`native_gt.qualification`，不能仅凭齐备数量认定题目可评分。所有批次的详细分母、状态及采样名单都在statistics.json中。

## 两种数据范围

| Split | 定义 |
| --- | --- |
| `full` | 保存全部通过准入和完整采集的Event，不按题量或领域数量删减 |
| `balanced` | 每批每个主领域最多200个Event；域内按系列、结束日轮询，稀缺域全部保留，不回补总量 |

主领域由版本化的PM标签规则派生，每Event只计一次，原始标签全部保留。Unclassified单列。名单在V1冻结，GT更新不重新采样；后来未决、失效或发生冲突的记录仍保留。

累计视图每Event使用一份完整快照：正式题集绑定优先，否则取最新冻结快照；采样视图取相同快照的子集。所有历史Parquet、各批名单和原始响应继续保留，不跨快照拼接输入与GT。

## 读取

使用`datasets==5.1.0`或兼容的新版本。复杂列采用官方Json类型，读取后自动得到Python列表、字典或None，不需要手动传features或再次解析JSON字符串。

```python
from datasets import load_dataset

data = load_dataset("SII-JiaWANG/ForeseeBench", "basic", split="balanced", streaming=True)
for event in data.take(1):
    print(event["source_id"], event["title"])
    print(event["markets"], event["native_gt"])
```

零条视图保留真实空文件，流式读取返回空迭代。`datasets==5.1.0`默认物化空split会报错；非空时可省略`streaming=True`，使用下标及`len(data)`。

`basic`与`extended`各有full／balanced两个split。需要精确复现时固定`revision`为HF提交。私有数据访问使用授权HF凭据。

| 主要字段 | 含义 |
| --- | --- |
| `source_id` / `event_id` / `batch_id` | 项目稳定Event ID / PM ID / 来源批次 |
| `title` / `slug` | 冻结Event原始标题与slug |
| `domain` / `primary_domain` | 原始有序标签 / 项目派生主领域 |
| `time` | 开始信息、全部Market计划结束时间最大值、源观察和冻结时间 |
| `markets` | 完整Market身份、题干、标签、版本、计划结束日期及outcome/token映射 |
| `native_gt` | V1为null；V3按冻结Market和outcome顺序保存支付、状态、观察及结算时间 |

扩展版增加Event和Market各自的完整描述、结算来源、评论树、缺父回复及成交。评论和成交不会被采样或聚合；采样集保留选中Event的全部上下文。原始Parquet的events/comments/trades/resolutions配置仍可读取，不兼容旧schema使用独立legacy配置。

新JSONL与Parquet物理schema为3.0。原始Event不再使用qa_id，不再重复输出options与markets，也不自动生成Event级QA答案。详细HTTP回执留在关联审计文件中。

## 时间与原生结果

新周批采用UTC：周一17:00开始准备，周二11:00冻结、17:00为计划发题时刻；实际发题后72小时作答、随后120小时GT回收、24小时后处理。每个Market都须满足`提交截止 < endDate <= GT截止`。开始时间不作准入限制。

先归档v1/v2及全部公开状态，在记录的Event ID边界内补查列表遗漏，再筛选。评论按Event专属游标收集；成交固定`taker_only=true`、`filter_type=TOKENS`、`filter_amount=0.000001`，保留API可见范围中的逐笔重数。

结算联合核验生命周期和精确token绑定。`payouts`来自各token的`final_price`，与冻结outcomes顺序一致；0/1、分数支付、缺失和冲突分别保存。0.5/0.5不表示一个结果有50%的真实发生概率。未知值不补零，行情价格不当作GT。已经取得的支付不会被后续空响应抹掉。

`endDate`是计划时间，`resolved_at`只保存实际可得的官方结算时间，缺失保持null。截止后停止回收，保留未决或不合格结果，不因此删除完整数据或改变采样名单。题量及分布只作统计。

原始管线不调用LLM，V2构题与V4构造题GT仍为接口。未来用户只提交答案和置信度；评价置信度需要实际答案生成的0/1正确性标签，不需要额外的gt_confidence。此数据集尚不提供构造题或模型评分。

价格和份数在规范Parquet中保持精确十进制字符串。需要精确读取JSONL时可用`simplejson`的`use_decimal=True`；常规HF客户端数值显示采用其浮点类型。来源使用条件见[Polymarket条款](https://polymarket.com/tos)。
