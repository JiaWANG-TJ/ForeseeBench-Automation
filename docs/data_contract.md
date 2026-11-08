# 数据契约：格式3.0

本文定义原始PM数据的读取视图。V1冻结元数据，V3添加原生结算事实；LLM构造题和题目GT仍属于未来V2/V4。

## 文件与分母

| 文件 | 内容 |
| --- | --- |
| `full/basic.jsonl` | 完整来源视图，包含全部合规Event的基础结构与原生GT |
| `full/extended.jsonl` | 同一Event集合，增加完整规则、评论关系与成交 |
| `balanced/basic.jsonl` | 同一冻结输入中每域最多200个Event的采样视图 |
| `balanced/extended.jsonl` | 同一采样集合的完整上下文 |
| `statistics.json` | 每批完整分母、各域各阶段数量、采样名单与GT状态 |

累计文件每Event选择一个完整快照，规则是正式题集绑定优先，否则取最新冻结快照。采样文件是相同快照的子集；历史各周全部输入及名单仍留在Parquet、manifest和归档中。统计同时列出每批数量与累计去重数量，不把两种分母混用。

`selection_origin=frozen_v1`表示名单已经在V1冻结；`historical_projection`表示从旧冻结来源按当前规则生成的读取投影。后者不改变旧时间协议，也不追认历史预测成绩。未知的历史来源／筛选计数保持null。

## 基础字段

| 字段 | 定义 |
| --- | --- |
| `source_id` | 项目按PM Event ID生成的稳定UUID，沿用原值 |
| `event_id` / `batch_id` | PM Event ID / 本行来源快照的批次ID |
| `slug` / `title` | 冻结Event原始slug及标题；slug缺失保留null |
| `domain` | 原始有序tags.label列表；缺失为null，真实空列表为[] |
| `primary_domain` | 版本化规则派生的单一主领域；不是PM官方主领域 |
| `time.start_at` | 可空的开始时间信息，不作为准入条件 |
| `time.end_at` | 全部冻结Market计划结束时间的最大值，不是实际结算时间 |
| `time.observed_at` / `time.frozen_at` | 该源元数据的获取时间 / 实际输入冻结时间 |
| `markets` | 全部冻结Market，包含原生身份、题干、标签、计划结束时间和outcome/token映射 |
| `native_gt` | V1为null；V3为下述完整原生结果结构 |

每个Market含`market_id, condition_id, label, question, version, end_at, outcomes`。`label`来自groupItemTitle，空值或重名保留；每个outcome含`label, index, token_id`。Event、Market与token ID全部保存为字符串，禁止转为浮点数。

扩展版增加顶层`description, comments, orphan_replies, token_trade_history`；各Market增加`description, resolution_source, neg_risk, game_start_at`。Event和Market描述各自取自原件，不互相补写。完整结构只存一份，不再重复生成options。

## 原生GT

`native_gt`包含：

| 字段 | 定义 |
| --- | --- |
| `status` | complete、partial、pending或conflict，描述原生支付证据齐备情况 |
| `qualification.validity` / `reason` | 时间／原生资格状态及原因，与支付事实分开 |
| `deadline_at` | 来源批次实际使用的GT截止；旧批次保留原规则 |
| `markets` | 与源Market集合完整对齐的逐Market记录 |

逐Market记录为`condition_id, status, resolution_status, payouts, resolved_at, observed_at`：

- `payouts`按该Market冻结outcomes顺序保存对应token的`final_price`，单位为每份份额的最终支付。它不是行情概率，也不是`/v2/resolutions.payouts`原始微单位数值。
- 未取得的支付槽位为null；不能根据其他Market均为No，推断缺失Market为Yes。本项目当前原生Market契约为两个互补资产，Event可以包含多个Market。
- `resolved`表示完整绑定下取得0/1支付；`fractional`表示完整、已结算且总支付为1的分数支付。分数支付不自动产生唯一赢家或概率标签。
- pending、missing_lifecycle、missing_or_invalid_binding、invalid_payout和conflict保留各自含义。HTTP原件包含完整失败和冲突证据。
- `resolved_at`只使用实际可得的官方时间；缺失保持null。`observed_at`是取证观察时刻，两者不能互换。
- Event层status只有在全部冻结Market支付齐备时才是complete；该状态不代表所有构造题已有答案或已经通过预测时间资格。

结果由`/v2/resolutions`生命周期与`/v2/tokens`联合核验，严格绑定`condition_id, token_id, module, outcome_index, opposite_token_id`。已经核实的0/1或分数支付不能被后续空响应覆盖。不同终态保留为冲突。

## 评论、成交与精度

`comments`保存主评论，`replies`递归保存回复；缺父回复及其子树放在`orphan_replies`。保留comment ID、父ID、公开用户ID／地址、profile.name/bio、正文和UTC时间。Parquet仍是一行一条评论或回复。

成交按Event保存全部返回记录，保留相同成交的重数，字段包括condition、token、option_label、outcome、BUY/SELL、单价、份数及时间。Parquet中的价格和份数是精确十进制字符串；JSONL输出原始精确十进制数值。需要精确算术时读取Parquet字符串或使用`simplejson.load(..., use_decimal=True)`，而非默认浮点转换。

## HF与版本

HF的`basic`、`extended`配置各有full／balanced两个split。复杂列声明为官方Json特性，`datasets==5.1.0`加载后仍是Python字典、列表或None；无需对单元格再次json.loads。原始JSONL直接下载也保持原嵌套结构。
零条视图保存真实空文件和零计数，不造占位行。该客户端默认物化空split会报错，使用`load_dataset(..., streaming=True)`可正常取得空迭代；非空数据同时支持物化和流式读取。

四类规范Parquet表为events、comments、trades、resolutions；新schema为3.0，历史不兼容schema使用独立legacy配置。原件、每次请求回执、配置及算法版本位于关联manifest和审计文件中。

源数据不再输出伪QA ID、Event级便捷答案或重复的详细取证树。`qa_id`和构造题的`groundtruth`只出现在未来V2/V4产物中。

## 预测与置信度接口

每个QA的提交值只有`answer`和`confidence`。答案支持非null JSON值，置信度是有限的0—1数值；字符串、布尔值、NaN、无穷或越界值均拒绝。该接口保存答案，不执行语义判分。

未来判分器用实际参考答案产生`correct=0/1`。置信度Brier loss为`(confidence-correct)^2`，不需要额外的GT置信度。PM原生支付是构造题GT的证据；市场报价及LLM对GT的自报把握不能代替事实标签。V2/V4处理器仍未配置。
