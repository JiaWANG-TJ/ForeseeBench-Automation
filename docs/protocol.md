# ForeseeBench 执行协议

本文件记录现行采集与执行规则，需求摘要同步在`文档/数据收集1104.md`。原始研究资料保留在私有开发仓库；旧规则不覆盖这里已确认的修订。

## 术语与边界

- Event 是原始采集单位，Market 是可独立结算的条件，token 是某个 outcome 的资产标识。
- `source_id` 是项目按 Event ID 派生的稳定 UUID；未来生成的 `qa_id` 引用它。
- V1 是无 GT 的冻结源数据，V3 是绑定 V1 的原生结算证据。V2 构题和 V4 构造题答案尚未实现。
- `bootstrap` 是完整初始源数据产品：全部候选均须完成审查、全部合格Event均须完成采集，使用真实冻结时间，允许发布主分支。
- `formal` 是按周一窗口运行的正式周批；`qualification` 仅是隔离工程核验，不能替代完整数据产品或进入主分支。
- 单 Market 与多 Market 均采集。采集层不按 `negRisk`、标题关键词、领域或语义关系筛选。
- `groupItemTitle` 空值和重名原样保留；是否能组成合法题目交给未来 V2。单 Market 的 Yes/No、Up/Down 等标签原样保留。
- PDF 中旧的分层采样、不同预测时点、LLM 路由和评分设想不改变本轮已确认时间规则。

## UTC 时间规则（协议 3.0）

时间沿用三天作答、五天回收，前后各预留一天，名义总周期十天；不另设十二天周期或总时长上限。全部合规数据保存并发布，另提供每域最多200个Event的采样视图；500—1000及领域数量仅作分析。全部参数来自`foreseebench.toml`，创建批次时冻结到`protocol.json`和V1清单。本次获授权退役的历史API数据按清理记录删除，新周期完全重新采集；之后的新冻结输入和结果继续保留必要原件及版本，不混入旧历史投影。

正常周批周一17:00开始准备，周二11:00截止读取特征并冻结，预留六小时构题与核验，周二17:00发题。周ID仍使用UTC周一日期。`startDate`、`createdAt`和`gameStartTime`仅作来源信息，不作为准入条件；缺失或无法解析的可选开始时间归一化为null，原值留在HTTP归档中。正式日期准入只检查全部冻结Market.endDate。

- 用户提交截止为**实际正式发布后 72 小时**；迟到发布必须重新核验日期和未决状态。
- 全部 Market 必须满足 `提交截止 < Market.endDate <= 提交截止 + 5天`。任何一个 Market 不满足即整 Event 排除，不截取子集。
- 所有Market指本批冻结时的完整成员集合。发布前发现结构变化时整Event排除；发布后新增成员另行保留在新来源观察中，不修改已经发布的分母。
- `time.end_at = max(全部冻结 Market.endDate)`，在V1即填写，并统一用于Parquet和四种读取视图。GT回填不修改该计划日期。
- 全批共用 `gt_deadline_at = submission_deadline + 120小时`，不再逐Event额外延长48小时。到该时刻停止请求GT；尚缺合格结果的Event完整保留并失效。
- `finalize_at = gt_deadline_at + 24小时`，保留V4和评估处理预算。实际发布时间发生延迟时重新检查完整72小时与五天窗口。
- 每周继续构建新批次，相邻批次可并行。只有正式题集中的Event消耗发布资格；进入原始库、V1或候选预选不消耗资格。正式发布清单是去重索引的事实来源，崩溃后可据此恢复。

例如2026-11-09 17:00开始准备，11-10 11:00冻结、17:00发布，11-13 17:00截止提交；每个Market.endDate在`(2026-11-13 17:00, 2026-11-18 17:00]`。11-18 17:00结束回收，11-19 17:00为后处理期限。截止前答案已经可确定的Event及其全部QA对所有参与者统一失效。

API 是分次观察，`observed_at` 表示实际获取时间，不能宣称多个接口提供原子时点快照。V2 语义审核必须核对目标在提交前不可确定，以及依据完整 PM 结算结果构造答案的规则；本轮不实现这一 LLM 处理器，也不会用 `closed=false` 代替其审核。

服务错过采集窗口时不倒填正式V1。`bootstrap`使用含真实创建时刻的独立批次ID，从启动时刻预留18小时采集、24小时到计划发布；在采集完整后可提前冻结，并记录真实冻结时间和`schedule_compliant=false`。启动时间不因提前冻结而倒推；发布时重核72小时与五天窗口。18小时截止仍不完整时记录`expired_incomplete`并停止该批次后续请求，不能延长旧截止或冒充完整产品；后续正式周批继续。`qualification`仅作隔离工程核验。
正式采集完成后等待周二11:00冻结；显式再次collect会重新枚举当前源池及刷新已完成流。新扫描开始即撤销旧完成标记，失败不能沿用上次完成状态。此前存在、后来不可读取的Event保留旧事实及明确排除原因；重新变得合格时完整重采评论与成交。

## 全量源库与分域选取

先保存不按日期、状态、领域或成交量筛选的原始Event来源，再筛选并采集全部合规Event的评论、成交和原生证据。完整集为`full`，每周采样集为`balanced`。不存在总量最低值、补满1000或整批全部有效的发布判断；采样不删除完整源数据，历史题集使用情况也不阻止合法原始数据入库。
主领域固定为TechScience、WeatherEnvironment、Crypto、BusinessEarnings、MacroEconomy、FinanceMarkets、GeopoliticsSecurity、PoliticsGovernance、Culture、Sports。`native_tags_10_v1`保留原七类优先顺序，再按明确标签拆分金融与政治，未分类仅用明确企业／健康标签补充。每Event只计一个主领域，Unclassified单列；这不是PM官方主领域或经过语义审核的真值。完整映射、版本和全部零供给领域写入清单，原始`domain`不变。
每个主领域及单列的Unclassified最多`domain_event_cap=200`个Event，少于上限全部保留，不向其他领域转移额度。域内按排序后的完整`series_ids`集合分层；缺少系列时用排序后的原始tag ID集合，再缺失时用Event ID。每层再按全部Market计划结束时间最大值的UTC日期分层，逐层轮询；层顺序和层内Event排序使用固定种子与稳定哈希，默认seed为20261105。名单不依赖API返回顺序或任何未来GT，算法版本为`domain_cap_series_day_v1`。
V1的`selection`冻结全部采样成员、原始领域覆盖及各系列／日期层可得和选中数量。V3继承这份名单，后续未决、提前、迟到或失效不会换题或删除成员。旧名单以原哈希保存，旧源文件投影为当前采样视图时明确标记`historical_projection`；不称为当时预先登记的新采样。
`statistics.json`同时记录十类领域和未分类项的来源、日期筛选、已核验原生未决、完整集、采样集及GT状态分母。日期筛选独立于关闭状态计数；未进行原生核验不冒充通过。旧快照缺少的历史统计填null，不以当前状态倒推。V2未来审核通过后才冻结正式QA；题集去重与原始库分开，完整发布分母始终保留。

## 采集与完整性

发现采用Gamma `/events/keyset`，显式`version=v1&version=v2&closed=any`，不传日期、领域、活跃状态或成交量筛选。以官方返回的最大可见正整数Event ID冻结上界，按最多100个ID的区间查询；每个区间沿服务端`next_cursor → after_cursor`到终止，短页不作终止证据。最大ID无法取得时不能声称平台为空。
列表未返回的ID逐一通过`/events/{id}`补查；只有实际404才计入不存在，其他失败阻止完成。原始HTTP响应先于区间检查点耐久归档；区间完成后再提交索引，失败后只重试未完成区间。全部区间完成后才逐页重放原件进行准入。覆盖收据明确ID范围、返回Event、404、双版本及请求时间；该范围之外和不可公开读取的对象不作已覆盖声明，分次请求不等于原子快照。
来源库同时保存原始HTTP及轻量索引，不再复制第二份完整Event JSON。`source-inventory.jsonl`逐行保留全部来源Event、主领域、状态、原因与回执引用，清单仅保存哈希、行数及分域／排除汇总。刷新期间消失的先前记录也保留，分别统计本次可读数与累计保留数。
Event 标题作为 question；description 单独保存；domain 是 Event 顶层 tags.label 的原始有序列表。
Market 原文、原生 outcome、版本和资产映射均存入源数据，后续 LLM 获得原始结构，不受预先构造的选择题限制。

评论仅采集当前 Event 的专属讨论，不纳入 Series 公共讨论区。使用 `/comments/keyset`，固定 `parent_entity_type=Event`、原始 Event ID、`limit=100`、`order=createdAt`、`ascending=true`；沿 `after_cursor` 遍历，末页省略 `next_cursor`（兼容官方 SDK 接受的 null）。短页不能提前结束，空或重复游标视为错误。旧 offset 接口在 offset 超过200时拒绝请求。
保留 comment ID、回复关系、正文、公开地址、姓名、简介与 UTC 时间；响应中明确的父实体必须与请求 Event 一致。
按 `userAddress` 查询官方 `/public-profile`，唯一的 `users[].id` 补入 `user_id`，并补全评论响应缺失的 `profile.name`、`profile.bio`。公开资料按地址使用有限缓存，保存独立 `profile_observed_at`；正式冻结同时检查评论和资料的获取时间。404、无唯一用户 ID 或资料确实缺失时保留 null，网络或服务错误保留失败并重试。
分页结束后回查缺失父评论；明确404的父评论保留缺失关系，其他查询错误阻止完成。每条评论按真实父评论 ID 关联，清单逐 Event 记录完成状态、主评论数、回复数、总数与缺父回复数。`commentCount` 仅作参考计数，不能替代逐页完成条件或明细计数。
tags缺失/null保留为domain=null，真实空标签列表才是[]；非筛选字段缺失不作为淘汰Event的理由。
评论接口没有历史快照能力，无法恢复采集前已删除内容；“完整”指成功遍历本轮官方可见内容。

成交用 Data API v2 的 Event 游标，固定 `taker_only=true`、`filter_type=TOKENS`、`filter_amount=0.000001`。官方定义taker模式为每笔fill仅返回一次；`filter_amount`缺省或0都会启用0.01默认下限，不能用0表示取消小额过滤。
只有 `next_cursor=null` 且 `has_more=false` 才完成。游标只携带页大小与(block_timestamp, sequence_id)位置，每页必须重发相同筛选条件。Event 形式固定提供近三年范围，`start/end` 不生效，冻结截止由本地检查；不将该接口表述为无限期链上历史。
保存所有可见逐笔成交；不聚合、不采样。不以 condition ID、transaction hash 或整行内容去重，重试仅替换同一采集页。
`row_id` 标识来源页内记录，不冒充链上 fill ID。价格和份数保存精确十进制文本，JSONL 导出恢复为数值。
正式流刷新使用完整重扫和事务替换；中断后的评论从头重扫以覆盖编辑与回复变化，成交使用稳定游标续传。
页面与检查点在同一数据库事务中提交；失败页回滚，错误不伪装为零行。
来源、参数、时间、响应哈希和完整原始响应集中在SQLite归档中。body按SHA-256去重、zlib压缩；逐请求FULL同步事务先于数据页检查点提交。
使用rollback journal，避免共享文件系统上的跨主机WAL约束。它是标准可查询归档，不是另一套业务状态库。
审计归档可能包含拒收响应，不作为预测模型输入。

## 原始结算

按冻结的 Market.version 选择资产：v1 用 clobTokenIds，v2 用 positionIds；始终按原生 outcomes 下标对齐。
回收 `/v2/resolutions` 与 `/v2/tokens`，按冻结 ID 校验 token 的 condition、module、outcome_index 和 opposite_token_id。
每批最多50个 token ID；未知 ID 缺席不能解释为输。
只有正式 resolved、两个 token 已有互补 1/0 支付，才能核验二元结果。分数支付、证据冲突和缺失分别保存状态。
`resolved_at` 是可选证据，缺失时仍可核验支付，但保留 null；禁止用 closedTime、updatedAt、last_update_timestamp 或抓取时间伪填。
时间资格使用真实`resolved_at`，或提交截止之后整组原生接口仍显示未决的观察，结合GT截止前取得的完整正式支付。保留`last_unresolved_at`、`gt_verified_at`及请求哈希；新回执同时记录`requested_at`与完整接收后的`observed_at`。未决时间证明取相关请求中最早的requested_at，不能用跨越提交截止后的接收时间掩盖更早请求；历史回执缺少该字段时保留其已有观察时间语义。
生命周期响应与每个 token 响应分别保存实际观察时间。`first_answer_observed_at` 累计最早的提议／终态或已结算 token 观察，防止跨截止返回或后续状态回退掩盖早泄漏。支付事实首次完整的时间为 `gt_verified_at`；时间资格所需证据全部取得的时间为 `qualification_verified_at`。新补到的可选 `resolved_at` 另有 `resolved_at_observed_at` 和回执，不能借用较早支付证据的时间通过截止。
一组请求中后续页面失败时，先前完整响应及回执仍交付判定；失败页不伪装为空结果，也不丢掉已经看见的提议或终态。采集、发布前核验及 V3 更新均保留这些已知事实。只有请求时刻或回执ID更新时不重复物化V3；首次获得提交截止后的未决证明会单独物化。`last_unresolved_at`表示最近一次进入材料化结果的未决证明，完整逐请求观察仍保留在归档中。
截止前任意 Market 已正式结算或进入提议／争议状态，标记 `early_resolution`。缺失 GT、未知时间资格、分数支付或冲突分别记录；到该 Event 截止仍无法合格，整 Event 失效。API 失败也不会让截止状态永久停在 pending。
全部Market才能得到Event级完整GT；`0、0、未知`不能补成`0、0、1`。`validity`为valid、invalid或pending，原始事实和有效性分别保存。到全批截止停止请求，不再自动补取迟到事实；此前已经收到的失败、迟到和冲突证据继续保留。未知生命周期或“未决但已有final_price”的矛盾响应不能证明未决。
原生GT以完整冻结Market及outcome/token绑定为单位保存，不再自动推导Event级QA答案。多个Yes、分数支付和缺失结果都保留原始事实，未来V4负责构造题的答案。
已核实的0/1支付及总支付为1的真实分数支付，不被后续缺行、网络失败或旧观察覆盖。后续不同的支付向量记为冲突，并保留先前完整事实。
合并以Market为单位，保留原验证证据及其真实观察时间；新完成的其他Market使用新观察时间，不倒填为早证据。相互矛盾的终态保持conflicting_evidence及先前已验证事实，不能靠重复矛盾响应自动消除冲突。

## 存储与接口

单进程通过文件锁持有DuckDB写入权，消费端读取不可变Parquet。生产由公开GitHub仓库的标准Ubuntu runner执行，使用单一并发组；数据库只在runner本地盘运行，不直接放在对象存储挂载上。
网络采用8个有界worker并共享每秒5次限速；DuckDB使用独立读取cursor和串行事务锁，SQLite归档也串行提交。全域来源与候选任务均有界流式处理；限速等待后再次检查截止，禁止等待跨截止后仍发请求。准入统一执行提交截止的开边界和五天结束窗口的闭边界。
HTTPX客户端按worker隔离；网络错误后先保留错误回执，再回收该worker的客户端并重试，避免代理TLS失败耗尽连接池。其他worker的连接不受回收影响。该路径已用本地CONNECT/TLS失败复现及回归核验，参见[httpcore上游问题](https://github.com/encode/httpcore/discussions/921)。
四类 Parquet 为 events、comments、trades、resolutions；Market 结构内嵌在 events，评论和成交只按 Event 保存一次。
按周和容量分片，默认 Zstd 3、512 MiB目标文件、最多65536行或64 MiB输入的row group。
开启 PyArrow CDC、page index 和 checksum，固定依赖及编码。单个超长值本身仍会占据相应内存，不截断内容。
输入文件冻结后保持原哈希；V3 仅引用输入并生成结算分片。更新没有变化的证据不重复物化快照。 V3证据按新增回执区间生成不可变SQLite片，清单引用全部先前证据片；不再每次复制累计归档。每片保存完整响应，并校验上一边界的请求ID以拒绝归档截断。全批截止时记录关闭状态，保留最后一次语义变化之后的所有观察。
每份 manifest 保存种类、阶段、配置哈希、行数、内容哈希、完整性与排除原因。发布历史不可变，可按 HF commit 重放。
正式和bootstrap快照都要求candidate/collecting/failed为0，不能把未处理或失败Event当作筛选排除来完成冻结。请求失败与采集失败历史追加保存，重试成功也不删除。
HF 使用预上传和一次 create_commit；parent_commit 检测并发冲突。首次V3发布同时提交V1父清单及全部引用文件。
远端绑定每周V1输入哈希，其他数据目录也不能覆盖既有冻结输入；只允许添加新的结算快照。未验证完成的本地快照不得发布。

四种视图改为`<view>/<format>/<partition>/<content-hash>.jsonl`分片；view为full/balanced，format为basic/extended。source_id的SHA-256首个十六进制字符确定16个稳定分区，目标分片128MiB，超大Event独占分片而不截断。清单精确列出当前有效分片，更新仅重写受影响分区；`statistics.json`继续保存完整统计。新/变化对象在不可变commit实际回读校验，复用对象核对同一commit的LFS SHA-256或已记录Git blob身份。首次回读中断时重试仍须完成该次新增对象的实读，不能仅因commit存在就跳过。用户可用`download-view`固定远端提交并逐片核验，按原字节合成累计JSONL；无需本地管线数据库。
JSONL行结构仍为3.0，以`source_id`标识原始Event，`qa_id`仅用于未来构造题。`source-metadata.json`在V1冻结时保存经原件核验的轻量来源摘要并绑定原归档哈希，供后续导出读取；它不替代完整原件。主字段为身份、原始标签、冻结的主领域、时间、单份`markets`结构与`native_gt`，详细回执留在审计归档。具体字段见[数据契约](data_contract.md)。
扩展版增加Event描述、每个Market的完整规则描述与结算来源、评论树、缺父回复和逐笔成交。相同文字若分别属于Event与Market，仍保留各自真实含义；不把缺失Market规则用Event描述补写。评论树不删节点或重复成交；循环关系拒绝导出。
累计视图每个Event选取一份完整快照：正式发题绑定优先，否则取最新冻结快照。采样视图取这些相同快照的冻结名单子集；每批原始名单和全部Parquet、归档仍保留。输入、评论、成交与GT不跨快照拼接。数量校验包括评论各层回复、缺父子树和逐笔成交重数。
V1导出`native_gt=null`；latest合并V3。结果流按Event ID与来源流对齐，不把整批详细取证JSON同时读入内存。原始source_id/event_id保持唯一，重复标题不会合并。来源哈希、文本或绑定不一致时拒绝导出。
时间协议仍为3.0；新Parquet物理schema与JSONL格式均为3.0。旧文件按原schema只读，HF原始表按不兼容schema分组。`basic`、`extended`配置各含`full`、`balanced` split，复杂字段采用HF官方Json类型；已用`datasets==5.1.0`验证普通加载，无需用户手动传features。原始JSONL内容和精确十进制文本不因客户端显示类型改变。依据[HF配置说明](https://huggingface.co/docs/hub/datasets-manual-configuration)与[读取文档](https://huggingface.co/docs/datasets/loading)。

V2/V4任务端口绑定输入manifest与哈希、阶段、handler版本、状态和产物引用。默认 not_configured，两个 validator 配置均为空。
接收外部产物时校验输入哈希和产物文件哈希，复制到不可变目录，状态为 awaiting_handler_validation。`stage-validate` 只能调用用户后续提供的真实 validator，结构校验不能替代语义审核；没有处理器时明确拒绝。
V4通过 `--v2-task-id` 绑定同批次已接收的V2产物及其哈希；未提供时依赖显式为空。
V2 JSONL 每个源 Event 一行，必须完整覆盖 V1 分母，包括不合格 Event 及原因；字段为 `event_id, source_id, source_hash, eligible, reason, review, questions`。合格行的 review 含 `answer_available_after` 和 `evidence_urls`；每道 question 含唯一 `qa_id`、题干、结构化 `answer_spec`、完整 `condition_ids`。源文本、QA 语义、真实不可提前确定的条件由外部 validator 审核；代码只做确定性的结构和绑定核验。
`benchmark-release --task-id` 只接受已审核 V2，再检查 PM 原生状态和完整来源映射；服务端实际发布时间与 72 小时截止写入 `benchmark/release.json`，正式问题写入 `questions.jsonl`。所有发布前排除原因和证据保留。
`submit --week --participant --file` 从服务端时钟生成UTC回执，绑定release和V2 manifest哈希；拒绝迟交、错版本和未知QA。每题只接收`answer`与`confidence`：答案可为非null的JSON值，置信度须是有限的0—1数值，拒绝布尔值、字符串及越界数值；不要求理由或选项概率分布。允许分批更新，按服务端sequence追加记录，不接受客户端自报时间。CLI是受控服务端入口。
提交日志为独立 `submissions.sqlite`，使用 SQLite FULL 同步短事务，不争用采集状态库的独占写入锁。持续采集期间仍能接收提交；读取同一参与者的更新时按 `(received_at, sequence)` 选取每道 QA 的最新回执。
V4必须绑定已发布V2及同批V3，逐QA输出`qa_id,event_id,source_id,groundtruth,validity`并覆盖全部发布分母。原生有效／失效状态已确定且真实V4 validator通过后，`benchmark-finalize`保存完整结果，状态为completed，并分别统计有效、失效、领域和处理时间；不再添加规模或领域数量的通过／不通过结论。
官方结算晚于提交截止不代表真实世界答案也晚于截止。V4 语义审核发现提前可知或题目无效时，必须给出 `invalid_reason=early_information/invalid_question` 及 `evidence_urls`，把整个 Event 的所有 QA 统一降为失效；不能把原生失效 Event 升回有效。
当前未注册真实处理器，两阶段均不执行构题或语义验证，不能进入正式预测评分。原生 V3 更新、未来 GT 等待与 LLM 阶段状态单独报告。

## 托管执行、恢复与资源边界

公开运行仓库为`JiaWANG-TJ/ForeseeBench-Automation`，私有开发仓库保留历史。GitHub每五分钟触发`cloud.yml`，调度可能延迟或漏跑；程序根据真实UTC时刻和批次状态恢复。一次处理最多2700秒，检查点宽限600秒，job硬超时3600秒。正式截止优先于执行预算；预算到期只交接检查点，不把未完成批次标成完整。GT和到期阶段优先于新采集，避免长扫描阻塞旧批次关闭。

`SII-JiaWANG/ForeseeBench-state`私有Bucket保存内容哈希对象、代次清单和最后提交的`completed.json`指针。上传前关闭数据库，逐对象回读并记录内容身份；只在整代完整后提交指针。恢复先读取控制文件，再按阶段加载所需输入。未下载的冻结证据只有在原SHA验证记录及当前Xet身份、大小一致时才能远端核验；已下载文件仍检查SHA，使用的Parquet仍检查行数。不把存在或大小当成完整性证明。

首次外部发布前，冻结输入和状态必须先关闭数据库并完成耐久检查点；随后才提交Dataset，完成回读后的publication回执再进入下一检查点。冷启动优先补发已经耐久但未完成发布的快照，然后推进新采集，避免长扫描导致发布一直等待。导出、上传、回读和重试均检查工作预算，并保留更早的全局硬截止。

保留最近两代完成的工作检查点，回收未被任何保留代次引用的中间对象；规范输入、结果和必要证据不因清理缓存而丢失。`publication.json`必须进入检查点，以区分首次回读未完成和已经核验的发布。免费存储预算不足或用量无法核验时停止新增写入，保持最后完整代次并报告，不自动付费或删除正式数据。

生产开关为GitHub仓库变量`FORESEEBENCH_ENABLED`；工程核验在独立Bucket前缀运行，不写正式Dataset。每天或阶段发生变化时仅把不含GT、原文和密钥的汇总状态写入`automation/status.json`。HF Dataset负责数据发布，Bucket负责工作状态；两者都不会自行执行此工作流。

统一LLM客户端在`foreseebench.llm`，验证HTTP状态、模型身份、完整停止、非空最终内容和JSON Schema，思考文本与最终内容分别保存。密钥仅从环境或GitHub Secrets读取。启智临时服务使用“公共科研项目”，实际权重为指定DeepSeek-V4.1-Flash-1017全局路径；平台选择项仅作登记关联。测试覆盖普通回答、JSON和思考模式，并从外部runner验证；无论测试结果如何，都下线此次创建的服务。后续四类业务处理器仍不启用，GPU卡时与GitHub免费CPU执行分别核算。

置信度评价不需要另一个`gt_confidence`：未来判分器依据构造题参考答案产生`correct=0/1`，再计算`(confidence-correct)^2`。该标签属于具体提交答案，不是所有题固定填1。Forecast-Dojo使用完整选项分布的多分类Brier；OpenForecaster的开放答案分数为`2*correct*confidence-confidence^2`，其均值等于ACC减去上述置信度Brier loss。这里只记录数据与接口契约，不实施LLM判分或冒充已经得到模型指标。[Forecast-Dojo源码](https://github.com/Forecast-Dojo/Forecast-Dojo/blob/877f377593978eedec7476c83221fac79c78f8c4/src/evaluation/metrics.py#L44-L75)、[OpenForecaster论文](https://arxiv.org/html/2512.25070v1#S3)。

## 上游依据与复用

规划期及实施前已核验以下一手资料，2026-11-04访问。复用 HTTPX、ijson、PyArrow、DuckDB、huggingface_hub/hf-xet 和 APScheduler；不重写这些基础设施。
采集采用薄HTTP适配器，以保留完整原始响应、精确十进制和流式内存边界。2026-11-05已重新核验官方 polymarket-client 0.16.0 的游标分页实现及实际末页结构。

- [PM事件结构](https://docs.polymarket.com/concepts/markets-events)、[官方Python SDK](https://github.com/Polymarket/py-sdk)、[Data API v2](https://docs.polymarket.com/api-reference/data-api/overview)。
- [成交接口](https://docs.polymarket.com/api-reference/feeds/list-trades)、[评论接口](https://docs.polymarket.com/api-reference/comments/list-comments)、[结算](https://docs.polymarket.com/api-reference/markets/get-resolution-state)、[tokens](https://docs.polymarket.com/api-reference/markets/look-up-tokens)。
- [评论游标响应结构](https://gamma-api.polymarket.com/schemas/CommentsKeysetListResponse.json)、[官方游标实现](https://github.com/Polymarket/py-sdk/blob/aa94ec431e3d4946576fb7c0d079fe1c14fcf676/src/polymarket/_internal/actions/gamma.py)、[公开用户资料](https://docs.polymarket.com/api-reference/profiles/get-public-profile-by-wallet-address)。
- [FutureX论文v3](https://arxiv.org/abs/2508.11987v3)、[Eval仓库](https://github.com/Futurex-ai/Futurex-Eval)：公开Eval主要是评分，不声称复用其未公开采集后端。规划期Online实读80条，不能用论文约500/周代替当前数量。
- [ForecastBench](https://github.com/forecastingresearch/forecastbench)：参考冻结与结果更新设计；其未结算价格代理不作为本项目GT。
- [Prophet Arena版本协议](https://www.prophetarena.co/docs/datasets)：参考固定版本、哈希和输入/标签隔离，不将动态接口的结算信息混入预测输入。
- [HF格式建议](https://huggingface.co/docs/hub/datasets-adding)、[Parquet CDC](https://huggingface.co/blog/parquet-cdc)、[Arrow](https://arrow.apache.org/docs/python/parquet/parquet.html)、[DuckDB](https://duckdb.org/docs/2.0/data/parquet/tips)。
- [Git is for Data，CIDR 2023](https://www.cidrdb.org/cidr2023/papers/p43-low.pdf)、[存储格式权衡研究](https://arxiv.org/abs/2411.14331)：用于版本共享与格式选择依据，不把论文性能直接当作本项目测量结果。

## 验收边界

测试覆盖时间端点、全Event筛选、原生结构、分页与重试、相同成交重数、正式支付与时间缺失、冻结输入不变、Parquet/JSONL一致、发布回退保护和阶段端口。
真实API核验与工程负载测试在 qualification 目录和HF独立验证分支内进行，不作为正式周批。
完整前瞻性验收必须真实经历周一冻结和未来结算；历史回放与当前抽样不代替该验收。
