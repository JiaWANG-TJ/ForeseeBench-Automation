# 完整集、分域采样与原生GT验收（2026-11-08）

本轮以`8f03cca`为代码基线。数量与领域分布改为描述性统计，删除最低500、补满1000、每域最低20和整批有效门槛；全部合规原始Event保存并发布。以下旧记录按原日期保留，其中旧配额和规模目标不再是当前生产规则。

## 已实现范围

- `full`保留全部通过准入与完整采集的Event；`balanced`每主领域最多200条，稀缺域全部保留，不回补总量。域内按系列及UTC结束日轮询，固定种子20261105；V1冻结名单，V3不重采样。
- 四份JSONL为full／balanced各自的basic／extended；统计保留来源、日期、原生未决、入选及GT状态的完整分母。未知历史筛选数量为null。
- 原始记录用`source_id`、`title`、单份`markets`和`native_gt`；补齐每个Market的原始规则与结算来源，详细请求证据继续保存在可关联归档中。
- 每个冻结Market按outcome顺序保存真实token支付。0/1、分数支付、缺失、重复绑定和冲突分开；已经核实的分数支付不会被后续缺行响应覆盖。支付完整性和时间资格分开。
- 用户提交只要求`answer`和`confidence`。置信度为有限的0—1数值；未来判分以答案与参考GT得到的0/1正确性标签计算Brier，不生成`gt_confidence`。
- V2/V4继续为处理器接口；本轮不实现LLM构题、QA真值生成或模型判分。

## 冻结数据实测

输入为`2026-11-02-bootstrap-20261106T081923Z`的既有协议2.0冻结快照。文件哈希逐项验证，输入未改写；新格式投影明确标记`historical_projection`，不能当作新3＋5周期的前瞻成绩。

| 视图 | Event | 评论与回复 | 逐笔成交 | 文件字节 |
| --- | ---: | ---: | ---: | ---: |
| full/basic | 3,292 | 不含 | 不含 | 52,274,195 |
| full/extended | 3,292 | 425 | 254,218 | 346,112,104 |
| balanced/basic | 627 | 不含 | 不含 | 8,457,939 |
| balanced/extended | 627 | 425 | 250,459 | 165,071,058 |

体育2,865条取200条，其余427条全部保留。采样按领域为：TechScience 58、WeatherEnvironment 13、Crypto 34、BusinessEarnings 32、MacroEconomy 39、FinanceMarkets 36、GeopoliticsSecurity 14、PoliticsGovernance 152、Culture 49、Sports 200、Unclassified 0。

该历史快照原生GT完整1条、部分5条、待定3,286条；时间资格仍为失效7条、待定3,285条，没有有效预测成绩。完整支付不等于通过时间资格。四份文件的全部行、评论节点、成交重数和哈希已核对。

`datasets==5.1.0`通过数据卡直接加载basic及extended，均得到full 3,292条、balanced 627条。采用HF官方Json字段类型，嵌套对象、数组和null正常解码，不要求使用者手传features。默认推断在可空嵌套数组上失败的原始记录保留，最终验证通过。

最终算法重导耗时91.204秒。另从冻结Parquet独立核对全部3,292个Event、69,695个Market、139,390个支付槽位、425个评论节点及254,218笔成交，字段、精确数值、重数及采样子集内容均一致，耗时27.735秒。

真实API另检查Event 1001461、1221270、1322920、1323591的全部16个Market，覆盖v1/v2：1个0/1支付、2个分数支付、13个未决；全部支付槽位与token返回逐项一致，未决保留null。已决样例只用于接口和映射核验，没有绕过日期准入进入正式批次。

复核记录位于`data/acceptance/full-balanced-20261108/`：`input-receipt.json`、`projection-report.json`、`independent-projection-verification.json`、`hf-card-loader-validation.json`、`live-native-verification.json`以及四份导出、`statistics.json`。可重运行该目录的`rebuild_projection.py`、`verify_projection.py`和`verify_live_native.py`；真实API重新运行会产生新的观察。

## 软件检查

- `uv run pytest -q`：135项通过，15.76秒。
- `uv run ruff check src tests scripts`：通过。
- `uv run mypy src`：9个源模块通过。
- 回归覆盖不足500、超过1000、领域悬殊、每域上限、系列／日期分层稳定性、V3不换名单、分数支付保留、全部Market对齐、HF发布回读和历史schema隔离。

## 两路审查与修复

Standards轴发现1项P2：日期子层原按日期升序，截断时偏向早日期。已改为种子、批次、领域、系列层和日期的稳定哈希排序；新增cap=1、单系列三日期的固定种子回归，旧实现失败、修复后通过。

Spec轴发现1项P3：零行数据的默认读取示例会报错。已改为流式安全示例并说明客户端限制，新增空bootstrap冻结、四份空文件、零统计、两种HF配置流式空迭代和冻结哈希回归；没有增加数量门槛或占位行。

修复提交`d6535ea`的受影响两文件共42项测试通过（4.27秒），Ruff、mypy及diff检查通过；两路独立增量复核均确认关闭，Standards与Spec未关闭项各0。没有重复无关的完整测试。

## HF发布与运行交接

- 最终数据发布到私有HF主分支，固定提交为[`e0e112e38703c0da88636f7ef44aed6f17236183`](https://huggingface.co/datasets/SII-JiaWANG/ForeseeBench/tree/e0e112e38703c0da88636f7ef44aed6f17236183)。四份JSONL和statistics.json均已从该提交回读，SHA-256与本地相同；原有根目录basic/extended别名退出当前HEAD，历史提交及冻结输入保留。
- HF客户端从固定远端提交正常加载basic与extended，每种均为full 3,292条、balanced 627条。逐Event确认采样行等于同快照完整行，每域最多200，嵌套GT已解码。回执为`hf-final-readback.json`。
- 代码已快进整合本地main。仅按PID／启动时钟／完整argv／cwd四项确认后终止旧ForeseeBench服务，备份状态数据库、调度状态和旧导出。新服务于`2026-11-08T09:18:28.002364+00:00`启动，PID 1794422、启动时钟1838428970，运行实现`d6535eafffbb2a3aff04e67cfcd801a95476d043`；首次tick为`09:18:28.913072Z`，`last_error=null`。
- 生效配置为作答72小时、GT120小时、后处理24小时、每域上限200；旧协议2.0批次继续只读，下次正式采集仍从`2026-11-09 17:00 UTC`开始。V2/V4没有真实处理器，不能据计划时刻声称正式QA已经可发布。
- 三份用户原有文档和22份冻结协议／清单／指针的哈希不变，42份Parquet／SQLite冻结文件的大小及修改时间不变；所检查的54个其他常驻进程身份均不变。生产五份导出与独立验收投影逐字节相同。

状态备份位于`data/inspection/full-balanced-cutover-20261108T090911Z/`。`runtime-verification.json`、`production-parity.json`、`protected-processes-after.json`记录运行与保护范围；当前`data/service.json`保存实际服务身份。

以上是软件与历史数据格式验证；真实未来周批、V2/V4及科学评测结果仍须各自验证。

---

# 协议3.0工程验收（2026-11-08，前一轮）

本次实施依据已确认的三天作答、五天GT回收、前后各一天、全周期严格小于十二天方案。历史协议2.0记录保留在下文，不代表当前生产规则。

## 实现范围

- 双版本、全状态的公开Event ID区间枚举，完整原件归档、详情补漏、404与失败区分、区间恢复及逐Event来源清单。
- 周一17:00准备、周二11:00冻结及17:00发题；完整72小时作答，全批共用120小时GT窗口，截止停止请求，后处理24小时。
- 开始日期仅作可选信息；全部冻结Markets、原始标签、支付缺失及错误历史保留。
- 十个版本化主领域、每Event单次计数、1000候选目标及500有效规模目标；领域、完整有效性和处理预算分别验收。
- 正式题集去重与原始采集分开；累计JSONL优先正式发布绑定，否则取最新完整快照，不跨快照拼接。
- V3只在事实或关键时间证明变化时物化；原生证据以不可变增量SQLite片保存，不反复复制累计归档。

## 已执行核验

- `uv run pytest -q`：**126 passed，14.32秒**。
- `uv run ruff check src tests scripts`：通过。
- `uv run mypy src`：9个源模块通过。
- `git diff --check`：通过。
- 固定输入重算：1,307,312个不同Event；两个历史周期的三天回收非体育日期候选为213／317，五天为354／1,645，与已确认报告一致。耗时53.488秒，峰值内存44.67MiB；这仅为本地重算，不代表网络全量采集耗时。逐Event决策CSV及输入／实现指纹在本地审计目录。
- 190份输入共35,322,907,321字节归档为4,975,533,483字节；逐文件解包流校验全部通过。原始输入未改写。

真实API契约检查于2026-11-08 04:12:39—04:12:46 UTC执行：完整读取Event 1323591、1322920、2948，共13个Markets，覆盖v1、v2以及列表遗漏但详情可见的对象；资产／condition／module／outcome_index／opposite绑定失败和生命周期缺行均为0。回执为`data/acceptance/protocol3/live-api-20261108T041239Z.json`，原始HTTP归档为同名SQLite。这是有界接口检查，不是全域生产采集或正式预测实验。

## 验证边界

原始数据管线与接口已按上述范围测试；V2/V4真实语义处理器仍未配置。领域每周稳定达到20—50、最终500—1000个完整Event全部按时取得有效GT，需要真实未来批次验证。本报告不将历史重建、软件测试或局部API检查写为科学验收通过。

## Standards

基线`30b87d9d6ed7e4cc179e24728c527830996b0efa`，首次审查实现`01b71dbd66c89f4b0cd18047508ba98cabfcfa0e`。独立规范审查确认0项标准违反、0项需提出的代码异味建议；修复提交`7f3479da805575e34f33ff0cd76ef37661c9763b`的增量复核仍为0项。

## Spec

独立需求审查发现1项P2：采集期间因正式发布去重或详情复核失败排除Event时，空payload使来源清单的领域、标签和回执丢失。修复统一保留来源元数据，已发布排除沿用既有来源，详情排除绑定最新原件。新增四条参数化回归覆盖发现、已发布、首轮详情和末轮详情排除，检查冻结清单的领域汇总、来源哈希及回执可查询性。审查代理复核确认P2关闭，无残留发现。

修复后的`tests/test_source_catalog.py tests/test_pipeline.py tests/test_full_construction.py`共17项通过，耗时7.35秒；Ruff、9个源模块的mypy和`git diff --check`再次通过。未重复无关测试。

审查计数：Standards 0项；Spec 1项P2已修复并关闭，未关闭0项。

## 运行交接

- 两次实现提交已快进整合到本地main。切换前按PID、完整argv、cwd和启动时钟重新确认旧ForeseeBench服务，仅终止该服务；旧状态数据库、WAL、调度状态和服务回执先保存在交接目录。
- 新服务运行实现`7f3479da805575e34f33ff0cd76ef37661c9763b`，于`2026-11-08T04:58:20.220012+00:00`启动，PID为3918095、启动时钟1836868191。首次tick为`2026-11-08T04:58:26.837828Z`，`last_error=null`，旧协议2.0批次明确列入`legacy_read_only`。
- 切换后22份协议／清单／指针元数据的SHA-256不变，78个冻结文件的大小及修改状态不变。用新程序验证旧批次最新清单，返回`valid=true`，包含3,292个Event、425条评论、254,218条成交及3,292条结算状态记录；状态记录数不代表已取得同等数量的有效GT。
- 生效配置为72小时作答、120小时GT回收及24小时后处理。下次正式采集窗口从`2026-11-09 17:00 UTC`开始；计划`11-10 11:00`冻结、`11-10 17:00`发题、`11-13 17:00`提交截止、`11-18 17:00`GT截止、`11-19 17:00`后处理截止。V2/V4仍无真实处理器，计划发题时刻不代表正式题集已经能够发布。
- 用户已有的三份文档改动保持原哈希。此次未重新发布HF数据，未将旧批次改写成协议3.0，也未提前生成未来正式批次。

交接回执与旧状态备份位于`data/inspection/protocol3-cutover-20261108T045818Z/`。本地服务已切换与未来数据质量验收是两项独立结论；新协议全域网络采集及完整前瞻周期仍待实际运行。

---

# 本周批次重新采集验收（2026-11-06）

本节记录清空 HF 主分支旧数据后，从头执行的完整重采。下方早期结果属于已经撤下的历史批次，不代表当前 HF 主分支的数据量。

## 范围与版本

- 采集实现固定为 `90330443b9f35f0cf57c1efcc0ea280e0517271d`，配置保持协议 2.0；本次未修改采集或筛选逻辑。
- 旧主分支的两个批次及导出共39个数据／清单文件，在提交 `0014e9e41735b2a6e7d04e2397ddbde0aa9d5f14` 中移除。仓库私有属性、提交历史和 `validation-20261104` 分支保留。
- 旧本地活动批次、数据库及WAL、导出和调度状态归档至 `data/retired/20261106T081626Z/`，不再参与去重或自动发布。
- 新批次为 `2026-11-02-bootstrap-20261106T081923Z`；实际启动参考时刻为 `2026-11-06T08:19:23.395815Z`，实际冻结为 `2026-11-06T10:38:11.600567Z`，不倒填周一冻结。
- HF 新修订为 [`a6d9f94e56214c155391dee608905093f880a86c`](https://huggingface.co/datasets/SII-JiaWANG/ForeseeBench/tree/a6d9f94e56214c155391dee608905093f880a86c)，当前主分支清单仅包含这个新批次。

## 实测结果

| 项目 | 数量 |
| --- | ---: |
| 完整发现并处理的候选 | 4,412 |
| 合规源 Event | 3,292 |
| 明确排除 | 1,120 |
| Market／唯一 condition | 69,695／69,695 |
| 唯一 token | 139,390 |
| 评论与回复 | 425 |
| 逐笔成交 | 254,218 |
| 分域候选选取 | 600 |

candidate／collecting／failed 均为0。采集中转为已决的3条历史异常仍留在记录中，未伪装为成功或删除。600个候选包括 Sports 173、Politics 166、EconomyFinance 107、TechScience 58、Culture 49、Crypto 34、WeatherEnvironment 13。

## 验证与运行

- 从7,704个原始元数据响应中，逐一找到3,292条冻结来源及其真实获取时刻；按固定协议重算的归一化源记录与 Parquet 完全一致。
- 冻结清单的全部文件哈希及行数通过核验；候选选取使用固定种子重算后完全一致。
- 两份 JSONL 逐条关联核验通过；全部评论节点和全部254,218条成交与 Parquet 一致，包括十进制数值、顺序和成交重数。两份文件各有3,292条非空 Event slug 和计划结束时间，扩展版每条仅有一次 Event 描述。
- 从固定 HF 新修订下载全部文件，对清单引用的5个数据／证据文件及两份 JSONL 核对 SHA-256，V1 清单哈希也与本地一致。旧批次路径不存在；验证分支仍为 `a858c4f3661ce02a9fe2e561af9fc5b6bca87708`。
- 回读期间遇到 TLS／连接中断，保留下载前缀并使用 HTTP Range 续传；完整扩展文件最终哈希匹配，没有用上传成功或文件大小代替回读验证。
- 定时服务于 `2026-11-06T11:46:45.407615+00:00` 恢复，PID为75639、启动时钟为1822038710；首次 tick 为 `2026-11-06T11:46:46.803258Z`，`last_error=null`，仅管理新活动批次，轮询间隔300秒。
- 当前交付为完整 V1 源数据及候选选取。真实 V2/V4 处理器仍未配置，未来GT继续按协议回收，不将本次重采视为正式预测评分验收。

完整回执与核验记录位于 `data/inspection/rebuild-20261106/20261106T081626Z/`：`receipt.json`、`verification-local.json`、`verification-exports.json`、`verification-remote.json`。

---

# 协议 2.0 验收（2026-11-05）

本节记录本次最终协议的实测结果；后文早期记录属于历史协议，不用于证明新协议的时间资格。

## 代码与复核

- 实现提交：`95f3771fd9065a3f31e007e50e323f0803211017`；审查基线：`9fb2dee`。
- `uv run pytest -q`：**100 passed，8.74 秒**。
- `uv run ruff check src tests`：通过；`uv run mypy src`：8 个源文件全部通过。
- Standards 和 Spec 两路独立复核均关闭全部已确认问题；最后一轮各自只执行 4 项针对性回归。
- 回归覆盖期限失效持久化、无新事实复用快照、生命周期与各 token 的真实获取时刻、迟到时间证据独立计时、部分请求失败保留成功证据，以及采集持锁期间独立进程接收提交。
- 测试中的 V2/V4 validator 是明确的测试夹具。生产配置没有真实 LLM 处理器，也没有实际构题或正式预测评分结果。

## 完整源数据构造

批次：`2026-11-02-bootstrap-20261105T155620Z`；完整发现和处理 **4,448 个候选**，其中 **3,409 个合规入库、1,039 个明确排除**，candidate/collecting/failed 均为 0。
4 条采集异常历史对应已决状态拒收，仍保存在清单中，不将历史错误抹掉或当作零行成功。

| 内容 | 本次新增数量 |
| --- | ---: |
| Event | 3,409 |
| Market／唯一 condition | 71,995／71,995 |
| 唯一 token | 143,990 |
| 评论与回复 | 349 |
| 逐笔成交 | 206,247 |

源 Event 的实际观察范围为 `2026-11-05T16:03:49.061492Z` 至 `2026-11-05T18:14:03.636641Z`；实际冻结为 `2026-11-05T18:25:53.188899Z`。这是窗口外 bootstrap，不倒填周一的正式冻结。

输入使用已冻结计划：提交截止 `2026-11-09T15:56:20.096506Z`，随后 30 天为结束日期上界。该时间是 V1 的选题参考，正式 V2 尚未发布；将来发题必须重新核验并从真实发布时间给予完整 72 小时。

冻结前已用最终生产规则逐一重算全部 **3,409** 条归一化结果，并与源响应的哈希及真实获取时刻核对一致。还检查了本轮归档中的 **7,857 个元数据响应、11,699 个生命周期响应、10,436 个 token 响应**；入库 Event 的历史取证中未发现已经取得的答案信号。全部评论与成交流均到达完成状态。
采集进程在 `a7c5511` 启动；之后修复了 GT、异常取证和提交路径。冻结前按 `95f3771` 复算和核对全部实际输入，冻结使用最终代码；两个阶段的代码文件哈希均留在 manifest 内。

全部 **206,247 笔成交**已在原始 API 响应、Parquet 与 GitHub 独立下载的扩展 JSONL 之间逐笔比对，字段、重数及来源行 ID 完全一致。全部 3,409 个 Event 均取得分页终止证据，其中 50 个使用多页。最早 Market 创建于 `2025-11-06T22:55:39.231710+00:00`，没有创建日期缺失，均处于接口提供的三年历史范围内；实际成交覆盖 `2025-11-07T20:50:15+00:00` 至 `2026-11-05T18:10:51+00:00`。核验记录为 `trade-integrity.json`、`history-coverage.json`。

## 实际分域选取

| 采样组 | 合规源供给 | 600 个候选中的数量 |
| --- | ---: | ---: |
| Crypto | 40 | 40 |
| Culture | 49 | 49 |
| EconomyFinance | 113 | 113 |
| Politics | 167 | 164 |
| Sports | 2,969 | 163 |
| TechScience | 59 | 59 |
| WeatherEnvironment | 12 | 12 |
| 合计 | 3,409 | 600 |

没有领域比例上限，没有重复 Event 或补造数据。原始 `domain` 保持原样，领域内使用固定种子 20261105 的稳定哈希排序。
`Science` 原标签只出现于 **1 个 Event**；TechScience 是科技与科学合组，不能把 59 条全部称为科学题。
600 个记录是 V1 的候选选取。真实 V2 处理器审核和发布前核验完成后才会产生正式问题集；原始源库 3,409 条及全部上下文完整保留。

## 冻结输入交付

- [HF V1 固定修订](https://huggingface.co/datasets/SII-JiaWANG/ForeseeBench/tree/79176866cc5ef4d0d891ac47ce7b9cd9d7b96196)。已下载完整快照，核对全部引用文件 SHA-256、Parquet 行数、父清单及两份 JSONL 的独立流式计数。
- [GitHub V1 数据 Release](https://github.com/JiaWANG-TJ/ForeseeBench/releases/tag/data-2026-11-05-protocol2-v1)。完整包包含 18 个仓库文件和内嵌 SHA256SUMS；同时提供两份完整 JSONL、总清单和回读记录。
- 已从 GitHub 独立下载全部 6 个资产，核对 GitHub 服务端摘要、本地原件、下载件及完整包内 18 个文件，全部一致。
- 两份累计 JSONL 均为 **4,825 个 Event**，其中 1,416 个是保留的历史源数据；扩展文件共有 **349 条评论、220,450 笔成交**。
- `basic.jsonl`：40,476,678 字节；SHA-256 `393af40f34b63c130cc42bf51e3189fe7dc5d42dbab2a842b2ba7d9f006fe82a`。
- `extended.jsonl`：221,802,455 字节；SHA-256 `a6c0ce8a0bcc55a9baa58fe3a450209433236e4fcf1d801ee91666a81d436862`。
- 完整输入包：315,901,123 字节；SHA-256 `bf50519ace363e410ff36308daf6e2f3f4af872777c1db713a0952163d20befc`。

本地验收回执集中在 `data/acceptance/protocol2/`：`code-verification.json`、`review.json`、`source-audit.json`、`hf-v1-readback.json`、`github-v1-readback.json`、`trade-integrity.json`。原始大数据与导出存于 `data/` 和 HF/GitHub 数据资产，未提交到源码 Git 历史。

## 本轮原生 GT 观察

新批次全部 **3,409 个 Event** 已完成生产管线的原生 GT 刷新，请求失败为 **0**。首个 V3 清单生成时间为 `2026-11-05T20:31:44.016373Z`。
当前 **3,408 个 Event 待决、1 个 Event 失效**，所有 Event 的完整派生 `groundtruth` 均为空；这不表示每个 Market 都没有出现答案信号。

Event `1304048`（`Will Perplexity's valuation hit __ by November 30?`）的一个 Market 在 `2026-11-05T19:42:57.611347Z` 被观察到 `proposed` 状态，因此整条 Event 已标为 `early_resolution`／`invalid`。该 Event 包含在 V1 的 600 个候选中；候选预览尚有 599 个未因该信号失效，仍不是正式发题数量。数据和所有 Market 均保留，后续取得正式支付也不会恢复其有效资格。
对应 condition 为 `0x595cd44edd7f2c76d84a1e9092b8e63e3df5c7dd4b7ebc78d4fa9a1269d84ace`，原始回执 ID 为 `bf120a1f-7574-4cf4-b85a-235453f62a66`、`ed258700-de66-4b1e-8a74-5ef99edd084e`。提议状态只用于失效判定，未当作正式 GT。

历史批次的 **1,416 个 Event** 也完成本轮原生 GT 刷新，失败为 **0**；累计刷新 **4,825 个 Event**。历史批次保留 `legacy_source_only`／`not_evaluated`，不按新协议追认其时间资格。

## 最终固定快照

[HF 最终修订 `701eaea5dde1155d82117e1497c59146a35d1a33`](https://huggingface.co/datasets/SII-JiaWANG/ForeseeBench/tree/701eaea5dde1155d82117e1497c59146a35d1a33) 已完整下载全部 **24 个文件**。全部引用文件哈希、Parquet 行数、父清单和两份 JSONL 的独立流式计数均通过核验。
进一步逐 Event 核对了 **4,825 条结算观察、98,789 个 Market** 与冻结源记录的一一对应，未丢失 Market，也没有向未完成核验的 Event 写入派生 GT。新旧批次共 **10 个 V1 文件**与先前发布的哈希完全一致。

| 交付文件 | 字节数 | SHA-256 |
| --- | ---: | --- |
| 完整 V3 包 | 445,492,323 | `5b6f7f7f7617020490eab8e59a754c64679fb8b34893b5165fdfa944ef1c015c` |
| `basic.jsonl` | 325,442,287 | `bd8d740b958ded1241d24613f3bc9038babd2c9799c1af77c8c07a396c88e107` |
| `extended.jsonl` | 506,768,064 | `51e14f63b966e545cd358484a22d5aefa1edd6b6c53e6f587e2ae03bc031b631` |

两份累计 JSONL 均为 **4,825 个 Event**；扩展版保留 **349 条评论、220,450 笔成交**。完整 V3 包包含固定 HF 快照的 24 个文件及内部校验和；独立验收归档包含 30 个文件，保留完整回执、复核脚本、统一配置和复现命令。

[GitHub 最终 V3 Release](https://github.com/JiaWANG-TJ/ForeseeBench/releases/tag/data-2026-11-05-protocol2-v3) 的 **7 个资产、1,277,801,939 字节**已全部独立下载。GitHub 服务端摘要、本地原件和下载件的 SHA-256 全部一致，完整数据包内 24 个文件与验收归档内 30 个文件也通过校验。GitHub 与 HF 均保持私有。
GitHub 大文件回读在小范围同内容哈希对照后，改由官方 CDN 直连并从已有字节续传；仅调整本次下载连接，常驻服务身份保持一致。对应回执为 `github-v3-readback.json`、`github-v3-acceptance-readback.json`、`github-v3-transfer.json`。[GitHub 官方资产下载接口](https://docs.github.com/en/rest/releases/assets#get-a-release-asset)

本次 HF 回读先使用 Xet，单个证据归档迟迟未完成后，仅将本次回读进程切换为官方支持的 `HF_HUB_DISABLE_XET=1`；HTTP 读取超时后自动续传，最终哈希校验通过。原始尝试、重试及常驻服务身份核对记录均保留，没有修改全局传输配置。[HF 环境变量说明](https://huggingface.co/docs/huggingface_hub/en/package_reference/environment_variables#hfhubdisablexet)
最终回执为 `hf-v3-readback.json`、`final-data-audit.json`、`download-transport-retry.json`；全部 Event 的完整派生 GT 仍为空，当前交付不构成正式预测评测结果。

## 常驻运行

完整手工回收与发布结束后，常驻调度于 `2026-11-05T21:30:31.372512+00:00` 恢复；PID 为 `3485939`，进程起始 ticks 为 `1816901307`。实际命令、工作目录及身份记录在 `data/service.json`。
新的首轮 tick 标记为 `2026-11-05T21:30:34.723302Z`，在 `2026-11-05T21:31:32.250310+00:00` 确认完整执行结束，`last_error=null`。本轮复用刚完成的 GT 周期，没有额外发起重复原生请求；真实接口验收来自上述完整 4,825 条刷新及其原始回执。
两个批次的刷新记录使用各自实际 V3 生成时间交接，未伪填成服务重启时间。后续仍由统一配置的 APScheduler 按 300 秒间隔检查，单实例执行；证据与日志继续保存于原数据目录。运行验收回执为 `data/acceptance/protocol2/runtime-verification.json`。
最终复核于 `2026-11-05T22:05:20.163600+00:00` 再次确认相同 PID／起始 ticks、命令及工作目录，最近原生接口响应为 `2026-11-05T22:05:19.733828Z` 的 HTTP 200。


---

# 完整数据构造与交付验收

## 2026-11-05 重建交付

按用户确认范围，清理2026-11-02周的旧生产数据后重新全量构造。仅收集Event专属评论及回复，公共讨论区排除。实际冻结时间为 **2026-11-05T08:22:28.903382Z**，批次为 `bootstrap`，`construction_complete=true`、`schedule_compliant=false`。

| 项目 | 数量 |
| --- | ---: |
| 已审查候选Event | 2,082 |
| 合格且完整采集的Event | 1,416 |
| 按规则排除 | 666 |
| 未处理／剩余采集失败 | 0／0 |
| Market | 26,794 |
| outcome token | 53,588 |
| 逐笔成交 | 14,203 |
| Event专属评论 | 0 |

排除原因：起始时间越界619、市场结束时间越界13、市场已关闭或结算34。全部Event的评论流完成状态均已记录。网络故障及重试保留在原始归档，首轮连接池停滞前的9条事件级失败记录保留；最终没有待处理失败。

- 数据：`data/batches/2026-11-02/V1/`；完整JSONL：`data/exports/`。
- [HF数据集](https://huggingface.co/datasets/SII-JiaWANG/ForeseeBench)：四个Parquet配置和两份完整JSONL。
- [GitHub数据Release](https://github.com/JiaWANG-TJ/ForeseeBench/releases/tag/data-2026-11-02-v1)：两份JSONL、清单、校验和及完整数据包。
- 重建记录：`data/inspection/rebuild-20261105/`；用户原有需求文档修改及HF工程验证分支均保留。

## 评论采集核验

- 官方 `/comments/keyset` 遍历到末页；保留主评论、各层回复和缺失父评论的回复。Parquet逐条存储，JSONL使用 `comments[].replies` 和 `orphan_replies`。
- 公开资料接口提供可确认的用户ID；补全发生在存储事务前，保留独立观察时间。
- 真实控制Event 16183：55条评论，42条主评论、13条回复，55条均取得用户ID，树形导出仍为55条。
- 真实控制Event 23246：16页、2,982条评论，其中1,594条主评论、1,388条回复；已越过旧offset接口的上限。
- 控制数据位于 `data/inspection/comment-controls-20261105/`，未加入本周生产数据。
- 官网浏览器页面本轮停留于加载状态，未声称网页逐条对照已通过；接口核验使用实际HTTP响应与官方契约。

## 交易历史核验

官方规定 `taker_only=true` 每笔fill返回一次；`filter_amount=0`仍使用0.01默认下限。采集保持 `filter_type=TOKENS`、`filter_amount=0.000001`，逐页重发相同条件，直到 `has_more=false` 且 `next_cursor=null`。

- 全部 **1,416 个Event、14,203笔成交**完成逐笔核验：API原始响应、Parquet和扩展JSONL的字段及重数完全一致，来源行ID也一致。
- 实际数据含 2 个多页Event；未发现跨Market混入、缺失页或未标注outcome。
- 同一真实Event以1000条页大小和37条页大小分别查询，得到相同246笔成交；小页遍历7页，价格、数量及金额一致。
- 官方Event成交接口固定提供近三年范围。本批最早Market创建于 `2026-10-20T02:35:56.111243+00:00`，创建时间缺失数为0；成交时间从 `2026-10-24T00:15:09+00:00` 至 `2026-11-05T07:59:33+00:00`。
- 份数合计 `4313201.513055`，逐笔价格乘份数合计 `2188608.2793570842246341` USDC，使用十进制运算。
- 完整核验报告及复核脚本：`data/inspection/trade-audit-20261105/release-verification.json`、`verify_release.py`；分页对照为 `pagination-control.json`。

## 实现与运行复核

评论、资料、回复树及并发事务回归已通过。Standards／Spec发现的父评论空响应误完成、事务内网络阻塞、嵌套异常重试和冲突重复评论四项问题均修复并复核关闭。

长时间实采还复现了httpcore代理TLS失败后连接池耗尽的问题。已采用worker独立HTTPX客户端，网络错误后只回收当前worker客户端；自定义transport改由工厂独立创建。该修复已有本地真实CONNECT/TLS故障回归，真实并发24次请求全部完成，2次临时错误成功恢复。生产仍使用8个worker和全局5请求/秒。

最终集成检查为78项测试、Ruff和Mypy通过；兼容仓库新增的原始Event `slug` 导出，JSONL格式为1.4。原始输入和结算更新仍分离，V2/V4保持预留接口。

以下保留早期工程核验记录用于追溯；其中3个Event的样本已经由上面的本周完整数据交付取代，不代表当前交付范围。

日期：2026-11-04 UTC。实现核心提交 `036004b`，后续补充协议迁移测试和本记录。

## 已执行验证

- `uv run pytest -q`：35个用例通过。
- `uv run ruff check src tests`：通过。
- `uv run mypy src`：7个源文件通过。
- 覆盖UTC窗口端点、整Event排除、原生单Market与混合主题保留、v1/v2资产选择、outcome顺序、迁移旧token隔离、重复证据、分数支付、缺失结算时间、评论回复分页、成交短页续传、页事务回滚、冻结不可变、发布回退和远端输入替换保护。
- 独立的 Standards 与 Spec 审查及复核已完成。确认的问题均通过针对性故障注入回归；未将审查角色分离当作统计独立性证明。

## 审查修复

| 轴 | 确认问题 | 最终验证 |
| --- | --- | --- |
| Standards | 旧失败批次阻塞新周批 | 按批次隔离异常，后续周批继续冻结 |
| Standards | manifest落盘后latest指针中断 | 显式重试和调度器自动重试均恢复，V1不覆盖V3 |
| Standards／Spec | 首次直接发布V3缺少V1父清单 | 提交经过哈希校验的父清单与全部引用文件 |
| Spec | 未关闭ZIP在进程崩溃后丢原始响应 | 改用逐请求FULL同步SQLite事务；os._exit故障后仍完整可读 |
| Spec | 新失败未进入同代V3清单 | 以结果和失败内容共同生成快照标识 |
| Spec | 提前／延期判断受token缺失掩盖 | 时间排除资格独立于支付完整性，两端边界均通过 |

V2/V4端口额外验证：重复查询保留已接收状态，V4绑定同批次V2产物哈希；仍为未配置／待handler校验状态，没有构造任何真实或替代QA。

## 早期真实API与HF核验（非本次完整交付）

最终核验执行相同生产采集器，使用 `qualification` 隔离目录，完整扫描当前周Market结束窗口，发现 **1,784个候选Event**。
这不是全部合格数量：本次明确限制收集3个完整Event，其余未审查候选仍保留在清单中。

| Event ID | 官方原始标题 | Market数 |
| --- | --- | --- |
| 1221265 | Counter-Strike: LPH Gaming vs  Phantom Academy (BO1) - ESEA Advanced Europe Regular Season | 1 |
| 1221266 | Counter-Strike: Falcons Force vs Xcity (BO1) - ESEA Advanced Europe Regular Season | 1 |
| 1242119 | HUS Agadir vs. RS Berkane | 3 |

上述样本共17笔成交、0条可见评论。0评论有成功分页终止响应支持，不是把错误填成0。
V3保存3条原生结算观察，当前均为pending，groundtruth均为null。

- HF V1修订：`c1283abd015b023f2e8d13ebe3722489ed43de25`。
- HF V3修订：[`a858c4f3661ce02a9fe2e561af9fc5b6bca87708`](https://huggingface.co/datasets/SII-JiaWANG/ForeseeBench/tree/a858c4f3661ce02a9fe2e561af9fc5b6bca87708)。
- 验证分支：`validation-20261104`，保持私有；未将核验数据混入main的正式周批。
- 已从固定V3修订下载全部引用文件，逐项检查SHA-256、Parquet行数与父V1清单哈希；全部通过。
- 再次执行publish返回unchanged=true，revision不变。
- 本地回执：`data/acceptance/hf-readback.json`；最终核验数据：`data/acceptance/qualification/`。

另对Event 16183执行真实接口契约探测：完整遍历取得55条可见评论，成交仅取3页共3,000条，明确记录 `trade_history_complete=false`。
该Event不属于正式周批，样本和测量位于 `data/probes/`。
真实已结算Market 6680431返回resolved状态及两个token的1/0正式支付，但未提供resolved_at；原始响应已归档，不能据此冒充时间资格通过。

## 存储测量

以下真实样本均通过Parquet无损回读，读取的是原始文本及精确十进制表示。

| 真实样本 | 行数 | JSONL字节 | Parquet字节 | 写入秒 | 读取秒 |
| --- | ---: | ---: | ---: | ---: | ---: |
| 评论 | 55 | 21,110 | 8,955 | 0.0050 | 0.0599 |
| 成交 | 3,000 | 1,658,677 | 280,641 | 0.0122 | 0.1264 |

小型评论样本的gzip更小（4,873字节）；Parquet的选择同时考虑列裁剪、结构类型、版本更新和大数据读取，不能宣称所有场景都最省空间。

对真实冻结成交样本作**合成重复负载**，通过实际Store暂存、冻结、校验与JSONL导出路径处理200,000行：

- 暂存6.64秒；Parquet冻结5.99秒；校验与扫描0.25秒；JSONL导出4.29秒。
- 峰值RSS约490 MiB；成交形成1个Parquet分片。
- 导出JSONL为102,129,904字节；全部200,000行保留。
- 重复数据的压缩率不代表自然市场分布，不用于估计正式数据集容量。
- 原始测量与输入哈希在 `data/storage-benchmark/measurements.json`。

从包含成交的工程核验快照重放，输出到一个新目录：

```bash
uv run python tests/benchmark_storage.py \
  --root data/qualification \
  --manifest data/qualification/batches/2026-11-02/V1/manifest.json \
  --output data/storage-benchmark-new --rows 200000
```

## 运行状态与边界

调度器已在本资源启动，UTC每300秒检查一次，首次心跳无错误；本轮部署身份记录在 `data/service.json`，日志为 `data/service.log`。
本周完整初始源数据已按真实采集时间交付；下次准点周批从 **2026-11-09 00:00 UTC** 开始，06:00冻结。2026-11-02源数据仍回填到原批次，GT回收窗口按该周五12:00开始。

原始源数据的完整构造已执行；LLM构题及正式预测评测不在本轮执行范围，尚未到来的真实GT不伪造。
PM缺失resolved_at的记录保持时间未验证并继续回收；不使用价格、closedTime、更新时间或LLM推测补齐。
