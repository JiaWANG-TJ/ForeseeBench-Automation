# 云端迁移、清理与模型接口验收

更新日期：2026-11-10。本机临时执行、进程恢复及HF检查点回读已经实测；GitHub托管执行仍受账户锁阻塞。独立HF Space的免费资格验证未通过，已暂停且未接管生产。本周批次在11:00 UTC截止后记录为`expired_incomplete`，未形成完整V1。

## 独立HF Space资格与部署代码（2026-11-10）

新增专用项目`SII-JiaWANG/ForeseeBench-Automation`，原有`procuregym`和`AgentProcure`未修改。实现复用`foreseebench.cloud`、独立子进程、限时执行、完成回执、远端执行租约、健康接口、UptimeRobot邮箱激活客户端和本机移交保护。私有数据及凭据不进入公开部署包。

| 实际检查 | 结果 |
| --- | --- |
| 免费ZeroGPU项目创建 | 成功；创建不等于运行资格通过 |
| 首次构建 | 失败；HF注入Gradio MCP依赖，与原界面锁的Pydantic冲突 |
| 修正后的构建 | 成功；源码`8e3d791`，Space提交`573555337edcb970a682cd81da4050dc61dadcab` |
| 实际启动 | 失败；平台返回`No @spaces.GPU function detected during startup`；应用日志同时记录7860端口被占用 |
| 切换免费CPU Basic | HTTP 402，明确要求PRO订阅；没有购买或启用付费硬件 |
| 最终运行状态 | 新Space为`PAUSED`，本机Supervisor仍运行；原有两个Space仍为私有、原提交、`SLEEPING` |
| 尚未通过的验收 | Space真实PM/HF资格无完成回执；两轮推进、冷恢复、UptimeRobot激活与三次定时访问、生产切换及49小时观察均未完成 |

因此，原先“免费ZeroGPU承载纯CPU采集”的假设不成立；本次完成的是部署适配代码和失败边界核验，未实现脱离本机的免费持续运行。后续部署入口只接受`cpu-basic`，在远端写入前拒绝ZeroGPU或付费硬件，不重复尝试已经确认不兼容的路径。HF目前区分CPU Basic的零小时费用与新计算Space的付费账户资格。[官方硬件说明](https://huggingface.co/docs/hub/spaces-gpus)、[ZeroGPU使用要求](https://huggingface.co/docs/hub/spaces-zerogpu)

精简回执位于`data/operations/hf-space-20261110/`，包括创建、两次部署、构建与启动日志、CPU资格402及最终状态。现有运行状态、冻结协议和截止保持原值，尚未执行`handoff`或写入`cloud-owner.json`。

软件验证：核心环境完整测试**225 passed、1 skipped，51.55秒**；被跳过的界面测试在隔离界面环境单独通过，相关界面与运行控制测试共7项通过。最后的界面语言及动画样式调整后，界面测试再次通过。Ruff、mypy和diff检查通过。平台依赖组合的解析及HF实际构建成功；本机界面测试使用锁定的74项库依赖和宿主同版本uv，uv轮子下载超时未作为应用成功证据。

本地浏览器核验375px竖屏和812px横屏无横向溢出；明暗主题、键盘刷新、中文页面语言和只读健康接口通过，控制台无错误。减少动画CSS规则已加载且页面无常驻动画；浏览器工具的媒体模拟返回协议错误，因此未声称完成系统级减少动画设置的实测。这些本地结果均不替代Space运行资格。

部署适配及修复均完成Standards和Spec两路复审；最后代码范围`8e3d791 → 55a4ca7`新增确认问题均为0。12:23:56 UTC再次确认Space暂停、资格前缀没有完成检查点、Dataset仍为私有原版本，原本机监督及工作进程身份不变。通过审查的是代码和失败处理，不是尚未实现的免费持续托管。

## 本周初采与恢复修复（2026-11-10）

本机Supervisor进程在修复检查前已连续运行约36小时。正式批次为`2026-11-09`，特征冻结截止仍为2026-11-10 11:00 UTC；计划发布时间17:00 UTC，准入的全部Market.endDate仍须位于`(2026-11-13 17:00, 2026-11-18 17:00]`。本轮未修改协议、配置、时间窗口或准入条件。

| 核验对象 | 实测结果 |
| --- | --- |
| 原始来源ID范围 | 1—1,350,254；13,503个完整区间 |
| 归档中的可见Event | 1,325,684；其余24,570个ID有实际404回执 |
| 修复前已处理的来源 | 52,147个：150个candidate、51,997个rejected；详细流记录尚为0 |
| 07:24 UTC实跑采样 | 已回放331,321个来源Event；07:22:51完成一次有界任务，随后继续推进，`last_error=null` |
| 先前初始批次 | `2026-11-02-bootstrap-20261108T182317Z`于11-09 12:23到期，保留`expired_incomplete`，未倒填完成 |

确认的根因是归档回放没有持久断点：每轮300秒结束后，下一轮重新从首个响应开始，原始扫描完成仍无法完成百万条来源的处理。三页输入的回归稳定复现此问题；修复后，同一来源扫描按已提交页续传。页面数据、成员标记、累计数量和回放位置在同一DuckDB事务提交，新扫描使用独立`source_pass`；失败事务不前移位置，完成前继续校验全部来源计数。

同时改用DuckDB支持的Arrow批量写入，并在回放期间复用已经完成的原始覆盖汇总。对相同505条真实元数据、10页、相同GPFS上的入库环节，耗时从1.129秒降为0.111秒；这只是该环节测量，不是整条管线或科学成绩。[DuckDB官方导入说明](https://duckdb.org/docs/current/clients/python/data_ingestion)

本周批次尚未冻结，恢复前已保留完整旧`run.json`，核对依赖锁、配置、协议及原始归档不变。新`run.json`保留完整旧实现记录、修复原因和保留对象哈希，该追溯链会进入冻结清单的`collection_implementation`；没有关闭正式批次的实现一致性检查。操作收据位于`data/operations/source-replay-20261110/implementation-recovery.json`。已过期初始批次保持原状。

最终代码通过**209项测试、Ruff、mypy及两路独立复审**。新增覆盖有界回放续传、失败事务回滚、显式刷新换代；Standards复审另核对null/空列表、精确十进制、字符串ID及Arrow关系清理。代码已同步开发仓库和公开运行仓库。

本机服务恢复后继续采集至原截止。`scheduler.json`记录本周批次于11:02:51 UTC按11:00截止关闭为`expired_incomplete`，V1清单不存在；不会再通过补采把该批次改为完整。12:07:51的调度汇总包含本周及先前初始批次，两批合计2,879个collected、1个failed、115个candidate、1,372,251个rejected；这些是两批合计状态，不能当作本周来源覆盖分母。原始HTTP归档及失败保留，后续正式周批继续。

## GitHub账户锁复测（2026-11-10）

付款信息截图仅显示空白账单资料，没有可见失败付款或待付账单。新的[最小运行42771152693](https://github.com/JiaWANG-TJ/ForeseeBench-Automation/actions/runs/42771152693)于06:50:02—06:50:05 UTC再次失败，执行步骤仍为0，注释为`The job was not started because your account is locked due to a billing issue.`。这确认了分配runner之前的平台阻塞，但未确认账户后台的具体触发原因；不能断言填写地址或增加预算就能解决。

公开仓库的标准runner仍属免费范围。对没有可处理付款问题的账户，官方路径是向GitHub支持请求复核billing lock；本项目未增加预算、绑定支付方式或启用付费执行。[免费执行规则](https://docs.github.com/en/billing/concepts/product-billing/github-actions)、[账户锁处理](https://docs.github.com/en/billing/how-tos/troubleshooting/locked-account)。本机执行期间，云端生产开关保持关闭；schedule记录显示skipped不能作为云端执行验收。

## 软件与审查

- 基线`f589910`；最终代码修复提交`3f0233e`。
- 最终完整测试：**201 passed，24.74秒**；Ruff通过，mypy的11个源模块通过，diff检查通过。
- Standards发现1项P2：实现版本变化未使导出缓存失效；已修复并独立复核关闭。
- Spec发现1项P1与1项P2：首次外部提交前未耐久保存冻结状态，以及发布路径可能跨越工作预算；均已修复并独立复核关闭。
- 冷runner回归保留原V1时间与哈希，并完成已提交但回读中断的发布；另覆盖待发布结果优先、关闭批次稀疏恢复、导出版本变化、上传／回读／重试截止与既有硬截止恢复。
- 阻塞中断实测使用localhost HTTP；原生Xet取消只核对已安装SDK源码，尚未进行真实Xet故障注入，不能混称。

## GitHub执行状态

公开运行仓库：[JiaWANG-TJ/ForeseeBench-Automation](https://github.com/JiaWANG-TJ/ForeseeBench-Automation)。完整源码、分片发布、检查点恢复、免费预算门禁及工作流已配置；生产开关`FORESEEBENCH_ENABLED=false`。

[资格运行42478848223](https://github.com/JiaWANG-TJ/ForeseeBench-Automation/actions/runs/42478848223)没有分配runner，执行步骤为0。GitHub注释明确为账户因billing issue被锁定。该结果不构成任何云端PM采集、Bucket恢复或Dataset发布资格通过。 最终代码的[再次运行42487612909](https://github.com/JiaWANG-TJ/ForeseeBench-Automation/actions/runs/42487612909)也在分配runner前因同一账户限制失败，未执行任何步骤。

私有Bucket`SII-JiaWANG/ForeseeBench-state`已创建。控制端对真实HF用量接口的只读预检通过；这不能替代GitHub上的状态保存与冷启动恢复。账户恢复后须用最终代码重新启动资格验证，并重跑同一执行验证恢复，再启用生产。

### 最小托管执行复测

针对失败通知，新增只允许手动触发的`runner-check.yml`：不用checkout、第三方action、项目代码、依赖、密钥或数据，只检查执行环境并输出`FORESEEBENCH_RUNNER_OK`。

[运行42503711841](https://github.com/JiaWANG-TJ/ForeseeBench-Automation/actions/runs/42503711841)于2026-11-08 17:11:30—17:11:32 UTC失败；runner名称为空，执行步骤为0，GitHub再次返回同一billing lock注释。`gh run watch 42503711841 --repo JiaWANG-TJ/ForeseeBench-Automation --exit-status`返回1，未通过就绪验证。

同时核验仓库为public、未禁用或归档，Actions启用且允许action，工作流active，无自托管runner，所需Secret名称存在；6个关键运行文件与公开仓库逐字节一致。因此，这次失败发生在项目代码执行之前，不能通过修改采集逻辑解除，也不能声称业务端到端已经可用。

GitHub官方说明：失败的付款预授权也可能锁住免费Actions，即使账单页面没有有效订阅或余额。若没有可处理项，应向GitHub支持请求复核账户锁；本项目不要求为公开标准runner购买付费计划。[官方处理路径](https://docs.github.com/en/billing/how-tos/troubleshooting/locked-account)

解锁后先重跑最小就绪检查并确认标志实际出现，再执行真实PM/HF资格验证及冷启动恢复，最后启用生产。源代码修改和本地测试不能替代这三个运行步骤。

旧本机服务已按PID、启动时钟、argv和cwd核对后停止，未继续在当前资源启动新采集。

### 账户截图与容量复核

用户提供的账单截图显示GitHub Free、Actions计费为0、计算用量不足0.01美元（包含12美元），制品存储0.2/0.5GB；不能据此认定欠费或额度耗尽。自动化仓库实时API返回制品数0、缓存数0及缓存占用0字节。截图是账户汇总，不能把其中0.2GB归属于本仓库。账户锁的具体触发原因仍需GitHub核实。

收到截图后的[复测42505605260](https://github.com/JiaWANG-TJ/ForeseeBench-Automation/actions/runs/42505605260)在17:34:06—17:34:09 UTC再次失败；runner名称为空、执行步骤为0，注释仍为`The job was not started because your account is locked due to a billing issue.`。本地YAML及最小任务的Bash语法检查通过，三个工作流均未配置制品上传；这些静态结果不改变真实托管检查失败的结论。

2026-11-08 17:32:47 UTC通过生产代码的只读容量检查测得：私有仓库与Bucket可见用量共8,013,325,428字节，其中本Dataset的`used_storage`为8,011,537,619字节；main当前文件共17,749字节，Bucket为0字节。再次核验Dataset仅有main、1个提交且无标签；清理后的配额读数尚未归零，不再重复删除数据。[HF说明](https://huggingface.co/docs/hub/storage-limits#super-squash-your-repository-using-the-api)允许清理后36小时内刷新配额。

GitHub的500MB额度用于制品，runner另有14GB临时磁盘；本项目长期数据存入HF。[GitHub计费](https://docs.github.com/en/billing/concepts/product-billing/github-actions)与[runner规格](https://docs.github.com/en/actions/reference/runners/github-hosted-runners)分别说明这两种空间。HF公布免费私有存储100GB，程序使用90,000,000,000字节保守停止阈值，留出写入余量。[HF额度](https://huggingface.co/docs/hub/storage-limits)

以当前可见用量和90GB阈值计算，若全部持久文件每周净增1GB，余量约82周；若净增3GB，约27周。这是条件计算，不能把单份导出大小当作总增长率。原始响应、数据库、版本快照和四种视图都需要计量；真实runner的临时磁盘峰值及一周总增长仍未实测。恢复逻辑只先下载控制文件，批次大文件按需下载；检查点保留两代所引用的对象，全部历史快照不会因此只保留两周。

## 历史清理

| 范围 | 实际结果 |
| --- | --- |
| 本地旧数据、退役副本和临时采集目录 | 75个明确目标已删除，可见分配量79,101,971,968字节 |
| 本地项目目录 | 清理前25,037,883,904字节；清理后1,033,419,264字节（源代码集成前测量） |
| 本项目HF客户端缓存 | 57,552,896字节已清理，其他仓库缓存与凭据保留 |
| GitHub旧数据Release | 3个Release及对应标签删除，资产合计1,995,212,447字节，均回查404 |
| HF Dataset | 旧文件和validation分支移除，main压缩为1个提交，仅保留说明、空清单与等待重采状态 |

HF清理后的固定修订为[`9013314fe756e3b73e4374a6a37c30383603d6f0`](https://huggingface.co/datasets/SII-JiaWANG/ForeseeBench/tree/9013314fe756e3b73e4374a6a37c30383603d6f0)，当前main文件总量**17,749字节**。平台存储读数当时仍为25,706,311,324字节；逻辑清理和后端空间回收是不同证据，未声称物理回收已经完成。[官方刷新时限](https://huggingface.co/docs/hub/storage-limits#super-squash-your-repository-using-the-api)

两个既有未跟踪文档哈希不变，需求文档原内容和用户修改保留为小型记录后按新协议修订；代码历史、模型权重和认证配置保留。没有重新复制大体积备份。

清理发生在GitHub账户锁定期间：旧写入者已经停止，用户明确授权退役的历史数据先完成清理。HF清楚显示等待重采，没有把清理完成称为新数据完成。

## 模型接口

指定全局路径的DeepSeek-V4.1-Flash-1017在“公共科研项目”临时部署，4卡H200、80核CPU、900GiB内存，vLLM 0.31.0镜像。第一轮因`python`命令不存在失败并删除；修正为保留镜像环境、使用python3模块入口后，第二轮完成加载与推理。

成功服务：`foreseebench-dsv41-probe-20261108-121153`。模型列表返回指定ID；一次性控制端调用实测如下：

| 用例 | 真实最终输出 | 请求耗时 |
| --- | --- | ---: |
| 普通文本 | `323` | 0.18秒 |
| JSON Schema | `{"product":323}` | 0.653秒 |
| 思考模式＋JSON | `{"product":323}`，并有非空reasoning | 0.333秒 |

这些是短接口探测，不是吞吐、长上下文或科学任务成绩。调用来自当前控制端，推理运行在远端启智GPU；GitHub来源的调用因账户锁定尚未执行。测试后立即请求下线，并复查服务为`STOPPED`、实例为终止状态。原服务配置保留，不占用运行中的GPU。

V2/V4业务处理器仍未配置，未生成QA、QA真值或用户评测分数。API Key未写入源码、日志或数据输出。

## 可检查记录

本地精简记录集中在`data/operations/cloud-reset-20261108/`：清理清单及前后测量、HF/GitHub删除回执、账户阻塞注释、模型创建与实际输出、下线回执、原需求修改保护记录。工程与未来周期结果分开，下一轮真实数据须完全从当前API重新获取。
