# 云端迁移、清理与模型接口验收

日期：2026-11-08。代码、历史清理和临时模型接口验证已完成；GitHub真实执行与新批次重采仍受账户限制阻塞，不能写为完整远端运行通过。

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
