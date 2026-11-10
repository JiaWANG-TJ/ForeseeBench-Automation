# 独立Space执行与UptimeRobot监控

目标仅为`SII-JiaWANG/ForeseeBench-Automation`。其他项目的Space、数据、密钥和运行服务不参与部署。

**2026-11-10实际状态：新Space已暂停，未接管生产。** ZeroGPU创建和修正后的构建成功，但平台启动检查拒绝纯CPU应用；随后请求`cpu-basic`返回HTTP 402，要求PRO订阅。当前零新增费用约束下，这条托管路径未通过资格。UptimeRobot未申请，冷恢复、生产切换和49小时观察均未开始。详细回执见[验收记录](acceptance.md)。

## 运行结构

UptimeRobot每300秒读取公开的`/health`；Space内部的单一工作进程执行原有`foreseebench.cloud`。监控请求只读取状态，不运行另一份采集程序。数据与完成检查点仍分别保存在私有ForeseeBench Dataset和Bucket。

Space使用Gradio界面和独立Python环境。界面依赖锁定在`deploy/hf_space/requirements.txt`，管线继续使用原`uv.lock`及Python 3.12.7；两套环境隔离，界面依赖不会改变数据处理依赖。
界面锁文件包含HF构建时注入的`gradio[oauth,mcp]`、`spaces`和`websockets`，避免只解析基础Gradio而遗漏平台约束。更新时使用`uv pip compile deploy/hf_space/requirements.in --python-version 3.12.12 --no-header --no-annotate --output-file deploy/hf_space/requirements.txt`，并核验HF实际构建日志。

每轮生产工作预算2700秒，检查点宽限600秒，监督层3600秒硬超时。资格验证每轮工作600秒。工作进程结束后读取本轮独立回执；没有回执、未确认检查点或非零退出均不能显示健康。失败后按300秒间隔重试。
控制请求使用明确超时；获取和释放执行权期间不沿用旧健康状态。等待超过下一轮间隔及60秒宽限，或控制线程停止时，健康接口返回503。

单进程锁之外，Space自身的`foreseebench-runtime`分支以HF `parent_commit`竞争执行权，防止两个副本同时写入。租约覆盖任务硬超时、停止宽限和120秒余量；进程退出后才释放。进程异常消失时，新副本可能需要等待旧租约到期。控制分支仅保存执行权元数据，不保存原始数据或凭据。

## 部署与资格验证

先提交并审查源码，然后运行：

```bash
python deploy/hf_space/deploy.py --output data/operations/space/deployment.json
```

命令先验证部署包，再操作专用Space；配置变化前后确认暂停，上传固定版本代码，默认设置`FORESEEBENCH_MODE=qualification`。本应用只接受`cpu-basic`，不使用ZeroGPU；不兼容硬件在远端写入前拒绝。创建或账户资格被拒绝时停止，不购买套餐或切换到付费硬件。CPU Basic没有小时费，但HF当前要求新计算Space具备付费计划；既有Space是否有使用资格仍以账户API核验为准。[官方硬件与资格说明](https://huggingface.co/docs/hub/spaces-gpus)

必须验证实际运行的`/health`版本、免费硬件、PM访问、完整检查点上传回读和冷启动续跑。资格状态使用独立Bucket前缀，不能发布生产Dataset。实际运行记录以验收报告为准，Space创建成功不等于资格通过。

## UptimeRobot

申请前确认`/health`健康。使用官方Agent流程，不把HF令牌交给监控服务：

```bash
export UPTIMEROBOT_EMAIL='your-address@example.com'
python deploy/hf_space/monitor.py --receipt data/operations/space/monitor-request.json
```

申请只会进入`pending_owner_confirmation`，需要邮箱所有者点击确认。回执防止重复提交；网络结果不明时保留`submission_unconfirmed`，不自动重发邮件。公开页面上的访问计数只是观测线索，仍需确认真实监控已激活并发生定时请求。

每5分钟访问旨在避免闲置休眠，不能承诺HF永不维护或重启。至少观察49小时，分别报告监控请求、工作进度、重启及恢复结果。只显示HTTP页面在线不能替代管线运行证据。

## 单一写入者切换

当前批次冻结或明确结束后，使用`handoff`停止本机并先写入迁移保护，再将关闭后的完整状态上传并回读验证。随后才启用Space生产模式，在同一启动锁内补全`data/service/cloud-owner.json`的移交回执；本机`start/restart`会拒绝再次启动。GitHub生产开关保持关闭。
实际移交使用`bash scripts/service.sh handoff`：持有同一启动锁，确认监督进程和写入锁都已释放，再耐久写入`handoff_pending`标记。后续上传及云端启用期间，本机启动保持阻止；成功后在同一锁内补全移交回执。不能先启用云端、后补本机保护。

回切必须先暂停Space并确认退出，再恢复最新完成检查点，最后撤下本机迁移标记。不得清除标记后直接使用旧数据库运行。所有冻结输入、失败记录和批次截止保留原值。
