#!/usr/bin/env bash
set -euo pipefail
umask 077

FORESEEBENCH_ROOT=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
export FORESEEBENCH_ROOT
service_root="$FORESEEBENCH_ROOT/data/service"
mkdir -p "$service_root"
if [[ -f "$service_root/environment.sh" ]]; then
  # 只读取本部署在共享目录中保存的环境配置，文件不进入Git或数据发布。
  source "$service_root/environment.sh"
fi
service_id=$(printf '%s' "$FORESEEBENCH_ROOT" | sha256sum | cut -c1-12)
export FORESEEBENCH_SOCKET="/tmp/foreseebench-${UID}-${service_id}.sock"
supervisor_config="$FORESEEBENCH_ROOT/scripts/supervisord.conf"
action="${1:-status}"

control() {
  "$FORESEEBENCH_ROOT/.venv/bin/supervisorctl" -c "$supervisor_config" "$@"
}

case "$action" in
  start|stop|restart|handoff)
    exec 9>"$service_root/start.lock"
    flock --exclusive 9
    ;;
esac

if [[ -f "$service_root/cloud-owner.json" && ( "$action" == start || "$action" == restart ) ]]; then
  printf '%s\n' '本项目已移交云端。恢复本机前须先停止云端并恢复最新检查点。' >&2
  exit 2
fi

shutdown() {
  if control pid >/dev/null 2>&1; then
    control shutdown
  fi
  for ((attempt=0; attempt<1000; attempt++)); do
    if [[ ! -S "$FORESEEBENCH_SOCKET" ]]; then
      exec 8>"$service_root/runner.lock"
      if ! flock --exclusive --nonblock 8; then
        printf '%s\n' '本项目写入进程仍持有执行锁，不能确认停止。' >&2
        return 1
      fi
      return 0
    fi
    sleep 0.1
  done
  printf '%s\n' 'Supervisor尚未完成停止，请检查本服务日志。' >&2
  return 1
}

case "$action" in
  start)
    uv sync --locked --directory "$FORESEEBENCH_ROOT"
    if control status foreseebench >/dev/null 2>&1; then
      control status foreseebench
      exit 0
    fi
    : "${HF_BUCKET:?请设置HF_BUCKET，或写入data/service/environment.sh}"
    "$FORESEEBENCH_ROOT/.venv/bin/python" - <<'PY'
import os
from pathlib import Path

import httpx
from huggingface_hub import HfApi

from foreseebench.cli import settings
from foreseebench.cloud import StorageGuard

config = settings(Path(os.environ["FORESEEBENCH_ROOT"]) / "foreseebench.toml")
response = httpx.get(config["network"]["gamma_url"] + "/events/keyset", params={"limit": 1}, timeout=15)
response.raise_for_status()
guard = StorageGuard(HfApi(), os.environ["HF_BUCKET"])
guard.include_dataset(config["publication"]["repo_id"])
guard.reserve({})
print("PM连接与HF容量读取预检通过")
PY
    if ! control pid >/dev/null 2>&1; then
      "$FORESEEBENCH_ROOT/.venv/bin/supervisord" -c "$supervisor_config" 9>&-
    fi
    control start foreseebench
    control status foreseebench
    ;;
  status)
    control status foreseebench
    ;;
  stop)
    shutdown
    ;;
  handoff)
    shutdown
    if [[ -f "$service_root/cloud-owner.json" ]]; then
      printf '%s\n' '迁移保护已经生效，保留原有移交记录。'
      exit 0
    fi
    "$FORESEEBENCH_ROOT/.venv/bin/python" - <<'PY'
import json
import os
from datetime import datetime, timezone
from pathlib import Path

directory = Path(os.environ["FORESEEBENCH_ROOT"]) / "data/service"
path = directory / "cloud-owner.json"
temporary = path.with_suffix(".tmp")
record = {"space": "SII-JiaWANG/ForeseeBench-Automation", "phase": "handoff_pending", "created_at": datetime.now(timezone.utc).isoformat()}
with temporary.open("w") as stream:
    json.dump(record, stream)
    stream.write("\n")
    stream.flush()
    os.fsync(stream.fileno())
os.replace(temporary, path)
fd = os.open(directory, os.O_DIRECTORY)
try:
    os.fsync(fd)
finally:
    os.close(fd)
print("本机写入者已停止，迁移保护已生效。")
PY
    ;;
  restart)
    control restart foreseebench
    control status foreseebench
    ;;
  logs)
    tail -n 80 "$service_root/worker.log"
    ;;
  *)
    printf '%s\n' '用法：bash scripts/service.sh {start|status|stop|restart|handoff|logs}' >&2
    exit 2
    ;;
esac
