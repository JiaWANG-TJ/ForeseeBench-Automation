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
  start|stop|restart)
    exec 9>"$service_root/start.lock"
    flock --exclusive 9
    ;;
esac

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
    control shutdown
    for ((attempt=0; attempt<1000; attempt++)); do
      if [[ ! -S "$FORESEEBENCH_SOCKET" ]]; then
        exit 0
      fi
      sleep 0.1
    done
    printf '%s\n' 'Supervisor尚未完成停止，请检查本服务日志。' >&2
    exit 1
    ;;
  restart)
    control restart foreseebench
    control status foreseebench
    ;;
  logs)
    tail -n 80 "$service_root/worker.log"
    ;;
  *)
    printf '%s\n' '用法：bash scripts/service.sh {start|status|stop|restart|logs}' >&2
    exit 2
    ;;
esac
