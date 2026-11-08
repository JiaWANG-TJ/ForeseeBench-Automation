#!/usr/bin/env bash
set -euo pipefail

# 作为启智运行命令的内容提交；不要求服务容器挂载代码仓库。
export VLLM_ENGINE_READY_TIMEOUT_S=3600
export VLLM_USE_RUST_FRONTEND=0
python3 - <<'PY'
import importlib.metadata
import json
from pathlib import Path
import subprocess
import torch

path = Path('/inspire/hdd/global_public/public_models/deepseek-ai/DeepSeek-V4.1-Flash-1017')
config = json.loads((path / 'config.json').read_text())
print({'vllm': importlib.metadata.version('vllm'), 'torch': torch.__version__,
       'cuda': torch.version.cuda, 'gpus': torch.cuda.device_count(), 'model_type': config['model_type']}, flush=True)
subprocess.run(['nvidia-smi', '--query-gpu=driver_version,name,memory.total', '--format=csv,noheader'], check=True)
assert config['model_type'] == 'deepseek_v41'
assert importlib.metadata.version('vllm').startswith('0.31.0')
assert torch.cuda.device_count() == 4
PY

exec python3 -m vllm.entrypoints.openai.api_server \
  --model /inspire/hdd/global_public/public_models/deepseek-ai/DeepSeek-V4.1-Flash-1017 \
  --served-model-name DeepSeek-V4.1-Flash-1017 \
  --host 0.0.0.0 --port "${PORT}" \
  --tokenizer-mode deepseek_v41 --tensor-parallel-size 4 \
  --engram-config '{"cpu_offload":true}' --reasoning-parser deepseek_v41 \
  --language-model-only --max-model-len 32768 --max-num-seqs 8 \
  --max-num-batched-tokens 4096 --gpu-memory-utilization 0.9
