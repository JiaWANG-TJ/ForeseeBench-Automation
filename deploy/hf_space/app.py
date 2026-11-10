"""公开健康状态；管线、数据和运行凭据留在独立子进程与私有存储。"""

from __future__ import annotations

import json
import os
import subprocess
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import gradio as gr
import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

try:
    from .runtime import HubLease, Runner, Settings
except ImportError:
    from runtime import HubLease, Runner, Settings


def status_markdown(state: dict[str, Any]) -> str:
    labels = {
        "starting": "等待首次有效检查点", "running": "正在推进任务", "waiting": "等待下一轮检查",
        "waiting_lease": "等待独占执行权", "error": "运行异常，需要检查", "stopped": "服务已停止",
        "acquiring_lease": "正在确认独占执行权", "releasing_lease": "正在确认本轮完成状态",
    }
    phase = labels.get(state["phase"], "状态未知")
    mode = "隔离资格验证" if state["mode"] == "qualification" else "正式数据管线"
    completed = state["last_completed_at"] or "尚无完成回执"
    counts = state["counts"]
    rows = "\n".join(f"| {name} | {count:,} |" for name, count in counts.items())
    statistics = f"| 状态 | Event数量 |\n|---|---:|\n{rows}" if rows else "尚无已确认的Event统计。"
    return (
        f"## {phase}\n\n运行模式：**{mode}**\n\n最近完成回执（UTC）：{completed}\n\n"
        f"## 已确认统计\n\n{statistics}\n\n批次是否完成和发布，以私有数据集的清单为准。"
    )


def create_app(runner: Runner, *, start_worker: bool = True) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        if start_worker:
            runner.start()
        try:
            yield
        finally:
            if start_worker:
                runner.close()

    app = FastAPI(lifespan=lifespan, docs_url=None, redoc_url=None, openapi_url=None)

    @app.get("/health")
    def health(request: Request) -> JSONResponse:
        if "uptimerobot" in request.headers.get("user-agent", "").lower():
            runner.observe_monitor()
        state = runner.snapshot()
        return JSONResponse(state, status_code=200 if state["healthy"] else 503, headers={"Cache-Control": "no-store"})

    with gr.Blocks(title="ForeseeBench运行状态") as page:
        gr.Markdown("# ForeseeBench\n\n原始数据自动采集与发布服务。此页面仅展示运行状态。")
        status = gr.Markdown(status_markdown(runner.snapshot()))
        refresh = gr.Button("刷新状态")
        refresh.click(lambda: status_markdown(runner.snapshot()), outputs=status, api_name=False)
        gr.Timer(30).tick(lambda: status_markdown(runner.snapshot()), outputs=status, api_name=False)
        gr.Markdown(
            "[查看数据集](https://huggingface.co/datasets/SII-JiaWANG/ForeseeBench) · "
            "[执行协议](https://github.com/JiaWANG-TJ/ForeseeBench-Automation/blob/main/docs/protocol.md)"
        )
    return gr.mount_gradio_app(
        app, page, path="/", blocked_paths=[str(runner.settings.control_dir), str(runner.settings.worker_root)],
        footer_links=[],
        head="<script>document.documentElement.lang = 'zh-CN';</script>",
        css="@media (prefers-reduced-motion: reduce) { *, *::before, *::after { "
        "animation: none !important; transition: none !important; scroll-behavior: auto !important; } }",
    )


def main() -> None:
    root = Path(__file__).resolve().parents[2]
    bundle = json.loads((root / "bundle.json").read_text())
    worker = root
    control = Path(os.environ.get("FORESEEBENCH_CONTROL_DIR", "/tmp/foreseebench-space"))
    control.mkdir(parents=True, exist_ok=True, mode=0o700)
    environment = dict(os.environ, UV_PROJECT_ENVIRONMENT=str(control / "venv"))
    subprocess.run(
        ["uv", "sync", "--frozen", "--no-dev", "--python", "3.12.7", "--directory", str(worker)],
        check=True, env=environment, timeout=600,
    )
    settings = Settings(
        worker_root=worker,
        python=str(control / "venv/bin/python"),
        control_dir=control,
        space_id=os.environ["SPACE_ID"],
        mode=os.environ.get("FORESEEBENCH_MODE", "qualification"),
        version=bundle["source_revision"],
    )
    runner = Runner(settings, HubLease(settings.space_id, settings.mode))
    uvicorn.run(create_app(runner), host="0.0.0.0", port=7860, workers=1)


if __name__ == "__main__":
    main()
