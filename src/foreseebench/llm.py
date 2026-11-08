"""独立的OpenAI兼容推理接口；不实现V2/V4业务处理器。"""

from __future__ import annotations

import argparse
import json
import os
import time
from pathlib import Path
from typing import Any, NoReturn

import httpx
from jsonschema import Draft202012Validator

from .contract import Record, dumps
from .store import atomic_json


def reject_nonfinite(value: str) -> NoReturn:
    raise ValueError(f"nonfinite_json_constant:{value}")


class LLMClient:
    """复用HTTPX与官方JSON Schema校验，拒绝空输出或被截断的答案。"""

    def __init__(
        self,
        base_url: str,
        api_key: str,
        model: str,
        *,
        timeout_seconds: float = 120,
        transport: httpx.BaseTransport | None = None,
    ):
        if not api_key or not model or timeout_seconds <= 0:
            raise ValueError("llm_configuration_missing_or_invalid")
        base = httpx.URL(base_url.rstrip("/"))
        if base.scheme not in {"http", "https"} or not base.host or base.query or base.fragment or base.userinfo:
            raise ValueError("llm_base_url_invalid")
        if not base.path.rstrip("/").endswith("/v1"):
            base = base.copy_with(path=base.path.rstrip("/") + "/v1")
        self.model = model
        self.http = httpx.Client(
            base_url=str(base) + "/",
            headers={"Authorization": f"Bearer {api_key}"},
            timeout=timeout_seconds,
            transport=transport,
        )

    def __enter__(self) -> LLMClient:
        return self

    def __exit__(self, *_: Any) -> None:
        self.http.close()

    def complete(
        self,
        messages: list[Record],
        *,
        schema: Record | None = None,
        thinking: bool = False,
        reasoning_effort: int = 75,
        max_tokens: int = 1024,
    ) -> Record:
        if not messages or max_tokens < 1 or not 0 <= reasoning_effort <= 100:
            raise ValueError("llm_request_invalid")
        payload: Record = {
            "model": self.model,
            "messages": messages,
            "temperature": 0,
            "max_tokens": max_tokens,
            "chat_template_kwargs": {"thinking": thinking, "reasoning_effort": reasoning_effort},
        }
        if schema is not None:
            Draft202012Validator.check_schema(schema)
            payload["response_format"] = {
                "type": "json_schema",
                "json_schema": {"name": "foreseebench_response", "strict": True, "schema": schema},
            }
        response = self.http.post("chat/completions", json=payload)
        response.raise_for_status()
        value = response.json()
        if value.get("model") != self.model or len(value.get("choices", [])) != 1:
            raise ValueError("llm_response_identity_or_choices_invalid")
        choice = value["choices"][0]
        if choice.get("finish_reason") != "stop":
            raise ValueError("llm_response_incomplete")
        message = choice.get("message", {})
        content = message.get("content")
        if not isinstance(content, str) or not content.strip():
            raise ValueError("llm_final_content_missing")
        parsed = json.loads(content, parse_constant=reject_nonfinite) if schema is not None else None
        if schema is not None:
            Draft202012Validator(schema).validate(parsed)
        return {
            "id": value.get("id"),
            "model": value["model"],
            "content": content,
            "parsed": parsed,
            "reasoning": message.get("reasoning"),
            "finish_reason": choice["finish_reason"],
            "usage": value.get("usage"),
        }


def probe(client: LLMClient) -> Record:
    """实际请求验证接口；结果不进入生产题目或模型能力评测。"""
    root = client.http.base_url.copy_with(path=client.http.base_url.path.removesuffix("v1/"))
    client.http.get(str(root) + "health").raise_for_status()
    models_response = client.http.get("models")
    models_response.raise_for_status()
    identities = [row["id"] for row in models_response.json().get("data", [])]
    if client.model not in identities:
        raise ValueError("served_model_missing")
    schema: Record = {
        "type": "object",
        "properties": {"product": {"type": "integer"}},
        "required": ["product"],
        "additionalProperties": False,
    }
    cases = []
    for name, structured, thinking in (("plain", False, False), ("json", True, False), ("thinking_json", True, True)):
        prompt = "Compute 17 times 19. " + (
            'Return only JSON with the key "product" and the integer result.'
            if structured
            else "Return only the integer result."
        )
        started = time.monotonic()
        result = client.complete(
            [{"role": "user", "content": prompt}],
            schema=schema if structured else None,
            thinking=thinking,
            max_tokens=2048 if thinking else 128,
        )
        correct = result["parsed"] == {"product": 323} if structured else result["content"].strip() == "323"
        if not correct:
            raise ValueError(f"probe_answer_incorrect:{name}")
        if thinking and not result.get("reasoning"):
            raise ValueError("probe_reasoning_missing")
        cases.append({"name": name, "seconds": round(time.monotonic() - started, 3), "response": result})
    return {"model": client.model, "model_ids": identities, "cases": cases, "passed": True, "scope": "interface_only"}


def main() -> None:
    parser = argparse.ArgumentParser(description="验证启智vLLM接口，不生成正式QA。")
    parser.add_argument(
        "--base-url", default=os.environ.get("LLM_BASE_URL"), required=not os.environ.get("LLM_BASE_URL")
    )
    parser.add_argument("--model", default=os.environ.get("LLM_MODEL", "DeepSeek-V4.1-Flash-1017"))
    parser.add_argument("--timeout", type=float, default=180)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    with LLMClient(
        args.base_url, os.environ.get("LLM_API_KEY", ""), args.model, timeout_seconds=args.timeout
    ) as client:
        report = probe(client)
    if args.output:
        atomic_json(args.output, report)
    print(dumps(report), flush=True)


if __name__ == "__main__":
    main()
