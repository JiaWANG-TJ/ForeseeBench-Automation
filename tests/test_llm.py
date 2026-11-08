import json

import httpx
import pytest

from foreseebench.llm import LLMClient


def test_structured_completion_keeps_final_content_separate_from_reasoning():
    def upstream(request):
        assert request.url.path == "/gateway/v1/chat/completions"
        assert request.headers["Authorization"] == "Bearer test-only-key"
        body = json.loads(request.content)
        assert body["chat_template_kwargs"] == {"thinking": True, "reasoning_effort": 75}
        assert body["response_format"]["json_schema"]["strict"] is True
        return httpx.Response(
            200,
            json={
                "id": "probe-1",
                "model": "test-model",
                "choices": [
                    {
                        "finish_reason": "stop",
                        "message": {"content": '{"product":323}', "reasoning": "Compute the product."},
                    }
                ],
                "usage": {"completion_tokens": 24},
            },
        )

    schema = {
        "type": "object",
        "properties": {"product": {"type": "integer"}},
        "required": ["product"],
        "additionalProperties": False,
    }
    with LLMClient(
        "https://model.example/gateway/v1", "test-only-key", "test-model", transport=httpx.MockTransport(upstream)
    ) as client:
        result = client.complete([{"role": "user", "content": "17 * 19?"}], schema=schema, thinking=True)
    assert result["parsed"] == {"product": 323}
    assert result["content"] == '{"product":323}'
    assert result["reasoning"] == "Compute the product."
    assert result["model"] == "test-model"


def test_probe_requires_real_content_in_plain_json_and_thinking_modes():
    from foreseebench.llm import probe

    def upstream(request):
        if request.url.path == "/health":
            return httpx.Response(200, text="ok")
        if request.url.path == "/v1/models":
            return httpx.Response(200, json={"data": [{"id": "test-model"}]})
        body = json.loads(request.content)
        structured = "response_format" in body
        return httpx.Response(
            200,
            json={
                "model": "test-model",
                "choices": [
                    {
                        "finish_reason": "stop",
                        "message": {
                            "content": '{"product":323}' if structured else "323",
                            "reasoning": "17 times 19" if body["chat_template_kwargs"]["thinking"] else None,
                        },
                    }
                ],
            },
        )

    with LLMClient(
        "https://model.example", "test-key", "test-model", transport=httpx.MockTransport(upstream)
    ) as client:
        report = probe(client)
    assert report["passed"] is True
    assert [case["name"] for case in report["cases"]] == ["plain", "json", "thinking_json"]


def test_nonfinite_json_is_not_a_valid_structured_answer():
    def upstream(request):
        return httpx.Response(
            200,
            json={
                "model": "test-model",
                "choices": [
                    {
                        "finish_reason": "stop",
                        "message": {"content": '{"confidence":NaN}'},
                    }
                ],
            },
        )

    with LLMClient(
        "https://model.example", "test-key", "test-model", transport=httpx.MockTransport(upstream)
    ) as client:
        with pytest.raises(ValueError, match="nonfinite"):
            client.complete([{"role": "user", "content": "test"}], schema={"type": "object"})


@pytest.mark.parametrize(
    "finish,content,model,expected",
    [
        ("length", "323", "test-model", "incomplete"),
        ("stop", "", "test-model", "missing"),
        ("stop", "323", "another-model", "identity"),
    ],
)
def test_incomplete_or_wrong_model_responses_are_failures(finish, content, model, expected):
    def upstream(request):
        return httpx.Response(
            200,
            json={
                "model": model,
                "choices": [
                    {
                        "finish_reason": finish,
                        "message": {"content": content, "reasoning": "unfinished"},
                    }
                ],
            },
        )

    with LLMClient(
        "https://model.example", "test-key", "test-model", transport=httpx.MockTransport(upstream)
    ) as client:
        with pytest.raises(ValueError, match=expected):
            client.complete([{"role": "user", "content": "test"}])
