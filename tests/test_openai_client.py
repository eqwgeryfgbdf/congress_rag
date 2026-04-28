import httpx
import pytest

from congress_rag.openai_client import (
    OpenAiClientError,
    extract_output_text,
    format_openai_error,
    validate_response_payload,
)


def test_extract_output_text_uses_direct_output_text() -> None:
    assert extract_output_text({"output_text": "  hello  "}) == "hello"


def test_extract_output_text_reads_response_content_items() -> None:
    payload = {
        "output": [
            {
                "content": [
                    {"type": "output_text", "text": "第一段"},
                    {"type": "output_text", "text": "第二段"},
                ]
            }
        ]
    }

    assert extract_output_text(payload) == "第一段\n第二段"


def test_format_openai_error_reads_error_message() -> None:
    response = httpx.Response(
        status_code=401,
        json={"error": {"message": "Invalid API key"}},
    )

    assert format_openai_error(response) == "OpenAI API returned HTTP 401: Invalid API key"


def test_validate_response_payload_reports_failed_status() -> None:
    payload = {
        "id": "resp_123",
        "status": "failed",
        "error": {"message": "Model execution failed"},
    }

    with pytest.raises(OpenAiClientError, match="Model execution failed"):
        validate_response_payload(payload)


def test_validate_response_payload_reports_incomplete_reason() -> None:
    payload = {
        "id": "resp_123",
        "status": "incomplete",
        "incomplete_details": {"reason": "max_output_tokens"},
    }

    with pytest.raises(OpenAiClientError, match="max_output_tokens"):
        validate_response_payload(payload)


def test_validate_response_payload_reports_refusal() -> None:
    payload = {
        "status": "completed",
        "output": [
            {
                "content": [
                    {"type": "refusal", "refusal": "I cannot help with that request."},
                ]
            }
        ],
    }

    with pytest.raises(OpenAiClientError, match="refused"):
        validate_response_payload(payload)
