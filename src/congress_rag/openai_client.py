"""Small OpenAI Responses API client used by the CLI."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import httpx


OPENAI_RESPONSES_URL = "https://api.openai.com/v1/responses"


class OpenAiClientError(RuntimeError):
    """Raised when the OpenAI API request cannot be completed."""


@dataclass(frozen=True)
class OpenAiResponse:
    """Text response returned by OpenAI."""

    response_id: str | None
    output_text: str


class OpenAiClient:
    """Minimal HTTP client for OpenAI's Responses API."""

    def __init__(
        self,
        *,
        api_key: str,
        timeout_seconds: float = 60.0,
        responses_url: str = OPENAI_RESPONSES_URL,
    ) -> None:
        if api_key.strip() == "":
            raise OpenAiClientError("OPENAI_API_KEY is empty.")
        self.api_key = api_key
        self.timeout_seconds = timeout_seconds
        self.responses_url = responses_url

    def create_response(
        self,
        *,
        model: str,
        instructions: str,
        input_text: str,
        previous_response_id: str | None = None,
    ) -> OpenAiResponse:
        """Create a text response with the Responses API."""

        body: dict[str, Any] = {
            "model": model,
            "instructions": instructions,
            "input": input_text,
            "store": False,
        }
        if previous_response_id is not None:
            body["previous_response_id"] = previous_response_id

        try:
            with httpx.Client(timeout=self.timeout_seconds) as client:
                response = client.post(
                    self.responses_url,
                    headers={
                        "Authorization": f"Bearer {self.api_key}",
                        "Content-Type": "application/json",
                    },
                    json=body,
                )
        except httpx.HTTPError as error:
            raise OpenAiClientError(f"OpenAI request failed: {error}") from error

        if response.status_code >= 400:
            raise OpenAiClientError(format_openai_error(response))

        try:
            payload = response.json()
        except ValueError as error:
            raise OpenAiClientError("OpenAI returned a non-JSON response.") from error

        validate_response_payload(payload)
        output_text = extract_output_text(payload)
        if output_text == "":
            raise OpenAiClientError("OpenAI response did not contain output text.")

        response_id = payload.get("id")
        return OpenAiResponse(
            response_id=response_id if isinstance(response_id, str) else None,
            output_text=output_text,
        )


def validate_response_payload(payload: dict[str, Any]) -> None:
    """Raise a descriptive error when a Responses API payload indicates failure."""

    status = payload.get("status")
    if isinstance(status, str) and status in {"failed", "cancelled", "incomplete"}:
        message = extract_response_failure_message(payload)
        raise OpenAiClientError(f"OpenAI response {status}: {message}")

    refusal = extract_refusal(payload)
    if refusal is not None:
        raise OpenAiClientError(f"OpenAI refused to answer: {refusal}")


def extract_response_failure_message(payload: dict[str, Any]) -> str:
    """Extract the most useful failure detail from a Responses API payload."""

    error_payload = payload.get("error")
    if isinstance(error_payload, dict):
        message = error_payload.get("message")
        if isinstance(message, str) and message.strip() != "":
            return message.strip()
        code = error_payload.get("code")
        if isinstance(code, str) and code.strip() != "":
            return code.strip()

    incomplete_details = payload.get("incomplete_details")
    if isinstance(incomplete_details, dict):
        reason = incomplete_details.get("reason")
        if isinstance(reason, str) and reason.strip() != "":
            return reason.strip()

    response_id = payload.get("id")
    if isinstance(response_id, str) and response_id.strip() != "":
        return f"response_id={response_id}"
    return "No failure details were provided."


def extract_refusal(payload: dict[str, Any]) -> str | None:
    """Extract model refusal text from a Responses API payload if present."""

    output_items = payload.get("output")
    if not isinstance(output_items, list):
        return None

    refusals: list[str] = []
    for output_item in output_items:
        if not isinstance(output_item, dict):
            continue
        content_items = output_item.get("content")
        if not isinstance(content_items, list):
            continue
        for content_item in content_items:
            if not isinstance(content_item, dict):
                continue
            refusal = content_item.get("refusal")
            if isinstance(refusal, str) and refusal.strip() != "":
                refusals.append(refusal.strip())
    if refusals:
        return "\n".join(refusals)
    return None


def extract_output_text(payload: dict[str, Any]) -> str:
    """Extract concatenated text from a Responses API payload."""

    direct_text = payload.get("output_text")
    if isinstance(direct_text, str) and direct_text.strip() != "":
        return direct_text.strip()

    output_items = payload.get("output")
    if not isinstance(output_items, list):
        return ""

    texts: list[str] = []
    for output_item in output_items:
        if not isinstance(output_item, dict):
            continue
        content_items = output_item.get("content")
        if not isinstance(content_items, list):
            continue
        for content_item in content_items:
            if not isinstance(content_item, dict):
                continue
            text = content_item.get("text")
            if isinstance(text, str) and text.strip() != "":
                texts.append(text.strip())
    return "\n".join(texts).strip()


def format_openai_error(response: httpx.Response) -> str:
    """Format an actionable error message from an OpenAI HTTP response."""

    try:
        payload = response.json()
    except ValueError:
        return f"OpenAI API returned HTTP {response.status_code}: {response.text}"

    error_payload = payload.get("error")
    if isinstance(error_payload, dict):
        message = error_payload.get("message")
        if isinstance(message, str) and message.strip() != "":
            return f"OpenAI API returned HTTP {response.status_code}: {message}"

    return f"OpenAI API returned HTTP {response.status_code}: {payload}"
