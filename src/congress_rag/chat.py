"""CLI chat helpers for asking OpenAI questions with optional local RAG context."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any


DEFAULT_CHAT_INSTRUCTIONS = """你是立法院逐字稿資料助理。
請優先根據提供的本機 RAG 內容回答；如果內容不足，請明確說明資料不足，不要假裝知道。
回答使用繁體中文，並在適合時引用會議日期、委員、主題或逐字稿 slug。"""


@dataclass(frozen=True)
class RagContextDocument:
    """One searchable RAG document loaded from JSONL."""

    document_id: str
    text: str
    metadata: dict[str, Any]


def build_chat_input(question: str, context_documents: list[RagContextDocument]) -> str:
    """Build the user input sent to OpenAI."""

    if not context_documents:
        return question

    context_blocks = []
    for index, document in enumerate(context_documents, start=1):
        metadata = document.metadata
        slug = metadata.get("slug", "")
        date = metadata.get("date", "")
        context_blocks.append(
            "\n".join(
                [
                    f"[資料 {index}] id={document.document_id} slug={slug} date={date}",
                    document.text,
                ]
            )
        )

    return "\n\n".join(
        [
            "以下是從本機 RAG JSONL 檢索到的資料：",
            "\n\n---\n\n".join(context_blocks),
            "使用者問題：",
            question,
        ]
    )


def load_rag_context(path: Path, *, question: str, top_k: int) -> list[RagContextDocument]:
    """Load and rank local RAG JSONL documents for a question."""

    if top_k < 1:
        raise ValueError("top_k must be greater than 0.")
    if not path.exists():
        raise FileNotFoundError(
            f"RAG context file not found: {path}. Run `congress-rag rag build --out {path}` first."
        )

    documents: list[RagContextDocument] = []
    with path.open("r", encoding="utf-8") as file:
        for line_number, line in enumerate(file, start=1):
            line = line.strip()
            if line == "":
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError as error:
                raise ValueError(f"Invalid JSONL at {path}:{line_number}: {error}") from error
            documents.append(document_from_payload(payload, path=path, line_number=line_number))

    ranked = sorted(
        documents,
        key=lambda document: score_document(question, document),
        reverse=True,
    )
    return [document for document in ranked[:top_k] if score_document(question, document) > 0]


def document_from_payload(payload: Any, *, path: Path, line_number: int) -> RagContextDocument:
    """Validate and convert one JSONL payload into a context document."""

    if not isinstance(payload, dict):
        raise ValueError(f"Expected JSON object at {path}:{line_number}.")

    document_id = payload.get("id")
    text = payload.get("text")
    metadata = payload.get("metadata", {})
    if not isinstance(document_id, str) or document_id.strip() == "":
        raise ValueError(f"Missing string id at {path}:{line_number}.")
    if not isinstance(text, str) or text.strip() == "":
        raise ValueError(f"Missing string text at {path}:{line_number}.")
    if not isinstance(metadata, dict):
        raise ValueError(f"Expected metadata object at {path}:{line_number}.")

    return RagContextDocument(document_id=document_id, text=text, metadata=metadata)


def score_document(question: str, document: RagContextDocument) -> int:
    """Score a context document using simple local lexical matching."""

    haystack = json.dumps(
        {
            "text": document.text,
            "metadata": document.metadata,
        },
        ensure_ascii=False,
    ).lower()
    score = 0
    for token in tokenize_query(question):
        if token in haystack:
            score += max(1, len(token))
    return score


def tokenize_query(question: str) -> list[str]:
    """Tokenize mixed Chinese and alphanumeric query text for lightweight retrieval."""

    normalized = question.lower()
    tokens = [token for token in re.findall(r"[a-z0-9_]{2,}", normalized)]
    cjk_chars = re.findall(r"[\u4e00-\u9fff]", normalized)
    tokens.extend(
        "".join(cjk_chars[index : index + 2])
        for index in range(0, max(0, len(cjk_chars) - 1))
    )
    if len(cjk_chars) == 1:
        tokens.append(cjk_chars[0])
    return list(dict.fromkeys(token for token in tokens if token.strip() != ""))
