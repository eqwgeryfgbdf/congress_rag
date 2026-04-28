"""Convert scraped congress data into embedding-ready RAG documents."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .config import ScraperConfig, ensure_data_dirs
from .db import CongressDb


TOPIC_SEPARATOR = "\x1f"


@dataclass(frozen=True)
class RagBuildResult:
    """Summary returned after writing RAG JSONL documents."""

    output_path: Path
    source_speeches: int
    chunks: int


@dataclass(frozen=True)
class SpeechRow:
    """Speech and joined metadata loaded from SQLite."""

    slug: str
    date: str | None
    meeting_title: str | None
    legislator_slug: str | None
    legislator_name: str | None
    respondents: str | None
    summary: str | None
    transcript: str
    ivod_url: str | None
    last_modified: str | None
    topic_slugs: list[str]
    topic_titles: list[str]


def build_rag_jsonl(
    config: ScraperConfig,
    output_path: Path,
    *,
    chunk_chars: int = 1800,
    overlap_chars: int = 200,
    limit: int | None = None,
) -> RagBuildResult:
    """Write speech transcript chunks as RAG-friendly JSONL."""

    validate_chunk_options(chunk_chars=chunk_chars, overlap_chars=overlap_chars, limit=limit)
    ensure_data_dirs(config)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    db = CongressDb(config)
    db.init_schema()

    source_speeches = 0
    chunks = 0
    with db.connect() as connection:
        rows = connection.execute(build_speech_query(limit)).fetchall()

    with output_path.open("w", encoding="utf-8") as file:
        for row in rows:
            speech = speech_row_from_mapping(dict(row))
            transcript_chunks = chunk_text(
                speech.transcript,
                chunk_chars=chunk_chars,
                overlap_chars=overlap_chars,
            )
            source_speeches += 1
            for chunk_index, transcript_chunk in enumerate(transcript_chunks):
                document = build_rag_document(
                    speech,
                    transcript_chunk=transcript_chunk,
                    chunk_index=chunk_index,
                    chunk_count=len(transcript_chunks),
                )
                file.write(json.dumps(document, ensure_ascii=False) + "\n")
                chunks += 1

    return RagBuildResult(output_path=output_path, source_speeches=source_speeches, chunks=chunks)


def validate_chunk_options(*, chunk_chars: int, overlap_chars: int, limit: int | None) -> None:
    """Validate chunking options before reading data."""

    if chunk_chars < 1:
        raise ValueError("chunk_chars must be greater than 0.")
    if overlap_chars < 0:
        raise ValueError("overlap_chars must be 0 or greater.")
    if overlap_chars >= chunk_chars:
        raise ValueError("overlap_chars must be smaller than chunk_chars.")
    if limit is not None and limit < 1:
        raise ValueError("limit must be greater than 0 when provided.")


def build_speech_query(limit: int | None) -> str:
    """Build the SQLite query used to load speeches and related metadata."""

    limit_clause = f"LIMIT {limit}" if limit is not None else ""
    return f"""
        SELECT
          speeches.slug,
          speeches.date,
          speeches.meeting_title,
          speeches.legislator_slug,
          legislators.name AS legislator_name,
          speeches.respondents,
          speeches.summary,
          speeches.transcript,
          speeches.ivod_url,
          speeches.last_modified,
          group_concat(topics.slug, '{TOPIC_SEPARATOR}') AS topic_slugs,
          group_concat(topics.title, '{TOPIC_SEPARATOR}') AS topic_titles
        FROM speeches
        LEFT JOIN legislators ON legislators.slug = speeches.legislator_slug
        LEFT JOIN speech_topics ON speech_topics.speech_slug = speeches.slug
        LEFT JOIN topics ON topics.slug = speech_topics.topic_slug
        WHERE speeches.transcript IS NOT NULL AND trim(speeches.transcript) != ''
        GROUP BY speeches.slug
        ORDER BY speeches.date DESC, speeches.slug
        {limit_clause}
    """


def speech_row_from_mapping(row: dict[str, Any]) -> SpeechRow:
    """Convert a SQLite row mapping into a typed speech row."""

    return SpeechRow(
        slug=str(row["slug"]),
        date=row.get("date"),
        meeting_title=row.get("meeting_title"),
        legislator_slug=row.get("legislator_slug"),
        legislator_name=row.get("legislator_name"),
        respondents=row.get("respondents"),
        summary=row.get("summary"),
        transcript=str(row["transcript"]),
        ivod_url=row.get("ivod_url"),
        last_modified=row.get("last_modified"),
        topic_slugs=split_grouped_values(row.get("topic_slugs")),
        topic_titles=split_grouped_values(row.get("topic_titles")),
    )


def split_grouped_values(value: str | None) -> list[str]:
    """Split SQLite group-concatenated values."""

    if value is None or value == "":
        return []
    return [item for item in value.split(TOPIC_SEPARATOR) if item]


def chunk_text(text: str, *, chunk_chars: int, overlap_chars: int) -> list[str]:
    """Split text into overlapping character chunks."""

    normalized_text = text.strip()
    if normalized_text == "":
        return []
    if len(normalized_text) <= chunk_chars:
        return [normalized_text]

    chunks: list[str] = []
    start = 0
    step = chunk_chars - overlap_chars
    while start < len(normalized_text):
        end = min(start + chunk_chars, len(normalized_text))
        chunk = normalized_text[start:end].strip()
        if chunk:
            chunks.append(chunk)
        if end == len(normalized_text):
            break
        start += step
    return chunks


def build_rag_document(
    speech: SpeechRow,
    *,
    transcript_chunk: str,
    chunk_index: int,
    chunk_count: int,
) -> dict[str, Any]:
    """Create one embedding document with text and structured metadata."""

    text = format_document_text(speech, transcript_chunk)
    return {
        "id": f"speech:{speech.slug}:chunk:{chunk_index}",
        "text": text,
        "metadata": {
            "source": "lawmaker.twreporter.org",
            "documentType": "speechTranscript",
            "slug": speech.slug,
            "url": f"https://lawmaker.twreporter.org/congress/a/{speech.slug}",
            "date": speech.date,
            "meetingTitle": speech.meeting_title,
            "legislatorSlug": speech.legislator_slug,
            "legislatorName": speech.legislator_name,
            "respondents": speech.respondents,
            "topicSlugs": speech.topic_slugs,
            "topicTitles": speech.topic_titles,
            "ivodUrl": speech.ivod_url,
            "lastModified": speech.last_modified,
            "chunkIndex": chunk_index,
            "chunkCount": chunk_count,
        },
    }


def format_document_text(speech: SpeechRow, transcript_chunk: str) -> str:
    """Format one chunk as human-readable context for embeddings."""

    lines = [
        f"日期: {speech.date or ''}",
        f"會議: {speech.meeting_title or ''}",
        f"委員: {speech.legislator_name or speech.legislator_slug or ''}",
        f"列席質詢對象: {speech.respondents or ''}",
        f"主題: {', '.join(speech.topic_titles)}",
        "摘要:",
        speech.summary or "",
        "",
        "逐字稿:",
        transcript_chunk,
    ]
    return "\n".join(lines).strip()
