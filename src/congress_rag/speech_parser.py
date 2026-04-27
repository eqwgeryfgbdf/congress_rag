"""Extract speech transcript data from Next.js SSR HTML."""

from __future__ import annotations

import html
import json
import re
from datetime import datetime
from urllib.parse import urlparse

from .models import ParsedSpeech


PUSH_RE = re.compile(r'self\.__next_f\.push\((\[\s*1\s*,\s*"(?:[^"\\]|\\.)*"\s*\])\)')
TITLE_RE = re.compile(r"<title>(.*?)</title>", re.DOTALL)
DATE_RE = re.compile(r"(20\d{2}/\d{1,2}/\d{1,2}|20\d{2}-\d{1,2}-\d{1,2})")
SUMMARY_RE = re.compile(r'"summary"\s*:\s*"((?:[^"\\]|\\.)*)"')
TRANSCRIPT_START_RE = re.compile(
    r"(?:^|\\n|\n|,|\")([\u4e00-\u9fffA-Za-z‧．・·\s]{1,30}"
    r"(?:委員|主席|院長|部長|主任委員)[^：\\\n]{0,20}：)"
)
TOPIC_LINK_RE = re.compile(r'href="/congress/topic/([^"?/#]+)[^"]*"[^>]*>\s*#?([^<]+)<')
TOPIC_SLUG_RE = re.compile(r"/congress/topic/(topic[\w-]+)")
IVOD_RE = re.compile(r"https?://[^\"'<>\s]*ivod[^\"'<>\s]*", re.IGNORECASE)
RESPONDENTS_RE = re.compile(r"列席質詢對象[／/]\s*(.*?)(?:\\n|\n|<|\")")


class SpeechParseError(ValueError):
    """Raised when the speech HTML cannot be parsed."""


def extract_rsc_text(html_text: str) -> str:
    """Concatenate decoded string chunks from Next.js Flight payloads."""

    chunks: list[str] = []
    for raw_payload in PUSH_RE.findall(html_text):
        try:
            payload = json.loads(raw_payload)
        except json.JSONDecodeError as error:
            raise SpeechParseError("Invalid Next.js Flight chunk in speech HTML") from error
        if len(payload) == 2 and isinstance(payload[1], str):
            chunks.append(payload[1])
    return "".join(chunks)


def clean_text(value: str | None) -> str | None:
    """HTML-unescape and normalize whitespace."""

    if value is None:
        return None
    cleaned = html.unescape(value)
    cleaned = re.sub(r"<[^>]+>", "", cleaned)
    cleaned = cleaned.replace("\\n", "\n")
    cleaned = re.sub(r"[ \t\r\f\v]+", " ", cleaned)
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)
    return cleaned.strip() or None


def extract_title(html_text: str) -> str | None:
    """Extract the document title."""

    match = TITLE_RE.search(html_text)
    if not match:
        return None
    return clean_text(match.group(1))


def extract_summary(rsc_text: str, html_text: str) -> str | None:
    """Extract AI summary text from RSC data or HTML metadata."""

    match = SUMMARY_RE.search(rsc_text)
    if match:
        try:
            return clean_text(json.loads(f'"{match.group(1)}"'))
        except json.JSONDecodeError:
            return clean_text(match.group(1))

    meta_match = re.search(r'<meta name="description" content="([^"]+)"', html_text)
    if meta_match:
        return clean_text(meta_match.group(1))
    return None


def extract_transcript(rsc_text: str) -> str:
    """Extract the full transcript from decoded RSC text."""

    match = TRANSCRIPT_START_RE.search(rsc_text)
    if not match:
        raise SpeechParseError("Could not locate transcript speaker line in speech HTML")

    transcript = rsc_text[match.start(1) :]
    # Flight data after the transcript often resumes with component references.
    stop_markers = [
        '\n1d:["$"',
        '\n1e:["$"',
        '\n1f:["$"',
        '\n20:["$"',
        '\\n1d:["$"',
        '\\n1e:["$"',
        '\\n1f:["$"',
        '\\n20:["$"',
        '\n:null',
        '\n["$"',
    ]
    stop_positions = [transcript.find(marker) for marker in stop_markers if transcript.find(marker) > 0]
    if stop_positions:
        transcript = transcript[: min(stop_positions)]

    cleaned = clean_text(transcript)
    if not cleaned:
        raise SpeechParseError("Transcript was empty after cleanup")
    return cleaned


def extract_topic_data(html_text: str) -> tuple[list[str], list[str]]:
    """Extract topic slugs and display labels from page links."""

    slug_to_title: dict[str, str] = {}
    for slug, title in TOPIC_LINK_RE.findall(html_text):
        slug_to_title.setdefault(slug, clean_text(title) or slug)
    for slug in TOPIC_SLUG_RE.findall(html_text):
        slug_to_title.setdefault(slug, slug)
    return list(slug_to_title.keys()), list(slug_to_title.values())


def extract_date(rsc_text: str, html_text: str) -> str | None:
    """Extract a speech date from RSC text or visible HTML."""

    match = DATE_RE.search(rsc_text) or DATE_RE.search(html_text)
    if not match:
        return None
    value = match.group(1).replace("/", "-")
    year, month, day = value.split("-")
    return f"{int(year):04d}-{int(month):02d}-{int(day):02d}"


def extract_ivod_url(html_text: str) -> str | None:
    """Extract an iVOD URL if the page embeds one."""

    match = IVOD_RE.search(html_text)
    return html.unescape(match.group(0)) if match else None


def extract_respondents(rsc_text: str, html_text: str) -> str | None:
    """Extract respondent text shown on the speech page."""

    match = RESPONDENTS_RE.search(rsc_text) or RESPONDENTS_RE.search(html_text)
    return clean_text(match.group(1)) if match else None


def parse_speech_html(
    *,
    slug: str,
    url: str,
    html_text: str,
    last_modified: datetime | None = None,
    fetched_at: datetime | None = None,
) -> ParsedSpeech:
    """Parse a speech page into a normalized data model."""

    rsc_text = extract_rsc_text(html_text)
    if not rsc_text:
        raise SpeechParseError(f"No Next.js Flight chunks found for speech {slug}")

    topic_slugs, topic_titles = extract_topic_data(html_text)
    path_slug = urlparse(url).path.rstrip("/").rsplit("/", maxsplit=1)[-1]
    speech_slug = slug or path_slug
    return ParsedSpeech(
        slug=speech_slug,
        url=url,
        title=extract_title(html_text),
        date=extract_date(rsc_text, html_text),
        meeting_title=None,
        respondents=extract_respondents(rsc_text, html_text),
        summary=extract_summary(rsc_text, html_text),
        transcript=extract_transcript(rsc_text),
        ivod_url=extract_ivod_url(html_text),
        topic_slugs=topic_slugs,
        topic_titles=topic_titles,
        last_modified=last_modified,
        fetched_at=fetched_at,
    )
