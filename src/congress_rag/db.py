"""SQLite persistence layer for congress scraper output."""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterable
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

from .config import ScraperConfig, ensure_data_dirs
from .models import Committee, Legislator, Meeting, ParsedSpeech, Session, Topic


SCHEMA_SQL = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS meetings (
  mid INTEGER PRIMARY KEY,
  term INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS sessions (
  id INTEGER PRIMARY KEY,
  mid INTEGER REFERENCES meetings(mid),
  session_term INTEGER,
  start_time TEXT,
  end_time TEXT
);

CREATE TABLE IF NOT EXISTS committees (
  slug TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  type TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS legislators (
  slug TEXT PRIMARY KEY,
  legislator_id INTEGER,
  mid INTEGER REFERENCES meetings(mid),
  name TEXT NOT NULL,
  party TEXT,
  constituency TEXT,
  type TEXT,
  image_url TEXT,
  last_modified TEXT,
  fetched_at TEXT
);

CREATE TABLE IF NOT EXISTS topics (
  slug TEXT PRIMARY KEY,
  title TEXT NOT NULL,
  speech_count INTEGER,
  legislator_count INTEGER,
  last_modified TEXT,
  fetched_at TEXT
);

CREATE TABLE IF NOT EXISTS speeches (
  slug TEXT PRIMARY KEY,
  mid INTEGER REFERENCES meetings(mid),
  legislator_slug TEXT REFERENCES legislators(slug),
  date TEXT,
  meeting_title TEXT,
  respondents TEXT,
  summary TEXT,
  transcript TEXT NOT NULL,
  ivod_url TEXT,
  last_modified TEXT,
  fetched_at TEXT
);

CREATE TABLE IF NOT EXISTS speech_topics (
  speech_slug TEXT REFERENCES speeches(slug) ON DELETE CASCADE,
  topic_slug TEXT REFERENCES topics(slug) ON DELETE CASCADE,
  PRIMARY KEY (speech_slug, topic_slug)
);

CREATE TABLE IF NOT EXISTS sync_state (
  resource TEXT PRIMARY KEY,
  last_synced_at TEXT NOT NULL,
  notes TEXT
);

CREATE INDEX IF NOT EXISTS idx_speeches_date ON speeches(date);
CREATE INDEX IF NOT EXISTS idx_speeches_last_modified ON speeches(last_modified);
CREATE INDEX IF NOT EXISTS idx_legislators_mid ON legislators(mid);
"""


def utc_now_iso() -> str:
    """Return the current UTC time in ISO-8601 format."""

    return datetime.now(timezone.utc).isoformat()


def to_iso(value: datetime | str | None) -> str | None:
    """Serialize an optional datetime or string to SQLite text."""

    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    return value


class CongressDb:
    """Small SQLite repository with explicit upsert helpers."""

    def __init__(self, config: ScraperConfig) -> None:
        self.config = config
        ensure_data_dirs(config)

    @contextmanager
    def connect(self) -> Iterator[sqlite3.Connection]:
        """Open a SQLite connection configured for row access."""

        connection = sqlite3.connect(self.config.db_path)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        try:
            yield connection
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def init_schema(self) -> None:
        """Create all database tables and indexes."""

        with self.connect() as connection:
            connection.executescript(SCHEMA_SQL)

    def upsert_meetings(self, meetings: Iterable[Meeting]) -> None:
        """Insert or update meeting rows."""

        rows = [(meeting.mid, meeting.term) for meeting in meetings]
        with self.connect() as connection:
            connection.executemany(
                """
                INSERT INTO meetings (mid, term)
                VALUES (?, ?)
                ON CONFLICT(mid) DO UPDATE SET term = excluded.term
                """,
                rows,
            )

    def upsert_sessions(self, sessions: Iterable[Session], mid: int) -> None:
        """Insert or update session rows."""

        rows = [
            (session.id, mid, session.term, session.start_time, session.end_time)
            for session in sessions
        ]
        with self.connect() as connection:
            connection.executemany(
                """
                INSERT INTO sessions (id, mid, session_term, start_time, end_time)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                  mid = excluded.mid,
                  session_term = excluded.session_term,
                  start_time = excluded.start_time,
                  end_time = excluded.end_time
                """,
                rows,
            )

    def upsert_committees(self, committees: Iterable[Committee]) -> None:
        """Insert or update committee rows."""

        rows = [(committee.slug, committee.name, committee.type) for committee in committees]
        with self.connect() as connection:
            connection.executemany(
                """
                INSERT INTO committees (slug, name, type)
                VALUES (?, ?, ?)
                ON CONFLICT(slug) DO UPDATE SET
                  name = excluded.name,
                  type = excluded.type
                """,
                rows,
            )

    def upsert_legislators(self, legislators: Iterable[Legislator], mid: int) -> None:
        """Insert or update legislator rows."""

        fetched_at = utc_now_iso()
        rows = []
        for legislator in legislators:
            party_value = None
            if legislator.party is not None:
                party_value = legislator.party.model_dump_json(by_alias=True)
            rows.append(
                (
                    legislator.slug,
                    legislator.id,
                    mid,
                    legislator.name,
                    party_value,
                    legislator.constituency,
                    legislator.type,
                    legislator.image_url,
                    to_iso(legislator.last_modified),
                    to_iso(legislator.fetched_at) or fetched_at,
                )
            )
        with self.connect() as connection:
            connection.executemany(
                """
                INSERT INTO legislators (
                  slug, legislator_id, mid, name, party, constituency, type,
                  image_url, last_modified, fetched_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(slug) DO UPDATE SET
                  legislator_id = excluded.legislator_id,
                  mid = excluded.mid,
                  name = excluded.name,
                  party = excluded.party,
                  constituency = excluded.constituency,
                  type = excluded.type,
                  image_url = excluded.image_url,
                  last_modified = COALESCE(excluded.last_modified, legislators.last_modified),
                  fetched_at = excluded.fetched_at
                """,
                rows,
            )

    def upsert_topics(self, topics: Iterable[Topic]) -> None:
        """Insert or update topic rows."""

        fetched_at = utc_now_iso()
        rows = [
            (
                topic.slug,
                topic.title,
                topic.speech_count,
                topic.legislator_count,
                to_iso(topic.last_modified),
                to_iso(topic.fetched_at) or fetched_at,
            )
            for topic in topics
        ]
        with self.connect() as connection:
            connection.executemany(
                """
                INSERT INTO topics (
                  slug, title, speech_count, legislator_count, last_modified, fetched_at
                )
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(slug) DO UPDATE SET
                  title = excluded.title,
                  speech_count = COALESCE(excluded.speech_count, topics.speech_count),
                  legislator_count = COALESCE(excluded.legislator_count, topics.legislator_count),
                  last_modified = COALESCE(excluded.last_modified, topics.last_modified),
                  fetched_at = excluded.fetched_at
                """,
                rows,
            )

    def upsert_speech(
        self,
        speech: ParsedSpeech,
        *,
        mid: int | None = None,
        legislator_slug: str | None = None,
    ) -> None:
        """Insert or update a speech and its topic links."""

        fetched_at = to_iso(speech.fetched_at) or utc_now_iso()
        with self.connect() as connection:
            connection.execute(
                """
                INSERT INTO speeches (
                  slug, mid, legislator_slug, date, meeting_title, respondents,
                  summary, transcript, ivod_url, last_modified, fetched_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(slug) DO UPDATE SET
                  mid = COALESCE(excluded.mid, speeches.mid),
                  legislator_slug = COALESCE(excluded.legislator_slug, speeches.legislator_slug),
                  date = COALESCE(excluded.date, speeches.date),
                  meeting_title = COALESCE(excluded.meeting_title, speeches.meeting_title),
                  respondents = COALESCE(excluded.respondents, speeches.respondents),
                  summary = COALESCE(excluded.summary, speeches.summary),
                  transcript = excluded.transcript,
                  ivod_url = COALESCE(excluded.ivod_url, speeches.ivod_url),
                  last_modified = COALESCE(excluded.last_modified, speeches.last_modified),
                  fetched_at = excluded.fetched_at
                """,
                (
                    speech.slug,
                    mid,
                    legislator_slug,
                    speech.date,
                    speech.meeting_title,
                    speech.respondents,
                    speech.summary,
                    speech.transcript,
                    speech.ivod_url,
                    to_iso(speech.last_modified),
                    fetched_at,
                ),
            )
            for topic_slug, title in zip(speech.topic_slugs, speech.topic_titles, strict=False):
                connection.execute(
                    """
                    INSERT INTO topics (slug, title, fetched_at)
                    VALUES (?, ?, ?)
                    ON CONFLICT(slug) DO UPDATE SET
                      title = COALESCE(excluded.title, topics.title),
                      fetched_at = excluded.fetched_at
                    """,
                    (topic_slug, title or topic_slug, fetched_at),
                )
                connection.execute(
                    """
                    INSERT OR IGNORE INTO speech_topics (speech_slug, topic_slug)
                    VALUES (?, ?)
                    """,
                    (speech.slug, topic_slug),
                )

    def set_sync_state(
        self,
        resource: str,
        last_synced_at: datetime | str,
        notes: str | None = None,
    ) -> None:
        """Persist a sync checkpoint."""

        with self.connect() as connection:
            connection.execute(
                """
                INSERT INTO sync_state (resource, last_synced_at, notes)
                VALUES (?, ?, ?)
                ON CONFLICT(resource) DO UPDATE SET
                  last_synced_at = excluded.last_synced_at,
                  notes = excluded.notes
                """,
                (resource, to_iso(last_synced_at), notes),
            )

    def get_sync_state(self, resource: str) -> datetime | None:
        """Return a sync checkpoint, if present."""

        with self.connect() as connection:
            row = connection.execute(
                "SELECT last_synced_at FROM sync_state WHERE resource = ?",
                (resource,),
            ).fetchone()
        if row is None:
            return None
        return datetime.fromisoformat(row["last_synced_at"])

    def table_counts(self) -> dict[str, int]:
        """Return counts for user-visible tables."""

        tables = [
            "meetings",
            "sessions",
            "committees",
            "legislators",
            "topics",
            "speeches",
            "speech_topics",
        ]
        with self.connect() as connection:
            return {
                table: int(connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
                for table in tables
            }

    def sync_states(self) -> list[dict[str, Any]]:
        """Return all sync checkpoint rows."""

        with self.connect() as connection:
            rows = connection.execute(
                "SELECT resource, last_synced_at, notes FROM sync_state ORDER BY resource"
            ).fetchall()
        return [dict(row) for row in rows]

    def export_jsonl(self, output_dir: Path) -> list[Path]:
        """Export core tables as JSONL files and return written paths."""

        output_dir.mkdir(parents=True, exist_ok=True)
        tables = ["meetings", "sessions", "committees", "legislators", "topics", "speeches"]
        written: list[Path] = []
        with self.connect() as connection:
            for table in tables:
                path = output_dir / f"{table}.jsonl"
                rows = connection.execute(f"SELECT * FROM {table}").fetchall()
                with path.open("w", encoding="utf-8") as file:
                    for row in rows:
                        file.write(json.dumps(dict(row), ensure_ascii=False) + "\n")
                written.append(path)
        return written
