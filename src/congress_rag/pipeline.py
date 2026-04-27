"""End-to-end metadata and speech synchronization pipeline."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Iterable

from tqdm import tqdm

from .api_client import CongressApiClient
from .config import ScraperConfig
from .db import CongressDb, utc_now_iso
from .http_client import CongressHttpClient, FetchError, NotFoundError, congress_http_client
from .models import Legislator, Meeting, ParsedSpeech, SitemapEntry, Topic, UrlKind
from .sitemap import fetch_sitemap_entries, filter_entries
from .speech_parser import SpeechParseError, parse_speech_html


logger = logging.getLogger(__name__)


@dataclass
class SpeechMetadata:
    """Metadata discovered from structured API speech lists."""

    slug: str
    mid: int
    legislator_slug: str
    date: str | None = None
    meeting_title: str | None = None
    summary: str | None = None
    topic_slugs: set[str] = field(default_factory=set)


@dataclass
class SyncResult:
    """Summary of a sync run."""

    sitemap_entries: int
    changed_speeches: int
    fetched_speeches: int
    failed_speeches: int
    max_lastmod: datetime | None


def now_utc() -> datetime:
    """Return timezone-aware UTC now."""

    return datetime.now(timezone.utc)


def choose_cutoff(
    *,
    db: CongressDb,
    full: bool,
    since: datetime | None,
) -> datetime | None:
    """Choose the timestamp cutoff for incremental sync."""

    if full:
        return None
    if since is not None:
        return since
    return db.get_sync_state("sitemap")


def enrich_with_metadata(
    speech: ParsedSpeech,
    metadata: SpeechMetadata | None,
) -> ParsedSpeech:
    """Merge structured API metadata into a parsed speech."""

    if metadata is None:
        return speech
    topic_slugs = list(dict.fromkeys([*speech.topic_slugs, *sorted(metadata.topic_slugs)]))
    return speech.model_copy(
        update={
            "date": speech.date or metadata.date,
            "meeting_title": metadata.meeting_title or speech.meeting_title,
            "summary": speech.summary or metadata.summary,
            "topic_slugs": topic_slugs,
            "topic_titles": speech.topic_titles
            if speech.topic_titles
            else [topic_slug for topic_slug in topic_slugs],
        }
    )


class CongressSyncPipeline:
    """Coordinates API metadata refreshes and transcript crawls."""

    def __init__(self, config: ScraperConfig | None = None) -> None:
        self.config = config or ScraperConfig()
        self.db = CongressDb(self.config)

    async def sync_metadata(
        self,
        api_client: CongressApiClient,
    ) -> tuple[list[Meeting], dict[int, list[Legislator]]]:
        """Refresh meetings, sessions, committees, legislators, and topics."""

        meetings = await api_client.meetings()
        self.db.upsert_meetings(meetings)

        committees = await api_client.committees()
        self.db.upsert_committees(committees)

        legislators_by_mid: dict[int, list[Legislator]] = {}
        for meeting in meetings:
            sessions = await api_client.sessions(meeting.term)
            self.db.upsert_sessions(sessions, meeting.mid)

            legislators = await api_client.legislators(meeting.mid)
            legislators_by_mid[meeting.mid] = legislators
            self.db.upsert_legislators(legislators, meeting.mid)

            topics = await api_client.topics(meeting.mid)
            self.db.upsert_topics(topics)

        self.db.set_sync_state("metadata", utc_now_iso(), "Metadata refreshed from public API")
        return meetings, legislators_by_mid

    async def discover_speech_metadata(
        self,
        api_client: CongressApiClient,
        meetings: Iterable[Meeting],
        legislators_by_mid: dict[int, list[Legislator]],
    ) -> dict[str, SpeechMetadata]:
        """Build speech metadata by walking lawmaker/topic speech-list endpoints."""

        by_slug: dict[str, SpeechMetadata] = {}
        for meeting in meetings:
            legislators = legislators_by_mid.get(meeting.mid, [])
            for legislator in tqdm(legislators, desc=f"Discover term {meeting.term} speech lists"):
                try:
                    topics = await api_client.legislator_topics(
                        legislator.slug,
                        meeting_term=meeting.term,
                        top=None,
                    )
                except FetchError as error:
                    logger.warning(
                        "Could not fetch topics for legislator %s term %s: %s",
                        legislator.slug,
                        meeting.term,
                        error,
                    )
                    continue

                for topic in topics:
                    try:
                        speeches = await api_client.speech_list_for_legislator_topic(
                            legislator.slug,
                            topic.slug,
                            mid=meeting.mid,
                        )
                    except FetchError as error:
                        logger.warning(
                            "Could not fetch speech list for %s/%s: %s",
                            legislator.slug,
                            topic.slug,
                            error,
                        )
                        continue

                    for item in speeches:
                        metadata = by_slug.setdefault(
                            item.slug,
                            SpeechMetadata(
                                slug=item.slug,
                                mid=meeting.mid,
                                legislator_slug=legislator.slug,
                            ),
                        )
                        metadata.date = metadata.date or item.date
                        metadata.meeting_title = metadata.meeting_title or item.title
                        metadata.summary = metadata.summary or item.summary_fallback
                        metadata.topic_slugs.add(topic.slug)
        return by_slug

    async def fetch_and_store_speech(
        self,
        client: CongressHttpClient,
        entry: SitemapEntry,
        metadata_by_slug: dict[str, SpeechMetadata],
    ) -> bool:
        """Fetch one speech page, parse it, and upsert it."""

        if not entry.slug:
            logger.error("Skipping speech sitemap entry without slug: %s", entry.loc)
            return False

        url = str(entry.loc)
        try:
            html_text = await client.get_text(url)
            parsed = parse_speech_html(
                slug=entry.slug,
                url=url,
                html_text=html_text,
                last_modified=entry.lastmod,
                fetched_at=now_utc(),
            )
            metadata = metadata_by_slug.get(entry.slug)
            enriched = enrich_with_metadata(parsed, metadata)
            self.db.upsert_speech(
                enriched,
                mid=metadata.mid if metadata else None,
                legislator_slug=metadata.legislator_slug if metadata else None,
            )
            return True
        except NotFoundError as error:
            logger.warning("Speech page disappeared: %s", error)
            return False
        except (FetchError, SpeechParseError) as error:
            logger.error("Failed to process speech %s: %s", entry.slug, error)
            return False

    async def sync_speeches(
        self,
        client: CongressHttpClient,
        entries: list[SitemapEntry],
        metadata_by_slug: dict[str, SpeechMetadata],
        *,
        concurrency: int,
    ) -> tuple[int, int]:
        """Fetch and store speech entries concurrently."""

        semaphore = asyncio.Semaphore(concurrency)
        fetched = 0
        failed = 0

        async def worker(entry: SitemapEntry) -> bool:
            async with semaphore:
                return await self.fetch_and_store_speech(client, entry, metadata_by_slug)

        tasks = [worker(entry) for entry in entries]
        for future in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="Fetch speeches"):
            ok = await future
            if ok:
                fetched += 1
            else:
                failed += 1
            if self.config.batch_sleep_seconds > 0:
                await asyncio.sleep(self.config.batch_sleep_seconds)
        return fetched, failed

    async def sync(
        self,
        *,
        full: bool = False,
        since: datetime | None = None,
        concurrency: int | None = None,
        limit: int | None = None,
    ) -> SyncResult:
        """Run a full or incremental synchronization."""

        self.db.init_schema()
        max_concurrency = concurrency or self.config.default_concurrency

        async with congress_http_client(self.config) as http_client:
            api_client = CongressApiClient(http_client, self.config)
            sitemap_entries = await fetch_sitemap_entries(http_client, self.config)
            self.db.upsert_topics(
                Topic(
                    slug=entry.slug,
                    title=entry.slug,
                    last_modified=entry.lastmod,
                    fetched_at=now_utc(),
                )
                for entry in sitemap_entries
                if entry.kind == UrlKind.TOPIC and entry.slug is not None
            )
            cutoff = choose_cutoff(db=self.db, full=full, since=since)

            changed_speeches = filter_entries(
                sitemap_entries,
                since=cutoff,
                kinds={UrlKind.SPEECH},
            )
            changed_speeches.sort(key=lambda entry: entry.lastmod or datetime.min.replace(tzinfo=timezone.utc))
            if limit is not None:
                changed_speeches = changed_speeches[:limit]

            meetings, legislators_by_mid = await self.sync_metadata(api_client)
            # The transcript pages already include date, summary, topics, and full text.
            # Avoid walking every lawmaker/topic speech-list endpoint during normal sync;
            # that path is available through discover_speech_metadata() for future enrichment.
            metadata_by_slug: dict[str, SpeechMetadata] = {}

            fetched, failed = await self.sync_speeches(
                http_client,
                changed_speeches,
                metadata_by_slug,
                concurrency=max_concurrency,
            )

        max_lastmod = max(
            (entry.lastmod for entry in changed_speeches if entry.lastmod is not None),
            default=None,
        )
        if max_lastmod is not None and failed == 0:
            self.db.set_sync_state("sitemap", max_lastmod, "Last completed sitemap delta sync")
        elif failed > 0:
            logger.warning("Checkpoint not advanced because %s speech fetches failed", failed)

        return SyncResult(
            sitemap_entries=len(sitemap_entries),
            changed_speeches=len(changed_speeches),
            fetched_speeches=fetched,
            failed_speeches=failed,
            max_lastmod=max_lastmod,
        )
