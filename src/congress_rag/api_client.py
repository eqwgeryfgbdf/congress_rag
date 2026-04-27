"""Typed wrappers around lawmaker.twreporter.org public JSON API."""

from __future__ import annotations

from urllib.parse import quote

from .config import ScraperConfig
from .http_client import CongressHttpClient, FetchError
from .models import Committee, Legislator, LegislatorTopic, Meeting, Session, SpeechListItem, Topic


class CongressApiClient:
    """Client for structured public API endpoints."""

    def __init__(self, http_client: CongressHttpClient, config: ScraperConfig) -> None:
        self.http_client = http_client
        self.config = config

    async def _data_list(self, path: str) -> list[dict]:
        payload = await self.http_client.get_json(path)
        status = payload.get("status")
        if status != "success":
            raise FetchError(f"API returned non-success status for {path}: {payload}")
        data = payload.get("data")
        if not isinstance(data, list):
            raise FetchError(f"API returned invalid data list for {path}: {payload}")
        return data

    async def meetings(self) -> list[Meeting]:
        """Fetch available Legislative Yuan terms."""

        rows = await self._data_list("/api/legislative-meeting")
        return [Meeting.model_validate(row) for row in rows]

    async def sessions(self, term: int) -> list[Session]:
        """Fetch sessions for a meeting term number."""

        rows = await self._data_list(f"/api/legislative-meeting/{term}/session")
        return [Session.model_validate(row) for row in rows]

    async def committees(self) -> list[Committee]:
        """Fetch committee metadata."""

        rows = await self._data_list("/api/committee")
        return [Committee.model_validate(row) for row in rows]

    async def legislators(self, mid: int) -> list[Legislator]:
        """Fetch legislators for a meeting ID."""

        rows = await self._data_list(f"/api/legislator?mid={mid}")
        return [Legislator.model_validate(row) for row in rows]

    async def topics(self, mid: int) -> list[Topic]:
        """Fetch topics for a meeting ID."""

        rows = await self._data_list(f"/api/topic?mid={mid}")
        return [Topic.model_validate(row) for row in rows]

    async def legislator_topics(
        self,
        legislator_slug: str,
        *,
        meeting_term: int,
        top: int | None = None,
        session_terms: list[int] | None = None,
    ) -> list[LegislatorTopic]:
        """Fetch a lawmaker's topics in a meeting term."""

        safe_slug = quote(legislator_slug)
        url = f"/api/legislator/{safe_slug}/topic?key=term&mt={meeting_term}"
        if session_terms:
            sessions = ",".join(str(term) for term in session_terms)
            url += f"&sts={sessions}"
        if top is not None:
            url += f"&top={top}"
        rows = await self._data_list(url)
        return [LegislatorTopic.model_validate(row) for row in rows]

    async def speech_list_for_legislator_topic(
        self,
        legislator_slug: str,
        topic_slug: str,
        *,
        mid: int,
        session_ids: list[int] | None = None,
    ) -> list[SpeechListItem]:
        """Fetch speech list for a lawmaker/topic pair."""

        safe_legislator_slug = quote(legislator_slug)
        safe_topic_slug = quote(topic_slug)
        url = (
            f"/api/legislator/{safe_legislator_slug}/topic/"
            f"{safe_topic_slug}/speech?mid={mid}"
        )
        if session_ids:
            url += "&sids=" + ",".join(str(session_id) for session_id in session_ids)
        rows = await self._data_list(url)
        return [SpeechListItem.model_validate(row) for row in rows]
