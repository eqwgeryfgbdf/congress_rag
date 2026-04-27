"""Typed models for congress metadata, sitemap entries, and speeches."""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, HttpUrl, field_validator


class UrlKind(str, Enum):
    """URL categories available in the public sitemap."""

    SPEECH = "speech"
    TOPIC = "topic"
    LEGISLATOR = "legislator"
    OTHER = "other"


class Meeting(BaseModel):
    """Legislative Yuan meeting term metadata."""

    model_config = ConfigDict(coerce_numbers_to_str=True)

    mid: int = Field(alias="id")
    term: int


class Session(BaseModel):
    """A session inside one Legislative Yuan term."""

    model_config = ConfigDict(coerce_numbers_to_str=True)

    id: int
    mid: int | None = None
    term: int = Field(description="Session term number within the meeting term.")
    start_time: str = Field(alias="startTime")
    end_time: str = Field(alias="endTime")


class Committee(BaseModel):
    """Committee metadata."""

    name: str
    slug: str
    type: Literal["standing", "ad-hoc"] | str


class PartyImageFile(BaseModel):
    """Nested party image file payload from the API."""

    url: str | None = None


class PartyImage(BaseModel):
    """Nested party image payload from the API."""

    image_file: PartyImageFile | None = Field(default=None, alias="imageFile")


class Party(BaseModel):
    """Party payload from the API."""

    image: PartyImage | None = None
    image_link: str | None = Field(default=None, alias="imageLink")


class LegislatorIdentity(BaseModel):
    """Nested legislator identity from the API."""

    slug: str
    image_link: str | None = Field(default=None, alias="imageLink")
    name: str


class Legislator(BaseModel):
    """A lawmaker in a specific meeting term."""

    model_config = ConfigDict(coerce_numbers_to_str=True)

    id: int
    mid: int | None = None
    legislator: LegislatorIdentity
    constituency: str | None = ""
    type: str | None = None
    party: Party | None = None
    tooltip: str | None = ""
    note: str | None = ""
    last_modified: datetime | None = None
    fetched_at: datetime | None = None

    @property
    def slug(self) -> str:
        """Return the stable lawmaker slug."""

        return self.legislator.slug

    @property
    def name(self) -> str:
        """Return the display name."""

        return self.legislator.name

    @property
    def image_url(self) -> str | None:
        """Return the public portrait URL if present."""

        return self.legislator.image_link


class TopicLegislator(BaseModel):
    """Top legislator data shown under a topic."""

    id: int
    count: int
    name: str
    image_link: str | None = Field(default=None, alias="imageLink")
    slug: str
    party: int | str | None = None


class Topic(BaseModel):
    """Topic metadata from the topic API or sitemap."""

    slug: str
    title: str
    speech_count: int | None = Field(default=None, alias="speechCount")
    legislator_count: int | None = Field(default=None, alias="legislatorCount")
    legislators: list[TopicLegislator] = Field(default_factory=list)
    last_modified: datetime | None = None
    fetched_at: datetime | None = None


class LegislatorTopic(BaseModel):
    """Topic list entry for a single lawmaker."""

    slug: str
    name: str
    title: str | None = None
    count: int
    speeches_count: int | None = Field(default=None, alias="speechesCount")


class SpeechListItem(BaseModel):
    """Speech listing returned by the public API."""

    date: str
    slug: str
    summary_fallback: str | None = Field(default=None, alias="summaryFallback")
    title: str


class ParsedSpeech(BaseModel):
    """Speech data parsed from a transcript page."""

    slug: str
    url: str
    title: str | None = None
    date: str | None = None
    meeting_title: str | None = None
    respondents: str | None = None
    summary: str | None = None
    transcript: str
    ivod_url: str | None = None
    topic_slugs: list[str] = Field(default_factory=list)
    topic_titles: list[str] = Field(default_factory=list)
    last_modified: datetime | None = None
    fetched_at: datetime | None = None


class SitemapEntry(BaseModel):
    """A single URL entry from the public sitemap."""

    loc: HttpUrl
    lastmod: datetime | None = None
    kind: UrlKind = UrlKind.OTHER
    slug: str | None = None

    @field_validator("kind", mode="before")
    @classmethod
    def validate_kind(cls, value: Any) -> UrlKind:
        """Coerce raw sitemap kind values into UrlKind."""

        if isinstance(value, UrlKind):
            return value
        try:
            return UrlKind(str(value))
        except ValueError:
            return UrlKind.OTHER


class SyncState(BaseModel):
    """Stored checkpoint for a sync resource."""

    resource: str
    last_synced_at: datetime
    notes: str | None = None
