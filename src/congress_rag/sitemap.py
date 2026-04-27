"""Sitemap download, parsing, and timestamp filtering."""

from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime
from io import BytesIO
from urllib.parse import urlparse

from lxml import etree

from .config import ScraperConfig
from .http_client import CongressHttpClient
from .models import SitemapEntry, UrlKind


SITEMAP_NS = "{http://www.sitemaps.org/schemas/sitemap/0.9}"


def classify_url(loc: str) -> tuple[UrlKind, str | None]:
    """Classify a sitemap URL and return its kind plus slug."""

    path = urlparse(loc).path.rstrip("/")
    if path.startswith("/congress/a/"):
        return UrlKind.SPEECH, path.rsplit("/", maxsplit=1)[-1]
    if path.startswith("/congress/topic/"):
        return UrlKind.TOPIC, path.rsplit("/", maxsplit=1)[-1]
    if path.startswith("/congress/lawmaker/"):
        return UrlKind.LEGISLATOR, path.rsplit("/", maxsplit=1)[-1]
    return UrlKind.OTHER, None


def parse_sitemap_bytes(xml_bytes: bytes) -> list[SitemapEntry]:
    """Parse a sitemap XML document into typed entries."""

    entries: list[SitemapEntry] = []
    context = etree.iterparse(BytesIO(xml_bytes), events=("end",), tag=f"{SITEMAP_NS}url")
    for _, element in context:
        loc_node = element.find(f"{SITEMAP_NS}loc")
        lastmod_node = element.find(f"{SITEMAP_NS}lastmod")
        if loc_node is None or not loc_node.text:
            element.clear()
            continue
        loc = loc_node.text.strip()
        kind, slug = classify_url(loc)
        lastmod = None
        if lastmod_node is not None and lastmod_node.text:
            lastmod = datetime.fromisoformat(lastmod_node.text.strip().replace("Z", "+00:00"))
        entries.append(SitemapEntry(loc=loc, lastmod=lastmod, kind=kind, slug=slug))
        element.clear()
    return entries


def filter_entries(
    entries: Iterable[SitemapEntry],
    *,
    since: datetime | None = None,
    kinds: set[UrlKind] | None = None,
) -> list[SitemapEntry]:
    """Filter sitemap entries by kind and lastmod cutoff."""

    filtered: list[SitemapEntry] = []
    for entry in entries:
        if kinds is not None and entry.kind not in kinds:
            continue
        if since is not None:
            if entry.lastmod is None or entry.lastmod <= since:
                continue
        filtered.append(entry)
    return filtered


async def fetch_sitemap_entries(
    client: CongressHttpClient,
    config: ScraperConfig,
) -> list[SitemapEntry]:
    """Download and parse the public sitemap."""

    sitemap_bytes = await client.get_bytes(f"{config.base_url}/sitemap.xml")
    return parse_sitemap_bytes(sitemap_bytes)
