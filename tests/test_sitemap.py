from datetime import datetime, timezone

from congress_rag.models import UrlKind
from congress_rag.sitemap import filter_entries, parse_sitemap_bytes


def test_parse_sitemap_classifies_public_urls() -> None:
    xml = b"""<?xml version="1.0" encoding="UTF-8"?>
    <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
      <url>
        <loc>https://lawmaker.twreporter.org/congress/a/152872</loc>
        <lastmod>2026-01-01T00:00:00.000Z</lastmod>
      </url>
      <url>
        <loc>https://lawmaker.twreporter.org/congress/topic/topic3-3-10</loc>
        <lastmod>2025-09-01T00:00:00.000Z</lastmod>
      </url>
    </urlset>
    """

    entries = parse_sitemap_bytes(xml)

    assert len(entries) == 2
    assert entries[0].kind == UrlKind.SPEECH
    assert entries[0].slug == "152872"
    assert entries[1].kind == UrlKind.TOPIC
    assert entries[1].slug == "topic3-3-10"


def test_filter_entries_by_kind_and_timestamp() -> None:
    entries = parse_sitemap_bytes(
        b"""<?xml version="1.0" encoding="UTF-8"?>
        <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
          <url><loc>https://lawmaker.twreporter.org/congress/a/1</loc><lastmod>2025-01-01T00:00:00.000Z</lastmod></url>
          <url><loc>https://lawmaker.twreporter.org/congress/a/2</loc><lastmod>2026-01-01T00:00:00.000Z</lastmod></url>
          <url><loc>https://lawmaker.twreporter.org/about</loc><lastmod>2026-01-01T00:00:00.000Z</lastmod></url>
        </urlset>
        """
    )

    filtered = filter_entries(
        entries,
        since=datetime(2025, 12, 31, tzinfo=timezone.utc),
        kinds={UrlKind.SPEECH},
    )

    assert [entry.slug for entry in filtered] == ["2"]
