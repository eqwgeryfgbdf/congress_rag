# Congress RAG Scraper

Python scraper for `https://lawmaker.twreporter.org/congress`.

It builds a local SQLite + JSONL knowledge base for RAG workflows:

- Legislative Yuan meetings and sessions
- Committees
- Legislators
- Topics
- Speech transcript pages (`/congress/a/{slug}`)
- Speech-to-topic relationships
- Timestamp checkpoints for incremental updates

The crawler uses the public `sitemap.xml` as the source of truth for changed URLs and public JSON endpoints for structured metadata.

## Install

```bash
python3.11 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

## Quick Start

Initialize the database:

```bash
congress-rag init
```

Smoke-test a small sync:

```bash
congress-rag sync --full --limit 25 --concurrency 4 --request-interval 0.25
```

Run a full bootstrap:

```bash
congress-rag sync --full --concurrency 8 --request-interval 0.25 --request-jitter 0.1
```

Run a normal incremental sync:

```bash
congress-rag sync
```

Manually sync from a date:

```bash
congress-rag sync --since 2026-01-01
```

Inspect counts and checkpoints:

```bash
congress-rag stats
```

Export JSONL for downstream embedding:

```bash
congress-rag export jsonl --out data/jsonl
```

## Traffic Protection

The HTTP client has a shared cooldown lock, so every request start is spaced out even when transcript fetching is concurrent.

Defaults:

- `--concurrency 8`: up to 8 transcript tasks can be in flight.
- `--request-interval 0.25`: wait at least 250 ms between request starts globally.
- `--request-jitter 0.1`: add up to 100 ms random delay to avoid a perfectly fixed rhythm.

For a gentler crawl:

```bash
congress-rag sync --full --concurrency 3 --request-interval 0.75 --request-jitter 0.25
```

For daily incremental updates, the defaults should be conservative because only changed sitemap entries are fetched.

## Incremental Update Model

The site exposes `https://lawmaker.twreporter.org/sitemap.xml`, where each speech URL has a `<lastmod>` timestamp.

The scraper stores the latest completed timestamp in `sync_state`:

1. Download `sitemap.xml`.
2. Keep `/congress/a/{slug}` entries where `lastmod > sync_state.last_synced_at`.
3. Re-fetch only those transcript pages.
4. Upsert by speech `slug`.
5. Advance the checkpoint only if every changed speech is processed successfully.

This makes daily updates cheap after the first bootstrap.

## Storage

The SQLite database lives at `data/congress.db`.

Core tables:

- `meetings`: Legislative Yuan terms.
- `sessions`: Sessions within each term.
- `committees`: Committee metadata.
- `legislators`: Lawmakers by stable slug.
- `topics`: Topic metadata.
- `speeches`: One row per transcript, including full `transcript` text.
- `speech_topics`: Many-to-many relation between speeches and topics.
- `sync_state`: Checkpoints for metadata and sitemap sync.

## RAG Handoff

For a simple embedding job, read from SQLite:

```sql
SELECT
  slug,
  date,
  meeting_title,
  summary,
  transcript
FROM speeches
WHERE transcript IS NOT NULL;
```

A typical document text can concatenate:

```text
日期: {date}
會議: {meeting_title}
摘要:
{summary}

逐字稿:
{transcript}
```

Metadata to keep with each vector:

- `slug`
- `date`
- `meeting_title`
- `legislator_slug`
- topic slugs from `speech_topics`

## Daily Cron Example

```cron
15 3 * * * cd /path/to/congress_rag && . .venv/bin/activate && congress-rag sync >> data/sync.log 2>&1
```

## Development

Run tests:

```bash
pytest
```

The scraper logs every failed fetch or parse with the affected URL. It retries network and 5xx failures with exponential backoff, but it does not silently ignore client errors.
