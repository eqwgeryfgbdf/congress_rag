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

Build embedding-ready RAG chunks from the local SQLite database:

```bash
congress-rag rag build --out data/rag/speeches.jsonl
```

Smoke-test the RAG conversion with a small sample:

```bash
congress-rag rag build --limit 25 --out data/rag/sample-speeches.jsonl
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

The built-in RAG converter writes one JSON object per chunk:

```json
{
  "id": "speech:152872:chunk:0",
  "text": "日期: ...\n會議: ...\n摘要:\n...\n逐字稿:\n...",
  "metadata": {
    "source": "lawmaker.twreporter.org",
    "documentType": "speechTranscript",
    "slug": "152872",
    "url": "https://lawmaker.twreporter.org/congress/a/152872",
    "date": "2024-05-23",
    "topicSlugs": ["topic3-17-6"],
    "topicTitles": ["資安防護"],
    "chunkIndex": 0,
    "chunkCount": 3
  }
}
```

Tune chunking for your embedding model:

```bash
congress-rag rag build --chunk-chars 1800 --overlap-chars 200
```

## OpenAI CLI Chat

Set your OpenAI API key:

```bash
cp .env.example .env
```

Then edit `.env`:

```dotenv
OPENAI_API_KEY=sk-...
OPENAI_MODEL=gpt-5.5
CONGRESS_RAG_FILE=data/rag/speeches.jsonl
CONGRESS_RAG_TOP_K=5
```

You can still override these values with shell env vars or CLI flags. To use a different env file:

```bash
congress-rag chat ask "請摘要資安相關討論" --env-file .env.local
```

Ask a single question with local RAG context from `data/rag/speeches.jsonl` if it exists:

```bash
congress-rag chat ask "請根據逐字稿摘要最近有哪些資安相關討論？"
```

Read a question from stdin:

```bash
echo "有哪些委員談到災後重建預算？" | congress-rag chat ask --stdin
```

Start an interactive terminal chat:

```bash
congress-rag chat ask --interactive
```

Use a specific RAG file or disable local context:

```bash
congress-rag chat ask "請摘要這些資料" --rag-file data/rag/sample-speeches.jsonl --top-k 3
congress-rag chat ask "只用模型知識回答：什麼是 RAG？" --no-rag
```

If the model response fails, the CLI reports the failure reason from OpenAI, including
HTTP errors, non-JSON responses, empty output, `failed`, `cancelled`, `incomplete`,
or model refusal payloads.

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
