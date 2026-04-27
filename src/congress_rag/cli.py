"""Command-line interface for the congress RAG scraper."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from pathlib import Path
from typing import Annotated

import typer

from .config import ScraperConfig, ensure_data_dirs
from .db import CongressDb
from .pipeline import CongressSyncPipeline


app = typer.Typer(help="Scrape lawmaker.twreporter.org congress data for RAG.")
export_app = typer.Typer(help="Export scraped data.")
app.add_typer(export_app, name="export")


def configure_logging(verbose: bool) -> None:
    """Configure human-readable logging."""

    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


def parse_since(value: str | None) -> datetime | None:
    """Parse CLI --since values into datetimes."""

    if value is None:
        return None
    try:
        if len(value) == 10:
            return datetime.fromisoformat(f"{value}T00:00:00+00:00")
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise typer.BadParameter(
            "Expected ISO date or datetime, e.g. 2026-01-01 or 2026-01-01T00:00:00+00:00"
        ) from error


@app.command("init")
def init_db(
    verbose: Annotated[bool, typer.Option("--verbose", "-v", help="Enable debug logging.")] = False,
) -> None:
    """Create the SQLite schema and data directories."""

    configure_logging(verbose)
    config = ScraperConfig()
    ensure_data_dirs(config)
    db = CongressDb(config)
    db.init_schema()
    typer.echo(f"Initialized database at {config.db_path}")


@app.command("sync")
def sync(
    full: Annotated[bool, typer.Option("--full", help="Ignore checkpoints and refetch all speeches.")] = False,
    since: Annotated[
        str | None,
        typer.Option("--since", help="Only fetch sitemap entries modified after this ISO date/datetime."),
    ] = None,
    concurrency: Annotated[
        int,
        typer.Option("--concurrency", min=1, max=32, help="Concurrent speech page fetches."),
    ] = 8,
    request_interval: Annotated[
        float,
        typer.Option(
            "--request-interval",
            min=0.0,
            help="Minimum seconds between HTTP request starts across the whole crawler.",
        ),
    ] = 0.25,
    request_jitter: Annotated[
        float,
        typer.Option(
            "--request-jitter",
            min=0.0,
            help="Random extra cooldown seconds added to each HTTP request interval.",
        ),
    ] = 0.1,
    limit: Annotated[
        int | None,
        typer.Option("--limit", min=1, help="Limit speech pages fetched; useful for smoke tests."),
    ] = None,
    verbose: Annotated[bool, typer.Option("--verbose", "-v", help="Enable debug logging.")] = False,
) -> None:
    """Run a full or incremental scraper sync."""

    configure_logging(verbose)
    pipeline = CongressSyncPipeline(
        ScraperConfig(
            default_concurrency=concurrency,
            request_interval_seconds=request_interval,
            request_jitter_seconds=request_jitter,
        )
    )
    result = asyncio.run(
        pipeline.sync(
            full=full,
            since=parse_since(since),
            concurrency=concurrency,
            limit=limit,
        )
    )
    typer.echo(f"Sitemap entries: {result.sitemap_entries}")
    typer.echo(f"Changed speeches: {result.changed_speeches}")
    typer.echo(f"Fetched speeches: {result.fetched_speeches}")
    typer.echo(f"Failed speeches: {result.failed_speeches}")
    typer.echo(f"Max lastmod: {result.max_lastmod.isoformat() if result.max_lastmod else 'n/a'}")


@app.command("stats")
def stats(
    verbose: Annotated[bool, typer.Option("--verbose", "-v", help="Enable debug logging.")] = False,
) -> None:
    """Print database table counts and checkpoints."""

    configure_logging(verbose)
    db = CongressDb(ScraperConfig())
    db.init_schema()
    typer.echo("Table counts:")
    for table, count in db.table_counts().items():
        typer.echo(f"  {table}: {count}")

    states = db.sync_states()
    typer.echo("Sync state:")
    if not states:
        typer.echo("  (none)")
        return
    for state in states:
        notes = f" - {state['notes']}" if state.get("notes") else ""
        typer.echo(f"  {state['resource']}: {state['last_synced_at']}{notes}")


@export_app.command("jsonl")
def export_jsonl(
    out: Annotated[
        Path,
        typer.Option("--out", help="Output directory for JSONL files."),
    ] = Path("data/jsonl"),
    verbose: Annotated[bool, typer.Option("--verbose", "-v", help="Enable debug logging.")] = False,
) -> None:
    """Export database tables to JSONL files."""

    configure_logging(verbose)
    db = CongressDb(ScraperConfig())
    db.init_schema()
    paths = db.export_jsonl(out)
    for path in paths:
        typer.echo(f"Wrote {path}")


if __name__ == "__main__":
    app()
