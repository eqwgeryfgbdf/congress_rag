"""Command-line interface for the congress RAG scraper."""

from __future__ import annotations

import asyncio
import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Annotated

import typer

from .chat import DEFAULT_CHAT_INSTRUCTIONS, build_chat_input, load_rag_context
from .config import ScraperConfig, ensure_data_dirs
from .db import CongressDb
from .env import get_env_path, load_env_file
from .openai_client import OpenAiClient, OpenAiClientError
from .pipeline import CongressSyncPipeline
from .rag import build_rag_jsonl


app = typer.Typer(help="Scrape lawmaker.twreporter.org congress data for RAG.")
export_app = typer.Typer(help="Export scraped data.")
rag_app = typer.Typer(help="Build embedding-ready RAG documents.")
chat_app = typer.Typer(help="Chat with OpenAI using optional local RAG context.")
app.add_typer(export_app, name="export")
app.add_typer(rag_app, name="rag")
app.add_typer(chat_app, name="chat")


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


@rag_app.command("build")
def build_rag(
    out: Annotated[
        Path,
        typer.Option("--out", help="Output JSONL path for RAG documents."),
    ] = Path("data/rag/speeches.jsonl"),
    chunk_chars: Annotated[
        int,
        typer.Option("--chunk-chars", min=1, help="Maximum transcript characters per chunk."),
    ] = 1800,
    overlap_chars: Annotated[
        int,
        typer.Option("--overlap-chars", min=0, help="Overlapping characters between chunks."),
    ] = 200,
    limit: Annotated[
        int | None,
        typer.Option("--limit", min=1, help="Limit source speeches for test runs."),
    ] = None,
    verbose: Annotated[bool, typer.Option("--verbose", "-v", help="Enable debug logging.")] = False,
) -> None:
    """Convert scraped speeches into embedding-ready JSONL chunks."""

    configure_logging(verbose)
    try:
        result = build_rag_jsonl(
            ScraperConfig(),
            out,
            chunk_chars=chunk_chars,
            overlap_chars=overlap_chars,
            limit=limit,
        )
    except ValueError as error:
        raise typer.BadParameter(str(error)) from error

    typer.echo(f"Source speeches: {result.source_speeches}")
    typer.echo(f"RAG chunks: {result.chunks}")
    typer.echo(f"Wrote {result.output_path}")


@chat_app.command("ask")
def chat_ask(
    question: Annotated[
        str | None,
        typer.Argument(help="Question to ask. Omit with --interactive for a terminal chat."),
    ] = None,
    model: Annotated[
        str | None,
        typer.Option("--model", help="OpenAI model name. Defaults to OPENAI_MODEL or gpt-5.5."),
    ] = None,
    rag_file: Annotated[
        Path | None,
        typer.Option(
            "--rag-file",
            help="Local RAG JSONL file. Defaults to CONGRESS_RAG_FILE or data/rag/speeches.jsonl.",
        ),
    ] = None,
    top_k: Annotated[
        int | None,
        typer.Option(
            "--top-k",
            min=1,
            help="Number of local RAG chunks. Defaults to CONGRESS_RAG_TOP_K or 5.",
        ),
    ] = None,
    no_rag: Annotated[
        bool,
        typer.Option("--no-rag", help="Ask OpenAI without loading local RAG context."),
    ] = False,
    stdin: Annotated[
        bool,
        typer.Option("--stdin", help="Read the question from stdin."),
    ] = False,
    interactive: Annotated[
        bool,
        typer.Option("--interactive", "-i", help="Start a terminal chat loop."),
    ] = False,
    instructions: Annotated[
        str | None,
        typer.Option(
            "--instructions",
            help="System instructions. Defaults to OPENAI_INSTRUCTIONS or the built-in prompt.",
        ),
    ] = None,
    env_file: Annotated[
        Path | None,
        typer.Option("--env-file", help="Env file path. Defaults to CONGRESS_RAG_ENV_FILE or .env."),
    ] = None,
    verbose: Annotated[bool, typer.Option("--verbose", "-v", help="Enable debug logging.")] = False,
) -> None:
    """Ask OpenAI a question, optionally grounded with local RAG JSONL chunks."""

    configure_logging(verbose)
    try:
        load_env_file(env_file or get_env_path())
    except ValueError as error:
        raise typer.BadParameter(str(error)) from error

    api_key = os.environ.get("OPENAI_API_KEY")
    if api_key is None or api_key.strip() == "":
        raise typer.BadParameter(
            "OPENAI_API_KEY is required. Add it to .env or export OPENAI_API_KEY='sk-...'."
        )
    resolved_model = resolve_env_text(model, env_name="OPENAI_MODEL", default_value="gpt-5.5")
    resolved_rag_file = resolve_env_path(
        rag_file,
        env_name="CONGRESS_RAG_FILE",
        default_value=Path("data/rag/speeches.jsonl"),
    )
    resolved_top_k = resolve_env_positive_int(top_k, env_name="CONGRESS_RAG_TOP_K", default_value=5)
    resolved_instructions = resolve_env_text(
        instructions,
        env_name="OPENAI_INSTRUCTIONS",
        default_value=DEFAULT_CHAT_INSTRUCTIONS,
    )

    client = OpenAiClient(api_key=api_key)
    try:
        if interactive:
            run_interactive_chat(
                client=client,
                model=resolved_model,
                instructions=resolved_instructions,
                rag_file=resolved_rag_file,
                top_k=resolved_top_k,
                no_rag=no_rag,
            )
            return

        resolved_question = resolve_question(question=question, stdin=stdin)
        answer = ask_openai_with_optional_context(
            client=client,
            model=resolved_model,
            instructions=resolved_instructions,
            question=resolved_question,
            rag_file=resolved_rag_file,
            top_k=resolved_top_k,
            no_rag=no_rag,
        )
    except (FileNotFoundError, ValueError, OpenAiClientError) as error:
        raise typer.BadParameter(str(error)) from error

    typer.echo(answer)


def resolve_env_text(value: str | None, *, env_name: str, default_value: str) -> str:
    """Resolve a text option from CLI, env, or a default."""

    if value is not None and value.strip() != "":
        return value
    env_value = os.environ.get(env_name)
    if env_value is not None and env_value.strip() != "":
        return env_value
    return default_value


def resolve_env_path(value: Path | None, *, env_name: str, default_value: Path) -> Path:
    """Resolve a path option from CLI, env, or a default."""

    if value is not None:
        return value
    env_value = os.environ.get(env_name)
    if env_value is not None and env_value.strip() != "":
        return Path(env_value)
    return default_value


def resolve_env_positive_int(value: int | None, *, env_name: str, default_value: int) -> int:
    """Resolve a positive integer from CLI, env, or a default."""

    if value is not None:
        return value
    env_value = os.environ.get(env_name)
    if env_value is None or env_value.strip() == "":
        return default_value
    try:
        resolved_value = int(env_value)
    except ValueError as error:
        raise ValueError(f"{env_name} must be an integer.") from error
    if resolved_value < 1:
        raise ValueError(f"{env_name} must be greater than 0.")
    return resolved_value


def resolve_question(*, question: str | None, stdin: bool) -> str:
    """Resolve a question from an argument or stdin."""

    if stdin:
        stdin_text = sys.stdin.read().strip()
        if stdin_text == "":
            raise ValueError("No question received on stdin.")
        return stdin_text
    if question is None or question.strip() == "":
        raise ValueError(
            "Question is required. Example: congress-rag chat ask '這些逐字稿談到哪些資安議題？'"
        )
    return question.strip()


def run_interactive_chat(
    *,
    client: OpenAiClient,
    model: str,
    instructions: str,
    rag_file: Path,
    top_k: int,
    no_rag: bool,
) -> None:
    """Run a terminal chat loop."""

    typer.echo("Start chatting. Type /exit to quit.")
    while True:
        question = typer.prompt("You").strip()
        if question in {"/exit", "/quit"}:
            return
        if question == "":
            continue
        try:
            answer = ask_openai_with_optional_context(
                client=client,
                model=model,
                instructions=instructions,
                question=question,
                rag_file=rag_file,
                top_k=top_k,
                no_rag=no_rag,
            )
        except (FileNotFoundError, ValueError, OpenAiClientError) as error:
            typer.echo(f"Error: {error}", err=True)
            continue
        typer.echo(f"Assistant: {answer}")


def ask_openai_with_optional_context(
    *,
    client: OpenAiClient,
    model: str,
    instructions: str,
    question: str,
    rag_file: Path,
    top_k: int,
    no_rag: bool,
) -> str:
    """Ask OpenAI with optional context loaded from local RAG JSONL."""

    context_documents = []
    if not no_rag and rag_file.exists():
        context_documents = load_rag_context(rag_file, question=question, top_k=top_k)
    input_text = build_chat_input(question, context_documents)
    response = client.create_response(
        model=model,
        instructions=instructions,
        input_text=input_text,
    )
    return response.output_text


if __name__ == "__main__":
    app()
