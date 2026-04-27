"""Configuration defaults for the congress RAG scraper."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


PACKAGE_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PACKAGE_ROOT / "data"
JSONL_DIR = DATA_DIR / "jsonl"
HTML_CACHE_DIR = DATA_DIR / "html"
DB_PATH = DATA_DIR / "congress.db"


@dataclass(frozen=True)
class ScraperConfig:
    """Runtime configuration for crawling and persistence."""

    base_url: str = "https://lawmaker.twreporter.org"
    user_agent: str = "congress-rag/0.1 (+https://lawmaker.twreporter.org/congress)"
    timeout_seconds: float = 30.0
    default_concurrency: int = 8
    request_interval_seconds: float = 0.25
    request_jitter_seconds: float = 0.1
    batch_sleep_seconds: float = 0.05
    data_dir: Path = DATA_DIR
    jsonl_dir: Path = JSONL_DIR
    html_cache_dir: Path = HTML_CACHE_DIR
    db_path: Path = DB_PATH


def ensure_data_dirs(config: ScraperConfig) -> None:
    """Create data directories used by the scraper."""

    config.data_dir.mkdir(parents=True, exist_ok=True)
    config.jsonl_dir.mkdir(parents=True, exist_ok=True)
    config.html_cache_dir.mkdir(parents=True, exist_ok=True)
