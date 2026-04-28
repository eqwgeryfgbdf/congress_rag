"""Load local environment variables for CLI commands."""

from __future__ import annotations

import os
from pathlib import Path


def load_env_file(path: Path) -> dict[str, str]:
    """Load KEY=VALUE pairs from a .env file without overriding existing env vars."""

    if not path.exists():
        return {}

    loaded: dict[str, str] = {}
    with path.open("r", encoding="utf-8") as file:
        for line_number, raw_line in enumerate(file, start=1):
            key, value = parse_env_line(raw_line, path=path, line_number=line_number)
            if key is None:
                continue
            loaded[key] = value
            os.environ.setdefault(key, value)
    return loaded


def parse_env_line(raw_line: str, *, path: Path, line_number: int) -> tuple[str | None, str]:
    """Parse one dotenv-style line."""

    line = raw_line.strip()
    if line == "" or line.startswith("#"):
        return None, ""
    if line.startswith("export "):
        line = line.removeprefix("export ").strip()
    if "=" not in line:
        raise ValueError(f"Invalid env line at {path}:{line_number}. Expected KEY=VALUE.")

    key, raw_value = line.split("=", 1)
    key = key.strip()
    if key == "":
        raise ValueError(f"Invalid env line at {path}:{line_number}. Missing key.")
    if not key.replace("_", "").isalnum() or key[0].isdigit():
        raise ValueError(f"Invalid env key at {path}:{line_number}: {key}")

    return key, strip_env_value(raw_value.strip())


def strip_env_value(value: str) -> str:
    """Strip optional quotes and inline comments from an env value."""

    if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
        return value[1:-1]
    if " #" in value:
        return value.split(" #", 1)[0].rstrip()
    return value


def get_env_path(default_path: Path = Path(".env")) -> Path:
    """Return the env file path configured for this process."""

    override = os.environ.get("CONGRESS_RAG_ENV_FILE")
    if override is None or override.strip() == "":
        return default_path
    return Path(override)
