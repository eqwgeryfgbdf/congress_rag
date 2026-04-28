import os
from pathlib import Path

import pytest

from congress_rag.env import load_env_file, parse_env_line


def test_load_env_file_sets_values_without_overriding_existing_env(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    env_path = tmp_path / ".env"
    env_path.write_text(
        "\n".join(
            [
                "# comment",
                "OPENAI_API_KEY=sk-from-file",
                'OPENAI_MODEL="gpt-5.5"',
                "CONGRESS_RAG_TOP_K=3 # inline comment",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("OPENAI_API_KEY", "sk-existing")

    loaded = load_env_file(env_path)

    assert loaded["OPENAI_API_KEY"] == "sk-from-file"
    assert os.environ["OPENAI_API_KEY"] == "sk-existing"
    assert os.environ["OPENAI_MODEL"] == "gpt-5.5"
    assert os.environ["CONGRESS_RAG_TOP_K"] == "3"


def test_parse_env_line_rejects_invalid_lines() -> None:
    with pytest.raises(ValueError, match="Expected KEY=VALUE"):
        parse_env_line("OPENAI_API_KEY", path=Path(".env"), line_number=1)
