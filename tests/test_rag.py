import json
from pathlib import Path

from congress_rag.config import ScraperConfig
from congress_rag.db import CongressDb
from congress_rag.models import ParsedSpeech
from congress_rag.rag import build_rag_jsonl, chunk_text


def make_config(tmp_path: Path) -> ScraperConfig:
    return ScraperConfig(
        data_dir=tmp_path,
        jsonl_dir=tmp_path / "jsonl",
        html_cache_dir=tmp_path / "html",
        db_path=tmp_path / "congress.db",
    )


def test_chunk_text_uses_overlap() -> None:
    chunks = chunk_text("abcdefghij", chunk_chars=4, overlap_chars=1)

    assert chunks == ["abcd", "defg", "ghij"]


def test_build_rag_jsonl_writes_embedding_documents(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    db = CongressDb(config)
    db.init_schema()
    db.upsert_speech(
        ParsedSpeech(
            slug="152872",
            url="https://lawmaker.twreporter.org/congress/a/152872",
            date="2024-05-23",
            meeting_title="邀請國家科學及技術委員會主任委員報告業務概況",
            respondents="國家科學及技術委員會主任委員",
            summary="資安警報摘要",
            transcript="葛委員如鈞：測試逐字稿第一段。主席：測試逐字稿第二段。",
            topic_slugs=["topic3-17-6"],
            topic_titles=["資安防護"],
        )
    )

    output_path = tmp_path / "rag" / "speeches.jsonl"
    result = build_rag_jsonl(
        config,
        output_path,
        chunk_chars=12,
        overlap_chars=2,
    )

    documents = [json.loads(line) for line in output_path.read_text(encoding="utf-8").splitlines()]
    assert result.source_speeches == 1
    assert result.chunks == len(documents)
    assert len(documents) > 1
    assert documents[0]["id"] == "speech:152872:chunk:0"
    assert "摘要:" in documents[0]["text"]
    assert documents[0]["metadata"]["topicTitles"] == ["資安防護"]
    assert documents[0]["metadata"]["chunkCount"] == len(documents)
