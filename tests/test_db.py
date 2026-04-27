from pathlib import Path

from congress_rag.config import ScraperConfig
from congress_rag.db import CongressDb
from congress_rag.models import Meeting, ParsedSpeech


def test_db_upserts_are_idempotent(tmp_path: Path) -> None:
    config = ScraperConfig(
        data_dir=tmp_path,
        jsonl_dir=tmp_path / "jsonl",
        html_cache_dir=tmp_path / "html",
        db_path=tmp_path / "congress.db",
    )
    db = CongressDb(config)
    db.init_schema()

    db.upsert_meetings([Meeting.model_validate({"id": "2", "term": 11})])
    db.upsert_meetings([Meeting.model_validate({"id": "2", "term": 11})])

    speech = ParsedSpeech(
        slug="152872",
        url="https://lawmaker.twreporter.org/congress/a/152872",
        date="2024-05-23",
        meeting_title="邀請國家科學及技術委員會主任委員報告業務概況",
        summary="摘要",
        transcript="葛委員如鈞：測試逐字稿。",
        topic_slugs=["topic3-17-6"],
        topic_titles=["資安防護"],
    )
    db.upsert_speech(speech, mid=2, legislator_slug=None)
    db.upsert_speech(speech, mid=2, legislator_slug=None)

    counts = db.table_counts()
    assert counts["meetings"] == 1
    assert counts["speeches"] == 1
    assert counts["speech_topics"] == 1
