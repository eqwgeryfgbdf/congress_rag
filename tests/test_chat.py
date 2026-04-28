import json
from pathlib import Path

import pytest

from congress_rag.chat import RagContextDocument, build_chat_input, load_rag_context, tokenize_query


def test_tokenize_query_supports_chinese_bigrams() -> None:
    assert "資安" in tokenize_query("資安議題")


def test_load_rag_context_ranks_matching_documents(tmp_path: Path) -> None:
    path = tmp_path / "speeches.jsonl"
    rows = [
        {
            "id": "speech:1:chunk:0",
            "text": "逐字稿: 這段討論交通建設。",
            "metadata": {"slug": "1", "topicTitles": ["交通"]},
        },
        {
            "id": "speech:2:chunk:0",
            "text": "逐字稿: 這段討論資安防護與資料安全。",
            "metadata": {"slug": "2", "topicTitles": ["資安防護"]},
        },
    ]
    path.write_text(
        "\n".join(json.dumps(row, ensure_ascii=False) for row in rows),
        encoding="utf-8",
    )

    documents = load_rag_context(path, question="資安有哪些討論？", top_k=1)

    assert len(documents) == 1
    assert documents[0].document_id == "speech:2:chunk:0"


def test_build_chat_input_includes_context_metadata(tmp_path: Path) -> None:
    path = tmp_path / "speeches.jsonl"
    row = {
        "id": "speech:2:chunk:0",
        "text": "日期: 2024-05-23\n逐字稿: 資安防護",
        "metadata": {"slug": "2", "date": "2024-05-23"},
    }
    document = load_rag_context_from_rows(path, [row])[0]

    chat_input = build_chat_input("請摘要", [document])

    assert "slug=2" in chat_input
    assert "使用者問題" in chat_input


def test_load_rag_context_reports_invalid_jsonl(tmp_path: Path) -> None:
    path = tmp_path / "speeches.jsonl"
    path.write_text("{not-json}", encoding="utf-8")

    with pytest.raises(ValueError, match="Invalid JSONL"):
        load_rag_context(path, question="資安", top_k=1)


def load_rag_context_from_rows(
    path: Path,
    rows: list[dict[str, object]],
) -> list[RagContextDocument]:
    path.write_text(
        "\n".join(json.dumps(row, ensure_ascii=False) for row in rows),
        encoding="utf-8",
    )
    return load_rag_context(path, question="資安", top_k=1)
