import json

from congress_rag.speech_parser import parse_speech_html


def make_push_chunk(text: str) -> str:
    payload = json.dumps([1, text], ensure_ascii=False)
    return f"<script>self.__next_f.push({payload})</script>"


def test_parse_speech_html_extracts_transcript_and_metadata() -> None:
    html = f"""
    <html>
      <head>
        <title>逐字稿｜葛如鈞：邀請國家科學及技術委員會主任委... - 報導者觀測站</title>
        <meta name="description" content="葛如鈞質疑警報事件。" />
      </head>
      <body>
        <a href="/congress/topic/topic3-17-6">#資安防護</a>
        {make_push_chunk(
            '12:{{"summary":"第一點摘要\\n第二點摘要"}}\\n'
            '2024/5/23\\n'
            '列席質詢對象／國家科學及技術委員會主任委員吳誠文率同有關人員\\n'
            '葛委員如鈞：（9時36分）謝謝主席。\\n'
            '主席：好，有請吳誠文主委。\\n'
            '1d:["$","component"]'
        )}
      </body>
    </html>
    """

    speech = parse_speech_html(
        slug="152872",
        url="https://lawmaker.twreporter.org/congress/a/152872",
        html_text=html,
    )

    assert speech.slug == "152872"
    assert speech.date == "2024-05-23"
    assert speech.title is not None
    assert "葛如鈞" in speech.title
    assert speech.summary == "第一點摘要\n第二點摘要"
    assert speech.respondents == "國家科學及技術委員會主任委員吳誠文率同有關人員"
    assert speech.transcript.startswith("葛委員如鈞")
    assert "主席：好" in speech.transcript
    assert speech.topic_slugs == ["topic3-17-6"]
    assert speech.topic_titles == ["資安防護"]
