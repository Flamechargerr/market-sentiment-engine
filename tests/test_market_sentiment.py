from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
import json

import pytest

from core.market_sentiment import GoogleNewsRssSource, LexiconSentimentScorer, MarketSentimentEngine
from core.sentiment_store import SentimentStore


SAMPLE_RSS = """<?xml version="1.0" encoding="UTF-8" ?>
<rss version="2.0">
  <channel>
    <title>Google News - AAPL stock</title>
    <item>
      <title>AAPL shares soar after earnings beat expectations</title>
      <link>https://example.com/aapl-soar</link>
      <description>Investors rally as Apple reports strong profits and raises guidance.</description>
      <pubDate>Sat, 28 Mar 2026 08:00:00 GMT</pubDate>
    </item>
    <item>
      <title>Apple faces lawsuit as regulators open investigation</title>
      <link>https://example.com/aapl-lawsuit</link>
      <description>Shares drop on probe news; analysts downgrade outlook.</description>
      <pubDate>Sat, 28 Mar 2026 06:00:00 GMT</pubDate>
    </item>
    <item>
      <title>Mixed session for Apple stock</title>
      <link>https://example.com/aapl-mixed</link>
      <description>No major catalysts; market awaits next update.</description>
      <pubDate>Sat, 28 Mar 2026 05:00:00 GMT</pubDate>
    </item>
  </channel>
</rss>
"""


class FakeResponse:
    def __init__(self, text: str, status_code: int = 200):
        self.text = text
        self.status_code = status_code


class FakeSession:
    def __init__(self, text: str, status_code: int = 200):
        self._text = text
        self._status = status_code
        self.calls = []

    def get(self, url, headers=None, timeout=None):
        self.calls.append({"url": url, "timeout": timeout})
        return FakeResponse(self._text, self._status)


def test_lexicon_scorer_directionality():
    scorer = LexiconSentimentScorer()

    s_pos, c_pos, meta_pos = scorer.score("shares soar after earnings beat; upgrade and strong profit")
    assert s_pos > 0
    assert 0 <= c_pos <= 1
    assert meta_pos["pos"] >= 1

    s_neg, c_neg, meta_neg = scorer.score("stock plunges after guidance cut; lawsuit and downgrade")
    assert s_neg < 0
    assert 0 <= c_neg <= 1
    assert meta_neg["neg"] >= 1


def test_engine_refresh_persists_snapshot_and_items(tmp_path: Path):
    db_path = str(tmp_path / "sentiment.db")
    store = SentimentStore(db_path=db_path)

    session = FakeSession(SAMPLE_RSS)
    source = GoogleNewsRssSource(session=session)
    engine = MarketSentimentEngine(store=store, sources=[source])

    out = engine.refresh(["AAPL"], window_hours=24, max_items_per_topic=10)
    assert "AAPL" in out
    assert out["AAPL"]["item_count"] == 3
    assert -1.0 <= out["AAPL"]["score"] <= 1.0
    assert out["AAPL"]["data"]["inserted"] >= 1

    snap = store.get_latest_snapshot("AAPL")
    assert snap is not None
    assert snap["topic"] == "AAPL"
    assert -1.0 <= float(snap["score"]) <= 1.0
    governance = (snap.get("data") or {}).get("governance") or {}
    model_metrics = (snap.get("data") or {}).get("model_metrics") or {}
    assert governance["fetched_item_count"] == 3
    assert governance["deduped_item_count"] == 3
    assert governance["scored_item_count"] == 3
    assert governance["inserted_item_count"] == 3
    assert governance["missing_data"] is False
    assert 0.0 <= model_metrics["average_confidence"] <= 1.0

    since = datetime(2026, 3, 20, tzinfo=timezone.utc)
    items = store.get_recent_items("AAPL", since=since, limit=50)
    assert len(items) == 3
    assert {i["url"] for i in items} == {
        "https://example.com/aapl-soar",
        "https://example.com/aapl-lawsuit",
        "https://example.com/aapl-mixed",
    }


def test_store_dedupes_on_repeat_refresh(tmp_path: Path):
    db_path = str(tmp_path / "sentiment.db")
    store = SentimentStore(db_path=db_path)

    session = FakeSession(SAMPLE_RSS)
    source = GoogleNewsRssSource(session=session)
    engine = MarketSentimentEngine(store=store, sources=[source])

    out1 = engine.refresh(["AAPL"], window_hours=24, max_items_per_topic=10)
    assert out1["AAPL"]["data"]["inserted"] == 3

    out2 = engine.refresh(["AAPL"], window_hours=24, max_items_per_topic=10)
    assert out2["AAPL"]["data"]["inserted"] == 0

    since = datetime(2026, 3, 20, tzinfo=timezone.utc)
    items = store.get_recent_items("AAPL", since=since, limit=50)
    assert len(items) == 3


def test_refresh_logs_governance_run_stats(tmp_path: Path):
    db_path = str(tmp_path / "sentiment.db")
    store = SentimentStore(db_path=db_path)
    engine = MarketSentimentEngine(store=store, sources=[GoogleNewsRssSource(session=FakeSession(SAMPLE_RSS))])

    engine.refresh(["AAPL"], window_hours=24, max_items_per_topic=10)

    row = store.fetch_one("SELECT stats_json FROM sentiment_runs ORDER BY id DESC LIMIT 1")
    assert row is not None
    stats = json.loads(row["stats_json"] or "{}")
    assert stats["topics_requested"] == 1
    assert stats["items_scored"] == 3
    assert stats["items_inserted"] == 3
    assert stats["topics_without_data"] == 0
    assert 0.0 <= stats["average_model_confidence"] <= 1.0
