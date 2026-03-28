from __future__ import annotations

import app as sentiment_app


class DummyStore:
    db_path = "/tmp/market_sentiment_test.db"

    def list_topics(self):
        return ["SPY", "BTC"]

    def fetch_one(self, query, params=()):
        query = query.lower()
        if "from sentiment_runs" in query:
            return {
                "started_at": "2026-03-28T09:55:00+00:00",
                "finished_at": "2026-03-28T10:00:00+00:00",
                "status": "success",
                "message": "ok",
                "stats_json": "{}",
            }
        if "from sentiment_items" in query:
            return {"count": 6}
        if "from sentiment_snapshots" in query:
            return {"count": 2}
        return None

    def get_history(self, limit=12, topic=None):
        rows = [
            {
                "id": 1,
                "topic": topic or "SPY",
                "score": 0.42,
                "label": "Bullish",
                "overall_label": "Bullish",
                "positive": 2,
                "negative": 1,
                "neutral": 1,
                "item_count": 2,
                "window_hours": 24,
                "computed_at": "2026-03-28T10:00:00+00:00",
                "data": {},
            }
        ]
        return rows[:limit]

    def get_latest_snapshot(self, topic=None):
        return {
            "computed_at": "2026-03-28T10:00:00+00:00",
            "overall_label": "Bullish",
        }


class DummyMarketSentiment:
    def __init__(self):
        self.store = DummyStore()

    def get_snapshot(self, topics=None):
        topics = topics or ["SPY"]
        return {
            topic: {
                "topic": topic,
                "score": 0.25,
                "positive": 2,
                "negative": 0,
                "neutral": 1,
                "item_count": 3,
                "window_hours": 24,
                "computed_at": "2026-03-28T10:00:00+00:00",
                "data": {},
                "items": [
                    {
                        "topic": topic,
                        "source": "Reuters",
                        "title": f"{topic} higher on strong earnings",
                        "summary": "Positive signal.",
                        "url": "https://example.com/1",
                        "published_at": "2026-03-28T10:00:00+00:00",
                        "sentiment_score": 0.5,
                        "confidence": 0.8,
                    }
                ],
            }
            for topic in topics
        }

    def refresh(self, *args, **kwargs):
        return self.get_snapshot(kwargs.get("topics"))


def test_endpoints(monkeypatch):
    sentiment_app.app.config["TESTING"] = True
    monkeypatch.setattr(sentiment_app, "get_market_sentiment_engine", lambda: DummyMarketSentiment())

    client = sentiment_app.app.test_client()

    assert client.get("/").status_code == 302
    assert client.get("/sentiment").status_code == 200

    snapshot = client.get("/api/sentiment/snapshot?topics=SPY,BTC")
    assert snapshot.status_code == 200
    data = snapshot.get_json()
    topics = data.get("selected_topics") or data.get("watchlist") or data.get("symbols")
    assert topics == ["SPY", "BTC"]
    assert data["topic_count"] == 2
    assert data["article_count"] == 2

    refresh = client.post(
        "/api/sentiment/refresh",
        json={"topics": ["SPY", "BTC"], "sync": True},
    )
    assert refresh.status_code == 200
    refresh_data = refresh.get_json()
    assert refresh_data["status"] == "success"
    assert "snapshot" in refresh_data

    assert client.get("/api/sentiment/history?limit=2").status_code == 200
    assert client.get("/api/sentiment/health").status_code == 200
    assert client.get("/api/sentiment/export?topics=SPY").status_code == 200
    assert client.get("/api/sentiment/topics?topics=SPY").status_code == 200
    assert client.get("/api/health?topics=SPY").status_code == 200
    health = client.get("/api/sentiment/health?topics=SPY").get_json()
    assert health["docs"]["deployment"] == "/docs/deployment.md"
    assert health["latest_snapshot_label"] == "Bullish"
