from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any, Optional

from flask import Flask, jsonify, redirect, render_template, request, url_for

from core.market_sentiment import MarketSentimentEngine
from core.sentiment_store import get_sentiment_store

app = Flask(__name__, template_folder="templates")

_ENGINE: Optional[MarketSentimentEngine] = None


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_topics(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, (list, tuple)):
        raw = ",".join(str(v) for v in value)
    else:
        raw = str(value)
    raw = raw.replace(";", ",")
    return [topic.strip() for topic in raw.split(",") if topic.strip()]


def _default_topics() -> list[str]:
    raw = os.getenv("MARKET_SENTIMENT_TOPICS", "SPY,NASDAQ,NIFTY,BTC")
    return _parse_topics(raw)


def _engine_db_path() -> Optional[str]:
    return os.getenv("MARKET_SENTIMENT_DB_PATH")


def get_market_sentiment_engine() -> MarketSentimentEngine:
    global _ENGINE
    if _ENGINE is None:
        _ENGINE = MarketSentimentEngine(store=get_sentiment_store(_engine_db_path()))
    return _ENGINE


def _empty_snapshot() -> dict[str, Any]:
    return {
        "status": "unavailable",
        "as_of": _now_iso(),
        "overall_score": 0.0,
        "overall_label": "Neutral",
        "label": "Neutral",
        "created_at": _now_iso(),
        "summary": {
            "tracked_topics": 0,
            "overall_score": 0.0,
            "overall_label": "Neutral",
            "top_positive": [],
            "top_negative": [],
            "most_bullish_entity": None,
            "most_bearish_entity": None,
        },
        "article_count": 0,
        "entity_count": 0,
        "topic_count": 0,
        "bullish_count": 0,
        "bearish_count": 0,
        "neutral_count": 0,
        "entities": [],
        "items": [],
        "watchlist": [],
        "selected_topics": [],
        "topics": [],
        "message": "Market sentiment engine not configured yet.",
    }


def _topic_label(score: float) -> str:
    if score >= 0.2:
        return "Bullish"
    if score <= -0.2:
        return "Bearish"
    return "Neutral"


def _combine_topic_snapshots(topic_snapshots: dict[str, dict[str, Any]], topics: Optional[list[str]] = None) -> dict[str, Any]:
    if not topic_snapshots:
        return _empty_snapshot()

    combined_items: list[dict[str, Any]] = []
    topic_cards: list[dict[str, Any]] = []
    total_weight = 0.0
    weighted_score = 0.0
    bullish = bearish = neutral = 0
    latest_at = None

    for topic, snap in topic_snapshots.items():
        if not isinstance(snap, dict):
            continue
        score = float(snap.get("score", 0.0) or 0.0)
        item_count = int(snap.get("item_count", 0) or 0)
        weight = max(item_count, 1)
        total_weight += weight
        weighted_score += score * weight
        computed_at = snap.get("computed_at") or snap.get("created_at")
        if computed_at and (latest_at is None or str(computed_at) > str(latest_at)):
            latest_at = computed_at

        local_items = []
        for item in snap.get("items") or []:
            try:
                sent = float(item.get("sentiment", item.get("sentiment_score", 0.0)))
            except Exception:
                sent = 0.0
            confidence = float(item.get("confidence", 0.0) or 0.0)
            label = _topic_label(sent)
            if sent > 0.15:
                bullish += 1
            elif sent < -0.15:
                bearish += 1
            else:
                neutral += 1
            normalized = {
                "topic": topic,
                "source": item.get("source", ""),
                "title": item.get("title", ""),
                "summary": item.get("summary", ""),
                "url": item.get("url", ""),
                "published_at": item.get("published_at"),
                "sentiment_score": sent,
                "confidence": confidence,
                "label": label,
            }
            local_items.append(normalized)
            combined_items.append(normalized)

        topic_cards.append(
            {
                "topic": topic,
                "score": round(score, 4),
                "label": _topic_label(score),
                "item_count": item_count,
                "bullish_count": sum(1 for item in local_items if item["sentiment_score"] > 0.15),
                "bearish_count": sum(1 for item in local_items if item["sentiment_score"] < -0.15),
                "neutral_count": sum(1 for item in local_items if -0.15 <= item["sentiment_score"] <= 0.15),
                "computed_at": computed_at,
                "latest_headline": local_items[0]["title"] if local_items else "No items cached yet",
            }
        )

    overall_score = weighted_score / total_weight if total_weight else 0.0
    sorted_items = sorted(combined_items, key=lambda item: item["sentiment_score"], reverse=True)
    top_positive = sorted_items[:3]
    top_negative = sorted(combined_items, key=lambda item: item["sentiment_score"])[:3]

    return {
        "status": "success",
        "as_of": latest_at or _now_iso(),
        "overall_score": round(overall_score, 4),
        "overall_label": _topic_label(overall_score),
        "label": _topic_label(overall_score),
        "created_at": latest_at or _now_iso(),
        "summary": {
            "tracked_topics": len(topic_snapshots),
            "overall_score": round(overall_score, 4),
            "overall_label": _topic_label(overall_score),
            "top_positive": top_positive,
            "top_negative": top_negative,
            "most_bullish_entity": top_positive[0] if top_positive else None,
            "most_bearish_entity": top_negative[0] if top_negative else None,
        },
        "article_count": len(combined_items),
        "entity_count": len(topic_snapshots),
        "topic_count": len(topic_snapshots),
        "bullish_count": bullish,
        "bearish_count": bearish,
        "neutral_count": neutral,
        "entities": topic_cards,
        "items": combined_items[:30],
        "watchlist": topics or [],
        "topics": topic_snapshots,
    }


def _select_topics(raw_topics: Optional[list[str]] = None) -> list[str]:
    selected = [topic for topic in _parse_topics(raw_topics) if topic]
    if selected:
        return selected
    engine = get_market_sentiment_engine()
    tracked = engine.store.list_topics()
    return tracked or _default_topics()


def get_market_sentiment_snapshot(topics: Optional[list[str]] = None) -> dict[str, Any]:
    engine = get_market_sentiment_engine()
    selected_topics = _select_topics(topics)

    try:
        if hasattr(engine, "get_snapshot"):
            try:
                topic_snapshots = engine.get_snapshot(topics=selected_topics)
            except TypeError:
                topic_snapshots = engine.get_snapshot(selected_topics)
        else:
            return _empty_snapshot()

        if not isinstance(topic_snapshots, dict):
            return _empty_snapshot()

        combined = _combine_topic_snapshots(topic_snapshots, topics=selected_topics)
        combined["status"] = "success"
        combined["symbols"] = selected_topics
        combined["watchlist"] = selected_topics
        combined["selected_topics"] = selected_topics
        combined["topic_count"] = len(selected_topics)
        return combined
    except Exception as exc:
        snapshot = _empty_snapshot()
        snapshot["status"] = "error"
        snapshot["message"] = str(exc)
        snapshot["symbols"] = selected_topics
        snapshot["watchlist"] = selected_topics
        snapshot["selected_topics"] = selected_topics
        snapshot["topic_count"] = len(selected_topics)
        return snapshot


def get_market_sentiment_history(limit: int = 12, topic: Optional[str] = None) -> list[dict[str, Any]]:
    engine = get_market_sentiment_engine()
    try:
        if hasattr(engine, "store") and hasattr(engine.store, "get_history"):
            rows = engine.store.get_history(limit=limit, topic=topic)
            return [dict(row) for row in rows]
        return []
    except Exception:
        return []


def get_market_sentiment_health(topics: Optional[list[str]] = None) -> dict[str, Any]:
    engine = get_market_sentiment_engine()
    store = getattr(engine, "store", None)
    docs = {"deployment": "/docs/deployment.md", "api": "/docs/api_reference.md"}
    if store is None:
        return {
            "status": "degraded",
            "service": "unavailable",
            "engine_name": "Unavailable",
            "database_path": None,
            "default_topics": _default_topics(),
            "selected_topics": topics or _default_topics(),
            "tracked_topics": [],
            "tracked_topic_count": 0,
            "item_count": 0,
            "snapshot_count": 0,
            "latest_snapshot_at": None,
            "latest_snapshot_label": None,
            "snapshot_age_minutes": None,
            "latest_run": None,
            "last_seen_at": None,
            "headline": "Market sentiment engine not attached",
            "docs": docs,
        }
    if not hasattr(store, "fetch_one"):
        latest_snapshot = store.get_latest_snapshot() if hasattr(store, "get_latest_snapshot") else None
        tracked_topics = store.list_topics() if hasattr(store, "list_topics") else []
        return {
            "status": "ok",
            "service": "ready",
            "engine_name": "MarketSentimentEngine",
            "database_path": getattr(store, "db_path", None),
            "default_topics": _default_topics(),
            "selected_topics": topics or _default_topics(),
            "tracked_topics": tracked_topics,
            "tracked_topic_count": len(tracked_topics),
            "item_count": 0,
            "snapshot_count": 0,
            "latest_snapshot_at": (latest_snapshot or {}).get("computed_at"),
            "latest_snapshot_label": (latest_snapshot or {}).get("overall_label"),
            "snapshot_age_minutes": None,
            "latest_run": None,
            "last_seen_at": None,
            "headline": "Production-ready public-news sentiment snapshot service",
            "docs": docs,
        }
    latest_snapshot = store.get_latest_snapshot()
    latest_run = store.fetch_one(
        """
        SELECT started_at, finished_at, status, message, stats_json
        FROM sentiment_runs
        ORDER BY id DESC
        LIMIT 1
        """
    )
    item_count_row = store.fetch_one("SELECT COUNT(*) AS count FROM sentiment_items") or {"count": 0}
    snapshot_count_row = store.fetch_one("SELECT COUNT(*) AS count FROM sentiment_snapshots") or {"count": 0}
    tracked_topics = store.list_topics()
    snapshot_age_minutes = None
    if latest_snapshot and latest_snapshot.get("computed_at"):
        try:
            parsed = datetime.fromisoformat(str(latest_snapshot["computed_at"]).replace("Z", "+00:00"))
            snapshot_age_minutes = max(int((datetime.now(timezone.utc) - parsed).total_seconds() // 60), 0)
        except Exception:
            snapshot_age_minutes = None

    return {
        "status": "ok",
        "service": "ready",
        "engine_name": "MarketSentimentEngine",
        "database_path": getattr(store, "db_path", None),
        "default_topics": _default_topics(),
        "selected_topics": topics or _default_topics(),
        "tracked_topics": tracked_topics,
        "tracked_topic_count": len(tracked_topics),
        "item_count": int(item_count_row.get("count", 0)),
        "snapshot_count": int(snapshot_count_row.get("count", 0)),
        "latest_snapshot_at": (latest_snapshot or {}).get("computed_at"),
        "latest_snapshot_label": (latest_snapshot or {}).get("overall_label"),
        "snapshot_age_minutes": snapshot_age_minutes,
        "latest_run": latest_run,
        "last_seen_at": (latest_run or {}).get("finished_at"),
        "headline": "Production-ready public-news sentiment snapshot service",
        "docs": docs,
    }


def _topics_from_request() -> list[str]:
    if request.method == "POST":
        payload = request.get_json(silent=True) or {}
        posted = _parse_topics(payload.get("topics"))
        if posted:
            return posted

    query = _parse_topics(request.args.get("topics"))
    if query:
        return query
    return _default_topics()


@app.route("/")
def home():
    topics = _topics_from_request()
    query = f"?topics={','.join(topics)}" if topics else ""
    # Keep the root URL as a clean redirect for demo sharing.
    return redirect(url_for("sentiment_page") + query)


@app.route("/sentiment")
def sentiment_page():
    topics = _topics_from_request()
    selected_topic = topics[0] if len(topics) == 1 else None
    available_topics = sorted(set(_default_topics()) | set(get_market_sentiment_health(topics=topics).get("tracked_topics", [])))
    return render_template(
        "sentiment.html",
        snapshot=get_market_sentiment_snapshot(topics=topics),
        history=get_market_sentiment_history(limit=8, topic=selected_topic),
        health=get_market_sentiment_health(topics=topics),
        default_topics=_default_topics(),
        available_topics=available_topics,
        selected_topics=topics,
        topics_csv=",".join(topics),
    )


@app.route("/api/health")
@app.route("/api/sentiment/health")
def api_health():
    topics = _topics_from_request()
    return jsonify(get_market_sentiment_health(topics=topics))


@app.route("/api/topics")
@app.route("/api/sentiment/topics")
def api_topics():
    engine = get_market_sentiment_engine()
    return jsonify(
        {
            "status": "success",
            "default_topics": _default_topics(),
            "tracked_topics": engine.store.list_topics(),
        }
    )

@app.route("/api/sentiment/snapshot")
def api_sentiment_snapshot():
    topics = _topics_from_request()
    try:
        return jsonify(get_market_sentiment_snapshot(topics=topics))
    except Exception as exc:
        return jsonify({"status": "error", "message": str(exc)}), 500


@app.route("/api/sentiment/export")
def api_sentiment_export():
    topics = _topics_from_request()
    payload = {
        "snapshot": get_market_sentiment_snapshot(topics=topics),
        "health": get_market_sentiment_health(topics=topics),
        "history": get_market_sentiment_history(limit=12, topic=topics[0] if len(topics) == 1 else None),
    }
    response = jsonify(payload)
    response.headers["Content-Disposition"] = "attachment; filename=market_sentiment_export.json"
    return response


@app.route("/api/sentiment/refresh", methods=["POST"])
def api_sentiment_refresh():
    payload = request.get_json(silent=True) or {}
    topics = _parse_topics(payload.get("topics")) or _default_topics()
    window_hours = int(payload.get("window_hours", 24))
    max_items = int(payload.get("limit", payload.get("max_items_per_topic", 12)))

    try:
        engine = get_market_sentiment_engine()
        results = engine.refresh(topics=list(topics), window_hours=window_hours, max_items_per_topic=max_items)
        snapshot = get_market_sentiment_snapshot(topics=topics)
        return jsonify(
            {
                "status": "success",
                "message": "Market sentiment refreshed successfully",
                "topics": topics,
                "window_hours": window_hours,
                "max_items_per_topic": max_items,
                "results": results,
                "snapshot": snapshot,
            }
        )
    except Exception as exc:
        return jsonify({"status": "error", "message": str(exc), "topics": topics}), 500


@app.route("/api/sentiment/history")
def api_sentiment_history():
    limit = int(request.args.get("limit", 12))
    topic = request.args.get("topic")
    try:
        return jsonify(
            {
                "status": "success",
                "history": get_market_sentiment_history(limit=limit, topic=topic),
                "topic": topic,
                "limit": limit,
            }
        )
    except Exception as exc:
        return jsonify({"status": "error", "message": str(exc)}), 500


if __name__ == "__main__":
    host = os.getenv("HOST", "0.0.0.0")
    port = int(os.getenv("PORT", "5055"))
    app.run(host=host, port=port, debug=True)
