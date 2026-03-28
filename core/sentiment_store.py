"""
SQLite persistence for market sentiment data.

This standalone version keeps the same public API as the InternMailer prototype
but removes the dependency on InternMailer-specific database helpers.
"""

from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Iterator

DEFAULT_SENTIMENT_DB = Path("/tmp/market_sentiment_engine/market_sentiment.db")


def resolve_sentiment_db_path(db_path: Optional[str] = None) -> str:
    """Resolve a writable sentiment database path."""
    if db_path:
        path = Path(db_path)
        if not path.is_absolute():
            path = Path("/tmp/market_sentiment_engine") / path
    else:
        path = DEFAULT_SENTIMENT_DB

    path.parent.mkdir(parents=True, exist_ok=True)
    return str(path)


def _to_iso(value: Any) -> str:
    if isinstance(value, datetime):
        dt = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).isoformat()
    if value is None:
        return datetime.now(timezone.utc).isoformat()
    return str(value)


def _label_for_score(score: float) -> str:
    if score >= 0.2:
        return "Bullish"
    if score <= -0.2:
        return "Bearish"
    return "Neutral"


@dataclass(frozen=True)
class SentimentItem:
    item_id: str
    topic: str
    source: str
    title: str
    url: str
    summary: str
    published_at: datetime
    fetched_at: datetime
    sentiment: float
    confidence: float
    raw: Dict[str, Any] = field(default_factory=dict)

    def to_row(self) -> Dict[str, Any]:
        return {
            "item_id": self.item_id,
            "topic": self.topic,
            "source": self.source,
            "title": self.title,
            "url": self.url,
            "summary": self.summary,
            "published_at": _to_iso(self.published_at),
            "fetched_at": _to_iso(self.fetched_at),
            "sentiment": float(self.sentiment),
            "confidence": float(self.confidence),
            "raw_json": json.dumps(self.raw or {}),
        }


@dataclass(frozen=True)
class SentimentSnapshot:
    topic: str
    score: float
    positive: int
    negative: int
    neutral: int
    item_count: int
    window_hours: int
    computed_at: datetime
    data: Dict[str, Any] = field(default_factory=dict)

    def to_row(self) -> Dict[str, Any]:
        return {
            "topic": self.topic,
            "score": float(self.score),
            "positive": int(self.positive),
            "negative": int(self.negative),
            "neutral": int(self.neutral),
            "item_count": int(self.item_count),
            "window_hours": int(self.window_hours),
            "computed_at": _to_iso(self.computed_at),
            "data_json": json.dumps(self.data or {}),
        }


class SentimentStore:
    """Persistence layer for market sentiment data."""

    def __init__(self, db_path: Optional[str] = None):
        self.db_path = resolve_sentiment_db_path(db_path)
        self._initialize_schema()

    @contextmanager
    def get_connection(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def fetch_one(self, query: str, params: tuple[Any, ...] = ()) -> Optional[Dict[str, Any]]:
        with self.get_connection() as conn:
            row = conn.execute(query, params).fetchone()
            return dict(row) if row else None

    def fetch_all(self, query: str, params: tuple[Any, ...] = ()) -> List[Dict[str, Any]]:
        with self.get_connection() as conn:
            rows = conn.execute(query, params).fetchall()
            return [dict(row) for row in rows]

    def _initialize_schema(self):
        with self.get_connection() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS sentiment_items (
                    item_id TEXT PRIMARY KEY,
                    topic TEXT NOT NULL,
                    source TEXT NOT NULL,
                    title TEXT NOT NULL,
                    url TEXT NOT NULL,
                    summary TEXT,
                    published_at TEXT NOT NULL,
                    fetched_at TEXT NOT NULL,
                    sentiment REAL NOT NULL,
                    confidence REAL NOT NULL,
                    raw_json TEXT NOT NULL,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS sentiment_snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    topic TEXT NOT NULL,
                    score REAL NOT NULL,
                    positive INTEGER NOT NULL,
                    negative INTEGER NOT NULL,
                    neutral INTEGER NOT NULL,
                    item_count INTEGER NOT NULL,
                    window_hours INTEGER NOT NULL,
                    computed_at TEXT NOT NULL,
                    data_json TEXT NOT NULL,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS sentiment_runs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    started_at TEXT NOT NULL,
                    finished_at TEXT NOT NULL,
                    topics_json TEXT NOT NULL,
                    status TEXT NOT NULL,
                    message TEXT,
                    stats_json TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_sentiment_items_topic_published
                ON sentiment_items(topic, published_at DESC)
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_sentiment_snapshots_topic_computed
                ON sentiment_snapshots(topic, computed_at DESC, id DESC)
                """
            )

    def upsert_items(self, items: Iterable[SentimentItem]) -> int:
        rows = [item.to_row() for item in items]
        if not rows:
            return 0

        with self.get_connection() as conn:
            item_ids = [row["item_id"] for row in rows if row.get("item_id")]
            existing: set[str] = set()
            if item_ids:
                chunk_size = 500
                for i in range(0, len(item_ids), chunk_size):
                    chunk = item_ids[i : i + chunk_size]
                    placeholders = ",".join("?" for _ in chunk)
                    found = conn.execute(
                        f"SELECT item_id FROM sentiment_items WHERE item_id IN ({placeholders})",
                        tuple(chunk),
                    ).fetchall()
                    existing.update(r[0] for r in found)

            conn.executemany(
                """
                INSERT INTO sentiment_items (
                    item_id, topic, source, title, url, summary,
                    published_at, fetched_at, sentiment, confidence, raw_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(item_id) DO UPDATE SET
                    topic=excluded.topic,
                    source=excluded.source,
                    title=excluded.title,
                    url=excluded.url,
                    summary=excluded.summary,
                    published_at=excluded.published_at,
                    fetched_at=excluded.fetched_at,
                    sentiment=excluded.sentiment,
                    confidence=excluded.confidence,
                    raw_json=excluded.raw_json
                """,
                [
                    (
                        row["item_id"],
                        row["topic"],
                        row["source"],
                        row["title"],
                        row["url"],
                        row["summary"],
                        row["published_at"],
                        row["fetched_at"],
                        row["sentiment"],
                        row["confidence"],
                        row["raw_json"],
                    )
                    for row in rows
                ],
            )
        return max(len(item_ids) - len(existing), 0)

    def save_snapshot(self, snapshot: SentimentSnapshot | Dict[str, Any]) -> int:
        row = snapshot.to_row() if hasattr(snapshot, "to_row") else self._snapshot_to_row(snapshot)
        with self.get_connection() as conn:
            cursor = conn.execute(
                """
                INSERT INTO sentiment_snapshots (
                    topic, score, positive, negative, neutral,
                    item_count, window_hours, computed_at, data_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    row["topic"],
                    row["score"],
                    row["positive"],
                    row["negative"],
                    row["neutral"],
                    row["item_count"],
                    row["window_hours"],
                    row["computed_at"],
                    row["data_json"],
                ),
            )
            return int(cursor.lastrowid)

    def _snapshot_to_row(self, snapshot: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "topic": snapshot.get("topic", ""),
            "score": float(snapshot.get("score", 0.0)),
            "positive": int(snapshot.get("positive", 0)),
            "negative": int(snapshot.get("negative", 0)),
            "neutral": int(snapshot.get("neutral", 0)),
            "item_count": int(snapshot.get("item_count", 0)),
            "window_hours": int(snapshot.get("window_hours", 24)),
            "computed_at": _to_iso(snapshot.get("computed_at")),
            "data_json": json.dumps(snapshot.get("data", {})),
        }

    def get_latest_snapshot(self, topic: Optional[str] = None) -> Optional[Dict[str, Any]]:
        if topic:
            row = self.fetch_one(
                """
                SELECT *
                FROM sentiment_snapshots
                WHERE topic = ?
                ORDER BY datetime(computed_at) DESC, id DESC
                LIMIT 1
                """,
                (topic,),
            )
        else:
            row = self.fetch_one(
                """
                SELECT *
                FROM sentiment_snapshots
                ORDER BY datetime(computed_at) DESC, id DESC
                LIMIT 1
                """
            )

        if not row:
            return None

        items = self.get_recent_items(topic=row["topic"], limit=12)
        return {
            "topic": row["topic"],
            "score": row["score"],
            "label": _label_for_score(float(row["score"])),
            "overall_label": _label_for_score(float(row["score"])),
            "positive": row["positive"],
            "negative": row["negative"],
            "neutral": row["neutral"],
            "item_count": row["item_count"],
            "window_hours": row["window_hours"],
            "computed_at": row["computed_at"],
            "data": json.loads(row["data_json"] or "{}"),
            "items": items,
        }

    def get_recent_items(
        self,
        topic: Optional[str] = None,
        limit: int = 25,
        since: Optional[datetime] = None,
    ) -> List[Dict[str, Any]]:
        since_iso = _to_iso(since) if since else None

        if topic and since_iso:
            rows = self.fetch_all(
                """
                SELECT *
                FROM sentiment_items
                WHERE topic = ? AND published_at >= ?
                ORDER BY datetime(published_at) DESC, fetched_at DESC
                LIMIT ?
                """,
                (topic, since_iso, limit),
            )
        elif topic:
            rows = self.fetch_all(
                """
                SELECT *
                FROM sentiment_items
                WHERE topic = ?
                ORDER BY datetime(published_at) DESC, fetched_at DESC
                LIMIT ?
                """,
                (topic, limit),
            )
        elif since_iso:
            rows = self.fetch_all(
                """
                SELECT *
                FROM sentiment_items
                WHERE published_at >= ?
                ORDER BY datetime(published_at) DESC, fetched_at DESC
                LIMIT ?
                """,
                (since_iso, limit),
            )
        else:
            rows = self.fetch_all(
                """
                SELECT *
                FROM sentiment_items
                ORDER BY datetime(published_at) DESC, fetched_at DESC
                LIMIT ?
                """,
                (limit,),
            )

        items: List[Dict[str, Any]] = []
        for row in rows:
            items.append(
                {
                    "item_id": row["item_id"],
                    "topic": row["topic"],
                    "source": row["source"],
                    "title": row["title"],
                    "url": row["url"],
                    "summary": row["summary"],
                    "published_at": row["published_at"],
                    "fetched_at": row["fetched_at"],
                    "sentiment": row["sentiment"],
                    "confidence": row["confidence"],
                    "raw": json.loads(row["raw_json"] or "{}"),
                }
            )
        return items

    def list_topics(self) -> List[str]:
        rows = self.fetch_all(
            """
            SELECT topic, MAX(computed_at) AS latest_at
            FROM sentiment_snapshots
            GROUP BY topic
            ORDER BY datetime(latest_at) DESC
            """
        )
        return [row["topic"] for row in rows if row["topic"]]

    def get_history(self, limit: int = 10, topic: Optional[str] = None) -> List[Dict[str, Any]]:
        if topic:
            rows = self.fetch_all(
                """
                SELECT *
                FROM sentiment_snapshots
                WHERE topic = ?
                ORDER BY datetime(computed_at) DESC, id DESC
                LIMIT ?
                """,
                (topic, limit),
            )
        else:
            rows = self.fetch_all(
                """
                SELECT *
                FROM sentiment_snapshots
                ORDER BY datetime(computed_at) DESC, id DESC
                LIMIT ?
                """,
                (limit,),
            )

        history = []
        for row in rows:
            history.append(
                {
                    "id": row["id"],
                    "topic": row["topic"],
                    "score": row["score"],
                    "label": _label_for_score(float(row["score"])),
                    "overall_label": _label_for_score(float(row["score"])),
                    "positive": row["positive"],
                    "negative": row["negative"],
                    "neutral": row["neutral"],
                    "item_count": row["item_count"],
                    "window_hours": row["window_hours"],
                    "computed_at": row["computed_at"],
                    "data": json.loads(row["data_json"] or "{}"),
                }
            )
        return history

    def log_run(
        self,
        topics: List[str],
        status: str,
        message: str,
        started_at: datetime,
        finished_at: datetime,
        stats: Dict[str, Any],
    ) -> None:
        with self.get_connection() as conn:
            conn.execute(
                """
                INSERT INTO sentiment_runs (
                    started_at, finished_at, topics_json, status, message, stats_json
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    _to_iso(started_at),
                    _to_iso(finished_at),
                    json.dumps(topics or []),
                    status,
                    message,
                    json.dumps(stats or {}),
                ),
            )


_store_instance: Optional[SentimentStore] = None


def get_sentiment_store(db_path: Optional[str] = None) -> SentimentStore:
    """Return a singleton sentiment store."""
    global _store_instance
    resolved_path = resolve_sentiment_db_path(db_path)
    if _store_instance is None or _store_instance.db_path != resolved_path:
        _store_instance = SentimentStore(resolved_path)
    return _store_instance

