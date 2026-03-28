"""
Market Sentiment Engine (MVP)
=============================
Fetches public text signals (RSS), scores sentiment via a small lexicon, and
aggregates per-topic sentiment snapshots persisted to SQLite.

This is intentionally lightweight:
- stdlib + requests only
- no vendor API keys required (uses public RSS search)
"""

from __future__ import annotations

import hashlib
import math
import re
import statistics
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from typing import Any, Dict, Iterable, List, Optional, Protocol, Tuple
from urllib.parse import quote_plus
import xml.etree.ElementTree as ET

import requests

from core.sentiment_store import SentimentItem, SentimentSnapshot, SentimentStore


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8", errors="ignore")).hexdigest()


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default


def _parse_datetime(value: Optional[str]) -> datetime:
    if not value:
        return _utc_now()
    v = (value or "").strip()
    # Try RFC 2822 (RSS pubDate)
    try:
        dt = parsedate_to_datetime(v)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        pass
    # Try ISO format
    try:
        dt = datetime.fromisoformat(v.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return _utc_now()


def _strip_html(text: str) -> str:
    # Keep it simple: remove tags; RSS descriptions can be HTML.
    return re.sub(r"<[^>]+>", " ", text or "").strip()


def _local_name(tag: str) -> str:
    # Handle namespaces like "{http://...}title"
    if "}" in tag:
        return tag.split("}", 1)[1]
    return tag


@dataclass(frozen=True)
class SourceItem:
    topic: str
    title: str
    url: str
    summary: str
    published_at: datetime
    source: str
    raw: Dict[str, Any]


class SourceAdapter(Protocol):
    name: str

    def fetch(self, topic: str, limit: int = 25) -> List[SourceItem]:
        ...


class GoogleNewsRssSource:
    """
    Public RSS search feed.

    Example:
    https://news.google.com/rss/search?q=AAPL%20stock&hl=en-US&gl=US&ceid=US:en
    """

    name = "google_news_rss"

    def __init__(
        self,
        locale_hl: str = "en-US",
        locale_gl: str = "US",
        locale_ceid: str = "US:en",
        session: Optional[requests.Session] = None,
    ):
        self.locale_hl = locale_hl
        self.locale_gl = locale_gl
        self.locale_ceid = locale_ceid
        self.session = session or requests.Session()

    def _build_url(self, topic: str) -> str:
        query = f"{topic} stock"
        q = quote_plus(query)
        return (
            "https://news.google.com/rss/search?q="
            + q
            + f"&hl={quote_plus(self.locale_hl)}&gl={quote_plus(self.locale_gl)}&ceid={quote_plus(self.locale_ceid)}"
        )

    def fetch(self, topic: str, limit: int = 25) -> List[SourceItem]:
        url = self._build_url(topic)
        headers = {
            "User-Agent": "internmailer-market-sentiment/1.0 (+https://localhost)",
            "Accept": "application/rss+xml, application/xml;q=0.9, */*;q=0.8",
        }
        resp = self.session.get(url, headers=headers, timeout=20)
        if resp.status_code >= 400:
            raise RuntimeError(f"{self.name}: HTTP {resp.status_code}")
        return parse_rss_or_atom(resp.text, topic=topic, source=self.name, limit=limit)


def parse_rss_or_atom(xml_text: str, topic: str, source: str, limit: int = 25) -> List[SourceItem]:
    """
    Parse a basic RSS or Atom feed into SourceItems.
    """
    if not (xml_text or "").strip():
        return []

    try:
        root = ET.fromstring(xml_text)
    except Exception:
        return []

    tag = _local_name(root.tag).lower()
    items: List[SourceItem] = []

    if tag == "rss":
        channel = root.find("./channel")
        if channel is None:
            channel = root
        for it in channel.findall("./item"):
            title = (it.findtext("title") or "").strip()
            link = (it.findtext("link") or "").strip()
            desc = (it.findtext("description") or "").strip()
            pub = (it.findtext("pubDate") or "").strip()
            published_at = _parse_datetime(pub)
            if not link:
                continue
            items.append(
                SourceItem(
                    topic=topic,
                    title=_strip_html(title),
                    url=link,
                    summary=_strip_html(desc),
                    published_at=published_at,
                    source=source,
                    raw={"pubDate": pub},
                )
            )
            if len(items) >= limit:
                break
        return items

    # Atom (<feed><entry>...)
    if tag == "feed":
        for entry in root.findall(".//{*}entry"):
            title = (entry.findtext("{*}title") or "").strip()
            summary = (entry.findtext("{*}summary") or entry.findtext("{*}content") or "").strip()
            updated = (entry.findtext("{*}updated") or entry.findtext("{*}published") or "").strip()
            published_at = _parse_datetime(updated)
            link = ""
            for link_el in entry.findall("{*}link"):
                href = link_el.attrib.get("href", "").strip()
                if href:
                    link = href
                    break
            if not link:
                continue
            items.append(
                SourceItem(
                    topic=topic,
                    title=_strip_html(title),
                    url=link,
                    summary=_strip_html(summary),
                    published_at=published_at,
                    source=source,
                    raw={"updated": updated},
                )
            )
            if len(items) >= limit:
                break
        return items

    # Unknown root; try a best-effort RSS item scan
    for it in root.findall(".//item"):
        title = (it.findtext("title") or "").strip()
        link = (it.findtext("link") or "").strip()
        desc = (it.findtext("description") or "").strip()
        pub = (it.findtext("pubDate") or "").strip()
        if not link:
            continue
        items.append(
            SourceItem(
                topic=topic,
                title=_strip_html(title),
                url=link,
                summary=_strip_html(desc),
                published_at=_parse_datetime(pub),
                source=source,
                raw={"pubDate": pub},
            )
        )
        if len(items) >= limit:
            break
    return items


class LexiconSentimentScorer:
    """
    Simple lexicon-based scorer with a finance-ish vocabulary.

    Output score is in [-1, 1], confidence in [0, 1].
    """

    def __init__(self):
        self.positive = {
            "beat",
            "beats",
            "beating",
            "surge",
            "surges",
            "soar",
            "soars",
            "rally",
            "rallies",
            "record",
            "upgrade",
            "upgraded",
            "upgrades",
            "outperform",
            "outperforms",
            "strong",
            "growth",
            "profit",
            "profits",
            "bull",
            "bullish",
            "buyback",
            "dividend",
            "guidance raised",
            "raises guidance",
            "expands",
            "expansion",
            "partnership",
            "win",
            "wins",
            "approval",
            "approved",
        }
        self.negative = {
            "miss",
            "misses",
            "missing",
            "drop",
            "drops",
            "plunge",
            "plunges",
            "slump",
            "slumps",
            "downgrade",
            "downgraded",
            "downgrades",
            "underperform",
            "weak",
            "decline",
            "loss",
            "losses",
            "bear",
            "bearish",
            "cut",
            "cuts",
            "guidance cut",
            "cuts guidance",
            "layoff",
            "layoffs",
            "lawsuit",
            "probe",
            "investigation",
            "fraud",
            "default",
            "bankruptcy",
            "halt",
            "halts",
        }
        self.negations = {"not", "no", "never", "without", "hardly"}

        # Precompile for speed and consistent tokenization.
        self._token_re = re.compile(r"[a-zA-Z][a-zA-Z0-9\.\-']+")

    def score(self, text: str) -> Tuple[float, float, Dict[str, Any]]:
        text = (text or "").strip()
        if not text:
            return 0.0, 0.0, {"pos": 0, "neg": 0, "matches": []}

        lower = text.lower()
        tokens = [t.lower() for t in self._token_re.findall(lower)]
        matches: List[str] = []

        pos = 0
        neg = 0

        # Phrase matches first (space-containing terms).
        for phrase in (p for p in self.positive if " " in p):
            if phrase in lower:
                pos += 1
                matches.append(f"+{phrase}")
        for phrase in (p for p in self.negative if " " in p):
            if phrase in lower:
                neg += 1
                matches.append(f"-{phrase}")

        # Token matches with a tiny negation window.
        for i, tok in enumerate(tokens):
            window = tokens[max(i - 2, 0) : i]
            is_negated = any(w in self.negations for w in window)
            if tok in self.positive:
                if is_negated:
                    neg += 1
                    matches.append(f"-not {tok}")
                else:
                    pos += 1
                    matches.append(f"+{tok}")
            elif tok in self.negative:
                if is_negated:
                    pos += 1
                    matches.append(f"+not {tok}")
                else:
                    neg += 1
                    matches.append(f"-{tok}")

        total = pos + neg
        if total == 0:
            return 0.0, 0.0, {"pos": 0, "neg": 0, "matches": []}

        # Normalize and clamp.
        score = (pos - neg) / total
        score = max(-1.0, min(1.0, score))

        # Confidence: monotonic with evidence; saturates quickly.
        confidence = min(1.0, math.log1p(total) / math.log(1 + 6))

        return float(score), float(confidence), {"pos": pos, "neg": neg, "matches": matches[:12]}


class TopicAggregator:
    def __init__(self, neutral_band: float = 0.1):
        self.neutral_band = float(neutral_band)

    def aggregate(
        self,
        items: List[SentimentItem],
        window_hours: int,
        now: Optional[datetime] = None,
    ) -> SentimentSnapshot:
        if not items:
            now = now or _utc_now()
            return SentimentSnapshot(
                topic="",
                score=0.0,
                positive=0,
                negative=0,
                neutral=0,
                item_count=0,
                window_hours=int(window_hours),
                computed_at=now,
                data={"weights_sum": 0.0},
            )

        now = now or _utc_now()
        half_life = max(window_hours / 2.0, 1.0)

        def weight(dt: datetime) -> float:
            age_h = max((now - dt).total_seconds() / 3600.0, 0.0)
            return math.exp(-age_h / half_life)

        wsum = 0.0
        ssum = 0.0
        pos = neg = neu = 0

        for it in items:
            w = weight(it.published_at) * max(min(it.confidence, 1.0), 0.0)
            if w <= 0:
                continue
            wsum += w
            ssum += float(it.sentiment) * w
            if it.sentiment > self.neutral_band:
                pos += 1
            elif it.sentiment < -self.neutral_band:
                neg += 1
            else:
                neu += 1

        score = (ssum / wsum) if wsum else 0.0
        return SentimentSnapshot(
            topic=items[0].topic,
            score=float(max(-1.0, min(1.0, score))),
            positive=int(pos),
            negative=int(neg),
            neutral=int(neu),
            item_count=int(len(items)),
            window_hours=int(window_hours),
            computed_at=now,
            data={"weights_sum": wsum, "half_life_hours": half_life},
        )


class MarketSentimentEngine:
    """
    Coordinates source adapters + scoring + persistence.

    This acts like a mini "sub-agent" system: each source adapter is isolated,
    and the engine aggregates their outputs.
    """

    def __init__(
        self,
        store: Optional[SentimentStore] = None,
        sources: Optional[List[SourceAdapter]] = None,
        scorer: Optional[LexiconSentimentScorer] = None,
        aggregator: Optional[TopicAggregator] = None,
    ):
        self.store = store or SentimentStore()
        self.sources = sources or [GoogleNewsRssSource()]
        self.scorer = scorer or LexiconSentimentScorer()
        self.aggregator = aggregator or TopicAggregator()

    def refresh(
        self,
        topics: List[str],
        window_hours: int = 24,
        max_items_per_topic: int = 25,
    ) -> Dict[str, Dict[str, Any]]:
        started = _utc_now()
        topics = [t.strip() for t in (topics or []) if (t or "").strip()]
        if not topics:
            return {}

        results: Dict[str, Dict[str, Any]] = {}
        run_inserted_total = 0
        run_scored_total = 0
        run_error_total = 0
        run_topics_without_data = 0
        run_confidence_values: List[float] = []

        for topic in topics:
            fetched_at = _utc_now()
            source_items: List[SourceItem] = []
            errors: List[str] = []

            for src in self.sources:
                try:
                    source_items.extend(src.fetch(topic, limit=max_items_per_topic))
                except Exception as e:
                    errors.append(f"{getattr(src, 'name', 'source')}: {e}")
            run_error_total += len(errors)

            # Deduplicate by URL.
            seen = set()
            deduped: List[SourceItem] = []
            for it in source_items:
                u = (it.url or "").strip()
                if not u or u in seen:
                    continue
                seen.add(u)
                deduped.append(it)
            duplicate_count = max(len(source_items) - len(deduped), 0)

            sentiment_items: List[SentimentItem] = []
            for it in deduped[:max_items_per_topic]:
                text = f"{it.title}\n\n{it.summary}".strip()
                score, confidence, meta = self.scorer.score(text)
                sentiment_items.append(
                    SentimentItem(
                        item_id=_sha1(f"{topic}|{it.url}"),
                        topic=topic,
                        source=it.source,
                        title=it.title[:500],
                        url=it.url,
                        summary=it.summary[:2000],
                        published_at=it.published_at,
                        fetched_at=fetched_at,
                        sentiment=float(score),
                        confidence=float(confidence),
                        raw={"meta": meta, **(it.raw or {})},
                    )
                )
                run_confidence_values.append(float(confidence))

            if not sentiment_items:
                run_topics_without_data += 1

            inserted = self.store.upsert_items(sentiment_items)
            run_inserted_total += inserted
            run_scored_total += len(sentiment_items)
            snapshot = self.aggregator.aggregate(sentiment_items, window_hours=window_hours, now=_utc_now())
            # Ensure topic set (aggregate() uses items[0], but empty-case returns topic="").
            score_values = [float(item.sentiment) for item in sentiment_items]
            confidence_values = [float(item.confidence) for item in sentiment_items]
            positive_count = sum(1 for value in score_values if value > 0.15)
            negative_count = sum(1 for value in score_values if value < -0.15)
            neutral_count = max(len(score_values) - positive_count - negative_count, 0)
            average_confidence = statistics.fmean(confidence_values) if confidence_values else 0.0
            governance = {
                "source_count": len(self.sources),
                "source_errors": len(errors),
                "fetched_item_count": len(source_items),
                "deduped_item_count": len(deduped),
                "duplicate_item_count": duplicate_count,
                "duplicate_ratio": round((duplicate_count / max(len(source_items), 1)), 4),
                "scored_item_count": len(sentiment_items),
                "inserted_item_count": inserted,
                "missing_data": len(sentiment_items) == 0,
            }
            model_metrics = {
                "average_confidence": round(average_confidence, 4),
                "max_confidence": round(max(confidence_values), 4) if confidence_values else 0.0,
                "min_confidence": round(min(confidence_values), 4) if confidence_values else 0.0,
                "positive_item_count": positive_count,
                "negative_item_count": negative_count,
                "neutral_item_count": neutral_count,
            }
            snapshot = SentimentSnapshot(
                topic=topic,
                score=snapshot.score,
                positive=snapshot.positive,
                negative=snapshot.negative,
                neutral=snapshot.neutral,
                item_count=snapshot.item_count,
                window_hours=snapshot.window_hours,
                computed_at=snapshot.computed_at,
                data={
                    **(snapshot.data or {}),
                    "source_count": len(self.sources),
                    "errors": errors,
                    "inserted": inserted,
                    "governance": governance,
                    "model_metrics": model_metrics,
                },
            )
            self.store.save_snapshot(snapshot)

            results[topic] = {
                "topic": topic,
                "score": snapshot.score,
                "positive": snapshot.positive,
                "negative": snapshot.negative,
                "neutral": snapshot.neutral,
                "item_count": snapshot.item_count,
                "window_hours": snapshot.window_hours,
                "computed_at": snapshot.computed_at.isoformat(),
                "data": snapshot.data,
            }

        self.store.log_run(
            topics=topics,
            status="success",
            message="refresh complete",
            started_at=started,
            finished_at=_utc_now(),
            stats={
                "topics_requested": len(topics),
                "topics_without_data": run_topics_without_data,
                "items_scored": run_scored_total,
                "items_inserted": run_inserted_total,
                "source_error_count": run_error_total,
                "average_model_confidence": (
                    round(statistics.fmean(run_confidence_values), 4) if run_confidence_values else 0.0
                ),
            },
        )
        return results

    def get_snapshot(self, topics: Optional[List[str]] = None) -> Dict[str, Any]:
        topics = [t.strip() for t in (topics or []) if (t or "").strip()]
        if not topics:
            topics = self.store.list_topics()
        out: Dict[str, Any] = {}
        for topic in topics:
            out[topic] = self.store.get_latest_snapshot(topic) or {}
        return out
