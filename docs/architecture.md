# Architecture

The app is intentionally split into three layers:

## 1. Ingestion and scoring

`core/market_sentiment.py` fetches public RSS feeds and scores headline text.

## 2. Persistence

`core/sentiment_store.py` stores scored items and snapshot history in SQLite.

Every refresh also records governance and quality-monitoring metadata:

- per-topic ingestion quality diagnostics (`fetched_item_count`, dedupe counts/ratio, source errors)
- per-topic model metrics (confidence summary and class distribution)
- run-level reproducibility stats in `sentiment_runs.stats_json` (topics requested, inserted/scored totals, average confidence)

## 3. Presentation

`app.py` and the templates expose the dashboard and JSON API.

## Design goals

- keep the code simple enough to read in one sitting
- keep the data model persistent
- keep the UI explainable in a demo
