# Testing

Run the full suite with:

```bash
python3 -m pytest -q
```

Run a quick syntax check with:

```bash
python3 -m py_compile app.py core/market_sentiment.py core/sentiment_store.py
```

The tests cover:

- scoring directionality
- item deduplication
- snapshot persistence
- Flask endpoints and dashboard rendering
