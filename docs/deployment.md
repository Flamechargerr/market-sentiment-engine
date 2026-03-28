# Deployment Guide

This app is designed to run as a small Python web service.

## Recommended options

### Local development

```bash
python3 app.py
```

### Gunicorn

```bash
gunicorn wsgi:app
```

### Docker

```bash
docker build -t market-sentiment-engine .
docker run -p 5055:5055 -e PORT=5055 market-sentiment-engine
```

### Render

Use the included `render.yaml` service definition. The project is small enough to deploy on a free plan for demos.

Suggested production settings:

- `PORT=5055`
- `MARKET_SENTIMENT_DB_PATH=/var/data/market_sentiment.db`
- `MARKET_SENTIMENT_TOPICS=SPY,NASDAQ,NIFTY,BTC`
- `FLASK_DEBUG=0`

For a demo deployment, use a persistent disk for the SQLite file and a scheduled refresh job if you want the dashboard to stay current without manual clicks.

## Notes

- Keep `MARKET_SENTIMENT_DB_PATH` on persistent storage in production.
- Schedule refreshes externally if you want automatic updates.
- The app is lightweight enough to run on a small instance.
