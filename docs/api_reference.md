# API Reference

## `GET /sentiment`
Renders the dashboard.

## `GET /api/sentiment/snapshot`
Returns the combined sentiment snapshot.

Optional query:

- `topics=SPY,BTC`

## `POST /api/sentiment/refresh`
Refreshes the watchlist.

Example:

```json
{
  "topics": ["SPY", "BTC"],
  "sync": true
}
```

## `GET /api/sentiment/history`
Returns snapshot history from SQLite.

## `GET /api/sentiment/health`
Returns engine and database status, including the latest snapshot metadata and helpful docs links.

## `GET /api/sentiment/export`
Returns a JSON bundle of the snapshot, health, and history.

Example response fields:

- `status`
- `engine_name`
- `database_path`
- `snapshot_age_minutes`
- `docs.deployment`
- `docs.api`
