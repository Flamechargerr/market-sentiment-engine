#!/usr/bin/env bash
set -euo pipefail

TOPICS="${1:-SPY,BTC}"
OUT="${2:-snapshot.json}"

curl -s "http://127.0.0.1:5055/api/sentiment/export?topics=${TOPICS}" > "${OUT}"
echo "Wrote ${OUT}"
