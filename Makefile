PYTHON ?= python3

.PHONY: run test lint serve

run:
	$(PYTHON) app.py

test:
	$(PYTHON) -m pytest -q

lint:
	$(PYTHON) -m py_compile app.py core/market_sentiment.py core/sentiment_store.py

serve:
	$(PYTHON) -m gunicorn wsgi:app
