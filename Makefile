PYTHON ?= python3.13
VENV ?= .venv
PIP := $(VENV)/bin/pip
PY := $(VENV)/bin/python
API_HOST ?= 0.0.0.0
API_PORT ?= 8000
API_LOOP ?= asyncio
API_APP ?= src.webapp.app:get_app
API_LOG_LEVEL ?= info

.PHONY: venv install install-dev test lint typecheck services-up services-down notebook run validate-scheduler run-api run-api-dev docker-build docker-up docker-up-api docker-up-scheduler docker-logs-api docker-logs-scheduler

venv:
	$(PYTHON) -m venv $(VENV)

install: venv
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements.txt

install-dev: venv
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements-dev.txt
	$(PY) -m ipykernel install --user --name ibpython-market-data --display-name "IBPython Market Data"

test:
	$(PY) -m pytest

lint:
	$(PY) -m ruff check src tests

typecheck:
	$(PY) -m mypy src/feeds/data_quality.py src/feeds/ohlcv_loader.py src/feeds/ibkr_historical.py src/feeds/snapshot_models.py src/webapp/routers/market_data_shared.py src/transport/ibkr_rate_limit.py

services-up:
	docker compose up -d redis questdb

services-down:
	docker compose down

notebook:
	$(PY) -m jupyter lab notebooks

run:
	$(PY) main.py

validate-scheduler:
	$(PY) scripts/validate_scheduler_jobs.py --schedule-dir schedulejob

run-api:
	@echo "Starting IBKRRestApp with $(PY) on $(API_HOST):$(API_PORT) using loop=$(API_LOOP)"
	$(PY) -m uvicorn $(API_APP) --host $(API_HOST) --port $(API_PORT) --factory --loop $(API_LOOP) --lifespan on --log-level $(API_LOG_LEVEL)

run-api-dev:
	@echo "Starting IBKRRestApp dev server with $(PY) on $(API_HOST):$(API_PORT) using loop=$(API_LOOP)"
	$(PY) -m uvicorn $(API_APP) --host $(API_HOST) --port $(API_PORT) --factory --reload --loop $(API_LOOP) --lifespan on --log-level $(API_LOG_LEVEL)

docker-build:
	docker compose build ibkr-rest-app ibkr-scheduler

docker-up:
	docker compose up -d redis questdb mysql ibkr-rest-app ibkr-scheduler

docker-up-api:
	docker compose up -d redis questdb mysql ibkr-rest-app

docker-up-scheduler:
	docker compose up -d redis questdb mysql ibkr-scheduler

docker-logs-api:
	docker compose logs -f ibkr-rest-app

docker-logs-scheduler:
	docker compose logs -f ibkr-scheduler
