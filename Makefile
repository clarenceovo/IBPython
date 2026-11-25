PYTHON ?= python3.14
VENV ?= .venv
PY := $(VENV)/bin/python
API_HOST ?= 0.0.0.0
API_PORT ?= 8000
API_LOOP ?= asyncio
API_APP ?= src.webapp.app:get_app
API_LOG_LEVEL ?= info

.PHONY: venv install install-dev test lint typecheck services-down notebook run-api run-api-dev docker-config docker-build docker-up docker-up-api docker-up-mcp docker-logs-api docker-logs-gateway

venv:
	$(PYTHON) -m venv $(VENV)

install: venv
	$(PY) -m pip install --upgrade pip
	$(PY) -m pip install -r requirements.txt

install-dev: venv
	$(PY) -m pip install --upgrade pip
	$(PY) -m pip install -r requirements-dev.txt
	$(PY) -m ipykernel install --user --name ibpython-market-data --display-name "IBPython Market Data"

test:
	$(PY) -m pytest

lint:
	$(PY) -m ruff check src tests

typecheck:
	$(PY) -m mypy src/feeds/data_quality.py src/feeds/ohlcv_loader.py src/feeds/ibkr_historical.py src/feeds/snapshot_models.py src/webapp/routers/market_data_shared.py src/transport/ibkr_rate_limit.py

services-down:
	docker compose down

notebook:
	$(PY) -m jupyter lab notebooks

run-api:
	@echo "Starting IBKRRestApp with $(PY) on $(API_HOST):$(API_PORT) using loop=$(API_LOOP)"
	$(PY) -m uvicorn $(API_APP) --host $(API_HOST) --port $(API_PORT) --factory --loop $(API_LOOP) --lifespan on --log-level $(API_LOG_LEVEL)

run-api-dev:
	@echo "Starting IBKRRestApp dev server with $(PY) on $(API_HOST):$(API_PORT) using loop=$(API_LOOP)"
	$(PY) -m uvicorn $(API_APP) --host $(API_HOST) --port $(API_PORT) --factory --reload --loop $(API_LOOP) --lifespan on --log-level $(API_LOG_LEVEL)

docker-config:
	docker compose config --quiet

docker-build:
	docker compose build ibkr-rest-app mcp-server

docker-up:
	docker compose up -d --build

docker-up-api:
	docker compose up -d --build ib-gateway ibkr-rest-app

docker-up-mcp:
	docker compose up -d --build ib-gateway mcp-server

docker-logs-api:
	docker compose logs -f ibkr-rest-app

docker-logs-gateway:
	docker compose logs -f ib-gateway
