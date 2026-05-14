PYTHON ?= python3.13
VENV ?= .venv
PIP := $(VENV)/bin/pip
PY := $(VENV)/bin/python
API_HOST ?= 0.0.0.0
API_PORT ?= 8000
API_LOOP ?= asyncio
API_APP ?= src.webapp.app:get_app
API_LOG_LEVEL ?= info

.PHONY: venv install install-dev test services-up services-down notebook run run-api run-api-dev docker-build docker-up

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

services-up:
	docker compose up -d redis questdb

services-down:
	docker compose down

notebook:
	$(PY) -m jupyter lab notebooks

run:
	$(PY) main.py

run-api:
	@echo "Starting IBKRRestApp with $(PY) on $(API_HOST):$(API_PORT) using loop=$(API_LOOP)"
	$(PY) -m uvicorn $(API_APP) --host $(API_HOST) --port $(API_PORT) --factory --loop $(API_LOOP) --lifespan on --log-level $(API_LOG_LEVEL)

run-api-dev:
	@echo "Starting IBKRRestApp dev server with $(PY) on $(API_HOST):$(API_PORT) using loop=$(API_LOOP)"
	$(PY) -m uvicorn $(API_APP) --host $(API_HOST) --port $(API_PORT) --factory --reload --loop $(API_LOOP) --lifespan on --log-level $(API_LOG_LEVEL)

docker-build:
	docker compose build ibkr-rest-app

docker-up:
	docker compose up -d redis questdb ibkr-rest-app
