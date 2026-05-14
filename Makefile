PYTHON ?= python3.13
VENV ?= .venv
PIP := $(VENV)/bin/pip
PY := $(VENV)/bin/python

.PHONY: venv install install-dev test services-up services-down notebook run

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
