.PHONY: help setup install lint format type test test-e2e ingest silver gold benchmark ml-train clean docker-up docker-down dagster-dev dbt-deps dbt-run dbt-test gx docs docs-serve

PYTHON ?= python
UV ?= uv
YEAR ?= 2024
MONTH ?= 01

help:
	@echo "GHArchive Lakehouse — Makefile targets"
	@echo ""
	@echo "  setup            Install deps with uv and start docker-compose stack"
	@echo "  install          Install Python deps (incl. dev) with uv"
	@echo "  lint             Run ruff (lint + format check)"
	@echo "  format           Run ruff format and ruff --fix"
	@echo "  type             Run mypy strict"
	@echo "  test             Run pytest (unit + integration) with coverage"
	@echo "  ingest YEAR=YYYY MONTH=MM   Backfill Bronze for one month"
	@echo "  silver           Run Bronze -> Silver transformations"
	@echo "  gold             Run dbt models (Silver -> Gold)"
	@echo "  benchmark        Run performance benchmark suite"
	@echo "  ml-train         Train churn model and log to MLflow"
	@echo "  dagster-dev      Start Dagster UI on :3000"
	@echo "  docker-up        Start docker-compose services"
	@echo "  docker-down      Stop docker-compose services"
	@echo "  clean            Remove caches and build artifacts"

setup: install docker-up
	@echo "Setup complete."

install:
	$(UV) sync --all-extras

lint:
	$(UV) run ruff check .
	$(UV) run ruff format --check .

format:
	$(UV) run ruff format .
	$(UV) run ruff check --fix .

type:
	$(UV) run mypy src

test:
	$(UV) run pytest

test-e2e:
	$(UV) run pytest -m e2e --override-ini="addopts=-ra --strict-markers --strict-config"

ingest:
	$(UV) run python -m src.ingestion.run --year $(YEAR) --month $(MONTH)

silver:
	$(UV) run python -m src.transformation.run

gold: dbt-deps
	$(UV) run python -m src.transformation.gold build

dbt-deps:
	cd dbt && $(UV) run dbt deps

dbt-run:
	$(UV) run python -m src.transformation.gold run

dbt-test:
	$(UV) run python -m src.transformation.gold test

gx:
	$(UV) run python -m src.quality.run_checkpoints

benchmark:
	$(UV) run python -m src.benchmarks.run

ml-train:
	$(UV) run python -m src.ml.train

dagster-dev:
	$(UV) run dagster dev -m dagster_project.definitions

docker-up:
	docker compose up -d

docker-down:
	docker compose down

docs:
	pip install -q "mkdocs-material>=9.5" "pymdown-extensions>=10.7" && mkdocs build --strict

docs-serve:
	pip install -q "mkdocs-material>=9.5" "pymdown-extensions>=10.7" && mkdocs serve

clean:
	rm -rf .pytest_cache .mypy_cache .ruff_cache .coverage coverage.xml htmlcov
	find . -type d -name "__pycache__" -prune -exec rm -rf {} +
	find . -type d -name "*.egg-info" -prune -exec rm -rf {} +
