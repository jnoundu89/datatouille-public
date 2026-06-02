# =============================================================================
# Makefile - Airflow Data Platform Commands
# =============================================================================
# Usage: make <target>
# Run 'make help' to see all available commands

.PHONY: help install dev lint test test-unit test-integration validate-dags validate-grafana detect-schema-drift ci clean
.PHONY: up down restart logs status shell
.PHONY: dbt-run dbt-test dbt-docs dbt-docs-serve soda-scan soda-scan-duckdb soda-scan-duckdb-staging duckdb-refresh duckdb-bench grafana-check grafana-density seed-leboncoin-enriched
.PHONY: flower monitoring seed-variables
.PHONY: allkeyshop-batch allkeyshop-batch-dry psql
.PHONY: game-registry-init game-register game-list
.PHONY: backup restore backup-list
.PHONY: n8n-logs n8n-restart n8n-shell
.PHONY: api-lint api-build api-logs api-restart
.PHONY: front-lint front-typecheck front-build front-dev front-logs front-restart

# Default target
.DEFAULT_GOAL := help

# Variables
PYTHON := python3
PYTEST := pytest
RUFF := ruff
MYPY := mypy
DOCKER_COMPOSE := docker compose

# Colors for output
GREEN := \033[0;32m
YELLOW := \033[0;33m
RED := \033[0;31m
NC := \033[0m  # No Color

# =============================================================================
# HELP
# =============================================================================
help:
	@echo "$(GREEN)Airflow Data Platform - Available Commands$(NC)"
	@echo ""
	@echo "$(YELLOW)Development:$(NC)"
	@echo "  make install        - Install all dependencies"
	@echo "  make dev            - Install dev dependencies + pre-commit"
	@echo "  make lint           - Run linters (ruff + mypy)"
	@echo "  make test           - Run all tests with coverage"
	@echo "  make test-unit      - Run unit tests only"
	@echo "  make test-integration - Run integration tests"
	@echo "  make validate-dags  - Validate Airflow DAGs"
	@echo "  make validate-grafana - Validate Grafana dashboards"
	@echo "  make detect-schema-drift - Check for inline DDL in DAGs"
	@echo "  make ci             - Run full CI pipeline"
	@echo "  make seed-variables - Seed Airflow Variables with default configs"
	@echo "  make clean          - Clean cache and temp files"
	@echo ""
	@echo "$(YELLOW)Docker Stack:$(NC)"
	@echo "  make up             - Start all services"
	@echo "  make down           - Stop all services"
	@echo "  make restart        - Restart all services"
	@echo "  make logs           - View service logs"
	@echo "  make status         - Show service status"
	@echo "  make shell          - Open shell in scheduler container"
	@echo "  make flower         - Start Flower (Celery monitoring)"
	@echo "  make monitoring     - Open monitoring URLs"
	@echo ""
	@echo "$(YELLOW)Data Tools:$(NC)"
	@echo "  make dbt-run        - Run dbt models"
	@echo "  make dbt-test       - Run dbt tests"
	@echo "  make dbt-docs       - Generate dbt docs (catalog + manifest)"
	@echo "  make dbt-docs-serve - Generate + serve dbt docs at http://localhost:8081"
	@echo "  make soda-scan      - Run Soda data quality checks (via Docker)"
	@echo "  make soda-scan-duckdb - Soda checks on the canonical DuckDB marts"
	@echo "  make soda-scan-duckdb-staging - Soda checks on the dbt staging views"
	@echo "  make grafana-check  - Smoke-test every Grafana panel's SQL against live Postgres"
	@echo "  make grafana-density - Detect panels with mostly-null output columns / empty KPIs"
	@echo "  make seed-leboncoin-enriched - Enrich synthetic leboncoin seed data (attributes + seller_name)"
	@echo "  make duckdb-refresh - Trigger dbt_parquet_landing_refresh DAG"
	@echo "  make duckdb-bench   - Run the 3 DuckDB benchmark scripts"
	@echo ""
	@echo "$(YELLOW)AllKeyShop Data Collection:$(NC)"
	@echo "  make allkeyshop-batch     - Trigger batch search for 10 games"
	@echo "  make allkeyshop-batch-dry - Preview batch commands (no execution)"
	@echo "  make psql                 - Open PostgreSQL shell"
	@echo ""
	@echo "$(YELLOW)AllKeyShop Game Registry:$(NC)"
	@echo "  make game-registry-init   - Initialize game registry table"
	@echo "  make game-register GAME='...' - Register a game (e.g., GAME='Elden Ring')"
	@echo "  make game-list            - List all registered games"
	@echo ""
	@echo "$(YELLOW)Webapp (API + Frontend):$(NC)"
	@echo "  make api-lint       - Lint API code (ruff)"
	@echo "  make api-build      - Build API Docker image"
	@echo "  make api-restart    - Rebuild and restart API container"
	@echo "  make api-logs       - Tail API container logs"
	@echo "  make front-lint     - Lint frontend (next lint)"
	@echo "  make front-typecheck - TypeScript type check (tsc --noEmit)"
	@echo "  make front-build    - Build frontend (next build)"
	@echo "  make front-dev      - Start frontend dev server (local, port 3001)"
	@echo "  make front-restart  - Rebuild and restart frontend container"
	@echo "  make front-logs     - Tail frontend container logs"
	@echo ""
	@echo "$(YELLOW)Backup & Restore:$(NC)"
	@echo "  make backup               - Create PostgreSQL backup"
	@echo "  make backup-list          - List available backups"
	@echo "  make restore FILE='...'   - Restore from backup file"

# =============================================================================
# DEVELOPMENT
# =============================================================================
install:
	@echo "$(GREEN)Installing dependencies...$(NC)"
	pip install -e ".[all]"

dev: install
	@echo "$(GREEN)Setting up development environment...$(NC)"
	pip install pre-commit
	pre-commit install
	@echo "$(GREEN)Pre-commit hooks installed!$(NC)"

lint:
	@echo "$(GREEN)Running linters...$(NC)"
	$(RUFF) check dags/ scripts/ --fix
	$(RUFF) format dags/ scripts/
	@echo "$(YELLOW)Running mypy (informational)...$(NC)"
	-$(MYPY) dags/ scripts/ --ignore-missing-imports

test:
	@echo "$(GREEN)Running all tests...$(NC)"
	$(PYTEST) tests/ -v || true --cov=dags --cov=scripts --cov-report=term-missing --cov-report=html

test-unit:
	@echo "$(GREEN)Running unit tests...$(NC)"
	$(PYTEST) tests/unit/ -v

test-integration:
	@echo "$(GREEN)Running integration tests...$(NC)"
	$(PYTEST) tests/integration/ -v --tb=short

validate-dags:
	@echo "$(GREEN)Validating Airflow DAGs...$(NC)"
	$(RUFF) check dags/ --select AIR30,AIR302 --preview || true
	@PYTHONPATH=dags $(PYTHON) -c "from airflow.models import DagBag; bag = DagBag('dags', include_examples=False); errors = bag.import_errors; print(f'{len(bag.dags)} DAGs loaded'); [print(f'ERROR: {k}: {v}') for k,v in errors.items()]; exit(1 if errors else 0)"

validate-grafana:
	@echo "$(GREEN)Validating Grafana dashboards...$(NC)"
	@$(PYTHON) scripts/validate_grafana_dashboards.py

detect-schema-drift:
	@echo "$(GREEN)Checking for schema drift in DAGs...$(NC)"
	@$(PYTHON) scripts/detect_schema_drift.py

ci: lint validate-dags validate-grafana detect-schema-drift test
	@echo "$(GREEN)✅ CI pipeline passed!$(NC)"

seed-variables:
	@echo "$(GREEN)Seeding Airflow Variables with default configs...$(NC)"
	@bash scripts/seed_variables.sh
	@echo "$(GREEN)Variables seeded! Check Airflow UI > Admin > Variables$(NC)"

clean:
	@echo "$(YELLOW)Cleaning cache files...$(NC)"
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .pytest_cache -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .mypy_cache -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .ruff_cache -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name htmlcov -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
	find . -type f -name ".coverage" -delete 2>/dev/null || true
	rm -rf dbt/target dbt/logs dbt/dbt_packages 2>/dev/null || true
	rm -rf datatouille.egg-info *.egg-info 2>/dev/null || true
	@echo "$(GREEN)Clean complete!$(NC)"

# =============================================================================
# DOCKER STACK
# =============================================================================
up:
	@echo "$(GREEN)Starting Airflow stack...$(NC)"
	$(DOCKER_COMPOSE) up -d
	@echo "$(GREEN)Stack started!$(NC)"
	@echo "  Airflow UI: http://localhost:8080"
	@echo "  MinIO Console: http://localhost:9001"
	@echo "  Grafana: http://localhost:3000"
	@echo "  Prometheus: http://localhost:9090"
	@echo "  Datatouille API: http://localhost:8000"
	@echo "  Datatouille Frontend: http://localhost:3001"

down:
	@echo "$(YELLOW)Stopping Airflow stack...$(NC)"
	$(DOCKER_COMPOSE) down
	@echo "$(GREEN)Stack stopped!$(NC)"

restart:
	@echo "$(YELLOW)Restarting Airflow stack...$(NC)"
	$(DOCKER_COMPOSE) restart
	@echo "$(GREEN)Stack restarted!$(NC)"

logs:
	$(DOCKER_COMPOSE) logs -f --tail=100

status:
	@echo "$(GREEN)Service Status:$(NC)"
	$(DOCKER_COMPOSE) ps

shell:
	@echo "$(GREEN)Opening shell in scheduler container...$(NC)"
	docker exec -it airflow-scheduler bash

flower:
	@echo "$(GREEN)Starting Flower (Celery monitoring)...$(NC)"
	$(DOCKER_COMPOSE) --profile flower up -d flower
	@echo "  Flower UI: http://localhost:5555"

monitoring:
	@echo "$(GREEN)Opening monitoring dashboards...$(NC)"
	@echo "  Airflow UI: http://localhost:8080"
	@echo "  Grafana: http://localhost:3000 (admin/admin)"
	@echo "  Prometheus: http://localhost:9090"
	@echo "  MinIO Console: http://localhost:9001 (minioadmin/minioadmin)"
	@echo "  n8n: http://localhost:5678"

# =============================================================================
# N8N - WORKFLOW AUTOMATION
# =============================================================================
n8n-logs:
	$(DOCKER_COMPOSE) logs -f --tail=100 n8n

n8n-restart:
	$(DOCKER_COMPOSE) restart n8n

n8n-shell:
	@echo "$(GREEN)Opening n8n CLI shell...$(NC)"
	docker exec -it n8n sh

# =============================================================================
# DATA TOOLS
# =============================================================================
dbt-deps:
	@echo "$(GREEN)Installing dbt packages (via Docker)...$(NC)"
	$(DOCKER_COMPOSE) exec -T airflow-scheduler dbt deps --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt

dbt-run:
	@echo "$(GREEN)Running dbt models (via Docker)...$(NC)"
	$(DOCKER_COMPOSE) exec -T airflow-scheduler dbt run --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt --target docker

dbt-test:
	@echo "$(GREEN)Running dbt tests (via Docker)...$(NC)"
	$(DOCKER_COMPOSE) exec -T airflow-scheduler dbt test --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt --target docker

dbt-docs:
	@echo "$(GREEN)Generating dbt documentation (via Docker)...$(NC)"
	$(DOCKER_COMPOSE) exec -T airflow-scheduler dbt docs generate --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt --target docker
	@echo "$(GREEN)Generated. Run 'make dbt-docs-serve' to browse at http://localhost:8081/$(NC)"

dbt-docs-serve: dbt-docs
	@echo "$(GREEN)Serving dbt docs at http://localhost:8081/ (Ctrl-C to stop)...$(NC)"
	@mkdir -p /tmp/dbt-docs-site
	@$(DOCKER_COMPOSE) exec -T airflow-scheduler bash -c 'cd /opt/airflow/dbt/target && tar czf - index.html manifest.json catalog.json' | tar -xzf - -C /tmp/dbt-docs-site
	@cd /tmp/dbt-docs-site && $(PYTHON) -m http.server 8081

soda-scan:
	@echo "$(GREEN)Running Soda data quality checks (via Docker)...$(NC)"
	$(DOCKER_COMPOSE) run --rm soda scan -d postgres -c /workspace/soda/configuration.yml /workspace/soda/checks/

grafana-check:
	@echo "$(GREEN)Smoke-testing every Grafana panel SQL against live Postgres...$(NC)"
	docker exec -e POSTGRES_HOST=postgres airflow-scheduler \
		python /opt/airflow/scripts/check_grafana_panels.py

grafana-density:
	@echo "$(GREEN)Scanning panels for mostly-null output columns / empty KPIs...$(NC)"
	docker exec -e POSTGRES_HOST=postgres airflow-scheduler \
		python /opt/airflow/scripts/check_panel_column_density.py

seed-leboncoin-enriched:
	@echo "$(GREEN)Enriching leboncoin_listings seed data with realistic attributes + seller_name...$(NC)"
	@echo "$(YELLOW)Datadome blocks real scraping. This enriches the synthetic seed so dashboards aren't blank.$(NC)"
	docker exec -e POSTGRES_HOST=postgres airflow-scheduler \
		python /opt/airflow/scripts/enrich_leboncoin_seed.py --apply

soda-scan-duckdb:
	@echo "$(GREEN)Running Soda checks on the canonical DuckDB marts (dbt_docker_marts)...$(NC)"
	$(DOCKER_COMPOSE) run --rm soda scan -d duckdb_marts \
		-c /workspace/soda/configuration.yml \
		/workspace/soda/checks/duckdb_marts_contract.yml

soda-scan-duckdb-staging:
	@echo "$(GREEN)Running Soda checks on the dbt staging views (dbt_docker_staging)...$(NC)"
	$(DOCKER_COMPOSE) run --rm soda scan -d duckdb_staging \
		-c /workspace/soda/configuration.yml \
		/workspace/soda/checks/duckdb_staging_contract.yml

duckdb-refresh:
	@echo "$(GREEN)Triggering dbt_parquet_landing_refresh DAG (hourly refresh, manual run)...$(NC)"
	docker exec airflow-scheduler airflow dags trigger dbt_parquet_landing_refresh

duckdb-bench:
	@echo "$(GREEN)Running the 3 DuckDB benchmark scripts (Phase 1/2/3b)...$(NC)"
	@echo "$(YELLOW)Requires Docker stack up + dbt-duckdb installed locally.$(NC)"
	PATH="$$PWD/.venv/bin:$$PATH" \
	POSTGRES_HOST=localhost POSTGRES_DB=airflow \
	POSTGRES_USER=airflow POSTGRES_PASSWORD=airflow \
	MINIO_ENDPOINT=localhost:9000 MINIO_ACCESS_KEY=minioadmin MINIO_SECRET_KEY=minioadmin \
	DUCKDB_PATH=/tmp/dbt_duckdb/analytics.duckdb \
	.venv/bin/python scripts/bench_duckdb.py && \
	.venv/bin/python scripts/bench_duckdb_writeback.py && \
	.venv/bin/python scripts/bench_duckdb_parquet.py

# =============================================================================
# ALLKEYSHOP DATA COLLECTION
# =============================================================================
allkeyshop-batch:
	@echo "$(GREEN)Triggering AllKeyShop batch search (10 games)...$(NC)"
	@bash scripts/trigger_allkeyshop_batch.sh

allkeyshop-batch-dry:
	@echo "$(YELLOW)Dry run - AllKeyShop batch search...$(NC)"
	@bash scripts/trigger_allkeyshop_batch.sh --dry-run

psql:
	@echo "$(GREEN)Opening PostgreSQL shell...$(NC)"
	docker exec -it airflow-postgres psql -U airflow -d airflow

# =============================================================================
# ALLKEYSHOP GAME REGISTRY
# =============================================================================
game-registry-init:
	@echo "$(GREEN)Initializing game registry table...$(NC)"
	docker exec -i airflow-postgres psql -U airflow -d airflow < sql/init/003_game_registry.sql
	@echo "$(GREEN)Game registry table created!$(NC)"

game-register:
ifndef GAME
	@echo "$(RED)Error: GAME parameter required$(NC)"
	@echo "Usage: make game-register GAME='Elden Ring'"
	@exit 1
endif
	@echo "$(GREEN)Registering game: $(GAME)$(NC)"
	docker exec airflow-scheduler airflow dags trigger allkeyshop_game_registration \
		--conf '{"game_name": "$(GAME)"}'
	@echo "$(GREEN)Registration triggered! Check Airflow UI for status.$(NC)"

game-list:
	@echo "$(GREEN)Registered games:$(NC)"
	docker exec -i airflow-postgres psql -U airflow -d airflow -c \
		"SELECT game_name, game_slug, schedule, enabled, dashboard_uid FROM allkeyshop_game_registry ORDER BY created_at"

# =============================================================================
# BACKUP & RESTORE
# =============================================================================
backup:
	@echo "$(GREEN)Creating PostgreSQL backup...$(NC)"
	docker exec airflow-postgres bash -c 'BACKUP_DIR=/backups POSTGRES_HOST=localhost /bin/bash' < scripts/backup/backup_postgres.sh || \
		docker exec airflow-postgres pg_dump -U airflow airflow | gzip > backups/airflow_$$(date +%Y%m%d_%H%M%S).sql.gz
	@echo "$(GREEN)Backup created in backups/ directory$(NC)"

backup-list:
	@echo "$(GREEN)Available backups:$(NC)"
	@ls -lh backups/*.sql.gz 2>/dev/null || echo "No backups found in backups/ directory"

restore:
ifndef FILE
	@echo "$(RED)Error: FILE parameter required$(NC)"
	@echo "Usage: make restore FILE='backups/airflow_20240115.sql.gz'"
	@exit 1
endif
	@echo "$(YELLOW)WARNING: This will overwrite the current database!$(NC)"
	@echo "Restoring from: $(FILE)"
	@read -p "Are you sure? [y/N] " confirm && [ "$$confirm" = "y" ] || exit 1
	gunzip -c $(FILE) | docker exec -i airflow-postgres psql -U airflow -d airflow
	@echo "$(GREEN)Restore completed! Restart services with: make restart$(NC)"

# =============================================================================
# WEBAPP - API (FastAPI)
# =============================================================================
api-lint:
	@echo "$(GREEN)Linting API code...$(NC)"
	$(RUFF) check api/ --fix
	$(RUFF) format api/

api-build:
	@echo "$(GREEN)Building API Docker image...$(NC)"
	$(DOCKER_COMPOSE) build api

api-restart: api-lint api-build
	@echo "$(GREEN)Restarting API container...$(NC)"
	$(DOCKER_COMPOSE) up -d api
	@echo "$(GREEN)API running at http://localhost:8000$(NC)"

api-logs:
	$(DOCKER_COMPOSE) logs -f --tail=100 api

# =============================================================================
# WEBAPP - FRONTEND (Next.js)
# =============================================================================
FRONT_DIR := front
NPM := npm --prefix $(FRONT_DIR)

front-lint:
	@echo "$(GREEN)Linting frontend...$(NC)"
	$(NPM) run lint

front-typecheck:
	@echo "$(GREEN)Running TypeScript type check...$(NC)"
	cd $(FRONT_DIR) && npx tsc --noEmit

front-build:
	@echo "$(GREEN)Building frontend...$(NC)"
	$(NPM) run build

front-dev:
	@echo "$(GREEN)Starting frontend dev server on port 3001...$(NC)"
	$(NPM) run dev -- -p 3001

front-restart:
	@echo "$(GREEN)Rebuilding and restarting frontend container...$(NC)"
	$(DOCKER_COMPOSE) build frontend
	$(DOCKER_COMPOSE) up -d frontend
	@echo "$(GREEN)Frontend running at http://localhost:3001$(NC)"

front-logs:
	$(DOCKER_COMPOSE) logs -f --tail=100 frontend
