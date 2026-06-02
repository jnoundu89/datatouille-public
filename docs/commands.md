# Developer Commands Reference

This document catalogs the available automated shortcuts provided by the project's root `Makefile` to simplify local development, testing, orchestration, and database operations.

---

## 💻 Local Development & CI

These commands run directly on your host machine to validate code quality and structure.

* **`make dev`**: Installs dependencies (Python packages) on your local machine and configures pre-commit git hooks.
* **`make lint`**: Runs `ruff` for code styling/formatting and `mypy` for static type checking validation.
* **`make test`**: Runs the complete unit and integration Pytest suites with coverage tracking.
* **`make ci`**: Executes the full local CI sequence (`lint`, `validate-dags`, `validate-grafana`, `detect-schema-drift`, `test`). Use this to verify changes before pushing.

---

## 🐳 Docker Stack Orchestration

Commands to easily manage the lifecycles of the local 12-container service topology.

* **`make up`**: Spins up the full Docker container stack in the background.
* **`make down`**: Stops and completely tears down the running Docker stack.
* **`make restart`**: Safely restarts all stack services.
* **`make status`**: Executes a standard `docker compose ps` to inspect running service health states.
* **`make logs`**: Streams system logs from all active containers in real-time.

---

## 📊 Data Transformation & Quality

Utilities to manually execute transformations, run quality sweeps, or interact with storage layers.

* **`make dbt-run`**: Executes all dbt models to compile raw sources into staging and gold models.
* **`make dbt-test`**: Runs native dbt testing suites (schema, uniques, referential integrity).
* **`make soda-scan`**: Scans database schemas against quality contracts via Soda Core rules.
* **`make seed-variables`**: Loads platform variables and configurations into the Airflow cluster database.
* **`make psql`**: Drops you into an interactive SQL prompt inside the PostgreSQL container.
