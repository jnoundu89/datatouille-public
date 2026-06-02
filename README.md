# datatouille

> "Anyone can cook data, but only the fearless can become great architects."

**datatouille** is a modern, high-performance data ingestion and transformation platform designed with modern software engineering practices. Built around **Apache Airflow 3.x** and utilizing a modular **Domain-Driven Design (DDD)** layout coupled with a robust **Medallion Architecture (Raw > Landing > Gold)**, it showcases a fully production-ready, idempotent data warehousing pipeline.

---

## 🌟 Technical Highlights

- **Domain-Driven Design (DDD)**: Data assets and ingestion pipelines are strictly partitioned into isolated business domains (`domain_meteo`, `domain_retail`), encouraging code reuse and ease of maintenance.
- **Medallion Storage Architecture**: Raw data ingestion into object storage, schema cleansing at the landing layer, and final high-performance analytical modeling.
- **Strict Data Quality Gates**: Soda Core checks run in-band within pipelines to enforce quality contracts before analytical models are updated.
- **DuckDB & dbt Transformation Engine**: Advanced analytical modeling is powered by dbt-core and DuckDB, capitalizing on high-speed columnar processing and Parquet snapshots.
- **Observability & Health Monitoring**: Continuous platform monitoring using Prometheus metrics, Grafana dashboards, and automated source health checks.

---

## 🏗️ Platform Architecture

```
                    External Sources (APIs & Scraping)
             ┌──────────────────┬──────────────────┐
             │   Meteo France   │    GeoRisques    │ ...
             └────────┬─────────┴────────┬─────────┘
                      │                  │
                      ▼                  ▼
             ┌─────────────────────────────────────┐
             │         Apache Airflow 3.1.6        │
             │       (CeleryExecutor + Redis)      │
             │                                     │
             │       8 Active Ingestion DAGs       │
             └──────────────────┬──────────────────┘
                                │
                      ┌─────────┴─────────┐
                      ▼                   ▼
                 ┌──────────┐       ┌──────────┐
                 │  MinIO   │       │ Postgres │
                 │  (S3)    │       │   16     │
                 │ Raw/Land │       │ Landing  │
                 └──────────┘       └────┬─────┘
                                         │
                                         ▼
                                  ┌──────────┐
                                  │   dbt    │
                                  │  (Gold)  │
                                  └────┬─────┘
                                       │
                           ┌───────────┴───────────┐
                           ▼                       ▼
                     ┌──────────┐            ┌──────────┐
                     │  Soda    │            │ Grafana  │
                     │ (Quality)│            │(Monitors)│
                     └──────────┘            └──────────┘
```

---

## 📊 Business Domains & Data Sources

| Domain | Source | Ingestion Type | Frequency | Description |
|--------|--------|----------------|-----------|-------------|
| **Meteorology** | Meteo France | API | Every 2 hours | Regional and departmental weather vigilance alerts |
| **Natural Risks** | GeoRisques | API | Weekly (Mon 6am) | French municipality natural hazard report tracking |
| **Retail Analytics** | toScrape | Web Scraping | Weekly (Sun 2am) | Books, authors & quotes catalog extraction via stealth Playwright sessions |

---

## 🛠️ Technical Stack

- **Orchestration**: Apache Airflow 3.1.6 (with CeleryExecutor & Redis broker)
- **Object Storage**: MinIO (S3-compatible)
- **Data Warehouse**: PostgreSQL 16
- **Analytical Layer**: dbt-core, DuckDB, Parquet
- **Quality Assurance**: Soda Core
- **Observability**: Prometheus & Grafana
- **CI/CD & Quality Gates**: Ruff, MyPy, Pre-commit hooks

---

## 📖 Documentation & Guides

To keep this main overview clean and direct, all setup instructions, configuration details, and development guidelines have been modularized into separate documents. Please refer to the guides below to get started:

- 📥 **[Deployment & Installation Guide](docs/installation.md)**: Steps to spin up the local stack of 12 Docker services, configure `.env` variables, and access service interfaces.
- ⚙️ **[Developer Commands Reference](docs/commands.md)**: A complete directory of Makefile commands for testing, linting, data quality checks, and local pipeline runs.
- 📐 **[Architecture & Conventions](docs/conventions.md)**: Standard pipeline patterns (Medallion schema flow), Airflow 3.x practices, and code styling constraints.

---

## 📄 License

This project is licensed under the Apache License 2.0. See the [LICENSE](LICENSE) file for more details.
