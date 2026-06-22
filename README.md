# Cadence

Pulse builds the pipeline. Cadence operates it on a schedule. Companion project: [Pulse](https://github.com/Mprtham/pulse)

![Airflow DAG graph](docs/dag-graph.png)
[Full grid view (PDF)](docs/retail_daily-grid-airflow.pdf)

---

## Why

Data pipelines fail. When they do, someone usually finds out hours later from a broken dashboard. Cadence runs the pipeline on a schedule, retries on failure, alerts the moment something stays broken, and can re-run any past day safely. It is the operations layer that keeps data trustworthy.

## Who it is for

Shows the orchestration skills a Data Engineer uses daily. The people who benefit: data teams who need pipelines that run unattended, and engineering leads who need to know the instant something breaks.

## What it does

- Runs on a timer (hourly)
- Runs four steps in strict order: generate, load, transform, test
- Retries failed steps automatically with exponential backoff
- Alerts on Discord after retries are exhausted
- Re-runs past dates without double-counting (idempotent)
- Blocks bad data from reaching consumers via a dbt test gate
- Blocks broken DAGs from merging via GitHub Actions

---

## The pipeline

```
generate -> load_to_duckdb -> dbt_run -> dbt_test
```

| Task | What it does |
|------|--------------|
| `generate` | Produces synthetic UK-retail order lines for the run date. Schema from UCI Online Retail II. Injects ~15% faults (returns, nulls, zero prices). |
| `load_to_duckdb` | Loads the CSV into DuckDB. Idempotent: deletes that date's rows first, then inserts. Re-running never double-counts. |
| `dbt_run` | Runs staging and mart models. Staging filters out faults. Marts aggregate daily revenue and revenue by country. |
| `dbt_test` | Runs schema tests. Fails loudly if data quality drops. Blocks downstream consumers. |

Every task: `retries=2`, exponential backoff, `on_failure_callback` to Discord.

---

## How to run

### Option A: Docker (recommended, production-like)

Requires Docker Desktop or Docker Engine.

```bash
# 1. Copy and configure env
cp .env.example .env
# Edit .env - set CADENCE_DISCORD_WEBHOOK_URL if you want Discord alerts.

# Linux/Mac only - set your UID so Airflow can write to mounted dirs
echo "AIRFLOW_UID=$(id -u)" >> .env

# 2. Initialise the database and create the admin user
docker compose up airflow-init

# 3. Start the stack
docker compose up -d

# 4. Open the UI
# http://localhost:8080  username: admin  password: admin

# 5. Unpause and trigger the DAG
docker compose exec airflow-webserver airflow dags unpause retail_daily
docker compose exec airflow-webserver airflow dags trigger retail_daily
```

### Option B: airflow standalone (local dev, no Docker)

```bash
# 1. Create and activate a virtual environment
python -m venv .venv
source .venv/bin/activate       # Windows: .venv\Scripts\activate

# 2. Install Airflow with constraints (required)
AIRFLOW_VERSION=2.9.3
PYTHON_VERSION=3.11
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
pip install duckdb dbt-duckdb

# 3. Set env vars
export AIRFLOW_HOME=$(pwd)/airflow
export CADENCE_DB_PATH=$(pwd)/data/cadence.duckdb
export CADENCE_DATA_DIR=/tmp/cadence
export AIRFLOW__CORE__DAGS_FOLDER=$(pwd)/dags
export PYTHONPATH=$(pwd)/include:$PYTHONPATH

# 4. Start (creates DB, creates admin user, starts scheduler + webserver)
airflow standalone

# 5. Trigger manually (in a second terminal, same env vars set)
airflow dags unpause retail_daily
airflow dags trigger retail_daily
```

---

## Backfill

Re-run any past date range. The idempotent loader makes this safe.

```bash
# Docker
docker compose exec airflow-webserver \
  airflow dags backfill retail_daily \
  --start-date 2025-01-01 \
  --end-date   2025-01-03

# Standalone
airflow dags backfill retail_daily \
  --start-date 2025-01-01 \
  --end-date   2025-01-03
```

Row counts for each date are replaced, never added to. Run it twice and the counts stay the same.

---

## Discord alerts

Set `CADENCE_DISCORD_WEBHOOK_URL` in `.env` to a Discord Incoming Webhook URL.

```
Server Settings -> Integrations -> Webhooks -> New Webhook -> Copy URL
```

When a task fails after both retries, the callback in `include/alerts.py` posts the DAG name, task name, run date, and log URL.

---

## dbt project

The `transform/` directory holds the dbt project.

```
transform/
  models/
    staging/
      stg_orders.sql        - filters faults, computes line totals
      schema.yml            - not_null, accepted_values tests
      sources.yml           - points at raw_orders in DuckDB
    marts/
      mart_daily_revenue.sql    - revenue by calendar day
      mart_country_revenue.sql  - revenue by country with share %
      schema.yml                - unique, not_null tests
```

Run manually:

```bash
cd transform
dbt run  --profiles-dir .
dbt test --profiles-dir .
```

---

## CI

GitHub Actions runs on every push and pull request:

1. Lints `dags/` and `include/` with ruff.
2. Imports the DAG and asserts task order and dependencies.

A broken DAG or a linting failure blocks the merge.

---

## Stack

Apache Airflow 2.9 | dbt-duckdb | DuckDB | Python 3.11 | Docker | GitHub Actions

---

## Authorship

Built by Prathamesh Mishra. Synthetic data is clearly labelled as such. No live production data.
