"""
Idempotent DuckDB loader.
Keyed on run_date: re-running the same date replaces that date's rows only.
"""
from __future__ import annotations

import logging
import os
from pathlib import Path

import duckdb

log = logging.getLogger(__name__)

CREATE_SCHEMA = "CREATE SCHEMA IF NOT EXISTS raw"

DDL = """
CREATE TABLE IF NOT EXISTS raw.raw_orders (
    invoice_no   VARCHAR,
    stock_code   VARCHAR,
    description  VARCHAR,
    quantity     INTEGER,
    invoice_date TIMESTAMP,
    price        DOUBLE,
    customer_id  VARCHAR,
    country      VARCHAR,
    run_date     DATE
)
"""


def get_db_path() -> str:
    path = os.environ.get("CADENCE_DB_PATH", "/opt/airflow/data/cadence.duckdb")
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    return path


def load_orders(run_date: str, csv_path: str) -> int:
    """
    Load orders from *csv_path* into raw.raw_orders for *run_date*.
    Deletes existing rows for that date first so re-runs are safe.
    Returns the number of rows inserted.
    """
    db_path = get_db_path()
    log.info("Connecting to DuckDB at %s", db_path)

    with duckdb.connect(db_path) as conn:
        conn.execute(CREATE_SCHEMA)
        conn.execute(DDL)

        conn.execute(
            "DELETE FROM raw.raw_orders WHERE run_date = ?",
            [run_date],
        )
        log.info("Deleted existing rows for run_date=%s", run_date)

        conn.execute(
            """
            INSERT INTO raw.raw_orders
            SELECT
                invoice_no,
                stock_code,
                description,
                CAST(quantity AS INTEGER),
                CAST(invoice_date AS TIMESTAMP),
                CAST(price AS DOUBLE),
                customer_id,
                country,
                CAST(run_date AS DATE)
            FROM read_csv_auto(?, header=true)
            """,
            [csv_path],
        )

        row_count = conn.execute(
            "SELECT COUNT(*) FROM raw.raw_orders WHERE run_date = ?",
            [run_date],
        ).fetchone()[0]

    log.info("Loaded %d rows for run_date=%s", row_count, run_date)
    return row_count
