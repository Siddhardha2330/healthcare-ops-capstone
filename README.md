# Healthcare Ops Capstone

Databricks healthcare Lakehouse capstone project using `PostgreSQL` as the OLTP source, `Amazon S3` as the raw cloud storage layer, and `Databricks` for Bronze, Silver, and Gold processing.

## Architecture

```text
PostgreSQL OLTP
-> CSV export
-> Amazon S3 raw zone
-> Databricks Bronze
-> Databricks Silver
-> Databricks Gold
-> SQL / Dashboard analytics
```

## Main Components

- `notebooks/`
  - Databricks notebooks for Bronze ingestion, Silver transformations, Gold KPIs, validation, and appointments full/incremental load

- `sql/`
  - PostgreSQL schema and sample data scripts
  - Dashboard analytics queries for Gold tables

- `documents/`
  - project notes, architecture summary, and pipeline explanation

- `screenshots/`
  - placeholder folder for Databricks, S3, dashboard, and workflow screenshots

## Databricks Catalog

```text
health_cat.bronze
health_cat.silver
health_cat.gold
```

## Notebooks

1. `01_bronze_ingestion.py`
2. `02_silver_transformations.py`
3. `03_gold_kpis.py`
4. `04_validation_checks.py`
5. `05_appointments_full_incremental_load.py`

## Pipeline Notes

- Bronze ingests raw healthcare CSVs from S3 into Delta tables.
- Silver performs cleaning, standardization, joins, and data quality checks.
- Gold creates KPI tables such as no-show rate, doctor utilization, department revenue, wait time trends, revisit rate, diagnostics volume, and feedback summary.
- Appointments are handled separately with full-load and incremental-load logic to model OLTP changes.

