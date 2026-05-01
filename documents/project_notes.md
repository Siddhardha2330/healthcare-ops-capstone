# Healthcare Ops Capstone Notes

## Quick Objective

Build a healthcare analytics pipeline that moves hospital operational data from `PostgreSQL` into `Databricks` through `Amazon S3`, then produces business KPIs for leadership reporting.

## Core Stack

- `PostgreSQL` for OLTP
- `Amazon S3` for raw storage
- `Databricks` for Bronze, Silver, Gold, and Workflow orchestration

## Main Healthcare Tables

- `patients`
- `appointments`
- `doctors`
- `departments`
- `billing`
- `diagnostics`
- `prescriptions`
- `feedback`

## External Dataset

- Kaggle no-show appointments dataset

## Key KPI Outputs

- no-show rate
- doctor utilization
- department revenue
- wait time trends
- patient revisit rate
- diagnostics volume
- feedback summary

## Important Design Choice

`appointments` is treated as the main transactional table and uses a full-load plus incremental-load design.

Reason:
- appointment records change over time
- no-show/completed/cancelled state can change
- new appointments continue to arrive

## Final Verified State

- Bronze appointments: `9`
- Silver appointments_clean: `9`
- Silver appointments_enriched: `9`
- Appointments load audit rows: `2`
- Data quality invalid appointments: `0`

## Implemented Optimizations

- appointments full-load plus incremental-load design
- Bronze Auto Loader style ingestion with safe batch fallback
- partition-aware Delta writes using `load_date` and `visit_month`
- broadcast joins for small reference data in Silver
- pre-joined Silver table for repeated Gold KPI use
- best-effort Delta `OPTIMIZE` hooks
- guarded cache hooks for serverless compatibility

## Main Interview Point

This project demonstrates both:

- a standard medallion pipeline
- a separate incremental fact-style process for appointments
