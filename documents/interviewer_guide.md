# Healthcare Ops Capstone - Interviewer Guide

## 1. Project Goal

This project was designed to show how healthcare operational data can be transformed into analytics-ready KPI tables using a Lakehouse architecture.

The main business goal is to give hospital leadership a unified view of:

- no-show patterns
- doctor workload
- department revenue
- patient wait times
- revisit behavior
- diagnostics demand
- patient satisfaction

## 2. Why This Project Matters

Healthcare operations often run on transactional systems, but reporting needs a separate analytical layer.

This project demonstrates that separation clearly:

- `PostgreSQL` handles daily hospital transactions
- `S3` stores raw exported data
- `Databricks` handles analytics transformation and KPI generation

That makes it a good example of `OLTP -> Data Lake -> Lakehouse -> Analytics`.

## 3. End-to-End Architecture

```text
PostgreSQL
-> exported healthcare CSV files
-> Amazon S3 raw zone
-> Databricks Bronze tables
-> Databricks Silver tables
-> Databricks Gold KPI tables
-> SQL / dashboard reporting
```

## 4. Simplified Schema Diagram

The project is centered around `appointments`, which connects the main operational entities.

The schema diagram in this document shows the main tables and the most relevant business columns used in the pipeline:

- `departments`
- `doctors`
- `patients`
- `appointments`
- `billing`
- `diagnostics`
- `prescriptions`
- `feedback`

The relationship pattern is:

- one department can have many doctors
- one doctor can handle many appointments
- one patient can have many appointments
- one appointment can produce billing, diagnostics, prescriptions, and feedback records

## 5. Data Sources

### OLTP healthcare data from PostgreSQL

- `patients`
- `appointments`
- `doctors`
- `departments`
- `billing`
- `diagnostics`
- `prescriptions`
- `feedback`

### External historical dataset

- Kaggle Medical Appointment No Shows dataset

This external dataset was used to strengthen no-show analytics and show multi-source ingestion.

## 6. Why `appointments` Is the Main Table

The main transactional dataset is `appointments`.

That is because it drives the most important operational questions:

- Which departments have the highest no-show rates?
- Which doctors are overloaded?
- What is the patient wait trend?
- Which patients revisit?
- Which appointments were updated, completed, cancelled, or missed?

Because appointment data changes frequently, this project gives it a separate full-load and incremental-load design.

## 7. Medallion Design

### Bronze Layer

Purpose:
- preserve raw source data
- make ingestion traceable
- support reprocessing

Bronze actions:
- ingest CSVs from S3
- preserve source-like structure
- add metadata

Key metadata columns:
- `source_file`
- `load_time`
- `batch_id`
- `source_system`

### Silver Layer

Purpose:
- clean, standardize, validate, and enrich data

Silver actions:
- standardize department names
- standardize doctor and patient text values
- convert and validate timestamps
- derive `patient_age`
- derive `wait_time_minutes`
- standardize appointment status
- clean no-show fields
- join appointments with doctors, departments, patients, and billing
- log invalid/unmatched records

### Gold Layer

Purpose:
- build business-ready KPI tables

Gold outputs:
- no-show KPI table
- doctor utilization KPI table
- department revenue KPI table
- wait time KPI table
- revisit KPI table
- diagnostics volume KPI table
- feedback KPI table

## 8. Notebook-by-Notebook Explanation

### `01_bronze_ingestion`

What it does:
- reads raw healthcare CSV files from S3
- writes Bronze Delta tables
- adds metadata columns

Tables ingested:
- `patients`
- `appointments`
- `doctors`
- `departments`
- `billing`
- `diagnostics`
- `prescriptions`
- `feedback`
- `kaggle_noshow_appointments`

### `05_appointments_full_incremental_load`

What it does:
- handles the most important table: `appointments`
- supports both `full` and `incremental` load modes
- tracks results in an audit table

Why it matters:
- this is the advanced pipeline feature in the project
- it models how OLTP changes can be applied to a Lakehouse table

### `02_silver_transformations`

What it does:
- cleans healthcare dimensions and facts
- creates validated appointments tables
- generates DQ summary tables

Main Silver tables:
- `doctors_clean`
- `departments_clean`
- `patients_clean`
- `appointments_clean`
- `appointments_enriched`
- `kaggle_noshow_clean`
- `dq_invalid_appointments`
- `dq_appointments_summary`

### `03_gold_kpis`

What it does:
- creates final KPI tables for analytics and dashboard use

Gold outputs:
- `gold_no_show_rate`
- `gold_doctor_utilization`
- `gold_department_revenue`
- `gold_wait_time_trends`
- `gold_patient_revisit_rate`
- `gold_diagnostics_volume`
- `gold_feedback_summary`
- `gold_kaggle_no_show_by_age`

### `04_validation_checks`

What it does:
- validates final pipeline output
- confirms expected row counts and key quality rules
- reads the appointments load audit table

## 9. Full Load vs Incremental Load

This is one of the strongest interview points in the project.

### Full load

Used to initialize the complete appointments table from a full source file.

### Incremental load

Used to process only new or changed appointments.

In this project:

- one existing appointment was updated
- one new appointment was inserted

This changed the final appointments count from `8` to `9`.

That proves the pipeline supports data change handling instead of only static loads.

## 10. Data Quality and Validation

Validation rules included:

- unique `appointment_id`
- mandatory `patient_id`
- mandatory `doctor_id`
- valid appointment timestamp
- valid billing amount
- unmatched doctor/patient detection

Final verified state:

- `appointments_total = 9`
- `appointments_invalid = 0`
- `null_patient_id = 0`
- `null_doctor_id = 0`
- `invalid_appointment_ts = 0`
- `unmatched_doctor = 0`
- `unmatched_patient = 0`

## 11. Final Workflow

The Databricks Workflow job is:

```text
Healthcare Ops Pipeline
```

Tasks:

1. Bronze ingestion
2. Appointments full/incremental processing
3. Silver transformations
4. Gold KPI generation
5. Validation checks

This shows the project is not only notebook-based, but also orchestrated as a repeatable pipeline.

## 12. Implemented Optimizations

The project includes practical optimizations that fit the current Databricks architecture:

- `appointments` uses a full-load plus incremental-load pattern instead of full refresh only
- Bronze ingestion includes an Auto Loader style implementation with safe fallback to batch CSV reads
- Bronze and appointments fact-style writes include partition-aware Delta storage using `load_date`
- Silver fact-style writes include partition-aware storage using `visit_month`
- small reference tables are broadcast-joined into `appointments_enriched`
- Gold queries read from pre-joined Silver data instead of repeating joins in every KPI build
- Delta `OPTIMIZE` hooks were added as best-effort maintenance steps
- cache hooks were added as best-effort optimization, but guarded because Databricks serverless does not support normal persist/cache behavior in this environment

These optimizations are designed to be technically valid while still remaining compatible with Databricks Free Edition.

## 13. What Was Learned

This project demonstrates:

- how to separate OLTP and analytics workloads
- how to build Bronze, Silver, and Gold tables in Databricks
- how to move healthcare data from PostgreSQL to S3 to Databricks
- how to design a fact table with incremental updates
- how to implement data quality checks
- how to build dashboard-ready KPI tables
- how to orchestrate Databricks notebooks using a Workflow job
