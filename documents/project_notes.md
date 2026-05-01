# Healthcare Ops Capstone Notes

## Goal

Build a Healthcare Ops analytics pipeline using:

- PostgreSQL as OLTP source
- Amazon S3 as raw cloud storage
- Databricks Free Edition for Bronze, Silver, and Gold processing

## Main Datasets

- patients
- doctors
- departments
- appointments
- billing
- diagnostics
- prescriptions
- feedback
- Kaggle no-show appointments dataset

## Key Outcomes

- no-show analysis
- doctor utilization
- department revenue
- wait time trends
- patient revisit analysis
- diagnostics volume
- feedback summary

## Incremental Pattern

Appointments use a required full-load plus incremental-load design:

- full load initializes the Bronze appointments table
- incremental load updates existing appointments and inserts new ones
- Silver and Gold are refreshed from the final Bronze state

## Final Verified State

- Bronze appointments: `9`
- Silver appointments_clean: `9`
- Silver appointments_enriched: `9`
- Appointments load audit rows: `2`
- Data quality invalid appointments: `0`

