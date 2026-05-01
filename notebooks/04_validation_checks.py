# Databricks notebook source
# MAGIC %md
# MAGIC # 04 Validation Checks
# MAGIC Basic source-to-bronze and data quality checks.

# COMMAND ----------

from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "health_cat", "Catalog")
dbutils.widgets.text("bronze_schema", "bronze", "Bronze schema")
dbutils.widgets.text("silver_schema", "silver", "Silver schema")
dbutils.widgets.text("expected_appointments_count", "9", "Expected appointments count")

catalog = dbutils.widgets.get("catalog")
bronze_schema = dbutils.widgets.get("bronze_schema")
silver_schema = dbutils.widgets.get("silver_schema")
expected_appointments_count = int(dbutils.widgets.get("expected_appointments_count"))
expected_counts = {
    "departments": 6,
    "doctors": 6,
    "patients": 8,
    "appointments": expected_appointments_count,
    "billing": 5,
    "diagnostics": 4,
    "prescriptions": 5,
    "feedback": 5,
}

rows = []
for table, expected in expected_counts.items():
    actual = spark.table(f"{catalog}.{bronze_schema}.{table}").count()
    rows.append((table, expected, actual, expected == actual))

display(spark.createDataFrame(rows, ["table_name", "expected_count", "actual_count", "passed"]))

# COMMAND ----------

checks = []
appointments = spark.table(f"{catalog}.silver.appointments_clean")
billing = spark.table(f"{catalog}.{bronze_schema}.billing")

checks.append(("unique appointment_id", appointments.count() == appointments.select("appointment_id").distinct().count()))
checks.append(("mandatory patient_id", appointments.filter(F.col("patient_id").isNull()).count() == 0))
checks.append(("mandatory doctor_id", appointments.filter(F.col("doctor_id").isNull()).count() == 0))
checks.append(("valid bill amount", billing.filter(F.col("bill_amount") < 0).count() == 0))
checks.append(("valid appointment datetime", appointments.filter(F.col("appointment_ts").isNull()).count() == 0))

display(spark.createDataFrame(checks, ["rule", "passed"]))

# COMMAND ----------

load_audit = spark.table(f"{catalog}.{bronze_schema}.appointments_load_audit")
display(load_audit.orderBy(F.col("audit_created_at").desc()))

