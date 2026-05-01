# Databricks notebook source
# MAGIC %md
# MAGIC # 02 Silver Transformations
# MAGIC Cleans Bronze data and creates curated Silver tables.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

dbutils.widgets.text("catalog", "health_cat", "Catalog")
dbutils.widgets.text("bronze_schema", "bronze", "Bronze schema")
dbutils.widgets.text("silver_schema", "silver", "Silver schema")
dbutils.widgets.text("write_mode", "overwrite", "Write mode")
dbutils.widgets.dropdown("cache_hot_tables", "true", ["true", "false"], "Cache hot tables")
dbutils.widgets.dropdown("optimize_silver", "true", ["true", "false"], "Optimize Silver tables")

catalog = dbutils.widgets.get("catalog")
bronze_schema = dbutils.widgets.get("bronze_schema")
silver_schema = dbutils.widgets.get("silver_schema")
write_mode = dbutils.widgets.get("write_mode")
cache_hot_tables = dbutils.widgets.get("cache_hot_tables") == "true"
optimize_silver = dbutils.widgets.get("optimize_silver") == "true"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{silver_schema}")

def write_delta_table(df, table_name, partition_cols=None):
    writer = df.write.mode(write_mode).format("delta")
    if write_mode == "overwrite":
        writer = writer.option("overwriteSchema", "true")
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.saveAsTable(f"{catalog}.{silver_schema}.{table_name}")


def optimize_table(table_name, zorder_cols=None):
    if not optimize_silver:
        return
    optimize_sql = f"OPTIMIZE {catalog}.{silver_schema}.{table_name}"
    if zorder_cols:
        optimize_sql += f" ZORDER BY ({', '.join(zorder_cols)})"
    try:
        spark.sql(optimize_sql)
        print(f"Optimized {table_name}")
    except Exception as exc:
        print(f"Skipping optimize for {table_name}: {exc}")


def try_cache(df, label):
    if not cache_hot_tables:
        return df, False
    try:
        cached_df = df.cache()
        cached_df.count()
        print(f"Cached {label}")
        return cached_df, True
    except Exception as exc:
        print(f"Skipping cache for {label}: {exc}")
        return df, False

# COMMAND ----------

appointments = spark.table(f"{catalog}.{bronze_schema}.appointments")
doctors = spark.table(f"{catalog}.{bronze_schema}.doctors")
departments = spark.table(f"{catalog}.{bronze_schema}.departments")
billing = spark.table(f"{catalog}.{bronze_schema}.billing")
patients = spark.table(f"{catalog}.{bronze_schema}.patients")

def standardize_text(column):
    return F.initcap(F.regexp_replace(F.trim(column), r"\s+", " "))

doctors_dim = doctors.select(
    F.col("doctor_id").cast("int").alias("doctor_id"),
    standardize_text(F.col("doctor_name")).alias("doctor_name"),
    F.col("department_id").cast("int").alias("department_id"),
    standardize_text(F.col("specialization")).alias("specialization"),
    F.col("status").alias("doctor_status"),
)
departments_dim = departments.select(
    F.col("department_id").cast("int").alias("department_id"),
    standardize_text(F.col("department_name")).alias("department_name"),
    standardize_text(F.col("location")).alias("location"),
)
billing_fact = billing.select(
    F.col("appointment_id").cast("int").alias("appointment_id"),
    F.col("bill_amount").cast("decimal(10,2)").alias("bill_amount"),
    standardize_text(F.col("payment_status")).alias("payment_status"),
    standardize_text(F.col("payment_method")).alias("payment_method"),
    F.to_timestamp("billing_datetime").alias("billing_datetime"),
)
patients_dim = patients.select(
    F.col("patient_id").cast("int").alias("patient_id"),
    standardize_text(F.col("patient_name")).alias("patient_name"),
    F.upper(F.trim("gender")).alias("gender"),
    F.to_date("date_of_birth").alias("date_of_birth"),
    standardize_text(F.col("city")).alias("city"),
    standardize_text(F.col("state")).alias("state"),
).withColumn(
    "patient_age",
    F.floor(F.months_between(F.current_date(), F.col("date_of_birth")) / 12).cast("int"),
)

write_delta_table(doctors_dim, "doctors_clean")
write_delta_table(departments_dim, "departments_clean")
write_delta_table(patients_dim, "patients_clean")

silver_appointments = (
    appointments
    .dropDuplicates(["appointment_id"])
    .withColumn("appointment_id", F.col("appointment_id").cast("int"))
    .withColumn("patient_id", F.col("patient_id").cast("int"))
    .withColumn("doctor_id", F.col("doctor_id").cast("int"))
    .withColumn(
        "no_show_int",
        F.when(F.lower(F.col("no_show_flag").cast("string")).isin("true", "t", "1", "yes", "y"), 1).otherwise(0),
    )
    .withColumn("no_show_flag_clean", F.col("no_show_int") == 1)
    .withColumn(
        "appointment_status_clean",
        F.when(F.lower(F.trim("appointment_status")).isin("completed", "complete"), "Completed")
        .when(F.lower(F.trim("appointment_status")).isin("no show", "no-show", "noshow"), "No Show")
        .when(F.lower(F.trim("appointment_status")).isin("cancelled", "canceled"), "Cancelled")
        .otherwise(standardize_text(F.col("appointment_status"))),
    )
    .withColumn("appointment_ts", F.to_timestamp("appointment_datetime"))
    .withColumn("checkin_ts", F.to_timestamp("checkin_datetime"))
    .withColumn("consultation_start_ts", F.to_timestamp("consultation_start_datetime"))
    .withColumn("consultation_end_ts", F.to_timestamp("consultation_end_datetime"))
    .withColumn("visit_month", F.date_format("appointment_ts", "yyyy-MM"))
    .withColumn("wait_time_minutes", (F.col("consultation_start_ts").cast("long") - F.col("checkin_ts").cast("long")) / 60)
    .withColumn("has_null_patient_id", F.col("patient_id").isNull())
    .withColumn("has_null_doctor_id", F.col("doctor_id").isNull())
    .withColumn("has_invalid_appointment_ts", F.col("appointment_ts").isNull())
)

write_delta_table(silver_appointments, "appointments_clean", ["visit_month"])
optimize_table("appointments_clean", ["appointment_id", "patient_id"])

# COMMAND ----------

appointments_enriched = (
    silver_appointments.alias("a")
    .join(F.broadcast(doctors_dim).alias("d"), "doctor_id", "left")
    .join(F.broadcast(departments_dim).alias("dept"), "department_id", "left")
    .join(F.broadcast(billing_fact).alias("b"), "appointment_id", "left")
    .join(F.broadcast(patients_dim).alias("p"), "patient_id", "left")
    .withColumn("is_unmatched_doctor", F.col("doctor_name").isNull())
    .withColumn("is_unmatched_patient", F.col("patient_name").isNull())
    .withColumn("is_valid_record", ~(F.col("has_null_patient_id") | F.col("has_null_doctor_id") | F.col("has_invalid_appointment_ts") | F.col("is_unmatched_doctor") | F.col("is_unmatched_patient")))
)

appointments_enriched, appointments_enriched_cached = try_cache(appointments_enriched, "appointments_enriched")

write_delta_table(appointments_enriched, "appointments_enriched", ["visit_month"])
optimize_table("appointments_enriched", ["doctor_id", "patient_id"])

display(appointments_enriched)

# COMMAND ----------

invalid_appointments = appointments_enriched.filter(~F.col("is_valid_record")).select(
    "appointment_id",
    "patient_id",
    "doctor_id",
    "appointment_status",
    "appointment_status_clean",
    "has_null_patient_id",
    "has_null_doctor_id",
    "has_invalid_appointment_ts",
    "is_unmatched_doctor",
    "is_unmatched_patient",
    "source_file",
    "load_time",
    "batch_id",
)

write_delta_table(invalid_appointments, "dq_invalid_appointments")

dq_summary = spark.createDataFrame(
    [
        ("appointments_total", appointments_enriched.count()),
        ("appointments_invalid", invalid_appointments.count()),
        ("null_patient_id", appointments_enriched.filter(F.col("has_null_patient_id")).count()),
        ("null_doctor_id", appointments_enriched.filter(F.col("has_null_doctor_id")).count()),
        ("invalid_appointment_ts", appointments_enriched.filter(F.col("has_invalid_appointment_ts")).count()),
        ("unmatched_doctor", appointments_enriched.filter(F.col("is_unmatched_doctor")).count()),
        ("unmatched_patient", appointments_enriched.filter(F.col("is_unmatched_patient")).count()),
    ],
    ["dq_rule", "record_count"],
)

write_delta_table(dq_summary, "dq_appointments_summary")
display(dq_summary)

if appointments_enriched_cached:
    appointments_enriched.unpersist()

# COMMAND ----------

kaggle = spark.table(f"{catalog}.{bronze_schema}.kaggle_noshow_appointments")
kaggle_clean = (
    kaggle
    .withColumnRenamed("PatientId", "patient_id")
    .withColumnRenamed("AppointmentID", "appointment_id")
    .withColumnRenamed("ScheduledDay", "scheduled_day")
    .withColumnRenamed("AppointmentDay", "appointment_day")
    .withColumnRenamed("No-show", "no_show")
    .withColumn("scheduled_ts", F.to_timestamp("scheduled_day"))
    .withColumn("appointment_date", F.to_date("appointment_day"))
    .withColumn("no_show_int", F.when(F.lower(F.col("no_show")) == "yes", 1).otherwise(0))
    .withColumn("no_show_flag_clean", F.col("no_show_int") == 1)
    .withColumn("lead_days", F.datediff("appointment_date", F.to_date("scheduled_ts")))
)

write_delta_table(kaggle_clean, "kaggle_noshow_clean")
display(kaggle_clean.limit(20))

