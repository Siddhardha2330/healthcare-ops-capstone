# Databricks notebook source
# MAGIC %md
# MAGIC # 05 Appointments Full And Incremental Load
# MAGIC Demonstrates full and incremental processing for the main OLTP transaction table: appointments.
# MAGIC
# MAGIC This follows the same pattern as the previous FMCG orders project:
# MAGIC full load once, then incremental load using a watermark and primary-key upsert logic.

# COMMAND ----------

from datetime import datetime
from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "health_cat", "Catalog")
dbutils.widgets.text("bronze_schema", "bronze", "Bronze schema")
dbutils.widgets.dropdown("load_type", "incremental", ["full", "incremental"], "Load type")
dbutils.widgets.text("primary_key", "appointment_id", "Primary key")
dbutils.widgets.text("watermark_column", "updated_at", "Watermark column")
dbutils.widgets.text("full_source_path", "s3://healthcare-ops-capstone-siddh-20260421235653/raw/postgresql/appointments/landing/full/", "Full source path")
dbutils.widgets.text("incremental_source_path", "s3://healthcare-ops-capstone-siddh-20260421235653/raw/postgresql/appointments/landing/incremental/", "Incremental source path")
dbutils.widgets.text("processed_path", "s3://healthcare-ops-capstone-siddh-20260421235653/raw/postgresql/appointments/processed/", "Processed path")
dbutils.widgets.text("batch_id", "", "Batch ID")
dbutils.widgets.dropdown("optimize_target", "true", ["true", "false"], "Optimize target table")

catalog = dbutils.widgets.get("catalog")
bronze_schema = dbutils.widgets.get("bronze_schema")
load_type = dbutils.widgets.get("load_type")
primary_key = dbutils.widgets.get("primary_key")
watermark_column = dbutils.widgets.get("watermark_column")
full_source_path = dbutils.widgets.get("full_source_path").rstrip("/") + "/"
incremental_source_path = dbutils.widgets.get("incremental_source_path").rstrip("/") + "/"
processed_path = dbutils.widgets.get("processed_path").rstrip("/") + "/"
batch_id = dbutils.widgets.get("batch_id") or datetime.now().strftime("%Y%m%d%H%M%S")
optimize_target = dbutils.widgets.get("optimize_target") == "true"

target_table = f"{catalog}.{bronze_schema}.appointments"
audit_table = f"{catalog}.{bronze_schema}.appointments_load_audit"
source_path = full_source_path if load_type == "full" else incremental_source_path

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{bronze_schema}")

print(f"Load type: {load_type}")
print(f"Source path: {source_path}")
print(f"Target table: {target_table}")
print(f"Batch ID: {batch_id}")

# COMMAND ----------

def read_appointments(path):
    return (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(path)
        .withColumn("load_time", F.current_timestamp())
        .withColumn("load_date", F.to_date("load_time"))
        .withColumn("batch_id", F.lit(batch_id))
        .withColumn("source_system", F.lit("postgresql_appointments"))
        .select("*", F.col("_metadata.file_path").alias("source_file"))
    )

source_df = read_appointments(source_path)
source_count = source_df.count()

display(source_df)
print(f"Source records read: {source_count}")

# COMMAND ----------

if load_type == "full":
    final_df = source_df
    action = "full_overwrite"

elif load_type == "incremental":
    existing_df = spark.table(target_table)
    last_loaded_ts = existing_df.select(F.max(F.to_timestamp(F.col(watermark_column))).alias("last_loaded_ts")).collect()[0]["last_loaded_ts"]
    print(f"Last loaded {watermark_column}: {last_loaded_ts}")

    incremental_df = source_df.filter(F.to_timestamp(F.col(watermark_column)) > F.lit(last_loaded_ts))
    incremental_count = incremental_df.count()
    print(f"Incremental records after watermark filter: {incremental_count}")

    if incremental_count == 0:
        final_df = existing_df
        action = "incremental_noop"
    else:
        filtered_existing = existing_df.join(
            incremental_df.select(primary_key).distinct(),
            on=primary_key,
            how="left_anti",
        )
        final_df = filtered_existing.unionByName(incremental_df, allowMissingColumns=True)
        action = "incremental_upsert"

else:
    raise ValueError(f"Unsupported load_type: {load_type}")

display(final_df.orderBy(primary_key))

# COMMAND ----------

(
    final_df.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("load_date")
    .format("delta")
    .saveAsTable(target_table)
)

final_count = spark.table(target_table).count()
print(f"Appointments target count after {action}: {final_count}")

if optimize_target:
    try:
        spark.sql(f"OPTIMIZE {target_table} ZORDER BY ({primary_key}, patient_id, doctor_id)")
        print(f"Optimized {target_table}")
    except Exception as exc:
        print(f"Skipping optimize for {target_table}: {exc}")

# COMMAND ----------

audit_df = spark.createDataFrame(
    [
        (
            batch_id,
            load_type,
            action,
            source_path,
            target_table,
            source_count,
            final_count,
            datetime.now().isoformat(timespec="seconds"),
        )
    ],
    [
        "batch_id",
        "load_type",
        "action",
        "source_path",
        "target_table",
        "source_count",
        "target_count_after_load",
        "audit_created_at",
    ],
)

(
    audit_df.write
    .mode("append")
    .format("delta")
    .saveAsTable(audit_table)
)

display(spark.table(audit_table).orderBy(F.col("audit_created_at").desc()))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Processed Folder Note
# MAGIC In production, source files can be moved from landing to processed after successful load.
# MAGIC For this capstone, the audit table records processed batches and the landing files are preserved for demo repeatability.
