# Databricks notebook source
# MAGIC %md
# MAGIC # 01 Bronze Ingestion
# MAGIC Creates Bronze Delta tables from healthcare raw CSV files.
# MAGIC
# MAGIC For Databricks Free Edition, run this notebook using Serverless compute.

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime

dbutils.widgets.text("catalog", "health_cat", "Catalog")
dbutils.widgets.text("bronze_schema", "bronze", "Bronze schema")
dbutils.widgets.text("silver_schema", "silver", "Silver schema")
dbutils.widgets.text("gold_schema", "gold", "Gold schema")
dbutils.widgets.text("raw_base", "s3://healthcare-ops-capstone-siddh-20260421235653/raw", "Raw base path")
dbutils.widgets.text("write_mode", "overwrite", "Write mode")
dbutils.widgets.text("batch_id", "", "Batch ID")
dbutils.widgets.dropdown("use_autoloader", "true", ["true", "false"], "Use Auto Loader")
dbutils.widgets.dropdown("optimize_bronze", "true", ["true", "false"], "Optimize Bronze tables")
dbutils.widgets.text("autoloader_state_base", "dbfs:/tmp/healthcare_ops/autoloader", "Auto Loader state base")

catalog = dbutils.widgets.get("catalog")
bronze_schema = dbutils.widgets.get("bronze_schema")
silver_schema = dbutils.widgets.get("silver_schema")
gold_schema = dbutils.widgets.get("gold_schema")
raw_base = dbutils.widgets.get("raw_base").rstrip("/")
write_mode = dbutils.widgets.get("write_mode")
batch_id = dbutils.widgets.get("batch_id") or datetime.now().strftime("%Y%m%d%H%M%S")
use_autoloader = dbutils.widgets.get("use_autoloader") == "true"
optimize_bronze = dbutils.widgets.get("optimize_bronze") == "true"
autoloader_state_base = dbutils.widgets.get("autoloader_state_base").rstrip("/")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{bronze_schema}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{silver_schema}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{gold_schema}")

# COMMAND ----------

def optimize_table(table_name, zorder_cols=None):
    if not optimize_bronze:
        return
    optimize_sql = f"OPTIMIZE {table_name}"
    if zorder_cols:
        optimize_sql += f" ZORDER BY ({', '.join(zorder_cols)})"
    try:
        spark.sql(optimize_sql)
        print(f"Optimized {table_name}")
    except Exception as exc:
        print(f"Skipping optimize for {table_name}: {exc}")


def load_csv_to_bronze(source_path, table_name, source_system):
    target_table = f"{catalog}.{bronze_schema}.{table_name}"
    autoloader_attempted = False

    if use_autoloader:
        autoloader_attempted = True
        state_root = f"{autoloader_state_base}/{table_name}/{batch_id}"
        schema_location = f"{state_root}/schema"
        checkpoint_location = f"{state_root}/checkpoint"

        try:
            if write_mode == "overwrite":
                spark.sql(f"DROP TABLE IF EXISTS {target_table}")

            stream_df = (
                spark.readStream
                .format("cloudFiles")
                .option("cloudFiles.format", "csv")
                .option("cloudFiles.inferColumnTypes", "true")
                .option("cloudFiles.schemaLocation", schema_location)
                .option("header", True)
                .load(source_path)
                .withColumn("load_time", F.current_timestamp())
                .withColumn("batch_id", F.lit(batch_id))
                .withColumn("source_system", F.lit(source_system))
                .withColumn("load_date", F.to_date("load_time"))
                .select("*", F.col("_metadata.file_path").alias("source_file"))
            )

            query = (
                stream_df.writeStream
                .format("delta")
                .outputMode("append")
                .option("checkpointLocation", checkpoint_location)
                .trigger(availableNow=True)
                .toTable(target_table)
            )
            query.awaitTermination()
            print(f"Loaded {table_name} using Auto Loader")
        except Exception as exc:
            print(f"Auto Loader unavailable for {table_name}, falling back to batch CSV ingestion: {exc}")

    if (not use_autoloader) or (autoloader_attempted and not spark.catalog.tableExists(target_table)):
        df = (
            spark.read
            .option("header", True)
            .option("inferSchema", True)
            .csv(source_path)
            .withColumn("load_time", F.current_timestamp())
            .withColumn("batch_id", F.lit(batch_id))
            .withColumn("source_system", F.lit(source_system))
            .withColumn("load_date", F.to_date("load_time"))
            .select("*", F.col("_metadata.file_path").alias("source_file"))
        )
        (
            df.write
            .mode(write_mode)
            .partitionBy("load_date")
            .format("delta")
            .saveAsTable(target_table)
        )
        print(f"Loaded {table_name} using batch CSV ingestion")

    optimize_table(target_table, ["load_date"])
    return spark.table(target_table).count()

# COMMAND ----------

postgres_tables = [
    "departments", "doctors", "patients", "appointments",
    "billing", "diagnostics", "prescriptions", "feedback"
]

results = []
for table in postgres_tables:
    count = load_csv_to_bronze(f"{raw_base}/postgresql/{table}.csv", table, "postgresql")
    results.append((table, count))

kaggle_count = load_csv_to_bronze(
    f"{raw_base}/external/kaggle_noshow_appointments/kaggle_noshow_appointments.csv",
    "kaggle_noshow_appointments",
    "kaggle"
)
results.append(("kaggle_noshow_appointments", kaggle_count))

display(spark.createDataFrame(results, ["table_name", "row_count"]))

# COMMAND ----------

display(spark.sql(f"SHOW TABLES IN {catalog}.{bronze_schema}"))

