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

catalog = dbutils.widgets.get("catalog")
bronze_schema = dbutils.widgets.get("bronze_schema")
silver_schema = dbutils.widgets.get("silver_schema")
gold_schema = dbutils.widgets.get("gold_schema")
raw_base = dbutils.widgets.get("raw_base").rstrip("/")
write_mode = dbutils.widgets.get("write_mode")
batch_id = dbutils.widgets.get("batch_id") or datetime.now().strftime("%Y%m%d%H%M%S")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{bronze_schema}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{silver_schema}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{gold_schema}")

# COMMAND ----------

def load_csv_to_bronze(source_path, table_name, source_system):
    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(source_path)
        .withColumn("load_time", F.current_timestamp())
        .withColumn("batch_id", F.lit(batch_id))
        .withColumn("source_system", F.lit(source_system))
        .select("*", F.col("_metadata.file_path").alias("source_file"))
    )
    target_table = f"{catalog}.{bronze_schema}.{table_name}"
    df.write.mode(write_mode).format("delta").saveAsTable(target_table)
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

