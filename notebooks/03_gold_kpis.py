# Databricks notebook source
# MAGIC %md
# MAGIC # 03 Gold KPIs
# MAGIC Builds Gold KPI tables for dashboards.

# COMMAND ----------

from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "health_cat", "Catalog")
dbutils.widgets.text("bronze_schema", "bronze", "Bronze schema")
dbutils.widgets.text("silver_schema", "silver", "Silver schema")
dbutils.widgets.text("gold_schema", "gold", "Gold schema")
dbutils.widgets.text("write_mode", "overwrite", "Write mode")
dbutils.widgets.dropdown("cache_hot_tables", "true", ["true", "false"], "Cache hot tables")
dbutils.widgets.dropdown("optimize_gold", "true", ["true", "false"], "Optimize Gold tables")

catalog = dbutils.widgets.get("catalog")
bronze_schema = dbutils.widgets.get("bronze_schema")
silver_schema = dbutils.widgets.get("silver_schema")
gold_schema = dbutils.widgets.get("gold_schema")
write_mode = dbutils.widgets.get("write_mode")
cache_hot_tables = dbutils.widgets.get("cache_hot_tables") == "true"
optimize_gold = dbutils.widgets.get("optimize_gold") == "true"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{gold_schema}")
appointments = spark.table(f"{catalog}.{silver_schema}.appointments_enriched")
kaggle = spark.table(f"{catalog}.{silver_schema}.kaggle_noshow_clean")
diagnostics = spark.table(f"{catalog}.{bronze_schema}.diagnostics")
feedback = spark.table(f"{catalog}.{bronze_schema}.feedback")

def write_delta_table(df, table_name, partition_cols=None):
    writer = df.write.mode(write_mode).format("delta")
    if write_mode == "overwrite":
        writer = writer.option("overwriteSchema", "true")
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.saveAsTable(f"{catalog}.{gold_schema}.{table_name}")


def optimize_table(table_name, zorder_cols=None):
    if not optimize_gold:
        return
    optimize_sql = f"OPTIMIZE {catalog}.{gold_schema}.{table_name}"
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


appointments, appointments_cached = try_cache(appointments, "appointments_enriched")
kaggle, kaggle_cached = try_cache(kaggle, "kaggle_noshow_clean")

# COMMAND ----------

no_show_rate = (
    appointments.groupBy("department_name")
    .agg(
        F.count("appointment_id").alias("total_appointments"),
        F.sum("no_show_int").alias("no_show_count")
    )
    .withColumn("no_show_rate_pct", F.round(F.col("no_show_count") / F.col("total_appointments") * 100, 2))
)
write_delta_table(no_show_rate, "gold_no_show_rate")
optimize_table("gold_no_show_rate")

# COMMAND ----------

doctor_utilization = (
    appointments.groupBy("doctor_id", "doctor_name", "department_name")
    .agg(
        F.count("appointment_id").alias("total_appointments"),
        F.sum(F.when(F.col("appointment_status_clean") == "Completed", 1).otherwise(0)).alias("completed_appointments"),
        F.sum(F.when(F.col("appointment_status_clean") == "No Show", 1).otherwise(0)).alias("no_show_appointments")
    )
)
write_delta_table(doctor_utilization, "gold_doctor_utilization")
optimize_table("gold_doctor_utilization")

# COMMAND ----------

department_revenue = (
    appointments.groupBy("department_name")
    .agg(F.round(F.sum("bill_amount"), 2).alias("total_revenue"))
    .fillna({"total_revenue": 0})
)
write_delta_table(department_revenue, "gold_department_revenue")
optimize_table("gold_department_revenue")

# COMMAND ----------

wait_time_trends = (
    appointments.groupBy("visit_month", "department_name")
    .agg(F.round(F.avg("wait_time_minutes"), 2).alias("avg_wait_time_minutes"))
)
write_delta_table(wait_time_trends, "gold_wait_time_trends", ["visit_month"])
optimize_table("gold_wait_time_trends", ["department_name"])

# COMMAND ----------

patient_revisit = (
    appointments.groupBy("patient_id")
    .agg(F.count("appointment_id").alias("visit_count"))
    .withColumn("is_revisit_patient", F.col("visit_count") > 1)
)
write_delta_table(patient_revisit, "gold_patient_revisit_rate")
optimize_table("gold_patient_revisit_rate")

# COMMAND ----------

diagnostics_volume = (
    diagnostics.groupBy("test_category", "test_name")
    .agg(F.count("diagnostic_id").alias("test_count"))
)
write_delta_table(diagnostics_volume, "gold_diagnostics_volume")
optimize_table("gold_diagnostics_volume")

feedback_summary = (
    feedback.agg(
        F.round(F.avg("rating"), 2).alias("avg_rating"),
        F.count("feedback_id").alias("feedback_count")
    )
)
write_delta_table(feedback_summary, "gold_feedback_summary")
optimize_table("gold_feedback_summary")

# COMMAND ----------

kaggle_no_show_by_age = (
    kaggle.withColumn("age_group", F.when(F.col("Age") < 18, "0-17").when(F.col("Age") < 35, "18-34").when(F.col("Age") < 55, "35-54").otherwise("55+"))
    .groupBy("age_group", "Gender")
    .agg(F.count("appointment_id").alias("appointments"), F.sum("no_show_int").alias("no_shows"))
    .withColumn("no_show_rate_pct", F.round(F.col("no_shows") / F.col("appointments") * 100, 2))
)
write_delta_table(kaggle_no_show_by_age, "gold_kaggle_no_show_by_age")
optimize_table("gold_kaggle_no_show_by_age")

# COMMAND ----------

for table in ["gold_no_show_rate", "gold_doctor_utilization", "gold_department_revenue", "gold_wait_time_trends", "gold_patient_revisit_rate", "gold_diagnostics_volume", "gold_feedback_summary", "gold_kaggle_no_show_by_age"]:
    print(table)
    display(spark.table(f"{catalog}.{gold_schema}.{table}"))

if appointments_cached:
    appointments.unpersist()
if kaggle_cached:
    kaggle.unpersist()

