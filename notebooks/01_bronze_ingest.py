# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Bronze: land the raw files
# MAGIC Reads the CSVs exactly as delivered (every column as text) and adds where each row came from and when.
# MAGIC Nothing is cleaned here. If something goes wrong later, bronze is the untouched copy to go back to.
# MAGIC
# MAGIC **Before running:** upload `hr_roster.csv`, `mfa_registrations.csv` and `signin_events.csv`
# MAGIC to the `raw` volume (Catalog → workspace → pds_identity → raw → Upload). Optional: `entra_signins_export.csv`
# MAGIC from `queries/export-entra-signins.kql` to run the same pipeline on real Entra ID logs.
# MAGIC `planted_labels.csv` is uploaded too, but only notebook 04 reads it, to score the detections.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from pyspark.sql import functions as F

FILES = {
    "bronze_hr_roster": "hr_roster.csv",
    "bronze_mfa_registrations": "mfa_registrations.csv",
    "bronze_signin_events": "signin_events.csv",
    "bronze_planted_labels": "planted_labels.csv",
    "bronze_entra_signins": "entra_signins_export.csv",   # optional: real tenant export
}

def land(table, file_name):
    path = f"{RAW_PATH}/{file_name}"
    try:  # on serverless (Spark Connect) a missing file only errors when the data is used, so the write is inside too
        df = (spark.read.option("header", True).option("inferSchema", False)
              .option("multiLine", True).option("escape", '"').csv(path)
              .withColumn("_source_file", F.lit(file_name))
              .withColumn("_ingested_at", F.current_timestamp()))
        df.write.mode("overwrite").option("overwriteSchema", True).saveAsTable(table)
    except Exception as e:
        if "PATH_NOT_FOUND" in str(e) or "does not exist" in str(e) or "not found" in str(e).lower():
            print(f"skip {table}: {file_name} not found")
            return None
        raise
    n = spark.table(table).count()
    print(f"{table:28s} {n:>8,} rows  <- {file_name}")
    return n

counts = {t: land(t, f) for t, f in FILES.items()}

# COMMAND ----------

display(spark.sql("SHOW TABLES").filter("tableName LIKE 'bronze_%'"))
