# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Config
# MAGIC Shared settings, loaded by every other notebook with `%run ./00_config`.
# MAGIC Runs on Databricks (Free Edition or any workspace) and locally through `tools/run_local.py`.

# COMMAND ----------

try:
    dbutils  # noqa: F821  (exists only on Databricks)
    ON_DATABRICKS = True
except NameError:
    ON_DATABRICKS = False

if ON_DATABRICKS:
    dbutils.widgets.text("catalog", "workspace")          # Free Edition's default catalog
    dbutils.widgets.text("schema", "pds_identity")
    CATALOG = dbutils.widgets.get("catalog")
    SCHEMA = dbutils.widgets.get("schema")
    spark.sql(f"USE CATALOG {CATALOG}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {SCHEMA}.raw")
    RAW_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/raw"         # upload the CSVs here
else:
    CATALOG = None
    SCHEMA = LOCAL_SCHEMA                                  # noqa: F821  (set by tools/run_local.py)
    RAW_PATH = LOCAL_RAW_PATH                              # noqa: F821
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")

spark.sql(f"USE {SCHEMA}")

# Thresholds used by the metrics and detections, in one place so they're easy to defend or tune
DORMANT_DAYS = 45                 # active employee with no successful sign-in for this long
IMPOSSIBLE_TRAVEL_KMH = 900       # faster than a commercial flight
IMPOSSIBLE_TRAVEL_MIN_KM = 500    # ignore short hops (IP geolocation is imprecise)
SPRAY_MIN_USERS = 20              # one IP failing against this many different users within an hour
MFA_FATIGUE_MIN_DENIALS = 5       # denied MFA prompts for one user within 30 minutes
PHISHING_RESISTANT = ("fido2", "windows_hello")
LEGACY_CLIENTS = ("IMAP4", "POP3", "Exchange ActiveSync", "Authenticated SMTP")

print(f"schema={SCHEMA} raw={RAW_PATH} databricks={ON_DATABRICKS}")
