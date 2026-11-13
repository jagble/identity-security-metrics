# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Silver: clean, check quality, join to HR
# MAGIC - Types fixed (timestamps, dates, numbers), usernames normalized to lowercase
# MAGIC - **Data quality checks**: duplicates removed, rows with no user or impossible timestamps **quarantined, not dropped silently**
# MAGIC - Every sign-in joined to HR (the source of truth) and to the user's registered MFA method
# MAGIC - Identities that sign in but **don't exist in HR** are flagged, not hidden
# MAGIC - Results of every check saved to `dq_results`, so data quality is itself a metric

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from pyspark.sql import functions as F, Window

hr = (spark.table("bronze_hr_roster")
      .select(F.col("employee_id").cast("int").alias("employee_id"), "display_name",
              F.lower(F.trim("upn")).alias("upn"), "department", "job_title", "home_city", "employment_status",
              F.to_date("hire_date").alias("hire_date"), F.to_date("termination_date").alias("termination_date"),
              F.to_date("account_disabled_date").alias("account_disabled_date"),
              (F.lower("is_privileged") == "true").alias("is_privileged")))
mfa = (spark.table("bronze_mfa_registrations")
       .select(F.lower(F.trim("upn")).alias("upn"), F.lower("method").alias("mfa_registered"),
               F.to_date("registered_date").alias("registered_date")))
hr.write.mode("overwrite").option("overwriteSchema", True).saveAsTable("silver_hr")
mfa.write.mode("overwrite").option("overwriteSchema", True).saveAsTable("silver_mfa")

# COMMAND ----------

raw = spark.table("bronze_signin_events")
total = raw.count()

typed = raw.select(
    "event_id",
    F.to_timestamp("timestamp_utc", "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("ts_utc"),
    F.lower(F.trim("upn")).alias("upn"), "app", "client_app", "ip_address", "city", "country",
    F.col("latitude").cast("double").alias("latitude"), F.col("longitude").cast("double").alias("longitude"),
    F.col("result_code").cast("int").alias("result_code"), "auth_requirement",
    F.expr("nullif(mfa_method, '')").alias("mfa_method"), "risk_level")

# Check 1: exact duplicate deliveries (same event_id)
deduped = typed.dropDuplicates(["event_id"])
dupes = total - deduped.count()

# Checks 2-3: rows that can't be trusted go to quarantine with a reason
# "Future" is judged against the data itself (not today's date), so the notebook gives the same answer whenever it runs:
# anything more than 7 days past the 99.9th percentile timestamp is clock skew.
p999 = deduped.agg(F.percentile_approx(F.unix_timestamp("ts_utc"), 0.999).alias("p")).first()["p"]
flagged = deduped.withColumn("dq_issue",
    F.when(F.col("upn").isNull() | (F.col("upn") == ""), "missing_user")
     .when(F.col("ts_utc").isNull(), "unparseable_timestamp")
     .when(F.unix_timestamp("ts_utc") > F.lit(p999 + 7 * 86400), "timestamp_in_future"))
quarantine = flagged.filter("dq_issue IS NOT NULL")
clean = flagged.filter("dq_issue IS NULL").drop("dq_issue")
quarantine.write.mode("overwrite").option("overwriteSchema", True).saveAsTable("silver_signins_quarantine")

# Join to HR and MFA registration. Left join: an identity missing from HR is a finding, not a row to lose.
signins = (clean.join(hr.drop("display_name"), "upn", "left").join(mfa.select("upn", "mfa_registered"), "upn", "left")
    .withColumn("in_hr", F.col("employee_id").isNotNull())
    .withColumn("success", F.col("result_code") == 0)
    .withColumn("single_factor", F.col("auth_requirement") == "singleFactorAuthentication")
    .withColumn("legacy_client", F.col("client_app").isin(*LEGACY_CLIENTS))
    .withColumn("is_foreign", ~F.col("country").startswith("US-"))
    .withColumn("ts_et", F.col("ts_utc") - F.expr("INTERVAL 5 HOURS"))   # dataset uses fixed EST
    .withColumn("date_utc", F.to_date("ts_utc")))
signins.write.mode("overwrite").option("overwriteSchema", True).saveAsTable("silver_signins")

# COMMAND ----------

# Check 4: identities signing in that HR doesn't know about
not_in_hr = spark.table("silver_signins").filter("NOT in_hr")
unknown_ids = not_in_hr.select("upn").distinct().count()
# Check 5: HR records with no MFA registration row at all
hr_no_mfa_row = hr.join(mfa, "upn", "left_anti").count()

dq = spark.createDataFrame([
    ("duplicate_events_removed", dupes, total),
    ("quarantined_missing_user", quarantine.filter("dq_issue = 'missing_user'").count(), total),
    ("quarantined_future_timestamp", quarantine.filter("dq_issue = 'timestamp_in_future'").count(), total),
    ("quarantined_unparseable_timestamp", quarantine.filter("dq_issue = 'unparseable_timestamp'").count(), total),
    ("signins_from_identities_not_in_hr", not_in_hr.count(), total),
    ("distinct_identities_not_in_hr", unknown_ids, None),
    ("hr_records_without_mfa_row", hr_no_mfa_row, hr.count()),
], "check STRING, rows_failed LONG, rows_total LONG") \
    .withColumn("pct_failed", F.round(100 * F.col("rows_failed") / F.col("rows_total"), 3)) \
    .withColumn("checked_at", F.current_timestamp())
dq.write.mode("overwrite").option("overwriteSchema", True).saveAsTable("dq_results")
display(dq)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Optional: real Entra ID sign-ins, mapped to the same shape
# MAGIC Proves the pipeline works on real logs, not just the synthetic set. Kept in its own table so the
# MAGIC real tenant's handful of users don't mix with the 340 synthetic employees.

# COMMAND ----------

if "bronze_entra_signins" in [t.name for t in spark.catalog.listTables()]:
    e = spark.table("bronze_entra_signins")
    entra = e.select(
        F.col("CorrelationId").alias("event_id"), F.to_timestamp("TimeGenerated").alias("ts_utc"),
        F.lower("UserPrincipalName").alias("upn"), F.col("AppDisplayName").alias("app"),
        F.col("ClientAppUsed").alias("client_app"), F.col("IPAddress").alias("ip_address"),
        F.col("City").alias("city"), F.col("CountryOrRegion").alias("country"),
        F.col("ResultType").cast("int").alias("result_code"),
        F.col("AuthenticationRequirement").alias("auth_requirement"),
        F.col("RiskLevelDuringSignIn").alias("risk_level"),
        F.col("ConditionalAccessStatus").alias("ca_status"))
    entra.write.mode("overwrite").option("overwriteSchema", True).saveAsTable("silver_entra_signins")
    print(f"silver_entra_signins: {entra.count():,} real sign-ins")
else:
    print("No real Entra export uploaded (optional).")
