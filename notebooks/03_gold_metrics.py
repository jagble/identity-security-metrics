# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Gold: identity metrics that matter
# MAGIC Each metric answers a question a CISO or IAM lead actually asks, and has a target, so the dashboard shows
# MAGIC **status**, not just numbers. Definitions are in `docs/01-design.md`.
# MAGIC
# MAGIC "Today" for every metric is the **last day in the data**, not the day the notebook runs, so results are reproducible.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from pyspark.sql import functions as F

def save(name, sql):
    df = spark.sql(sql)
    df.write.mode("overwrite").option("overwriteSchema", True).saveAsTable(name)
    print(f"{name:32s} {spark.table(name).count():>7,} rows")

AS_OF = spark.sql("SELECT to_date(max(ts_utc)) d FROM silver_signins").first()["d"]
PR = ", ".join(f"'{m}'" for m in PHISHING_RESISTANT)
print("as of", AS_OF)

# COMMAND ----------

# MAGIC %md ### Account-level tables

# COMMAND ----------

# Who is active at the as-of date, with their MFA method and last successful sign-in
save("gold_identity_inventory", f"""
SELECT h.employee_id, h.upn, h.display_name, h.department, h.job_title, h.is_privileged,
       h.employment_status, h.hire_date, h.termination_date, h.account_disabled_date,
       coalesce(m.mfa_registered, 'none') AS mfa_registered,
       coalesce(m.mfa_registered, 'none') IN ({PR}) AS phishing_resistant,
       s.last_success, s.successes_90d
FROM silver_hr h
LEFT JOIN silver_mfa m USING (upn)
LEFT JOIN (SELECT upn, max(ts_utc) AS last_success, count(*) AS successes_90d
           FROM silver_signins WHERE success GROUP BY upn) s USING (upn)
""")

save("gold_dormant_accounts", f"""
SELECT upn, display_name, department, hire_date, last_success,
       datediff(DATE'{AS_OF}', to_date(last_success)) AS days_since_last_success
FROM gold_identity_inventory
WHERE employment_status = 'Active'
  AND hire_date <= date_sub(DATE'{AS_OF}', {DORMANT_DAYS})
  AND (last_success IS NULL OR to_date(last_success) < date_sub(DATE'{AS_OF}', {DORMANT_DAYS}))
""")

save("gold_leaver_exposure", """
SELECT i.upn, i.display_name, i.department, i.termination_date, i.account_disabled_date,
       datediff(i.account_disabled_date, i.termination_date) AS days_to_disable,
       count(s.event_id) AS signins_after_termination,
       max(s.ts_utc) AS last_signin_after_termination,
       concat_ws(', ', collect_set(s.app)) AS apps_used_after_termination
FROM gold_identity_inventory i
LEFT JOIN silver_signins s
  ON s.upn = i.upn AND s.success AND to_date(s.ts_utc) > i.termination_date
WHERE i.employment_status = 'Terminated'
GROUP BY ALL
""")

save("gold_privileged_mfa", """
SELECT upn, display_name, job_title, mfa_registered, phishing_resistant
FROM gold_identity_inventory WHERE is_privileged AND employment_status = 'Active'
""")

save("gold_legacy_auth", """
SELECT upn, count(*) AS legacy_signins, concat_ws(', ', collect_set(client_app)) AS clients,
       max(ts_utc) AS last_seen
FROM silver_signins WHERE legacy_client AND success AND in_hr GROUP BY upn
""")

save("gold_unknown_identities", """
SELECT upn, count(*) AS signins, min(ts_utc) AS first_seen, max(ts_utc) AS last_seen,
       concat_ws(', ', collect_set(app)) AS apps, concat_ws(', ', collect_set(client_app)) AS clients
FROM silver_signins WHERE NOT in_hr GROUP BY upn
""")

# COMMAND ----------

# MAGIC %md ### Trend and breakdown tables (for charts)

# COMMAND ----------

save("gold_mfa_by_department", f"""
SELECT department, count(*) AS active_users,
       round(100 * avg(CASE WHEN mfa_registered <> 'none' THEN 1 ELSE 0 END), 1) AS pct_mfa_registered,
       round(100 * avg(CASE WHEN phishing_resistant THEN 1 ELSE 0 END), 1) AS pct_phishing_resistant,
       round(100 * avg(CASE WHEN mfa_registered = 'sms' THEN 1 ELSE 0 END), 1) AS pct_sms,
       sum(CASE WHEN mfa_registered = 'none' THEN 1 ELSE 0 END) AS users_without_mfa
FROM gold_identity_inventory WHERE employment_status = 'Active'
GROUP BY department ORDER BY pct_phishing_resistant
""")

save("gold_signins_daily", """
SELECT date_utc, count(*) AS signins, sum(CAST(success AS INT)) AS successes,
       sum(CAST(NOT success AS INT)) AS failures,
       sum(CASE WHEN success AND single_factor THEN 1 ELSE 0 END) AS single_factor_successes,
       sum(CASE WHEN risk_level IN ('medium','high') THEN 1 ELSE 0 END) AS risky_signins,
       sum(CASE WHEN is_foreign THEN 1 ELSE 0 END) AS foreign_signins
FROM silver_signins GROUP BY date_utc ORDER BY date_utc
""")

save("gold_failures_by_reason", """
SELECT result_code,
       CASE result_code WHEN 50126 THEN 'Invalid username or password'
                        WHEN 50074 THEN 'MFA required (prompted)'
                        WHEN 500121 THEN 'MFA denied or not completed'
                        WHEN 50105 THEN 'User not assigned to app'
                        WHEN 53003 THEN 'Blocked by Conditional Access'
                        WHEN 50053 THEN 'Account locked'
                        ELSE 'Other' END AS reason,
       count(*) AS failures, count(DISTINCT upn) AS users, count(DISTINCT ip_address) AS source_ips
FROM silver_signins WHERE NOT success GROUP BY result_code ORDER BY failures DESC
""")

# COMMAND ----------

# MAGIC %md ### The KPI table: one row per metric, with a target and a status

# COMMAND ----------

k = spark.sql(f"""
WITH active AS (SELECT * FROM gold_identity_inventory WHERE employment_status = 'Active'),
     succ   AS (SELECT * FROM silver_signins WHERE success AND in_hr),
     lv     AS (SELECT * FROM gold_leaver_exposure)
SELECT
  round(100 * avg(CASE WHEN a.mfa_registered <> 'none' THEN 1 ELSE 0 END), 1)                     AS mfa_coverage,
  round(100 * avg(CASE WHEN a.phishing_resistant THEN 1 ELSE 0 END), 1)                          AS pr_all,
  (SELECT round(100 * avg(CASE WHEN phishing_resistant THEN 1 ELSE 0 END), 1) FROM gold_privileged_mfa) AS pr_priv,
  (SELECT round(100 * avg(CASE WHEN single_factor THEN 1 ELSE 0 END), 2) FROM succ)               AS single_factor_pct,
  (SELECT count(*) FROM gold_legacy_auth)                                                         AS legacy_users,
  (SELECT count(*) FROM gold_dormant_accounts)                                                    AS dormant,
  (SELECT count(*) FROM lv WHERE signins_after_termination > 0)                                   AS post_term_users,
  (SELECT count(*) FROM lv WHERE account_disabled_date IS NULL)                                   AS not_disabled,
  (SELECT percentile_approx(days_to_disable, 0.5) FROM lv WHERE days_to_disable IS NOT NULL)      AS median_disable_days,
  (SELECT max(days_to_disable) FROM lv)                                                           AS max_disable_days,
  (SELECT count(*) FROM succ WHERE risk_level IN ('medium','high'))                               AS risky_successes,
  (SELECT count(*) FROM gold_unknown_identities)                                                  AS unknown_ids
FROM active a
""").first()

rows = [
  # metric, value, unit, target, good_if ('>=' or '<='), why it matters
  ("MFA registration coverage", k.mfa_coverage, "%", 100, ">=", "Users with no MFA can be taken over with a password alone"),
  ("Phishing-resistant MFA (all users)", k.pr_all, "%", 50, ">=", "SMS and push can be phished or fatigued; FIDO2/passkeys can't"),
  ("Phishing-resistant MFA (privileged)", k.pr_priv, "%", 100, ">=", "Admin takeover is the worst case; federal guidance (OMB M-22-09) requires it"),
  ("Successful sign-ins with single factor", k.single_factor_pct, "%", 0, "<=", "Each one is a password-only door"),
  ("Users on legacy authentication", k.legacy_users, "users", 0, "<=", "IMAP/ActiveSync skip MFA entirely"),
  (f"Dormant accounts ({DORMANT_DAYS}+ days)", k.dormant, "accounts", 0, "<=", "Unused but enabled accounts are easy, unnoticed targets"),
  ("Leavers who signed in after termination", k.post_term_users, "users", 0, "<=", "The JML failure auditors look for first"),
  ("Leavers never disabled", k.not_disabled, "accounts", 0, "<=", "Open access with no owner"),
  ("Median days from termination to disable", k.median_disable_days, "days", 0, "<=", "Speed of offboarding"),
  ("Worst-case days from termination to disable", k.max_disable_days, "days", 1, "<=", "The tail is where incidents happen"),
  ("Risky sign-ins that succeeded (medium/high)", k.risky_successes, "sign-ins", 0, "<=", "Risk detected but access still granted"),
  ("Identities signing in that HR doesn't know", k.unknown_ids, "identities", 0, "<=", "Unowned accounts, often service accounts"),
]
kpis = spark.createDataFrame(
    [(m, float(v) if v is not None else None, u, float(t), g, w) for m, v, u, t, g, w in rows],
    "metric STRING, value DOUBLE, unit STRING, target DOUBLE, good_if STRING, why_it_matters STRING") \
  .withColumn("status", F.when(F.col("value").isNull(), "n/a")
              .when((F.col("good_if") == ">=") & (F.col("value") >= F.col("target")), "on target")
              .when((F.col("good_if") == "<=") & (F.col("value") <= F.col("target")), "on target")
              .otherwise("needs action")) \
  .withColumn("as_of", F.lit(str(AS_OF)))
kpis.write.mode("overwrite").option("overwriteSchema", True).saveAsTable("gold_kpis")
display(kpis.select("metric", "value", "unit", "target", "status"))
