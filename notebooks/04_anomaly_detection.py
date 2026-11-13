# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Detecting suspicious sign-ins: rules vs machine learning
# MAGIC 1. **Features** for every sign-in: travel speed from the user's previous successful sign-in, rare country for that user,
# MAGIC    hour of day, how many users the same IP failed against, MFA denials in a row, legacy client, risk level
# MAGIC 2. **Rules**: explainable detections for known attack patterns (impossible travel, password spray, MFA fatigue,
# MAGIC    sign-in after termination, privileged account off-hours from a new country)
# MAGIC 3. **Machine learning**: an Isolation Forest that learns what normal looks like and scores how unusual each sign-in is,
# MAGIC    with no labels and no rules
# MAGIC 4. **Scoring against the answer key** (`planted_labels.csv`): precision and recall for each approach, per scenario.
# MAGIC    The detections never see the labels. They're only used here, afterwards, to grade them.

# COMMAND ----------

# MAGIC %pip install scikit-learn --quiet

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from pyspark.sql import functions as F, Window

s = spark.table("silver_signins").filter("upn IS NOT NULL")
by_user = Window.partitionBy("upn").orderBy("ts_utc")
before = by_user.rowsBetween(Window.unboundedPreceding, -1)
user_30m = Window.partitionBy("upn").orderBy(F.col("ts_utc").cast("long")).rangeBetween(-1800, 0)

def haversine_km(lat1, lon1, lat2, lon2):
    lat1, lon1, lat2, lon2 = (F.col(c) for c in (lat1, lon1, lat2, lon2))
    r = F.lit(6371.0)
    dlat, dlon = F.radians(lat2 - lat1), F.radians(lon2 - lon1)
    a = F.sin(dlat / 2) ** 2 + F.cos(F.radians(lat1)) * F.cos(F.radians(lat2)) * F.sin(dlon / 2) ** 2
    return 2 * r * F.asin(F.sqrt(a))

# how often each user signs in from each country (a country they almost never use is suspicious)
country_share = (s.groupBy("upn", "country").count()
                 .withColumn("share", F.col("count") / F.sum("count").over(Window.partitionBy("upn")))
                 .select("upn", "country", F.col("share").alias("country_share")))
# how many different users each IP failed against in each hour (password spray shape)
ip_hour = (s.filter("NOT success").groupBy("ip_address", F.date_trunc("hour", "ts_utc").alias("hr"))
           .agg(F.countDistinct("upn").alias("ip_failed_users_1h")))

f = (s.join(country_share, ["upn", "country"], "left")
      .withColumn("hr", F.date_trunc("hour", "ts_utc"))
      .join(ip_hour, ["ip_address", "hr"], "left")
      # travel is measured from the user's previous SUCCESSFUL sign-in: a failed attempt by an attacker
      # isn't the user travelling, and comparing to it would flag the real user's next sign-in
      .withColumn("prev_lat", F.last(F.when(F.col("success"), F.col("latitude")), ignorenulls=True).over(before))
      .withColumn("prev_lon", F.last(F.when(F.col("success"), F.col("longitude")), ignorenulls=True).over(before))
      .withColumn("prev_ts", F.last(F.when(F.col("success"), F.col("ts_utc")), ignorenulls=True).over(before))
      .withColumn("distance_km", F.coalesce(haversine_km("prev_lat", "prev_lon", "latitude", "longitude"), F.lit(0.0)))
      .withColumn("minutes_since_prev",
                  F.coalesce((F.col("ts_utc").cast("long") - F.col("prev_ts").cast("long")) / 60, F.lit(1e6)))
      .withColumn("speed_kmh", F.when(F.col("minutes_since_prev") > 0,
                                      F.col("distance_km") / (F.col("minutes_since_prev") / 60)).otherwise(0.0))
      .withColumn("mfa_denials_30m", F.sum(F.when(F.col("result_code") == 500121, 1).otherwise(0)).over(user_30m))
      .withColumn("hour_et", F.hour("ts_et"))
      .withColumn("weekend", F.dayofweek("ts_et").isin(1, 7).cast("int"))
      .withColumn("risk_num", F.when(F.col("risk_level") == "high", 3).when(F.col("risk_level") == "medium", 2)
                               .when(F.col("risk_level") == "low", 1).otherwise(0))
      .fillna({"ip_failed_users_1h": 0, "country_share": 1.0}))
f.write.mode("overwrite").option("overwriteSchema", True).saveAsTable("gold_signin_features")
f = spark.table("gold_signin_features")

# COMMAND ----------

# MAGIC %md ### Rules

# COMMAND ----------

rules = {
    "impossible_travel": f"speed_kmh > {IMPOSSIBLE_TRAVEL_KMH} AND distance_km > {IMPOSSIBLE_TRAVEL_MIN_KM} AND success",
    "password_spray_ip": f"ip_failed_users_1h >= {SPRAY_MIN_USERS}",
    "mfa_fatigue": f"mfa_denials_30m >= {MFA_FATIGUE_MIN_DENIALS}",
    "sign_in_after_termination": "success AND termination_date IS NOT NULL AND to_date(ts_utc) > termination_date",
    "privileged_off_hours_new_country": "is_privileged AND success AND (hour_et < 5 OR hour_et >= 22) AND country_share < 0.05",
}
rule_hits = None
for name, cond in rules.items():
    hit = f.filter(cond).select("event_id", "upn", "ts_utc", "ip_address", "country", F.lit(name).alias("detector"),
                                F.lit(None).cast("double").alias("score"))
    rule_hits = hit if rule_hits is None else rule_hits.unionByName(hit)
print({r.detector: r["count"] for r in rule_hits.groupBy("detector").count().collect()})

# COMMAND ----------

# MAGIC %md ### Machine learning: Isolation Forest
# MAGIC Unsupervised: it never sees a label. It isolates points that are easy to separate from the crowd.
# MAGIC The top-scoring sign-ins are flagged for review. The cutoff (how many to flag) is a business decision:
# MAGIC flag more and you catch more, but analysts review more false alarms.

# COMMAND ----------

import numpy as np
from sklearn.ensemble import IsolationForest

FEATURES = ["hour_et", "weekend", "distance_km", "speed_kmh", "country_share", "ip_failed_users_1h",
            "mfa_denials_30m", "risk_num"]
pdf = f.select("event_id", "upn", "ts_utc", "ip_address", "country", "success", "single_factor", "legacy_client",
               *FEATURES).toPandas()
X = pdf[FEATURES].astype(float).copy()
X["speed_kmh"] = np.log1p(X["speed_kmh"].clip(upper=20000))
X["distance_km"] = np.log1p(X["distance_km"])
X["failed"] = (~pdf["success"].astype(bool)).astype(float)
X["single_factor"] = pdf["single_factor"].astype(float)
X["legacy_client"] = pdf["legacy_client"].astype(float)

model = IsolationForest(n_estimators=300, contamination="auto", random_state=42).fit(X)
pdf["score"] = -model.score_samples(X)           # higher = more unusual
ML_TOP_N = 250                                   # ~0.4% of sign-ins go to an analyst
ml = pdf.nlargest(ML_TOP_N, "score")[["event_id", "upn", "ts_utc", "ip_address", "country", "score"]]
ml_hits = spark.createDataFrame(ml).withColumn("detector", F.lit("isolation_forest")) \
    .select("event_id", "upn", "ts_utc", "ip_address", "country", "detector", "score")

detections = rule_hits.unionByName(ml_hits)
detections.write.mode("overwrite").option("overwriteSchema", True).saveAsTable("gold_detections")
print(f"gold_detections: {spark.table('gold_detections').count():,}")

# COMMAND ----------

# MAGIC %md ### Scoring against the answer key (event-level)

# COMMAND ----------

labels = spark.table("bronze_planted_labels").filter("level = 'event'") \
    .select(F.col("key").alias("event_id"), "scenario")
n_planted = labels.count()

def score(det_df, name):
    flagged = det_df.select("event_id").distinct()
    tp = flagged.join(labels, "event_id").count()
    n = flagged.count()
    return (name, n, tp, round(100 * tp / n, 1) if n else None, round(100 * tp / n_planted, 1))

evals = [score(spark.table("gold_detections").filter(F.col("detector") == "isolation_forest"), "isolation_forest"),
         score(spark.table("gold_detections").filter(F.col("detector") != "isolation_forest"), "rules (all)"),
         score(spark.table("gold_detections"), "rules + ML combined")]
ev = spark.createDataFrame(evals, "approach STRING, flagged LONG, true_positives LONG, precision_pct DOUBLE, recall_pct DOUBLE")
ev.write.mode("overwrite").option("overwriteSchema", True).saveAsTable("gold_detection_eval")
display(ev)

# Recall per planted scenario: which approach caught what
per = (labels.join(spark.table("gold_detections").select("event_id", "detector").distinct()
                   .withColumn("is_ml", F.col("detector") == "isolation_forest"), "event_id", "left")
       .groupBy("scenario").agg(
           F.countDistinct("event_id").alias("planted"),
           F.countDistinct(F.when(~F.col("is_ml"), F.col("event_id"))).alias("caught_by_rules"),
           F.countDistinct(F.when(F.col("is_ml"), F.col("event_id"))).alias("caught_by_ml"))
       .orderBy("scenario"))
per.write.mode("overwrite").option("overwriteSchema", True).saveAsTable("gold_detection_by_scenario")
display(per)

# COMMAND ----------

# MAGIC %md ### Scoring the account-level metrics against the answer key

# COMMAND ----------

acct = spark.table("bronze_planted_labels").filter("level = 'account'").select(F.lower("key").alias("upn"), "scenario")
found = (
    spark.table("gold_dormant_accounts").select("upn", F.lit("dormant_45_days").alias("scenario"))
    .unionByName(spark.table("gold_leaver_exposure").filter("account_disabled_date IS NULL")
                 .select("upn", F.lit("terminated_not_disabled").alias("scenario")))
    .unionByName(spark.table("gold_legacy_auth").select("upn", F.lit("legacy_authentication").alias("scenario")))
    .unionByName(spark.table("gold_identity_inventory").filter("mfa_registered = 'none'")
                 .select("upn", F.lit("no_mfa_registered").alias("scenario")))
    .unionByName(spark.table("gold_privileged_mfa").filter("NOT phishing_resistant")
                 .select("upn", F.lit("privileged_without_phishing_resistant_mfa").alias("scenario")))
    .unionByName(spark.table("gold_unknown_identities").select("upn", F.lit("identity_not_in_hr").alias("scenario"))))
key = (acct.withColumn("planted", F.lit(1)).join(found.withColumn("found", F.lit(1)), ["upn", "scenario"], "full")
       .groupBy("scenario").agg(F.sum(F.coalesce("planted", F.lit(0))).alias("planted"),
                                F.sum(F.when(F.col("planted").isNotNull() & F.col("found").isNotNull(), 1).otherwise(0)).alias("found"),
                                F.sum(F.when(F.col("planted").isNull(), 1).otherwise(0)).alias("unexpected"))
       .withColumn("missed", F.col("planted") - F.col("found")).orderBy("scenario"))
key.write.mode("overwrite").option("overwriteSchema", True).saveAsTable("gold_answer_key_check")
display(key)
