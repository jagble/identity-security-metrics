# Databricks AI/BI Dashboard: Identity Security Metrics

How to build the dashboard in Databricks from the gold tables. Every tile answers one question a security leader asks.

**Create it:** Dashboards → Create dashboard → **Data** tab → add each dataset below as SQL → **Canvas** tab → add the widgets.
Table names assume catalog `workspace` and schema `pds_identity`.

## Layout

```
┌──────────────┬──────────────┬──────────────┬──────────────┐
│ MFA coverage │ Phishing-res.│ Phishing-res.│ Leavers who  │   row 1: counters (KPI)
│   (all)      │   (all)      │ (privileged) │ signed in    │
├──────────────┴──────────────┴──────────────┴──────────────┤
│ KPI scorecard table: metric · value · target · status      │   row 2: one table, status colored
├─────────────────────────────┬─────────────────────────────┤
│ Sign-ins per day: failures, │ Phishing-resistant MFA by   │   row 3: trend + breakdown
│ single-factor, risky        │ department (bar)            │
├─────────────────────────────┼─────────────────────────────┤
│ Detections by scenario:     │ Leaver exposure: days to    │   row 4: detection + JML
│ rules vs ML (bar)           │ disable, sign-ins after     │
├─────────────────────────────┴─────────────────────────────┤
│ Detections to review (table, filter by detector)           │   row 5: analyst work queue
└────────────────────────────────────────────────────────────┘
```

## Datasets

**kpis**: the scorecard and the counters
```sql
SELECT metric, value, unit, target, status, why_it_matters, as_of
FROM workspace.pds_identity.gold_kpis
```
Counters: filter `metric` to one value each, show `value`, and use `target` in the description.
Color the scorecard's `status` column: *needs action* red, *on target* green.

**daily**: sign-in trend
```sql
SELECT date_utc, failures, single_factor_successes, risky_signins, foreign_signins
FROM workspace.pds_identity.gold_signins_daily
```
Line chart, x = `date_utc`, y = the four counts. The password spray shows up as a one-day spike in failures.

**mfa_by_dept**
```sql
SELECT department, active_users, pct_phishing_resistant, pct_sms, users_without_mfa
FROM workspace.pds_identity.gold_mfa_by_department
```
Horizontal bar, `pct_phishing_resistant` by `department`, sorted ascending: the worst departments on top.

**detection_by_scenario**
```sql
SELECT scenario, planted, caught_by_rules, caught_by_ml
FROM workspace.pds_identity.gold_detection_by_scenario
```
Grouped bar: what rules caught versus what machine learning caught, per attack type.

**leavers**
```sql
SELECT display_name, department, termination_date, account_disabled_date,
       coalesce(CAST(days_to_disable AS STRING), 'never') AS days_to_disable, signins_after_termination
FROM workspace.pds_identity.gold_leaver_exposure
ORDER BY signins_after_termination DESC, days_to_disable DESC NULLS FIRST
```

**review_queue**
```sql
SELECT d.ts_utc, d.upn, d.detector, round(d.score, 3) AS ml_score, d.ip_address, d.country
FROM workspace.pds_identity.gold_detections d
ORDER BY d.ts_utc DESC
```
Table with a filter widget on `detector`.

## Genie (optional)

Add the gold tables to a **Genie space** and ask in plain English, for example:
*"Which departments have the lowest phishing-resistant MFA?"* or *"Which leavers signed in after their termination date?"*
Check the SQL Genie writes before trusting the answer.
