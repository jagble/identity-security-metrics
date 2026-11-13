# Tableau Public: the same metrics, a second tool

The same gold tables, rebuilt as an executive dashboard in **Tableau Public** (free). Two tools, one set of definitions,
so the numbers must match. If they don't, one of the dashboards is wrong.

> **Tableau Public is public.** Everything published there can be seen by anyone. Only publish the synthetic PDS data,
> never the real Entra export.

## 1. Export the gold tables from Databricks

In the **SQL editor**, run each query and use **Download → CSV**:

| File | Query |
|---|---|
| `kpis.csv` | `SELECT * FROM workspace.pds_identity.gold_kpis` |
| `signins_daily.csv` | `SELECT * FROM workspace.pds_identity.gold_signins_daily` |
| `mfa_by_department.csv` | `SELECT * FROM workspace.pds_identity.gold_mfa_by_department` |
| `detection_by_scenario.csv` | `SELECT * FROM workspace.pds_identity.gold_detection_by_scenario` |
| `leaver_exposure.csv` | `SELECT * FROM workspace.pds_identity.gold_leaver_exposure` |

## 2. Build four sheets

Connect to each CSV as a **Text file** data source.

| Sheet | Build |
|---|---|
| **Scorecard** | `kpis`: Rows = `Metric`, Text = `Value`, `Target`. Color by `Status` (needs action = red, on target = green) |
| **Sign-in trend** | `signins_daily`: Columns = `Date Utc` (day), Rows = `Failures`, `Risky Signins`, `Single Factor Successes` as a dual or combined line |
| **MFA by department** | `mfa_by_department`: Rows = `Department`, Columns = `Pct Phishing Resistant`, sorted ascending. Reference line at the 50% target |
| **Rules vs ML** | `detection_by_scenario`: Rows = `Scenario`, Columns = `Measure Values` (`Caught By Rules`, `Caught By Ml`, `Planted`) |

## 3. Dashboard and story

- **Dashboard** (1200 × 800): scorecard across the top, trend and department bars in the middle, rules vs ML at the bottom
- **Title:** "PDS Identity Security: where the risk is"
- **Caption** under each chart: one sentence on *what to do*, not what the chart shows
- Optional: a **Story** with 3 points, matching the executive briefing (`docs/02-executive-briefing.md`)

## 4. Publish

File → Save to Tableau Public → copy the link into the project README.
