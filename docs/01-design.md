# Design: Identity Security Metrics

## 1. The goal

Turn raw identity data (HR records, MFA registrations and sign-in logs) into a short list of **metrics that matter**,
each with a target, plus detections for suspicious sign-ins. Then present it the way a security leader needs it: a
dashboard and a one-page briefing that says what to fix first.

Identity platforms (Entra ID, Okta, Ping) each have their own reports, but they keep logs for a limited time and can't
join to HR. A data platform like Databricks can keep everything, join it, and answer questions no single console can,
like *"which leavers still signed in after HR terminated them?"*

## 2. Questions → metrics

| Question a CISO asks | Metric | Definition | Target |
|---|---|---|---|
| Can anyone get in with just a password? | **MFA registration coverage** | Active employees with any MFA method ÷ active employees | 100% |
| | **Single-factor successful sign-ins** | Successful sign-ins by HR identities with `singleFactorAuthentication` ÷ all successful sign-ins | 0% |
| | **Users on legacy authentication** | Users with successful IMAP/POP/ActiveSync/SMTP sign-ins, which bypass MFA | 0 |
| Would our MFA survive phishing? | **Phishing-resistant MFA, all users** | Active employees whose method is FIDO2/passkey or Windows Hello ÷ active employees | 50% (interim) |
| | **Phishing-resistant MFA, privileged** | Same, for admin accounts | 100% |
| Do leavers lose access on time? | **Leavers who signed in after termination** | Terminated employees with a successful sign-in dated after their HR termination date | 0 |
| | **Leavers never disabled** | Terminated employees whose account was never disabled | 0 |
| | **Days from termination to disable** | Median and worst case of `account_disabled_date − termination_date` | 0 median, ≤ 1 worst |
| Are there accounts nobody uses or owns? | **Dormant accounts** | Active employees hired 45+ days ago with no successful sign-in in 45 days | 0 |
| | **Identities not in HR** | Accounts signing in that the HR source of truth doesn't know | 0 |
| Is risk being acted on? | **Risky sign-ins that succeeded** | Successful sign-ins rated medium or high risk | 0 |
| Is the data itself trustworthy? | **Data quality checks** | Duplicates, missing users, impossible timestamps, identities missing from HR (`dq_results`) | Tracked, not hidden |

"Today" for every metric is the **last day in the data**, so a re-run gives the same answer.

## 3. Data

**Synthetic, and labeled as synthetic.** My real Entra tenant has a handful of users, too few for meaningful percentages.
`data/generate_pds_identity_data.py` builds 340 PDS employees and 90 days of sign-ins shaped like Entra ID sign-in logs.
It's deterministic (seed 42), so anyone can regenerate the exact same data. IP addresses come only from the RFC 5737
documentation ranges.

| File | Contents |
|---|---|
| `hr_roster.csv` | Employee ID, name, department, title, status, hire/termination dates, the date the account was actually disabled, privileged flag |
| `mfa_registrations.csv` | Registered MFA method per user (none, SMS, authenticator, FIDO2, Windows Hello) |
| `signin_events.csv` | Timestamp, user, app, client, IP, city/country, coordinates, result code, single or multifactor, MFA method, risk level |
| `planted_labels.csv` | **The answer key**: every problem planted on purpose. Only used afterwards, to score the results |

**Real data path:** `queries/export-entra-signins.kql` exports real sign-ins from Log Analytics, and notebook 02 maps
them to the same shape (`silver_entra_signins`). That proves the pipeline handles real Entra logs, not just my own format.

### Planted problems (the answer key)

Same approach as Project 1's answer key: decide what should be found **before** running anything, then check.

| Level | Scenario | Planted |
|---|---|---|
| Account | No MFA registered | Users with no method |
| Account | Privileged accounts with SMS instead of phishing-resistant MFA | 2 admins |
| Account | Legacy authentication (IMAP, ActiveSync) | 4 users |
| Account | Dormant 45+ days | 12 users |
| Account | Terminated in HR, account never disabled | 2 leavers |
| Account | Service account signing in that isn't in HR | 1 |
| Event | Password spray: one IP, 75 accounts, 40 minutes, 02:10 ET | 2 succeed against accounts with no MFA |
| Event | Post-compromise mailbox access from the spray IP | Follow-on sign-ins |
| Event | Impossible travel (stolen session) | 3 users |
| Event | MFA fatigue: 12 denied pushes, then one approved | 1 user |
| Event | Sign-ins after termination | The 2 never-disabled leavers |
| Event | Privileged sign-in at 03:10 ET from a new country, approved by SMS | 1 admin |

Plus realistic **dirty data** for the quality checks: duplicate deliveries, rows with no user, and clock-skewed timestamps.

## 4. Pipeline (medallion architecture)

| Layer | Tables | What happens |
|---|---|---|
| **Bronze** | `bronze_*` | Files landed as-is, every column as text, with source file and load time |
| **Silver** | `silver_hr`, `silver_mfa`, `silver_signins`, `silver_signins_quarantine`, `dq_results` | Types fixed, users normalized, duplicates removed, bad rows quarantined with a reason, every sign-in joined to HR and MFA |
| **Gold** | `gold_kpis`, `gold_identity_inventory`, `gold_dormant_accounts`, `gold_leaver_exposure`, `gold_privileged_mfa`, `gold_legacy_auth`, `gold_unknown_identities`, `gold_mfa_by_department`, `gold_signins_daily`, `gold_failures_by_reason` | Business-ready metrics and the lists behind them |
| **Gold (detection)** | `gold_signin_features`, `gold_detections`, `gold_detection_eval`, `gold_detection_by_scenario`, `gold_answer_key_check` | Features, flagged sign-ins, and the scoring |

## 5. Detection: rules vs machine learning

**Rules** are explainable and catch known patterns:

| Rule | Logic |
|---|---|
| Impossible travel | Faster than 900 km/h and more than 500 km from the user's **previous successful** sign-in |
| Password spray | One IP failing against 20+ different users within an hour |
| MFA fatigue | 5+ denied MFA prompts for one user within 30 minutes |
| Sign-in after termination | Successful sign-in dated after the HR termination date |
| Privileged off-hours, new country | Admin, success, 22:00 to 05:00 ET, from a country under 5% of their history |

**Machine learning:** an **Isolation Forest** (scikit-learn), unsupervised: it never sees a label. It scores how easy
each sign-in is to separate from normal behavior, using hour, weekend, travel distance and speed, how rare the country
is for that user, failures from the same IP, MFA denials, single factor, legacy client and risk level. The top 250
(about 0.4% of sign-ins) go to review. That cutoff is a business choice: more flags catch more, and cost analyst time.

**Scoring:** precision (of what was flagged, how much was really planted) and recall (of what was planted, how much was
caught), for rules, ML and both combined, plus recall per scenario. The interesting question isn't which wins: it's
**what each one misses, and why**.

## 6. Mapping to Okta and Ping

The sign-in schema is deliberately close to what identity platforms emit, so the same pipeline could ingest them.
Approximate field mapping for the **Okta System Log**:

| This project | Okta System Log |
|---|---|
| `timestamp_utc` | `published` |
| `upn` | `actor.alternateId` |
| `app` | `target[]` (the app instance) |
| `result_code` / success | `outcome.result` (`SUCCESS`, `FAILURE`) and `outcome.reason` |
| `ip_address` | `client.ipAddress` |
| `city`, `country`, coordinates | `client.geographicalContext` |
| `mfa_method` | `authenticationContext` and the authenticator events (e.g. `user.authentication.auth_via_mfa`) |
| `risk_level` | `debugContext.debugData` (risk and behavior details) |

## 7. Limits

- The data is synthetic. It shows the method and the pipeline, not real PDS results
- The dataset uses fixed Eastern Standard Time and ignores daylight saving
- IP geolocation is imprecise in real logs, which is why impossible travel ignores short distances
- An Isolation Forest is a baseline. Production fraud detection adds per-user baselines, device signals and feedback from analysts
