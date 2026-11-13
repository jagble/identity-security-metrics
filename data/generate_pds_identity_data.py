"""
Generate a synthetic identity dataset for Potomac Defense Systems (PDS), a fictional company.

Writes four CSV files:
  hr_roster.csv          340 employees: department, title, status, hire/termination dates,
                         the date their account was actually disabled, privileged flag
  mfa_registrations.csv  the MFA method each person has registered (or none)
  signin_events.csv      ~90 days of sign-in events shaped like Entra ID sign-in logs
  planted_labels.csv     THE ANSWER KEY: every problem planted on purpose, at event or account level.
                         Detections never read this file. It is only used to score them afterwards.

Everything is fictional. IP addresses come only from the documentation ranges in RFC 5737
(192.0.2.0/24, 198.51.100.0/24, 203.0.113.0/24), so none belong to a real network.

Usage:  python generate_pds_identity_data.py --out ./output [--seed 42] [--end 2026-11-15]
"""
import argparse
import csv
import os
import random
from datetime import date, datetime, timedelta, timezone

DOMAIN = "PotomacDefenseSystems.onmicrosoft.com"
N_EMPLOYEES = 340
DAYS = 90
ET_OFFSET = -5  # Eastern Standard Time; the dataset ignores daylight saving for simplicity

DEPARTMENTS = {  # department: (weight, titles)
    "Engineering":        (0.30, ["Systems Engineer", "Software Engineer", "RF Engineer", "Test Engineer"]),
    "Manufacturing":      (0.15, ["Production Technician", "Quality Inspector", "Manufacturing Lead"]),
    "Program Management": (0.10, ["Program Manager", "Project Coordinator", "Scheduler"]),
    "Contracts":          (0.06, ["Contracts Administrator", "Subcontracts Manager"]),
    "Finance":            (0.07, ["Financial Analyst", "Cost Accountant"]),
    "Accounts Payable":   (0.04, ["AP Specialist", "AP Supervisor"]),
    "Human Resources":    (0.05, ["HR Generalist", "Recruiter"]),
    "Security":           (0.05, ["Security Analyst", "Facility Security Officer"]),
    "IT":                 (0.08, ["Systems Administrator", "Help Desk Technician", "Identity Engineer"]),
    "Business Development": (0.10, ["Capture Manager", "Proposal Writer"]),
}

CITIES = {  # name: (state/country, lat, lon)
    "Reston":           ("VA", 38.9586, -77.3570),   # HQ
    "Arlington":        ("VA", 38.8816, -77.0910),
    "Alexandria":       ("VA", 38.8048, -77.0469),
    "Fredericksburg":   ("VA", 38.3032, -77.4605),
    "Richmond":         ("VA", 37.5407, -77.4360),
    "Washington":       ("DC", 38.9072, -77.0369),
    "Bethesda":         ("MD", 38.9847, -77.0947),
    "Baltimore":        ("MD", 39.2904, -76.6122),
    "Huntsville":       ("AL", 34.7304, -86.5861),
    "Colorado Springs": ("CO", 38.8339, -104.8214),
}
SITES = {"Reston": (10, 60), "Huntsville": (100, 120), "Colorado Springs": (150, 170)}  # offices: corporate IP ranges
HOME_WEIGHTS = [0.14, 0.13, 0.10, 0.10, 0.09, 0.12, 0.09, 0.08, 0.08, 0.07]
FOREIGN = {  # used only by planted attacks
    "Amsterdam":   ("Netherlands", 52.3676, 4.9041),
    "Sao Paulo":   ("Brazil", -23.5505, -46.6333),
    "Bucharest":   ("Romania", 44.4268, 26.1025),
    "Lagos":       ("Nigeria", 6.5244, 3.3792),
    "Hanoi":       ("Vietnam", 21.0278, 105.8342),
    "Moscow":      ("Russia", 55.7558, 37.6173),
}

FIRST = ["James", "Mary", "Robert", "Patricia", "John", "Jennifer", "Michael", "Linda", "David", "Elizabeth",
         "William", "Barbara", "Richard", "Susan", "Joseph", "Jessica", "Thomas", "Sarah", "Charles", "Karen",
         "Daniel", "Nancy", "Matthew", "Lisa", "Anthony", "Betty", "Mark", "Sandra", "Steven", "Ashley",
         "Andrew", "Kimberly", "Kevin", "Emily", "Brian", "Donna", "George", "Michelle", "Edward", "Carol",
         "Kwame", "Abena", "Priya", "Wei", "Carlos", "Fatima", "Tunde", "Mei", "Diego", "Aisha"]
LAST = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez",
        "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson", "Taylor", "Moore", "Jackson", "Martin", "Lee",
        "Thompson", "White", "Harris", "Clark", "Lewis", "Robinson", "Walker", "Young", "Allen", "King",
        "Wright", "Scott", "Green", "Baker", "Adams", "Nelson", "Mensah", "Patel", "Chen", "Okafor"]

APPS = [("Office 365", 0.45), ("ServiceNow", 0.15), ("PDS Inventory Portal", 0.12),
        ("Cisco VPN", 0.18), ("Azure Portal", 0.10)]
METHODS = [("none", 0.05), ("sms", 0.22), ("authenticator", 0.55), ("fido2", 0.10), ("windows_hello", 0.08)]


def pick(rng, weighted):
    items, weights = zip(*weighted)
    return rng.choices(items, weights=weights, k=1)[0]


class Gen:
    def __init__(self, seed, end):
        self.rng = random.Random(seed)
        self.end = datetime.combine(end, datetime.min.time(), tzinfo=timezone.utc)
        self.start = self.end - timedelta(days=DAYS)
        self.events, self.labels = [], []
        self.n = 0

    # ---------- people ----------
    def roster(self):
        rng = self.rng
        people, used = [], set()
        dept_items = [(d, w) for d, (w, _) in DEPARTMENTS.items()]
        for i in range(N_EMPLOYEES):
            eid = 100001 + i
            if eid == 100170:
                first, last = "Monica", "Hayes"
            elif eid == 100215:
                first, last = "Chris", "Douglas"
            else:
                while True:
                    first, last = rng.choice(FIRST), rng.choice(LAST)
                    if (first, last) not in used and (first, last) not in {("Monica", "Hayes"), ("Chris", "Douglas")}:
                        break
            used.add((first, last))
            dept = "Security" if eid == 100170 else "Engineering" if eid == 100215 else pick(rng, dept_items)
            title = rng.choice(DEPARTMENTS[dept][1])
            home = rng.choices(list(CITIES), weights=HOME_WEIGHTS, k=1)[0]
            hire = self.start.date() - timedelta(days=rng.randint(30, 3000))
            if rng.random() < 0.05:  # joiners during the window
                hire = self.start.date() + timedelta(days=rng.randint(5, 70))
            people.append(dict(employee_id=eid, display_name=f"{first} {last}",
                               upn=f"{first.lower()}.{last.lower()}@{DOMAIN}", department=dept, job_title=title,
                               home_city=home, employment_status="Active", hire_date=hire,
                               termination_date=None, account_disabled_date=None, is_privileged=False))
        # privileged: 8 IT staff
        it = [p for p in people if p["department"] == "IT"]
        for p in rng.sample(it, min(8, len(it))):
            p["is_privileged"] = True
        # leavers: 20 terminated during the window, each with a real-world disable lag
        candidates = [p for p in people if p["employee_id"] not in (100170, 100215)
                      and not p["is_privileged"] and p["hire_date"] < self.start.date()]
        leavers = rng.sample(candidates, 20)
        for k, p in enumerate(leavers):
            term = self.start.date() + timedelta(days=rng.randint(10, 75))
            p["employment_status"], p["termination_date"] = "Terminated", term
            if k < 2:
                lag = None                                   # never disabled: PLANTED
            elif k < 6:
                lag = rng.randint(3, 12)                     # slow offboarding
            else:
                lag = rng.choice([0, 0, 0, 1])               # normal
            p["account_disabled_date"] = None if lag is None else term + timedelta(days=lag)
            if lag is None:
                self.label("account", p["upn"], "terminated_not_disabled")
        self.people = people
        return people

    def mfa(self):
        rng = self.rng
        regs = []
        privileged = [p for p in self.people if p["is_privileged"]]
        weak_admins = [p["upn"] for p in privileged[:2]]  # PLANTED: admins without phishing-resistant MFA
        for p in self.people:
            if p["is_privileged"]:
                m = "sms" if p["upn"] in weak_admins else rng.choice(["fido2", "fido2", "windows_hello"])
            else:
                m = pick(rng, METHODS)
            p["mfa_method"] = m
            regs.append(dict(upn=p["upn"], method=m, registered_date=p["hire_date"] + timedelta(days=rng.randint(0, 5))))
        for u in weak_admins:
            self.label("account", u, "privileged_without_phishing_resistant_mfa")
        for p in self.people:
            if p["mfa_method"] == "none":
                self.label("account", p["upn"], "no_mfa_registered")
        return regs

    # ---------- events ----------
    def label(self, level, key, scenario):
        self.labels.append(dict(level=level, key=key, scenario=scenario))

    def emit(self, ts, p_or_upn, app, city, country, lat, lon, ip, client="Browser", result="0",
             requirement="multiFactor", method=None, risk="none", scenario=None):
        self.n += 1
        eid = f"evt-{self.n:07d}"
        upn = p_or_upn if isinstance(p_or_upn, str) else p_or_upn["upn"]
        self.events.append(dict(event_id=eid, timestamp_utc=ts.strftime("%Y-%m-%dT%H:%M:%SZ"), upn=upn, app=app,
                                client_app=client, ip_address=ip, city=city, country=country,
                                latitude=f"{lat:.4f}", longitude=f"{lon:.4f}", result_code=result,
                                auth_requirement=requirement, mfa_method=method or "", risk_level=risk))
        if scenario:
            self.label("event", eid, scenario)
        return eid

    def normal_activity(self):
        rng = self.rng
        legacy_users = [p["upn"] for p in rng.sample([p for p in self.people if not p["is_privileged"]
                                                        and p["mfa_method"] != "none"], 4)]
        for u in legacy_users:
            self.label("account", u, "legacy_authentication")
        active_people = [p for p in self.people if p["employment_status"] == "Active" and p["hire_date"] < self.start.date()]
        dormant = {p["upn"]: self.start.date() + timedelta(days=rng.randint(5, 40))
                   for p in rng.sample([p for p in active_people if not p["is_privileged"]
                                        and p["employee_id"] not in (100170, 100215)], 12)}
        for u in dormant:
            self.label("account", u, "dormant_45_days")
        self.legacy_users, self.dormant = legacy_users, dormant

        for p in self.people:
            home = CITIES[p["home_city"]]
            home_ip = f"{rng.choice(['198.51.100', '203.0.113'])}.{rng.randint(1, 200)}"
            # normal work stops on the termination date; any later sign-in is planted in attacks()
            last_day = p["termination_date"] or self.end.date()
            if p["upn"] in dormant:
                last_day = dormant[p["upn"]]
            d = max(p["hire_date"], self.start.date())
            while d < min(last_day, self.end.date()):
                weekday = d.weekday() < 5
                if (weekday and rng.random() > 0.10) or (not weekday and rng.random() < 0.08):
                    for _ in range(rng.randint(1, 5) if weekday else 1):
                        hour_et = min(22, max(5, int(rng.gauss(12, 2.6))))
                        ts = datetime.combine(d, datetime.min.time(), tzinfo=timezone.utc) + \
                            timedelta(hours=hour_et - ET_OFFSET, minutes=rng.randint(0, 59), seconds=rng.randint(0, 59))
                        onsite = weekday and rng.random() < 0.25
                        site = p["home_city"] if p["home_city"] in SITES else "Reston"   # nearest PDS office
                        city = site if onsite else p["home_city"]
                        st, lat, lon = CITIES[city]
                        ip = f"192.0.2.{rng.randint(*SITES[site])}" if onsite else home_ip
                        client = "Browser" if rng.random() < 0.7 else "Mobile Apps and Desktop clients"
                        requirement, method = "multiFactor", p["mfa_method"]
                        if p["upn"] in legacy_users and rng.random() < 0.4:
                            client = rng.choice(["IMAP4", "Exchange ActiveSync"])
                            requirement, method = "singleFactorAuthentication", ""
                        if p["mfa_method"] == "none":
                            requirement, method = "singleFactorAuthentication", ""
                        r = rng.random()
                        result = "0" if r < 0.965 else "50126" if r < 0.99 else "50074"
                        risk = "low" if rng.random() < 0.01 else "none"
                        self.emit(ts, p, pick(rng, APPS), city, f"US-{st}", lat, lon, ip, client, result,
                                  requirement, method, risk)
                d += timedelta(days=1)

    def non_human(self):
        # a service account that exists in the directory but not in HR: a data quality check should catch it
        d = self.start.date()
        while d < self.end.date():
            ts = datetime.combine(d, datetime.min.time(), tzinfo=timezone.utc) + timedelta(hours=7, minutes=2)
            self.emit(ts, f"svc-backup@{DOMAIN}", "Office 365", "Reston", "US-VA", *CITIES["Reston"][1:],
                      ip="192.0.2.5", client="Exchange ActiveSync", requirement="singleFactorAuthentication")
            d += timedelta(days=1)
        self.label("account", f"svc-backup@{DOMAIN}", "identity_not_in_hr")

    def attacks(self):
        rng, people = self.rng, self.people
        active = [p for p in people if p["employment_status"] == "Active" and p["upn"] not in self.dormant]

        # 1. password spray from one IP, early morning, 75 accounts, ~40 minutes
        t0 = self.end - timedelta(days=12) + timedelta(hours=7, minutes=10)   # 02:10 ET
        city, (country, lat, lon) = "Moscow", FOREIGN["Moscow"]
        spray_ip = "203.0.113.250"
        no_mfa = [p for p in active if p["mfa_method"] == "none"]
        victims = rng.sample(no_mfa, 2)
        targets = rng.sample([p for p in active if p not in victims], 75)
        for k, p in enumerate(targets + victims):
            ts = t0 + timedelta(seconds=k * 30 + rng.randint(0, 20))
            ok = p in victims
            self.emit(ts, p, "Office 365", city, country, lat, lon, spray_ip, "Browser",
                      "0" if ok else "50126", "singleFactorAuthentication", "", "high" if ok else "medium",
                      scenario="password_spray_success" if ok else "password_spray")
        for p in victims:  # attacker reads mail afterwards
            for m in range(4):
                self.emit(t0 + timedelta(minutes=50 + m * 7), p, "Office 365", city, country, lat, lon, spray_ip,
                          "Browser", "0", "singleFactorAuthentication", "", "high", scenario="post_compromise_access")

        # 2. impossible travel: normal home sign-in, then success abroad 40-90 minutes later (stolen session/token)
        for p, where in zip(rng.sample([p for p in active if p["mfa_method"] not in ("none",)], 3),
                            ["Amsterdam", "Sao Paulo", "Hanoi"]):
            day = self.end - timedelta(days=rng.randint(3, 60))
            base = day.replace(hour=14, minute=rng.randint(0, 30))
            st, lat, lon = CITIES[p["home_city"]]
            self.emit(base, p, "Office 365", p["home_city"], f"US-{st}", lat, lon,
                      f"198.51.100.{rng.randint(1, 200)}", method=p["mfa_method"])
            country, flat, flon = FOREIGN[where]
            self.emit(base + timedelta(minutes=rng.randint(40, 90)), p, "Office 365", where, country, flat, flon,
                      f"198.51.100.{rng.randint(201, 249)}", method=p["mfa_method"], risk="medium",
                      scenario="impossible_travel")

        # 3. MFA fatigue: 12 denied pushes in 15 minutes, then the user approves one
        p = rng.choice([p for p in active if p["mfa_method"] == "authenticator" and not p["is_privileged"]])
        t = self.end - timedelta(days=20) + timedelta(hours=27)  # 22:00 ET
        country, lat, lon = FOREIGN["Lagos"]
        for k in range(12):
            self.emit(t + timedelta(seconds=75 * k), p, "Office 365", "Lagos", country, lat, lon, "203.0.113.77",
                      "Browser", "500121", "multiFactor", "authenticator", "medium", scenario="mfa_fatigue")
        self.emit(t + timedelta(minutes=16), p, "Office 365", "Lagos", country, lat, lon, "203.0.113.77",
                  "Browser", "0", "multiFactor", "authenticator", "high", scenario="mfa_fatigue_success")

        # 4. terminated but never disabled: keeps signing in to ServiceNow and Office 365
        for p in [p for p in people if p["employment_status"] == "Terminated" and p["account_disabled_date"] is None]:
            st, lat, lon = CITIES[p["home_city"]]
            for k in range(rng.randint(3, 6)):
                d = p["termination_date"] + timedelta(days=rng.randint(2, 25))
                if d >= self.end.date():
                    continue
                ts = datetime.combine(d, datetime.min.time(), tzinfo=timezone.utc) + timedelta(hours=rng.randint(14, 22))
                self.emit(ts, p, rng.choice(["ServiceNow", "Office 365"]), p["home_city"], f"US-{st}", lat, lon,
                          f"203.0.113.{rng.randint(1, 200)}", method=p["mfa_method"],
                          scenario="sign_in_after_termination")

        # 5. privileged account at 03:10 ET from a new country, approved with SMS
        admin = next(p for p in people if p["is_privileged"] and p["mfa_method"] == "sms")
        t = self.end - timedelta(days=6) + timedelta(hours=8, minutes=10)
        country, lat, lon = FOREIGN["Bucharest"]
        self.emit(t, admin, "Azure Portal", "Bucharest", country, lat, lon, "198.51.100.240", "Browser", "0",
                  "multiFactor", "sms", "medium", scenario="privileged_off_hours_new_country")

    def dirty_data(self):
        """Realistic data problems for the silver layer to catch."""
        rng = self.rng
        for e in rng.sample(self.events, int(len(self.events) * 0.005)):   # duplicate deliveries
            self.events.append(dict(e))
        for e in rng.sample(self.events, 10):                               # missing user
            e2 = dict(e); self.n += 1; e2["event_id"] = f"evt-{self.n:07d}"; e2["upn"] = ""
            self.events.append(e2)
        for e in rng.sample(self.events, 3):                                # clock skew: timestamps in the future
            e2 = dict(e); self.n += 1; e2["event_id"] = f"evt-{self.n:07d}"
            e2["timestamp_utc"] = (self.end + timedelta(days=400)).strftime("%Y-%m-%dT%H:%M:%SZ")
            self.events.append(e2)
        rng.shuffle(self.events)


def write(path, rows, fields):
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for r in rows:
            w.writerow({k: ("" if r.get(k) is None else r.get(k)) for k in fields})


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="output")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--end", default="2026-11-15", help="last day of the 90-day window (UTC)")
    a = ap.parse_args()
    os.makedirs(a.out, exist_ok=True)

    g = Gen(a.seed, date.fromisoformat(a.end))
    people = g.roster()
    regs = g.mfa()
    g.normal_activity()
    g.non_human()
    g.attacks()
    g.dirty_data()

    write(os.path.join(a.out, "hr_roster.csv"), people,
          ["employee_id", "display_name", "upn", "department", "job_title", "home_city", "employment_status",
           "hire_date", "termination_date", "account_disabled_date", "is_privileged"])
    write(os.path.join(a.out, "mfa_registrations.csv"), regs, ["upn", "method", "registered_date"])
    write(os.path.join(a.out, "signin_events.csv"), g.events,
          ["event_id", "timestamp_utc", "upn", "app", "client_app", "ip_address", "city", "country", "latitude",
           "longitude", "result_code", "auth_requirement", "mfa_method", "risk_level"])
    write(os.path.join(a.out, "planted_labels.csv"), g.labels, ["level", "key", "scenario"])

    ev = sum(1 for l in g.labels if l["level"] == "event")
    print(f"employees={len(people)} sign-ins={len(g.events)} planted: {ev} events, {len(g.labels) - ev} accounts")


if __name__ == "__main__":
    main()
