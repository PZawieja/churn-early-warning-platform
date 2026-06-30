#!/usr/bin/env python3
"""
generate_dashboard.py
---------------------
Generates a self-contained HTML churn risk dashboard from DuckDB.

Usage:
    # Real data (run dbt seed + run + snapshot first):
    python scripts/generate_dashboard.py --db dev.duckdb

    # Sample data — no DuckDB needed:
    python scripts/generate_dashboard.py --sample

    # Custom output path:
    python scripts/generate_dashboard.py --sample --out reports/churn_dashboard.html
"""

import argparse
import json
import sys
from datetime import date, datetime
from math import sqrt

try:
    import duckdb
except ImportError:
    duckdb = None


# ---------------------------------------------------------------------------
# Sample data — 15 accounts with deliberate risk spread for demo purposes
# Columns: id, plan, mrr, days_to_renewal, tier, score, usage_s, comm_s,
#          support_s, wau, usage_trend, active_ratio, features,
#          days_since_event, payment_fail, p1p2_tickets
# ---------------------------------------------------------------------------
SAMPLE_ALL = [
    ("acc_001", "Enterprise",  8500,   7,  "HIGH",   84, 88, 92, 65,  0, -0.92, 0.00, 0, 14, True,  2),
    ("acc_007", "Growth",      3200,  22,  "HIGH",   78, 82, 80, 65,  2, -0.67, 0.10, 1,  8, False, 1),
    ("acc_013", "Enterprise",  6800,  45,  "HIGH",   71, 79, 72, 55,  3, -0.58, 0.15, 2,  5, False, 0),
    ("acc_002", "Growth",      1200,  16,  "MEDIUM", 64, 60, 78, 48,  3, -0.51, 0.15, 3,  2, False, 1),
    ("acc_004", "Enterprise",  9200,  28,  "MEDIUM", 59, 55, 74, 42,  5, -0.43, 0.25, 4,  3, True,  0),
    ("acc_008", "Starter",      850,  62,  "MEDIUM", 52, 65, 45, 38,  1, -0.71, 0.08, 1,  6, False, 0),
    ("acc_009", "Growth",      2800,  78,  "MEDIUM", 48, 42, 62, 35,  6, -0.28, 0.30, 4,  2, False, 0),
    ("acc_012", "Starter",      620,  55,  "MEDIUM", 45, 58, 38, 32,  2, -0.45, 0.20, 2,  4, False, 0),
    ("acc_014", "Enterprise",  7400,  88,  "MEDIUM", 41, 38, 55, 28,  8, -0.15, 0.40, 5,  1, False, 0),
    ("acc_003", "Growth",      2400, 120,  "LOW",    32, 28, 42, 22, 12, +0.08, 0.60, 6,  1, False, 0),
    ("acc_005", "Enterprise", 11200,  95,  "LOW",    28, 22, 38, 25, 18, +0.12, 0.72, 8,  0, False, 0),
    ("acc_006", "Starter",      480, 180,  "LOW",    18, 15, 28, 12,  4, +0.22, 0.80, 5,  1, False, 0),
    ("acc_010", "Growth",      1800, 200,  "LOW",    15, 12, 20, 10,  9, +0.35, 0.75, 7,  0, False, 0),
    ("acc_011", "Enterprise",  5600, 155,  "LOW",    11,  8, 18,  8, 15, +0.18, 0.85, 9,  0, False, 0),
    ("acc_015", "Starter",      390, 220,  "LOW",     8,  6, 14,  5,  3, +0.45, 0.90, 4,  0, False, 0),
]

FIELDS = [
    "account_id", "plan_name", "mrr", "days_to_renewal", "risk_tier",
    "risk_score", "usage_score", "commercial_score", "support_score",
    "wau", "usage_trend_pct", "active_user_ratio", "distinct_features_used",
    "days_since_last_event", "had_payment_failure_30d", "p1_p2_tickets_30d",
]

PRIORITY_LABELS = {1: "CRITICAL", 2: "URGENT", 3: "HIGH", 4: "WATCH"}

def alert_priority(row):
    tier = row["risk_tier"]
    days = row["days_to_renewal"]
    if tier == "HIGH":
        if days <= 14: return 1
        if days <= 30: return 2
        return 3
    if tier == "MEDIUM" and days <= 90:
        return 4
    return None

def top_driver(row):
    u, c, s = row["usage_score"], row["commercial_score"], row["support_score"]
    m = max(u, c, s)
    if m == u: return "usage"
    if m == c: return "commercial"
    return "support"


# ---------------------------------------------------------------------------
# Data fetch
# ---------------------------------------------------------------------------
def fetch_from_duckdb(db_path):
    if duckdb is None:
        sys.exit("duckdb Python package required. Run: pip install duckdb")
    con = duckdb.connect(db_path, read_only=True)
    try:
        rows = con.execute("""
            select
                account_id, plan_name, mrr, days_to_renewal, risk_tier,
                risk_score, usage_score, commercial_score, support_score,
                wau, usage_trend_pct, active_user_ratio, distinct_features_used,
                days_since_last_event, had_payment_failure_30d, p1_p2_tickets_30d,
                scored_week
            from dev.marts.mart_churn_risk_scores
            order by risk_score desc
        """).fetchall()
        cols = [d[0] for d in con.description]
        accounts = [dict(zip(cols, r)) for r in rows]

        # Pull alert-specific fields from alerts table
        alert_rows = con.execute("""
            select account_id, alert_priority, top_risk_driver,
                   consecutive_high_weeks, first_alerted_at
            from dev.marts.mart_churn_alerts
        """).fetchall()
        alert_cols = [d[0] for d in con.description]
        alert_map = {r[0]: dict(zip(alert_cols, r)) for r in alert_rows}

        scored_week = accounts[0]["scored_week"] if accounts else date.today()
        return accounts, alert_map, str(scored_week)
    finally:
        con.close()


def use_sample():
    accounts = [dict(zip(FIELDS, row)) for row in SAMPLE_ALL]
    alert_map = {}
    for a in accounts:
        p = alert_priority(a)
        if p:
            alert_map[a["account_id"]] = {
                "account_id": a["account_id"],
                "alert_priority": p,
                "top_risk_driver": top_driver(a),
                "consecutive_high_weeks": 1 if a["risk_tier"] == "HIGH" else 0,
                "first_alerted_at": None,
            }
    scored_week = date.today().strftime("%B %d, %Y")
    return accounts, alert_map, scored_week


# ---------------------------------------------------------------------------
# HTML rendering helpers
# ---------------------------------------------------------------------------
PLAN_COLORS = {"Enterprise": "#818cf8", "Growth": "#34d399", "Starter": "#94a3b8"}
TIER_CLASS  = {"HIGH": "tier-high", "MEDIUM": "tier-medium", "LOW": "tier-low"}
PRIORITY_CLASS = {1: "p1", 2: "p2", 3: "p3", 4: "p4"}

def fmt_mrr(v):
    if v >= 1000: return f"${v/1000:.1f}K"
    return f"${v:,}"

def fmt_trend(v):
    if v is None: return '<span class="muted">—</span>'
    pct = v * 100
    cls = "trend-up" if pct > 0 else "trend-down" if pct < -10 else "muted"
    arrow = "↑" if pct > 0 else "↓"
    return f'<span class="{cls}">{arrow} {abs(pct):.0f}%</span>'

def fmt_ratio(v):
    if v is None: return '<span class="muted">—</span>'
    pct = v * 100
    cls = "trend-up" if pct >= 60 else "trend-down" if pct < 25 else "muted-light"
    return f'<span class="{cls}">{pct:.0f}%</span>'

def score_bar(score, height=6):
    # gradient bar: green→amber→red clipped to score width
    return f"""<div class="score-bar-wrap">
      <div class="score-bar" style="height:{height}px">
        <div class="score-fill" style="width:{score}%"></div>
      </div>
      <span class="score-num">{score:.0f}</span>
    </div>"""

def mini_breakdown(u, c, s):
    total = max(u + c + s, 1)
    wu = u / total * 100
    wc = c / total * 100
    ws = s / total * 100
    return f"""<div class="breakdown-bar" title="Usage {u:.0f} / Commercial {c:.0f} / Support {s:.0f}">
      <div class="bd-u" style="width:{wu}%"></div>
      <div class="bd-c" style="width:{wc}%"></div>
      <div class="bd-s" style="width:{ws}%"></div>
    </div>"""

def tier_badge(tier):
    return f'<span class="tier-badge tier-{tier.lower()}">{tier}</span>'

def plan_badge(plan):
    color = PLAN_COLORS.get(plan, "#94a3b8")
    return f'<span class="plan-badge" style="color:{color}">{plan}</span>'

def priority_badge(p):
    labels = {1: "① CRITICAL", 2: "② URGENT", 3: "③ HIGH", 4: "④ WATCH"}
    return f'<span class="pri-badge p{p}">{labels[p]}</span>'

def driver_badge(d):
    icons = {"usage": "📊", "commercial": "💰", "support": "🎫"}
    return f'<span class="driver-badge">{icons.get(d,"")} {d}</span>'

def alert_table_row(a, alert_info):
    p = alert_info["alert_priority"]
    return f"""<tr>
      <td>{priority_badge(p)}</td>
      <td><span class="account-id">{a["account_id"]}</span></td>
      <td>{plan_badge(a["plan_name"])}</td>
      <td class="num">{fmt_mrr(a["mrr"])}</td>
      <td class="num {'urgent' if a['days_to_renewal'] <= 30 else ''}">{a["days_to_renewal"]}d</td>
      <td>{score_bar(a["risk_score"])}</td>
      <td>{driver_badge(alert_info["top_risk_driver"])}</td>
      <td>{mini_breakdown(a["usage_score"], a["commercial_score"], a["support_score"])}</td>
      <td>{fmt_trend(a["usage_trend_pct"])}</td>
      <td>{fmt_ratio(a["active_user_ratio"])}</td>
    </tr>"""

def all_accounts_row(a):
    return f"""<tr>
      <td><span class="account-id">{a["account_id"]}</span></td>
      <td>{plan_badge(a["plan_name"])}</td>
      <td class="num">{fmt_mrr(a["mrr"])}</td>
      <td>{tier_badge(a["risk_tier"])}</td>
      <td>{score_bar(a["risk_score"], height=4)}</td>
      <td class="num muted-light">{a["usage_score"]:.0f}</td>
      <td class="num muted-light">{a["commercial_score"]:.0f}</td>
      <td class="num muted-light">{a["support_score"]:.0f}</td>
      <td class="num {'urgent' if a['days_to_renewal'] <= 30 else 'muted'}">{a["days_to_renewal"]}d</td>
    </tr>"""


# ---------------------------------------------------------------------------
# HTML template
# ---------------------------------------------------------------------------
HTML = """\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Churn Early Warning — Risk Dashboard</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&display=swap" rel="stylesheet">
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.min.js"></script>
<style>
:root{
  --bg:#07090f;
  --s1:#0e1117;
  --s2:#131720;
  --s3:#1a2030;
  --border:rgba(255,255,255,0.055);
  --border-h:rgba(255,255,255,0.11);
  --txt:#dde3f0;
  --muted:#5a6478;
  --muted-l:#8899b0;
  --high:#f04040;
  --high-bg:rgba(240,64,64,0.10);
  --high-b:rgba(240,64,64,0.28);
  --med:#f5a623;
  --med-bg:rgba(245,166,35,0.10);
  --med-b:rgba(245,166,35,0.28);
  --low:#2ecc81;
  --low-bg:rgba(46,204,129,0.08);
  --low-b:rgba(46,204,129,0.22);
  --acc:#7c6cf0;
  --acc-l:rgba(124,108,240,0.15);
}
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:'Inter',sans-serif;background:var(--bg);color:var(--txt);min-height:100vh;padding:28px 32px;font-size:13px}
a{color:var(--muted-l);text-decoration:none}
a:hover{color:var(--txt)}

/* ── HEADER ── */
.hdr{display:flex;justify-content:space-between;align-items:center;margin-bottom:28px;padding-bottom:20px;border-bottom:1px solid var(--border)}
.hdr-left{display:flex;align-items:center;gap:14px}
.live-dot{width:9px;height:9px;border-radius:50%;background:var(--acc);box-shadow:0 0 0 3px rgba(124,108,240,0.2),0 0 16px rgba(124,108,240,0.5);animation:pulse 2.4s ease-in-out infinite}
@keyframes pulse{0%,100%{opacity:1;box-shadow:0 0 0 3px rgba(124,108,240,0.2),0 0 16px rgba(124,108,240,0.5)}50%{opacity:.6;box-shadow:0 0 0 5px rgba(124,108,240,0.08),0 0 8px rgba(124,108,240,0.3)}}
.hdr-brand h1{font-size:17px;font-weight:700;letter-spacing:-.4px;color:#eef1f8}
.hdr-brand p{font-size:11px;color:var(--muted);margin-top:2px;letter-spacing:.3px}
.hdr-meta{text-align:right}
.hdr-week{font-size:13px;font-weight:500;color:var(--muted-l)}
.hdr-sub{font-size:11px;color:var(--muted);margin-top:3px}
.pipeline-tag{display:inline-flex;align-items:center;gap:5px;background:var(--s2);border:1px solid var(--border);border-radius:20px;padding:3px 10px;font-size:10px;color:var(--muted);letter-spacing:.2px;margin-top:6px}

/* ── KPI CARDS ── */
.kpi-row{display:grid;grid-template-columns:repeat(4,1fr);gap:14px;margin-bottom:20px}
.kpi{background:var(--s1);border:1px solid var(--border);border-radius:14px;padding:20px 22px;position:relative;overflow:hidden;transition:border-color .2s,transform .2s;animation:fadeUp .4s ease both}
.kpi:hover{border-color:var(--border-h);transform:translateY(-1px)}
.kpi::before{content:'';position:absolute;top:0;left:0;right:0;height:2px;border-radius:14px 14px 0 0}
.kpi.red::before{background:linear-gradient(90deg,var(--high),transparent)}
.kpi.amber::before{background:linear-gradient(90deg,var(--med),transparent)}
.kpi.green::before{background:linear-gradient(90deg,var(--low),transparent)}
.kpi.purple::before{background:linear-gradient(90deg,var(--acc),transparent)}
.kpi-label{font-size:10px;font-weight:600;letter-spacing:.9px;text-transform:uppercase;color:var(--muted);margin-bottom:10px}
.kpi-val{font-size:34px;font-weight:800;letter-spacing:-2px;line-height:1}
.kpi.red .kpi-val{color:var(--high)}
.kpi.amber .kpi-val{color:var(--med)}
.kpi.green .kpi-val{color:var(--low)}
.kpi.purple .kpi-val{color:var(--acc)}
.kpi-sub{font-size:11px;color:var(--muted);margin-top:6px}
.kpi:nth-child(1){animation-delay:.05s}.kpi:nth-child(2){animation-delay:.10s}
.kpi:nth-child(3){animation-delay:.15s}.kpi:nth-child(4){animation-delay:.20s}

/* ── CHARTS ── */
.charts-row{display:grid;grid-template-columns:260px 1fr;gap:14px;margin-bottom:20px;animation:fadeUp .4s .25s ease both}
.card{background:var(--s1);border:1px solid var(--border);border-radius:14px;padding:22px 24px}
.card-title{font-size:10px;font-weight:600;letter-spacing:.9px;text-transform:uppercase;color:var(--muted);margin-bottom:18px}

/* donut */
.donut-wrap{display:flex;flex-direction:column;align-items:center;gap:18px}
.donut-legend{width:100%}
.leg-item{display:flex;align-items:center;justify-content:space-between;padding:6px 0;border-bottom:1px solid var(--border)}
.leg-item:last-child{border-bottom:none}
.leg-left{display:flex;align-items:center;gap:8px;font-size:12px}
.leg-dot{width:7px;height:7px;border-radius:2px}
.leg-count{font-size:14px;font-weight:700}

/* scatter */
.scatter-note{font-size:10px;color:var(--muted);margin-top:10px;text-align:center}

/* ── TABLES ── */
.table-card{background:var(--s1);border:1px solid var(--border);border-radius:14px;overflow:hidden;margin-bottom:14px;animation:fadeUp .4s .3s ease both}
.table-hdr{padding:16px 24px;display:flex;justify-content:space-between;align-items:center;border-bottom:1px solid var(--border)}
.table-hdr-left{display:flex;align-items:center;gap:10px}
.tbl-title{font-size:10px;font-weight:600;letter-spacing:.9px;text-transform:uppercase;color:var(--muted)}
.badge-pill{background:var(--acc-l);color:var(--acc);border-radius:20px;padding:2px 10px;font-size:11px;font-weight:600}
.tbl-hint{font-size:11px;color:var(--muted)}

table{width:100%;border-collapse:collapse}
thead th{padding:9px 14px;text-align:left;font-size:10px;font-weight:600;color:var(--muted);text-transform:uppercase;letter-spacing:.6px;background:var(--s2);border-bottom:1px solid var(--border);white-space:nowrap}
thead th.num{text-align:right}
tbody tr{border-bottom:1px solid var(--border);transition:background .12s}
tbody tr:last-child{border-bottom:none}
tbody tr:hover{background:rgba(255,255,255,0.018)}
td{padding:11px 14px;vertical-align:middle}
td.num{text-align:right;font-variant-numeric:tabular-nums}
td.urgent{color:var(--high);font-weight:600}

/* Account ID */
.account-id{font-family:'JetBrains Mono','Fira Code','Cascadia Code',monospace;font-size:12px;color:var(--muted-l);letter-spacing:-.3px}

/* Priority badge */
.pri-badge{display:inline-flex;align-items:center;gap:4px;padding:3px 9px;border-radius:6px;font-size:10px;font-weight:700;letter-spacing:.3px;white-space:nowrap}
.p1{background:rgba(240,64,64,0.14);color:#f87171;border:1px solid rgba(240,64,64,0.3)}
.p2{background:rgba(245,166,35,0.12);color:#fbbf24;border:1px solid rgba(245,166,35,0.3)}
.p3{background:rgba(240,64,64,0.07);color:#fca5a5;border:1px solid rgba(240,64,64,0.18)}
.p4{background:rgba(234,179,8,0.08);color:#ca8a04;border:1px solid rgba(234,179,8,0.18)}

/* Tier badge */
.tier-badge{padding:2px 8px;border-radius:5px;font-size:10px;font-weight:700;letter-spacing:.4px}
.tier-high{background:var(--high-bg);color:var(--high);border:1px solid var(--high-b)}
.tier-medium{background:var(--med-bg);color:var(--med);border:1px solid var(--med-b)}
.tier-low{background:var(--low-bg);color:var(--low);border:1px solid var(--low-b)}

/* Plan badge */
.plan-badge{font-size:11px;font-weight:500}

/* Score bar */
.score-bar-wrap{display:flex;align-items:center;gap:10px;min-width:120px}
.score-bar{flex:1;border-radius:3px;background:var(--s3);overflow:hidden}
.score-fill{height:100%;background:linear-gradient(90deg,#2ecc81 0%,#f5a623 55%,#f04040 100%);border-radius:3px}
.score-num{font-weight:700;font-size:13px;min-width:28px;text-align:right;color:var(--txt)}

/* Component breakdown */
.breakdown-bar{display:flex;height:5px;border-radius:3px;overflow:hidden;width:72px;gap:1px;cursor:help}
.bd-u{background:#7c6cf0}.bd-c{background:#f5a623}.bd-s{background:#22b8cf}

/* Driver badge */
.driver-badge{font-size:11px;color:var(--muted-l);background:var(--s2);border:1px solid var(--border);padding:2px 8px;border-radius:5px;white-space:nowrap}

/* Trend */
.trend-up{color:var(--low)}.trend-down{color:var(--high)}.muted{color:var(--muted)}.muted-light{color:var(--muted-l)}

/* Legend row (below tables) */
.legend-row{display:flex;gap:18px;padding:10px 24px;border-top:1px solid var(--border);background:var(--s2)}
.legend-item{display:flex;align-items:center;gap:6px;font-size:10px;color:var(--muted)}
.swatch{width:10px;height:4px;border-radius:2px}

/* Footer */
.footer{margin-top:20px;padding-top:16px;border-top:1px solid var(--border);display:flex;justify-content:space-between;font-size:11px;color:var(--muted)}

/* Animation */
@keyframes fadeUp{from{opacity:0;transform:translateY(10px)}to{opacity:1;transform:translateY(0)}}
</style>
</head>
<body>

<!-- ── HEADER ── -->
<div class="hdr">
  <div class="hdr-left">
    <div class="live-dot"></div>
    <div class="hdr-brand">
      <h1>Churn Early Warning</h1>
      <p>Risk Intelligence Dashboard</p>
    </div>
  </div>
  <div class="hdr-meta">
    <div class="hdr-week">Week of SCORED_WEEK</div>
    <div class="hdr-sub">Generated GENERATED_AT</div>
    <div class="pipeline-tag">⚙ dbt pipeline &middot; DuckDB &middot; Snowflake-ready</div>
  </div>
</div>

<!-- ── KPI CARDS ── -->
<div class="kpi-row">
  <div class="kpi red">
    <div class="kpi-label">Accounts at Risk</div>
    <div class="kpi-val">ALERT_COUNT</div>
    <div class="kpi-sub">require CSM action this week</div>
  </div>
  <div class="kpi amber">
    <div class="kpi-label">MRR Exposed</div>
    <div class="kpi-val">MRR_AT_RISK</div>
    <div class="kpi-sub">across risk accounts</div>
  </div>
  <div class="kpi red">
    <div class="kpi-label">HIGH Tier</div>
    <div class="kpi-val">HIGH_COUNT</div>
    <div class="kpi-sub">score &ge; 70 &middot; immediate action</div>
  </div>
  <div class="kpi purple">
    <div class="kpi-label">Avg Risk Score</div>
    <div class="kpi-val">AVG_SCORE</div>
    <div class="kpi-sub">across all TOTAL_COUNT accounts</div>
  </div>
</div>

<!-- ── CHARTS ── -->
<div class="charts-row">
  <div class="card">
    <div class="card-title">Risk Distribution</div>
    <div class="donut-wrap">
      <canvas id="donut" width="180" height="180"></canvas>
      <div class="donut-legend">
        <div class="leg-item">
          <div class="leg-left"><div class="leg-dot" style="background:var(--high)"></div><span>HIGH</span></div>
          <span class="leg-count" style="color:var(--high)">HIGH_COUNT</span>
        </div>
        <div class="leg-item">
          <div class="leg-left"><div class="leg-dot" style="background:var(--med)"></div><span>MEDIUM</span></div>
          <span class="leg-count" style="color:var(--med)">MED_COUNT</span>
        </div>
        <div class="leg-item">
          <div class="leg-left"><div class="leg-dot" style="background:var(--low)"></div><span>LOW</span></div>
          <span class="leg-count" style="color:var(--low)">LOW_COUNT</span>
        </div>
      </div>
    </div>
  </div>

  <div class="card">
    <div class="card-title">Risk Score vs Days to Renewal &mdash; bubble size = MRR</div>
    <canvas id="scatter" style="max-height:240px"></canvas>
    <div class="scatter-note">
      Danger zone (top-left): high risk + imminent renewal = immediate CSM call
    </div>
  </div>
</div>

<!-- ── ALERT PRIORITY QUEUE ── -->
<div class="table-card">
  <div class="table-hdr">
    <div class="table-hdr-left">
      <span class="tbl-title">Priority Queue</span>
      <span class="badge-pill">ALERT_COUNT accounts</span>
    </div>
    <span class="tbl-hint">sorted: priority &rarr; risk score</span>
  </div>
  <table>
    <thead>
      <tr>
        <th>Priority</th>
        <th>Account</th>
        <th>Plan</th>
        <th class="num">MRR</th>
        <th class="num">Renewal</th>
        <th>Risk Score</th>
        <th>Top Driver</th>
        <th>U / C / S</th>
        <th>Usage Trend</th>
        <th>Seat Util</th>
      </tr>
    </thead>
    <tbody>
ALERT_ROWS
    </tbody>
  </table>
  <div class="legend-row">
    <div class="legend-item"><div class="swatch" style="background:#7c6cf0"></div>Usage score</div>
    <div class="legend-item"><div class="swatch" style="background:#f5a623"></div>Commercial score</div>
    <div class="legend-item"><div class="swatch" style="background:#22b8cf"></div>Support score</div>
  </div>
</div>

<!-- ── ALL ACCOUNTS ── -->
<div class="table-card">
  <div class="table-hdr">
    <div class="table-hdr-left">
      <span class="tbl-title">All Accounts</span>
      <span class="badge-pill">TOTAL_COUNT</span>
    </div>
    <span class="tbl-hint">full scoring output &middot; mart_churn_risk_scores</span>
  </div>
  <table>
    <thead>
      <tr>
        <th>Account</th>
        <th>Plan</th>
        <th class="num">MRR</th>
        <th>Tier</th>
        <th>Risk Score</th>
        <th class="num">Usage</th>
        <th class="num">Commercial</th>
        <th class="num">Support</th>
        <th class="num">Days to Renewal</th>
      </tr>
    </thead>
    <tbody>
ALL_ROWS
    </tbody>
  </table>
</div>

<!-- ── FOOTER ── -->
<div class="footer">
  <span>Churn Early Warning Platform &mdash; dbt + DuckDB + Snowflake</span>
  <a href="https://github.com/PZawieja/churn-early-warning-platform" target="_blank">
    github.com/PZawieja/churn-early-warning-platform
  </a>
</div>

<script>
// ── Donut chart ──
const donutCtx = document.getElementById('donut');
new Chart(donutCtx, {
  type: 'doughnut',
  data: {
    labels: ['HIGH', 'MEDIUM', 'LOW'],
    datasets: [{
      data: DONUT_DATA,
      backgroundColor: ['rgba(240,64,64,0.8)','rgba(245,166,35,0.8)','rgba(46,204,129,0.7)'],
      borderColor: ['#f04040','#f5a623','#2ecc81'],
      borderWidth: 1,
      hoverOffset: 6
    }]
  },
  options: {
    cutout: '68%',
    plugins: {
      legend: { display: false },
      tooltip: {
        backgroundColor: '#131720',
        borderColor: 'rgba(255,255,255,0.08)',
        borderWidth: 1,
        titleColor: '#dde3f0',
        bodyColor: '#8899b0',
        callbacks: { label: ctx => `  ${ctx.label}: ${ctx.parsed} accounts` }
      }
    }
  }
});

// ── Bubble / scatter chart ──
const scatterCtx = document.getElementById('scatter');
const pts = SCATTER_DATA;

const dangerZone = {
  id: 'dangerZone',
  beforeDraw(chart) {
    const { ctx, scales: { x, y } } = chart;
    if (!x || !y) return;
    const x0 = x.left, x1 = x.getPixelForValue(90);
    const y0 = y.getPixelForValue(100), y1 = y.getPixelForValue(50);
    ctx.save();
    ctx.fillStyle = 'rgba(240,64,64,0.04)';
    ctx.fillRect(x0, y0, x1 - x0, y1 - y0);
    ctx.strokeStyle = 'rgba(240,64,64,0.18)';
    ctx.setLineDash([4, 4]);
    ctx.strokeRect(x0, y0, x1 - x0, y1 - y0);
    ctx.restore();
  }
};

Chart.register(dangerZone);

new Chart(scatterCtx, {
  type: 'bubble',
  data: {
    datasets: [
      {
        label: 'HIGH',
        data: pts.filter(d => d.tier === 'HIGH').map(d => ({x: d.days, y: d.score, r: d.r, id: d.id, mrr: d.mrr})),
        backgroundColor: 'rgba(240,64,64,0.65)',
        borderColor: '#f04040',
        borderWidth: 1.5,
      },
      {
        label: 'MEDIUM',
        data: pts.filter(d => d.tier === 'MEDIUM').map(d => ({x: d.days, y: d.score, r: d.r, id: d.id, mrr: d.mrr})),
        backgroundColor: 'rgba(245,166,35,0.65)',
        borderColor: '#f5a623',
        borderWidth: 1.5,
      },
      {
        label: 'LOW',
        data: pts.filter(d => d.tier === 'LOW').map(d => ({x: d.days, y: d.score, r: d.r, id: d.id, mrr: d.mrr})),
        backgroundColor: 'rgba(46,204,129,0.45)',
        borderColor: '#2ecc81',
        borderWidth: 1,
      }
    ]
  },
  options: {
    responsive: true,
    scales: {
      x: {
        min: 0, max: 240,
        title: { display: true, text: 'Days to Renewal', color: '#5a6478', font: { size: 11 } },
        grid: { color: 'rgba(255,255,255,0.04)' },
        ticks: { color: '#5a6478', font: { size: 11 } }
      },
      y: {
        min: 0, max: 100,
        title: { display: true, text: 'Risk Score', color: '#5a6478', font: { size: 11 } },
        grid: { color: 'rgba(255,255,255,0.04)' },
        ticks: { color: '#5a6478', font: { size: 11 } }
      }
    },
    plugins: {
      legend: {
        labels: { color: '#8899b0', font: { size: 11 }, boxWidth: 10, padding: 16 }
      },
      tooltip: {
        backgroundColor: '#131720',
        borderColor: 'rgba(255,255,255,0.08)',
        borderWidth: 1,
        titleColor: '#dde3f0',
        bodyColor: '#8899b0',
        callbacks: {
          title: items => items[0].raw.id,
          label: item => [
            `  Score: ${item.raw.y}`,
            `  Renewal in: ${item.raw.x}d`,
            `  MRR: $${item.raw.mrr.toLocaleString()}`
          ]
        }
      }
    }
  }
});
</script>
</body>
</html>
"""


# ---------------------------------------------------------------------------
# HTML generator
# ---------------------------------------------------------------------------
def make_html(accounts, alert_map, scored_week):
    tier_counts = {"HIGH": 0, "MEDIUM": 0, "LOW": 0}
    for a in accounts:
        tier_counts[a["risk_tier"]] = tier_counts.get(a["risk_tier"], 0) + 1

    alerts = sorted(
        [a for a in accounts if a["account_id"] in alert_map],
        key=lambda a: (alert_map[a["account_id"]]["alert_priority"], -a["risk_score"])
    )

    mrr_at_risk = sum(a["mrr"] for a in alerts)
    avg_score = sum(a["risk_score"] for a in accounts) / len(accounts) if accounts else 0

    scatter_pts = [
        {
            "id": a["account_id"],
            "days": a["days_to_renewal"],
            "score": round(a["risk_score"], 1),
            "tier": a["risk_tier"],
            "mrr": a["mrr"],
            "r": min(max(round(sqrt(a["mrr"] / 400), 1), 4), 22),
        }
        for a in accounts
    ]

    alert_rows_html = "\n".join(alert_table_row(a, alert_map[a["account_id"]]) for a in alerts)
    all_rows_html   = "\n".join(all_accounts_row(a) for a in accounts)

    html = HTML
    replacements = {
        "SCORED_WEEK":   str(scored_week),
        "GENERATED_AT":  datetime.now().strftime("%Y-%m-%d %H:%M"),
        "ALERT_COUNT":   str(len(alerts)),
        "MRR_AT_RISK":   fmt_mrr(mrr_at_risk),
        "HIGH_COUNT":    str(tier_counts.get("HIGH", 0)),
        "MED_COUNT":     str(tier_counts.get("MEDIUM", 0)),
        "LOW_COUNT":     str(tier_counts.get("LOW", 0)),
        "AVG_SCORE":     f"{avg_score:.0f}",
        "TOTAL_COUNT":   str(len(accounts)),
        "DONUT_DATA":    json.dumps([tier_counts.get("HIGH",0), tier_counts.get("MEDIUM",0), tier_counts.get("LOW",0)]),
        "SCATTER_DATA":  json.dumps(scatter_pts),
        "ALERT_ROWS":    alert_rows_html,
        "ALL_ROWS":      all_rows_html,
    }
    for key, val in replacements.items():
        html = html.replace(key, val)
    return html


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Generate churn risk HTML dashboard.")
    parser.add_argument("--db",     default="dev.duckdb", help="Path to DuckDB file")
    parser.add_argument("--out",    default="dashboard.html", help="Output HTML path")
    parser.add_argument("--sample", action="store_true",  help="Use sample data (no DuckDB)")
    args = parser.parse_args()

    if args.sample or duckdb is None:
        if not args.sample:
            print("duckdb not installed — falling back to sample data.", file=sys.stderr)
        accounts, alert_map, scored_week = use_sample()
    else:
        accounts, alert_map, scored_week = fetch_from_duckdb(args.db)

    html = make_html(accounts, alert_map, scored_week)

    with open(args.out, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"✓ Dashboard written to {args.out}  ({len(accounts)} accounts, {len(alert_map)} alerts)")


if __name__ == "__main__":
    main()
