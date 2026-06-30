# Churn Early-Warning Platform

B2B SaaS churn prediction pipeline built in **Snowflake + dbt**. Scores every account weekly, outputs a `risk_tier` (HIGH / MEDIUM / LOW), and surfaces high-risk accounts for downstream Slack/email alerts before renewal.

> Portfolio project demonstrating dbt layered architecture, incremental models, SCD2 snapshots, and rule-based ML-ready scoring. No live Snowflake connection required — runs locally via DuckDB.

---

## Architecture

```mermaid
flowchart LR
    subgraph Raw ["Raw (seeds)"]
        A[accounts]
        B[product_events]
        C[billing_events]
        D[support_tickets]
    end

    subgraph Staging ["Staging (views)"]
        SA[stg_accounts]
        SB[stg_product_events]
        SC[stg_billing_events]
        SD[stg_support_tickets]
    end

    subgraph Intermediate ["Intermediate (tables)"]
        W[int_account_activity_weekly]
        SS[int_account_support_signals]
        BS[int_account_billing_signals]
        F[int_churn_features]
    end

    subgraph Marts ["Marts (tables)"]
        R[mart_churn_risk_scores]
        AL[mart_churn_alerts]
    end

    subgraph Snapshots ["Snapshots (SCD2)"]
        SN[snp_churn_risk_scores]
    end

    A & B & C & D --> SA & SB & SC & SD
    SA & SB --> W
    SD --> SS
    SC & SB & SA --> BS
    W & SS & BS --> F
    F --> R
    R --> AL
    R --> SN
    SN --> AL
```

### Scoring weights

| Component | Weight | Signals |
|---|---|---|
| Usage | 45% | WAU trend, active user ratio, feature breadth, recency |
| Commercial | 30% | Days to renewal, seat utilization, payment failures, downgrades |
| Support | 25% | P1/P2 ticket volume, open backlog, resolution time |

**Risk thresholds:** HIGH ≥ 70 · MEDIUM 40–69 · LOW < 40

---

## Dashboard

A self-contained HTML dashboard ships with the project. No server required — open in any browser.

```bash
# Preview with built-in sample data (no DuckDB needed)
python scripts/generate_dashboard.py --sample

# Real data (after dbt seed + run + snapshot)
python scripts/generate_dashboard.py --db dev.duckdb
```

### Overview — KPIs + risk distribution + renewal timeline

![Dashboard overview](blog_shot_overview.png)

### Priority Queue — CSM action queue with alert tier, score breakdown, and usage signals

![Priority queue](blog_shot_priority_queue.png)

### All Accounts — full scored account list with tier badges

![All accounts](blog_shot_all_accounts.png)

---

## Quick start (DuckDB local dev)

### Prerequisites

```bash
pip install dbt-duckdb dbt-utils duckdb
```

### Run

```bash
# 1. Install dbt packages
dbt deps

# 2. Load seed data (synthetic accounts, events, billing, tickets)
dbt seed

# 3. Run all models
dbt run

# 4. Capture risk score history (SCD2 snapshot)
dbt snapshot

# 5. Re-run alerts to pick up snapshot data
dbt run --select mart_churn_alerts

# 6. Run tests
dbt test

# 7. Dry-run Slack alert
python scripts/notify_slack.py --db dev.duckdb --dry-run
```

### Expected DAG lineage

```
stg_accounts, stg_product_events
    → int_account_activity_weekly
        → int_churn_features (+ support + billing signals)
            → mart_churn_risk_scores
                → snp_churn_risk_scores (snapshot)
                → mart_churn_alerts
```

---

## Project structure

```
churn-early-warning-platform/
├── models/
│   ├── staging/          # Thin rename/cast views over raw sources
│   ├── intermediate/     # Business logic: usage spine, support, billing, feature set
│   └── marts/            # Scoring + alerts — consumed by downstream tools
├── snapshots/            # SCD2 history of risk score changes
├── seeds/                # Synthetic CSV data for local dev/testing
├── scripts/
│   ├── generate_dashboard.py  # HTML dashboard generator (--sample or --db)
│   └── notify_slack.py        # Weekly Slack digest (reads mart_churn_alerts)
├── tests/generic/        # Custom generic tests
├── dbt_project.yml
├── profiles.yml          # DuckDB dev + Snowflake prod
└── packages.yml          # dbt-utils
```

---

## Key design decisions

**Weekly granularity** — reduces noise in B2B usage patterns vs daily.

**4-week rolling window** — primary usage baseline balances recency with signal stability.

**Account × week spine** — generated from `stg_accounts × range(12)` so zero-usage weeks are explicit rows, not gaps. Without this, window functions silently skip dormant accounts.

**Incremental on `int_account_activity_weekly`** — unique key `(account_id, week_start)`, 13-week lookback filter on incremental runs to recompute rolling averages correctly.

**SCD2 snapshot** — `snp_churn_risk_scores` captures tier/score changes over time. Powers `consecutive_high_weeks` (current HIGH streak length) and `first_alerted_at` (when did this account first go HIGH?) in `mart_churn_alerts`.

**Rule-based scoring + ML layer** — rule-based model (Sessions 1–3) provides interpretable baseline. ML model (Session 6) trains on the same `int_churn_features` feature set and writes predictions back to DuckDB as a source for `mart_churn_risk_scores_ml`. Both run in parallel — `tier_agreement` column shows where they agree or diverge.

---

## ML pipeline (Session 6)

Gradient Boosting / Logistic Regression trained on the `int_churn_features` feature set. Compares ML and rule-based scores side by side.

```bash
# 1. Train — generates synthetic labelled data, cross-validates, saves model
python scripts/ml_pipeline.py train --db dev.duckdb

# 2. Score — loads model, writes ml_output.ml_predictions to DuckDB
python scripts/ml_pipeline.py score --db dev.duckdb

# 3. Build the ML comparison mart
dbt run --select mart_churn_risk_scores_ml

# 4. Compare — prints rule-based vs ML side by side with tier agreement stats
python scripts/ml_pipeline.py compare --db dev.duckdb
```

### Sample output — `compare`

```
  ACCOUNT              PLAN            MRR   RULE    ML  PROB  TIER(R)  TIER(ML)     Δ  AGR
  acc_011              enterprise   $6,000   26.1  89.0 0.890  LOW      HIGH     +62.9  ≠  ← ML upgraded
  acc_007              growth       $  980   17.1  77.5 0.775  LOW      HIGH     +60.4  ≠  ← ML upgraded
  acc_002              growth       $1,200   63.8  62.7 0.627  MEDIUM   MEDIUM    -1.2  ✓

  Tier agreement:  60%  |  Avg Δ: +6.3  |  ML HIGH: 2 accounts ($6,980 MRR at risk)
```

Key finding: ML model surfaces accounts the rule-based model missed — `p1_p2_tickets_30d` (support signal) ranked as the #1 feature by LR coefficients, ahead of usage and commercial signals, challenging the initial 45/30/25 weighting assumption.

---

## Snowflake production

Update `profiles.yml` with real credentials and run:

```bash
dbt run --target prod
dbt snapshot --target prod
```

Syntax differences (DuckDB → Snowflake) are documented inline in each model.

---

## Roadmap

| Session | Status | Deliverables |
|---|---|---|
| 1 | ✅ Done | Architecture, usage spine, scoring model |
| 2 | ✅ Done | Real support + billing signal models |
| 3 | ✅ Done | Seeds, schema tests, Slack notification script |
| 4 | ✅ Done | SCD2 snapshot, alert history columns, README |
| 5 | ✅ Done | Self-contained HTML dashboard + Python generator |
| 6 | ✅ Done | ML pipeline: LR + GBM, feature importance, rule vs ML comparison |
