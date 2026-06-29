# Churn Early-Warning Platform

B2B SaaS churn prediction pipeline. Scores every account weekly, outputs a `risk_tier` (HIGH / MEDIUM / LOW), and surfaces high-risk accounts for downstream Slack/email alerts before renewal.

## Architecture

```
stg_accounts
stg_product_events      →  int_account_activity_weekly  →  int_churn_features  →  mart_churn_risk_scores
stg_billing_events      →  int_account_billing_signals  ↗
stg_support_tickets     →  int_account_support_signals  ↗
                                                                                 →  mart_churn_alerts
```

## Scoring model

- **Usage** (45%): trend, active user ratio, feature breadth, recency
- **Commercial** (30%): renewal urgency, seat utilization, payment failures, downgrades
- **Support** (25%): P1/P2 tickets, open ticket volume, resolution time

Risk thresholds: HIGH ≥ 70 · MEDIUM 40–69 · LOW < 40

## Setup

```bash
# Install dbt packages
dbt deps

# Compile to verify SQL
dbt compile

# Run all models
dbt run

# Run tests
dbt test
```

## Status

- **Session 1** (complete): activity spine, feature extraction, risk scoring
- **Session 2** (planned): support/billing signals, alerts, schema tests
