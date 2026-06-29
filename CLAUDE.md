# Churn Early-Warning Platform — Claude Code Briefing

## What this project is

B2B SaaS churn prediction pipeline built in **Snowflake + dbt**. Scores every account weekly,
outputs a `risk_tier` (HIGH / MEDIUM / LOW), and surfaces high-risk accounts for downstream
Slack/email alerts before renewal.

This is a portfolio/interview project. No live Snowflake connection yet — models should be
syntactically valid and structurally sound, ready to wire to real sources.

---

## Session 1 decisions (already made — do not re-discuss)

- **Weekly granularity** (not daily) — reduces noise in B2B usage patterns
- **4-week rolling window** as primary usage baseline
- **Rule-based weighted scoring** for now; ML upgrade planned for Session 4
- **Scoring weights:** usage 45% · commercial 30% · support 25%
- **Risk thresholds:** HIGH ≥ 70 · MEDIUM 40–69 · LOW < 40
- **Incremental strategy** on `int_account_activity_weekly` (unique key: account_id + week_start)
- **DuckDB for local dev** (Option B) — models run locally via `dbt run`; Snowflake profile stubbed for prod

---

## Your task: scaffold the full dbt project

### 1. Git init

```bash
git init
git add .
git commit -m "chore: initial scaffold from Session 1"
```

### 2. dbt project structure to create

```
churn-early-warning-platform/
├── dbt_project.yml
├── profiles.yml                  # Snowflake profile, vars filled with placeholders
├── packages.yml                  # dbt-utils
├── .gitignore
├── README.md
│
├── models/
│   ├── staging/
│   │   ├── _sources.yml          # source definitions for all 4 raw tables
│   │   ├── _staging.yml          # column-level docs + not_null/unique tests
│   │   ├── stg_accounts.sql
│   │   ├── stg_product_events.sql
│   │   ├── stg_billing_events.sql
│   │   └── stg_support_tickets.sql
│   │
│   ├── intermediate/
│   │   ├── _intermediate.yml
│   │   ├── int_account_activity_weekly.sql   ← SESSION 1 MODEL (see below)
│   │   ├── int_churn_features.sql            ← SESSION 1 MODEL (see below)
│   │   ├── int_account_support_signals.sql   ← STUB (Session 2)
│   │   └── int_account_billing_signals.sql   ← STUB (Session 2)
│   │
│   └── marts/
│       ├── _marts.yml
│       ├── mart_churn_risk_scores.sql        ← SESSION 1 MODEL (see below)
│       └── mart_churn_alerts.sql             ← STUB (Session 2)
│
└── tests/
    └── generic/
        └── .gitkeep
```

---

## Session 1 models — paste these verbatim

### models/intermediate/int_account_activity_weekly.sql

```sql
{{
    config(
        materialized='incremental'
        , unique_key=['account_id', 'week_start']
        , on_schema_change='sync_all_columns'
    )
}}

/*
    int_account_activity_weekly
    ---------------------------
    Account × week spine with product usage aggregates.
    Covers a rolling 12-week history; incremental on week_start.

    Grain: one row per account_id × week_start (Monday).

    DuckDB-native syntax. Snowflake differences noted inline.
*/

with

-- DuckDB: generate_series; Snowflake: table(generator(rowcount => 12)) + seq4()
weeks as (
    select unnest(range(12)) as n
)

, spine as (
    select
        a.account_id
        , (date_trunc('week', current_date) - (w.n * interval '1 week'))::date as week_start
    from {{ ref('stg_accounts') }} a
    cross join weeks w
    where a.is_active = true
)

, product_events as (
    select
        account_id
        , date_trunc('week', event_ts)::date          as week_start
        , count(distinct user_id)                     as wau
        , count(distinct session_id)                  as session_count
        , count(*)                                    as event_count
        , count(distinct feature_name)                as distinct_features_used
        , max(event_ts)                               as last_event_ts
    from {{ ref('stg_product_events') }}
    {% if is_incremental() %}
        -- DuckDB: interval arithmetic; Snowflake: dateadd('week', -13, ...)
        where event_ts >= date_trunc('week', current_date) - interval '13 weeks'
    {% endif %}
    group by 1, 2
)

, accounts as (
    select
        account_id
        , seats_contracted
        , mrr
        , plan_name
        , renewal_date
    from {{ ref('stg_accounts') }}
)

, activity as (
    select
        s.account_id
        , s.week_start
        -- DuckDB: + interval; Snowflake: dateadd('day', 6, ...)
        , (s.week_start + interval '6 days')::date    as week_end
        , coalesce(e.wau, 0)                          as wau
        , coalesce(e.session_count, 0)                as session_count
        , coalesce(e.event_count, 0)                  as event_count
        , coalesce(e.distinct_features_used, 0)       as distinct_features_used
        , e.last_event_ts
        , a.seats_contracted
        , case
            when a.seats_contracted > 0
            then round(coalesce(e.wau, 0) / a.seats_contracted, 4)
            else null
          end                                         as active_user_ratio
        -- DuckDB: datediff(part, start, end); same in Snowflake
        , datediff(
            'day'
            , e.last_event_ts
            , (s.week_start + interval '6 days')::date
          )                                           as days_since_last_event
        , a.mrr
        , a.plan_name
        , a.renewal_date
        , datediff(
            'day'
            , (s.week_start + interval '6 days')::date
            , a.renewal_date
          )                                           as days_to_renewal
    from spine s
    left join product_events e
        on  e.account_id = s.account_id
        and e.week_start = s.week_start
    inner join accounts a
        on a.account_id = s.account_id
)

, with_trends as (
    select
        *
        , avg(wau) over (
            partition by account_id
            order by week_start
            rows between 3 preceding and current row
          )                                           as wau_rolling_4w
        , avg(wau) over (
            partition by account_id
            order by week_start
            rows between 7 preceding and 4 preceding
          )                                           as wau_rolling_prev_4w
        , wau - lag(wau, 1) over (
            partition by account_id order by week_start
          )                                           as wau_wow_delta
    from activity
)

select
    account_id
    , week_start
    , week_end
    , wau
    , session_count
    , event_count
    , distinct_features_used
    , last_event_ts
    , seats_contracted
    , active_user_ratio
    , days_since_last_event
    , mrr
    , plan_name
    , renewal_date
    , days_to_renewal
    , wau_rolling_4w
    , wau_rolling_prev_4w
    , wau_wow_delta
    , case
        when wau_rolling_prev_4w = 0 then null
        else round(
            (wau_rolling_4w - wau_rolling_prev_4w) / nullif(wau_rolling_prev_4w, 0)
            , 4
        )
      end                                             as usage_trend_pct
    -- DuckDB: current_timestamp (no parens); Snowflake: current_timestamp()
    , current_timestamp                               as updated_at

from with_trends
```

---

### models/intermediate/int_churn_features.sql

```sql
{{
    config(
        materialized='table'
    )
}}

/*
    int_churn_features
    ------------------
    One row per account: latest-week feature set ready for scoring.
    Support + billing signals are stubbed (zeros/defaults) until Session 2.

    Grain: one row per account_id (current week snapshot).
*/

with

activity as (
    select *
    from {{ ref('int_account_activity_weekly') }}
    where week_start = date_trunc('week', current_date)
)

, support as (
    select
        account_id
        , 0                                          as open_tickets_count
        , 0                                          as p1_p2_tickets_30d
        , null::float                                as avg_ticket_resolution_days
    from {{ ref('stg_accounts') }}
)

, billing as (
    select
        account_id
        , false                                      as had_payment_failure_30d
        , false                                      as had_downgrade_90d
        , 1.0                                        as seat_contracted_ratio
    from {{ ref('stg_accounts') }}
)

select
    a.account_id
    , a.week_start                                   as feature_week
    , a.wau
    , a.wau_rolling_4w
    , a.wau_rolling_prev_4w
    , a.wau_wow_delta
    , a.usage_trend_pct
    , a.active_user_ratio
    , a.distinct_features_used
    , a.days_since_last_event
    , a.mrr
    , a.plan_name
    , a.renewal_date
    , a.days_to_renewal
    , a.seats_contracted
    , b.seat_contracted_ratio
    , b.had_payment_failure_30d
    , b.had_downgrade_90d
    , s.open_tickets_count
    , s.p1_p2_tickets_30d
    , s.avg_ticket_resolution_days
    , (a.days_to_renewal between 0 and 90)           as is_in_renewal_window
    , (a.wau = 0)                                    as is_zero_usage_week
    , (a.active_user_ratio < 0.25)                   as is_low_utilization

from activity a
left join support s  on s.account_id = a.account_id
left join billing b  on b.account_id = a.account_id
```

---

### models/marts/mart_churn_risk_scores.sql

```sql
{{
    config(
        materialized='table'
        , tags=['churn', 'weekly']
    )
}}

/*
    mart_churn_risk_scores
    ----------------------
    Final churn risk output. One row per account, refreshed weekly.

    risk_score (0–100) = usage_score * 0.45
                       + commercial_score * 0.30
                       + support_score * 0.25

    risk_tier: HIGH >= 70 | MEDIUM 40–69 | LOW < 40
*/

with

features as (
    select * from {{ ref('int_churn_features') }}
)

, usage_scored as (
    select
        account_id
        , greatest(0, least(100,
            case
                when usage_trend_pct is null       then 50
                when usage_trend_pct <= -0.50      then 100
                when usage_trend_pct <= -0.25      then 75
                when usage_trend_pct <= -0.10      then 50
                when usage_trend_pct <= 0.05       then 25
                else                                    0
            end
          ))                                       as usage_trend_score
        , greatest(0, least(100,
            case
                when is_zero_usage_week            then 100
                when active_user_ratio < 0.10      then 90
                when active_user_ratio < 0.25      then 65
                when active_user_ratio < 0.50      then 35
                when active_user_ratio < 0.75      then 15
                else                                    0
            end
          ))                                       as active_user_score
        , greatest(0, least(100,
            case
                when distinct_features_used = 0    then 100
                when distinct_features_used = 1    then 70
                when distinct_features_used <= 3   then 40
                when distinct_features_used <= 6   then 20
                else                                    0
            end
          ))                                       as feature_breadth_score
        , greatest(0, least(100,
            case
                when days_since_last_event is null then 100
                when days_since_last_event >= 28   then 90
                when days_since_last_event >= 14   then 60
                when days_since_last_event >= 7    then 30
                else                                    0
            end
          ))                                       as recency_score
    from features
)

, commercial_scored as (
    select
        account_id
        , greatest(0, least(100,
            case
                when days_to_renewal < 0           then 0
                when days_to_renewal <= 14          then 90
                when days_to_renewal <= 30          then 70
                when days_to_renewal <= 60          then 45
                when days_to_renewal <= 90          then 25
                else                                    0
            end
          ))                                       as renewal_urgency_score
        , greatest(0, least(100,
            case
                when seat_contracted_ratio < 0.25  then 90
                when seat_contracted_ratio < 0.50  then 60
                when seat_contracted_ratio < 0.75  then 30
                else                                    0
            end
          ))                                       as seat_utilization_score
        , case when had_payment_failure_30d then 80 else 0 end
                                                   as payment_failure_score
        , case when had_downgrade_90d then 60 else 0 end
                                                   as downgrade_score
    from features
)

, support_scored as (
    select
        account_id
        , greatest(0, least(100,
            case
                when p1_p2_tickets_30d >= 3        then 90
                when p1_p2_tickets_30d = 2         then 65
                when p1_p2_tickets_30d = 1         then 35
                else                                    0
            end
          ))                                       as critical_ticket_score
        , greatest(0, least(100,
            case
                when open_tickets_count >= 5       then 70
                when open_tickets_count >= 3       then 45
                when open_tickets_count >= 1       then 20
                else                                    0
            end
          ))                                       as open_ticket_score
        , greatest(0, least(100,
            case
                when avg_ticket_resolution_days is null then 0
                when avg_ticket_resolution_days > 14    then 70
                when avg_ticket_resolution_days > 7     then 40
                else                                         0
            end
          ))                                       as resolution_time_score
    from features
)

, component_scores as (
    select
        f.account_id
        , round((u.usage_trend_score + u.active_user_score
                 + u.feature_breadth_score + u.recency_score) / 4.0, 2)  as usage_score
        , round((c.renewal_urgency_score + c.seat_utilization_score
                 + c.payment_failure_score + c.downgrade_score) / 4.0, 2) as commercial_score
        , round((s.critical_ticket_score + s.open_ticket_score
                 + s.resolution_time_score) / 3.0, 2)                     as support_score
    from features f
    inner join usage_scored u      on u.account_id = f.account_id
    inner join commercial_scored c on c.account_id = f.account_id
    inner join support_scored s    on s.account_id = f.account_id
)

select
    f.account_id
    , f.feature_week                                 as scored_week
    , f.plan_name
    , f.mrr
    , f.renewal_date
    , f.days_to_renewal
    , f.is_in_renewal_window
    , cs.usage_score
    , cs.commercial_score
    , cs.support_score
    , round(
        cs.usage_score      * 0.45
        + cs.commercial_score * 0.30
        + cs.support_score    * 0.25
      , 2)                                          as risk_score
    , case
        when round(cs.usage_score * 0.45 + cs.commercial_score * 0.30
                   + cs.support_score * 0.25, 2) >= 70 then 'HIGH'
        when round(cs.usage_score * 0.45 + cs.commercial_score * 0.30
                   + cs.support_score * 0.25, 2) >= 40 then 'MEDIUM'
        else                                            'LOW'
      end                                           as risk_tier
    , f.wau
    , f.usage_trend_pct
    , f.active_user_ratio
    , f.distinct_features_used
    , f.days_since_last_event
    , f.had_payment_failure_30d
    , f.p1_p2_tickets_30d
    , current_timestamp()                           as scored_at

from features f
inner join component_scores cs on cs.account_id = f.account_id
order by risk_score desc
```

---

## Staging model contracts

All 4 staging models must expose exactly these columns (rename/cast from source as needed):

Types use **DuckDB conventions** (`varchar` → `text` or `varchar`, `timestamp` not `timestamp_ntz`).
Snowflake equivalent in parentheses where different.

### stg_accounts
| column | type (DuckDB) | notes |
|---|---|---|
| account_id | varchar | PK |
| account_name | varchar | |
| plan_name | varchar | |
| mrr | double | monthly recurring revenue USD (Snowflake: float) |
| seats_contracted | integer | |
| renewal_date | date | |
| is_active | boolean | |
| created_at | timestamp | (Snowflake: timestamp_ntz) |

### stg_product_events
| column | type (DuckDB) | notes |
|---|---|---|
| event_id | varchar | PK |
| account_id | varchar | FK → stg_accounts |
| user_id | varchar | |
| session_id | varchar | |
| feature_name | varchar | |
| event_ts | timestamp | (Snowflake: timestamp_ntz) |

### stg_billing_events
| column | type (DuckDB) | notes |
|---|---|---|
| event_id | varchar | PK |
| account_id | varchar | FK |
| event_type | varchar | e.g. payment_failure, downgrade, upgrade |
| amount | double | (Snowflake: float) |
| event_ts | timestamp | (Snowflake: timestamp_ntz) |

### stg_support_tickets
| column | type (DuckDB) | notes |
|---|---|---|
| ticket_id | varchar | PK |
| account_id | varchar | FK |
| severity | varchar | P1 / P2 / P3 / P4 |
| status | varchar | open / resolved / closed |
| created_at | timestamp | (Snowflake: timestamp_ntz) |
| resolved_at | timestamp | nullable (Snowflake: timestamp_ntz) |

---

## Stub models for Session 2

Create these as valid SQL stubs (SELECT with correct column names, no logic yet):

- `int_account_support_signals.sql` — grain: account_id, outputs: open_tickets_count, p1_p2_tickets_30d, avg_ticket_resolution_days
- `int_account_billing_signals.sql` — grain: account_id, outputs: had_payment_failure_30d, had_downgrade_90d, seat_contracted_ratio
- `mart_churn_alerts.sql` — grain: account_id, selects HIGH risk accounts from mart_churn_risk_scores with days_to_renewal <= 90

---

## dbt_project.yml key config

```yaml
name: churn_early_warning
version: '1.0.0'
config-version: 2

profile: churn_early_warning

model-paths: ["models"]
test-paths: ["tests"]

models:
  churn_early_warning:
    staging:
      +materialized: view
      +schema: staging
    intermediate:
      +materialized: table
      +schema: intermediate
    marts:
      +materialized: table
      +schema: marts
```

## profiles.yml (DuckDB dev + Snowflake prod)

```yaml
churn_early_warning:
  target: dev
  outputs:
    # Local dev — runs with dbt-duckdb, no credentials needed
    # Install: pip install dbt-duckdb
    dev:
      type: duckdb
      path: dev.duckdb        # file created in project root
      threads: 4

    # Production — wire up when Snowflake is available
    # Install: pip install dbt-snowflake
    prod:
      type: snowflake
      account: "YOUR_ACCOUNT"
      user: "YOUR_USER"
      password: "YOUR_PASSWORD"
      role: "YOUR_ROLE"
      database: "YOUR_DB"
      warehouse: "YOUR_WH"
      schema: "churn"
      threads: 4
```

> **Note:** `dev.duckdb` is a local file — add it to `.gitignore`.

---

## After scaffolding

```bash
# 1. Install adapters
pip install dbt-duckdb dbt-utils

# 2. Install dbt packages (dbt_utils)
dbt deps

# 3. Compile — validates all refs and SQL with no DB connection needed
dbt compile

# 4. Run against local DuckDB
dbt run --target dev

# 5. Confirm DAG looks correct
dbt docs generate && dbt docs serve
# Expected lineage: stg_* → int_account_activity_weekly → int_churn_features → mart_churn_risk_scores

# 6. Commit
git add .
git commit -m "feat: Session 1 scaffold — activity spine + scoring model (DuckDB dev)"
```

---

## Session 2 targets (do NOT build now)

- Replace stubs: `int_account_support_signals`, `int_account_billing_signals`
- Build `mart_churn_alerts` with real threshold logic
- Add dbt schema tests (not_null, unique, accepted_values, relationships)
- Slack notification integration
