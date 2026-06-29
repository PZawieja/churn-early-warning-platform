{{
    config(
        materialized='table'
        , tags=['churn', 'weekly', 'alerts']
    )
}}

/*
    mart_churn_alerts
    -----------------
    Accounts requiring CSM action this week.

    Scope: HIGH risk accounts + MEDIUM accounts inside their 90-day renewal window.

    Adds alert_priority for routing and top_risk_driver for explainability.
    Adds consecutive_high_weeks and first_alerted_at from snapshot history.

    Grain: one row per account_id (current week).

    alert_priority:
        1 = CRITICAL  — HIGH risk, renewal ≤ 14 days
        2 = URGENT    — HIGH risk, renewal ≤ 30 days
        3 = HIGH      — HIGH risk, renewal > 30 days
        4 = WATCH     — MEDIUM risk, inside renewal window

    Dependency: run `dbt snapshot` before this model so snp_churn_risk_scores
    contains current data. On first snapshot run, consecutive_high_weeks = 1
    for all HIGH accounts (expected — no prior history exists yet).
*/

with

scores as (
    select * from {{ ref('mart_churn_risk_scores') }}
)

/*
    Alert history sourced from SCD2 snapshot.

    consecutive_high_weeks:
        Weeks elapsed since the account's most recent entry into HIGH tier.
        Calculated from dbt_valid_from of the current HIGH record (dbt_valid_to is null).
        Resets to 0 if account is not currently HIGH.
        On first snapshot run this equals 1 for all HIGH accounts.

    first_alerted_at:
        Earliest dbt_valid_from where risk_tier = 'HIGH'.
        Stays fixed once set; helps CSMs distinguish new vs chronic risks.
*/
, alert_history as (
    select
        account_id
        , min(case when risk_tier = 'HIGH' then dbt_valid_from end)         as first_alerted_at
        , max(case
            when risk_tier = 'HIGH' and dbt_valid_to is null
            then datediff('week', dbt_valid_from::date, current_date) + 1
            else 0
          end)                                                               as consecutive_high_weeks
    from {{ ref('snp_churn_risk_scores') }}
    group by 1
)

, alerts as (
    select
        s.account_id
        , s.scored_week
        , s.plan_name
        , s.mrr
        , s.renewal_date
        , s.days_to_renewal
        , s.is_in_renewal_window
        , s.risk_score
        , s.risk_tier
        , s.usage_score
        , s.commercial_score
        , s.support_score
        , s.wau
        , s.usage_trend_pct
        , s.active_user_ratio
        , s.distinct_features_used
        , s.days_since_last_event
        , s.had_payment_failure_30d
        , s.p1_p2_tickets_30d
        , case
            when s.risk_tier = 'HIGH' and s.days_to_renewal <= 14 then 1
            when s.risk_tier = 'HIGH' and s.days_to_renewal <= 30 then 2
            when s.risk_tier = 'HIGH'                              then 3
            when s.risk_tier = 'MEDIUM' and s.is_in_renewal_window then 4
          end                                                                as alert_priority
        , case
            when greatest(s.usage_score, s.commercial_score, s.support_score)
                = s.usage_score      then 'usage'
            when greatest(s.usage_score, s.commercial_score, s.support_score)
                = s.commercial_score then 'commercial'
            else                          'support'
          end                                                                as top_risk_driver
        -- History from snapshot
        , coalesce(h.consecutive_high_weeks, 0)                             as consecutive_high_weeks
        , h.first_alerted_at
        , current_timestamp                                                  as alerted_at

    from scores s
    left join alert_history h on h.account_id = s.account_id
    where
        s.risk_tier = 'HIGH'
        or (s.risk_tier = 'MEDIUM' and s.is_in_renewal_window)
)

select *
from alerts
order by alert_priority asc, risk_score desc
