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

    Grain: one row per account_id (current week).

    alert_priority:
        1 = CRITICAL  — HIGH risk, renewal ≤ 14 days
        2 = URGENT    — HIGH risk, renewal ≤ 30 days
        3 = HIGH      — HIGH risk, renewal > 30 days
        4 = WATCH     — MEDIUM risk, inside renewal window
*/

with

scores as (
    select * from {{ ref('mart_churn_risk_scores') }}
)

, alerts as (
    select
        account_id
        , scored_week
        , plan_name
        , mrr
        , renewal_date
        , days_to_renewal
        , is_in_renewal_window
        , risk_score
        , risk_tier
        , usage_score
        , commercial_score
        , support_score

        -- Priority for CSM queue ordering
        , case
            when risk_tier = 'HIGH' and days_to_renewal <= 14  then 1
            when risk_tier = 'HIGH' and days_to_renewal <= 30  then 2
            when risk_tier = 'HIGH'                             then 3
            when risk_tier = 'MEDIUM' and is_in_renewal_window then 4
          end                                                   as alert_priority

        -- Which component is driving the risk score
        , case
            when greatest(usage_score, commercial_score, support_score)
                 = usage_score                                  then 'usage'
            when greatest(usage_score, commercial_score, support_score)
                 = commercial_score                             then 'commercial'
            else                                                    'support'
          end                                                   as top_risk_driver

        -- Key signals surfaced for the alert message
        , wau
        , usage_trend_pct
        , active_user_ratio
        , distinct_features_used
        , days_since_last_event
        , had_payment_failure_30d
        , p1_p2_tickets_30d

        , current_timestamp                                     as alerted_at

    from scores
    where
        risk_tier = 'HIGH'
        or (risk_tier = 'MEDIUM' and is_in_renewal_window)
)

select *
from alerts
order by alert_priority, risk_score desc
