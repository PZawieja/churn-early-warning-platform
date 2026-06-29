{{
    config(
        materialized='table'
        , tags=['churn', 'weekly', 'alerts']
    )
}}

-- STUB: Session 2 — add alert metadata, notification tracking, dedup logic

select
    account_id,
    scored_week,
    plan_name,
    mrr,
    renewal_date,
    days_to_renewal,
    risk_score,
    risk_tier

from {{ ref('mart_churn_risk_scores') }}
where
    risk_tier = 'HIGH'
    and days_to_renewal <= 90
order by risk_score desc
