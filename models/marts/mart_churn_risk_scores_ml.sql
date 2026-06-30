{{
    config(
        materialized='table'
        , tags=['churn', 'weekly', 'ml']
    )
}}

/*
    mart_churn_risk_scores_ml
    -------------------------
    Side-by-side comparison of ML model scores and rule-based scores.

    Grain: one row per account_id (current week).

    Prerequisites — run in this order:
        dbt run                          → builds mart_churn_risk_scores
        dbt snapshot                     → refreshes snp_churn_risk_scores
        python scripts/ml_pipeline.py train --db dev.duckdb
        python scripts/ml_pipeline.py score --db dev.duckdb
        dbt run --select mart_churn_risk_scores_ml   ← this model

    ML scores come from ml_output.ml_predictions, written by ml_pipeline.py.
    Rule-based scores come from mart_churn_risk_scores.

    tier_agreement values:
        AGREE       — both approaches assign the same tier
        ML_HIGHER   — ML flagged HIGH, rule-based did not
        ML_LOWER    — rule-based flagged HIGH, ML did not
        DIFFER      — tiers differ (non-HIGH case)
*/

with

ml as (
    select * from {{ source('ml_output', 'ml_predictions') }}
)

, rule as (
    select * from {{ ref('mart_churn_risk_scores') }}
)

select
    r.account_id
    , r.scored_week
    , r.plan_name
    , r.mrr
    , r.renewal_date
    , r.days_to_renewal
    , r.is_in_renewal_window

    -- ML scores
    , m.risk_score_ml
    , m.risk_tier_ml
    , m.churn_probability
    , m.model_name
    , m.cv_auc

    -- Rule-based scores (for comparison)
    , r.risk_score                                      as risk_score_rule
    , r.risk_tier                                       as risk_tier_rule
    , r.usage_score
    , r.commercial_score
    , r.support_score

    -- Delta and agreement
    , round(m.risk_score_ml - r.risk_score, 2)          as score_delta
    , case
        when m.risk_tier_ml = r.risk_tier               then 'AGREE'
        when m.risk_tier_ml = 'HIGH'
            and r.risk_tier != 'HIGH'                   then 'ML_HIGHER'
        when r.risk_tier = 'HIGH'
            and m.risk_tier_ml != 'HIGH'                then 'ML_LOWER'
        else                                                 'DIFFER'
      end                                               as tier_agreement

    -- Signal snapshot (useful for CSM context)
    , r.wau
    , r.usage_trend_pct
    , r.active_user_ratio
    , r.distinct_features_used
    , r.days_since_last_event
    , r.had_payment_failure_30d
    , r.p1_p2_tickets_30d

    , m.scored_at

from rule r
inner join ml m
    on m.account_id = r.account_id

order by m.risk_score_ml desc
