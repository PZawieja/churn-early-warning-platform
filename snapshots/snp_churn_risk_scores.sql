{% snapshot snp_churn_risk_scores %}

{{
    config(
        target_schema='snapshots'
        , unique_key='account_id'
        , strategy='check'
        , check_cols=['risk_tier', 'risk_score']
        , invalidate_hard_deletes=True
    )
}}

/*
    snp_churn_risk_scores
    ---------------------
    SCD Type 2 history of account risk scores.

    Captures every risk_tier or risk_score change per account. Enables:
      - consecutive_high_weeks  → how long has the current HIGH streak been?
      - first_alerted_at        → when did this account first hit HIGH?
      - tier transition audits  → "went HIGH → LOW → HIGH again?"

    Grain: one current row per account_id (dbt_valid_to IS NULL = active record).
    Historical rows have dbt_valid_to set to the timestamp of the change.

    Run order:
        dbt snapshot                        -- capture current state / deltas
        dbt run --select mart_churn_alerts  -- then alerts read from snapshot

    DuckDB note: snapshots land in a separate schema ('snapshots').
    The dbt_valid_from / dbt_valid_to columns use UTC timestamps.
*/

select
    account_id
    , scored_week
    , plan_name
    , mrr
    , risk_tier
    , risk_score
    , usage_score
    , commercial_score
    , support_score

from {{ ref('mart_churn_risk_scores') }}

{% endsnapshot %}
