{{
    config(
        materialized='table'
        , tags=['churn', 'weekly']
    )
}}

with

features as (
    select * from {{ ref('int_churn_features') }}
),

usage_scored as (
    select
        account_id,
        greatest(0, least(
            100,
            case
                when usage_trend_pct is null then 50
                when usage_trend_pct <= -0.50 then 100
                when usage_trend_pct <= -0.25 then 75
                when usage_trend_pct <= -0.10 then 50
                when usage_trend_pct <= 0.05 then 25
                else 0
            end
        )) as usage_trend_score,
        greatest(0, least(
            100,
            case
                when is_zero_usage_week then 100
                when active_user_ratio < 0.10 then 90
                when active_user_ratio < 0.25 then 65
                when active_user_ratio < 0.50 then 35
                when active_user_ratio < 0.75 then 15
                else 0
            end
        )) as active_user_score,
        greatest(0, least(
            100,
            case
                when distinct_features_used = 0 then 100
                when distinct_features_used = 1 then 70
                when distinct_features_used <= 3 then 40
                when distinct_features_used <= 6 then 20
                else 0
            end
        )) as feature_breadth_score,
        greatest(0, least(
            100,
            case
                when days_since_last_event is null then 100
                when days_since_last_event >= 28 then 90
                when days_since_last_event >= 14 then 60
                when days_since_last_event >= 7 then 30
                else 0
            end
        )) as recency_score
    from features
),

commercial_scored as (
    select
        account_id,
        greatest(0, least(
            100,
            case
                when days_to_renewal < 0 then 0
                when days_to_renewal <= 14 then 90
                when days_to_renewal <= 30 then 70
                when days_to_renewal <= 60 then 45
                when days_to_renewal <= 90 then 25
                else 0
            end
        )) as renewal_urgency_score,
        greatest(0, least(
            100,
            case
                when seat_contracted_ratio < 0.25 then 90
                when seat_contracted_ratio < 0.50 then 60
                when seat_contracted_ratio < 0.75 then 30
                else 0
            end
        )) as seat_utilization_score,
        case when had_payment_failure_30d then 80 else 0 end
            as payment_failure_score,
        case when had_downgrade_90d then 60 else 0 end
            as downgrade_score
    from features
),

support_scored as (
    select
        account_id,
        greatest(0, least(
            100,
            case
                when p1_p2_tickets_30d >= 3 then 90
                when p1_p2_tickets_30d = 2 then 65
                when p1_p2_tickets_30d = 1 then 35
                else 0
            end
        )) as critical_ticket_score,
        greatest(0, least(
            100,
            case
                when open_tickets_count >= 5 then 70
                when open_tickets_count >= 3 then 45
                when open_tickets_count >= 1 then 20
                else 0
            end
        )) as open_ticket_score,
        greatest(0, least(
            100,
            case
                when avg_ticket_resolution_days is null then 0
                when avg_ticket_resolution_days > 14 then 70
                when avg_ticket_resolution_days > 7 then 40
                else 0
            end
        )) as resolution_time_score
    from features
),

component_scores as (
    select
        f.account_id,
        round((
            u.usage_trend_score + u.active_user_score
            + u.feature_breadth_score + u.recency_score
        ) / 4.0, 2) as usage_score,
        round((
            c.renewal_urgency_score + c.seat_utilization_score
            + c.payment_failure_score + c.downgrade_score
        ) / 4.0, 2) as commercial_score,
        round((
            s.critical_ticket_score + s.open_ticket_score
            + s.resolution_time_score
        ) / 3.0, 2) as support_score
    from features as f
    inner join usage_scored as u on f.account_id = u.account_id
    inner join commercial_scored as c on f.account_id = c.account_id
    inner join support_scored as s on f.account_id = s.account_id
)

select
    f.account_id,
    f.feature_week as scored_week,
    f.plan_name,
    f.mrr,
    f.renewal_date,
    f.days_to_renewal,
    f.is_in_renewal_window,
    cs.usage_score,
    cs.commercial_score,
    cs.support_score,
    f.wau,
    f.usage_trend_pct,
    f.active_user_ratio,
    f.distinct_features_used,
    f.days_since_last_event,
    f.had_payment_failure_30d,
    f.p1_p2_tickets_30d,
    round(
        cs.usage_score * 0.45
        + cs.commercial_score * 0.30
        + cs.support_score * 0.25,
        2
    ) as risk_score,
    case
        when round(
            cs.usage_score * 0.45 + cs.commercial_score * 0.30
            + cs.support_score * 0.25, 2
        ) >= 70 then 'HIGH'
        when round(
            cs.usage_score * 0.45 + cs.commercial_score * 0.30
            + cs.support_score * 0.25, 2
        ) >= 40 then 'MEDIUM'
        else 'LOW'
    end as risk_tier,
    current_timestamp() as scored_at

from features as f
inner join component_scores as cs on f.account_id = cs.account_id
order by risk_score desc
