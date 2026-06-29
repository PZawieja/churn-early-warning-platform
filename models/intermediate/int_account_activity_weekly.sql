{{
    config(
        materialized='incremental'
        , unique_key=['account_id', 'week_start']
        , on_schema_change='sync_all_columns'
    )
}}

with

spine as (
    select
        a.account_id,
        dateadd('week', -seq4(), date_trunc('week', current_date)) as week_start
    from {{ ref('stg_accounts') }} as a
    cross join table(generator(rowcount => 12))
    where a.is_active = true
),

product_events as (
    select
        account_id,
        date_trunc('week', event_ts) as week_start,
        count(distinct user_id) as wau,
        count(distinct session_id) as session_count,
        count(*) as event_count,
        count(distinct feature_name) as distinct_features_used,
        max(event_ts) as last_event_ts
    from {{ ref('stg_product_events') }}
    {% if is_incremental() %}
        where event_ts >= dateadd('week', -13, date_trunc('week', current_date))
    {% endif %}
    group by 1, 2
),

accounts as (
    select
        account_id,
        seats_contracted,
        mrr,
        plan_name,
        renewal_date
    from {{ ref('stg_accounts') }}
),

activity as (
    select
        s.account_id,
        s.week_start,
        e.last_event_ts,
        a.seats_contracted,
        a.mrr,
        a.plan_name,
        a.renewal_date,
        dateadd('day', 6, s.week_start) as week_end,
        coalesce(e.wau, 0) as wau,
        coalesce(e.session_count, 0) as session_count,
        coalesce(e.event_count, 0) as event_count,
        coalesce(e.distinct_features_used, 0) as distinct_features_used,
        case
            when a.seats_contracted > 0
                then round(coalesce(e.wau, 0) / a.seats_contracted, 4)
        end as active_user_ratio,
        datediff(
            'day',
            e.last_event_ts,
            dateadd('day', 6, s.week_start)
        ) as days_since_last_event,
        datediff('day', dateadd('day', 6, s.week_start), a.renewal_date)
            as days_to_renewal
    from spine as s
    left join product_events as e
        on
            s.account_id = e.account_id
            and s.week_start = e.week_start
    inner join accounts as a
        on s.account_id = a.account_id
),

with_trends as (
    select
        *,
        avg(wau) over (
            partition by account_id
            order by week_start
            rows between 3 preceding and current row
        ) as wau_rolling_4w,
        avg(wau) over (
            partition by account_id
            order by week_start
            rows between 7 preceding and 4 preceding
        ) as wau_rolling_prev_4w,
        wau - lag(wau, 1) over (
            partition by account_id order by week_start
        ) as wau_wow_delta
    from activity
)

select
    account_id,
    week_start,
    week_end,
    wau,
    session_count,
    event_count,
    distinct_features_used,
    last_event_ts,
    seats_contracted,
    active_user_ratio,
    days_since_last_event,
    mrr,
    plan_name,
    renewal_date,
    days_to_renewal,
    wau_rolling_4w,
    wau_rolling_prev_4w,
    wau_wow_delta,
    case
        when wau_rolling_prev_4w = 0 then null
        else round(
            (wau_rolling_4w - wau_rolling_prev_4w)
            / nullif(wau_rolling_prev_4w, 0),
            4
        )
    end as usage_trend_pct,
    current_timestamp() as updated_at

from with_trends
