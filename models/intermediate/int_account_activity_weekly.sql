{{
    config(
        materialized='incremental'
        , unique_key=['account_id', 'week_start']
        , on_schema_change='sync_all_columns'
    )
}}

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
