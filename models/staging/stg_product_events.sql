with

source as (
    select * from {{ source('raw', 'product_events') }}
)

select
    cast(event_id as varchar) as event_id,
    cast(account_id as varchar) as account_id,
    cast(user_id as varchar) as user_id,
    cast(session_id as varchar) as session_id,
    cast(feature_name as varchar) as feature_name,
    cast(event_ts as timestamp_ntz) as event_ts

from source
