-- models/staging/stg_race_control.sql

with source as (
    select * from {{ source('raw_f1', 'raw_race_control') }}
),

parsed as (
    select
        try_cast(session_key as string) as session_key,
        parse_json(record_content) as payload
    from source
)

select
    session_key,
    try_cast(payload:date::string as timestamp_tz) as event_at,
    payload:category::string as category,
    payload:flag::string as flag_type,
    payload:message::string as message_text,
    try_cast(payload:driver_number::string as integer) as driver_number,
    try_cast(payload:lap_number::string as integer) as lap_number
from parsed