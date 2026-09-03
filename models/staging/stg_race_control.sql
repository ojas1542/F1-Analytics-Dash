-- models/staging/stg_race_control.sql
--
-- Batch and streaming both land in this one table (the only dataset where
-- their table names happen to coincide), but the streaming sink only ever
-- populates record_content, leaving the outer session_key column null for
-- those rows -- their session_key only exists inside the JSON payload, same
-- as every other streaming-origin table. Coalesce to the payload as a
-- fallback so streaming rows aren't silently dropped by the not_null test.

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
    coalesce(session_key, try_cast(payload:session_key::string as string)) as session_key,
    try_cast(payload:date::string as timestamp_tz) as event_at,
    payload:category::string as category,
    payload:flag::string as flag_type,
    payload:message::string as message_text,
    try_cast(payload:driver_number::string as integer) as driver_number,
    try_cast(payload:lap_number::string as integer) as lap_number
from parsed