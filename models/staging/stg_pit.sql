-- models/staging/stg_pit.sql
--
-- Unions the batch loader's raw_pit (VARCHAR record_content, needs
-- parse_json) with the streaming sink's raw_pit_stops (VARIANT
-- record_content, already parsed). See stg_car_data.sql for the same
-- pattern with more detail.

with batch_source as (
    select * from {{ source('raw_f1', 'raw_pit') }}
),

streaming_source as (
    select * from {{ source('raw_f1', 'raw_pit_stops') }}
),

batch_parsed as (
    select
        try_cast(session_key as string) as session_key,
        parse_json(record_content) as payload,
        'batch' as source_system
    from batch_source
),

streaming_parsed as (
    select
        try_cast(record_content:session_key::string as string) as session_key,
        record_content as payload,
        'streaming' as source_system
    from streaming_source
),

unioned as (
    select * from batch_parsed
    union all
    select * from streaming_parsed
),

deduped as (
    -- See stg_laps.sql for why dedup is needed here.
    select session_key, source_system, payload
    from unioned
    qualify row_number() over (
        partition by
            session_key,
            source_system,
            payload:driver_number::string,
            payload:lap_number::string
        order by session_key
    ) = 1
)

select
    session_key,
    source_system,
    try_cast(payload:driver_number::string as integer) as driver_number,
    try_cast(payload:lap_number::string as integer) as lap_number,
    try_cast(payload:date::string as timestamp_tz) as pit_at,
    try_cast(payload:pit_duration::string as float) as pit_duration_seconds,
    try_cast(payload:meeting_key::string as integer) as meeting_key
from deduped
