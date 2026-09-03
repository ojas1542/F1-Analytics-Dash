-- models/staging/stg_car_data.sql
--
-- Unions two independently-landed raw tables carrying the same OpenF1
-- car_data records: the batch loader's raw_car_data (VARCHAR record_content,
-- needs parse_json) and the Kafka Connect Snowflake streaming sink's
-- raw_car_telemetry (VARIANT record_content, already parsed). source_system
-- distinguishes provenance since either path can populate the same session.

with batch_source as (
    select * from {{ source('raw_f1', 'raw_car_data') }}
),

streaming_source as (
    select * from {{ source('raw_f1', 'raw_car_telemetry') }}
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
)

select
    session_key,
    source_system,

    -- Extract VARIANT fields with explicit casting
    try_cast(payload:driver_number::string as integer) as driver_number,
    try_cast(payload:date::string as timestamp_tz) as recorded_at,
    try_cast(payload:speed::string as float) as speed_kmh,
    try_cast(payload:rpm::string as integer) as engine_rpm,
    try_cast(payload:gear::string as integer) as gear,
    try_cast(payload:throttle::string as float) as throttle_pct,
    try_cast(payload:brake::string as float) as brake_pct,
    try_cast(payload:drs::string as integer) as drs_status

from unioned