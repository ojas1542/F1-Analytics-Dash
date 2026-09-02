-- models/staging/stg_laps.sql

with source as (
    select * from {{ source('raw_f1', 'raw_laps') }}
),

parsed as (
    select
        try_cast(session_key as string) as session_key,
        parse_json(record_content) as payload
    from source
)

select
    session_key,
    try_cast(payload:driver_number::string as integer) as driver_number,
    try_cast(payload:lap_number::string as integer) as lap_number,
    try_cast(payload:lap_duration::string as float) as lap_duration_seconds,
    try_cast(payload:duration_sector_1::string as float) as s1_duration_seconds,
    try_cast(payload:duration_sector_2::string as float) as s2_duration_seconds,
    try_cast(payload:duration_sector_3::string as float) as s3_duration_seconds,
    try_cast(payload:is_pit_out_lap::string as boolean) as is_pit_out_lap,
    try_cast(payload:date_start::string as timestamp_tz) as lap_started_at
from parsed