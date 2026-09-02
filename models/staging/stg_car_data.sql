-- models/staging/stg_car_data.sql

with source as (
    select * from {{ source('raw_f1', 'raw_car_data') }}
),

parsed as (
    select
        try_cast(session_key as string) as session_key,
        parse_json(record_content) as payload

    from source
)

select
    session_key,
    
    -- Extract VARIANT fields with explicit casting
    try_cast(payload:driver_number::string as integer) as driver_number,
    try_cast(payload:date::string as timestamp_tz) as recorded_at,
    try_cast(payload:speed::string as float) as speed_kmh,
    try_cast(payload:rpm::string as integer) as engine_rpm,
    try_cast(payload:gear::string as integer) as gear,
    try_cast(payload:throttle::string as float) as throttle_pct,
    try_cast(payload:brake::string as float) as brake_pct,
    try_cast(payload:drs::string as integer) as drs_status

from parsed