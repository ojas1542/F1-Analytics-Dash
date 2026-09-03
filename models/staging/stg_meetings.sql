-- models/staging/stg_meetings.sql
--
-- Batch-only, year-grain: same shape as stg_sessions.sql -- see that file
-- for why the outer key column (extract_year) isn't the natural key, and
-- why dedup is needed here.

with source as (
    select * from {{ source('raw_f1', 'raw_meetings') }}
),

parsed as (
    select parse_json(record_content) as payload
    from source
),

deduped as (
    select payload
    from parsed
    qualify row_number() over (
        partition by payload:meeting_key::string
        order by payload:meeting_key::string
    ) = 1
)

select
    try_cast(payload:meeting_key::string as integer) as meeting_key,
    payload:meeting_name::string as meeting_name,
    payload:meeting_official_name::string as meeting_official_name,
    payload:location::string as location,
    payload:country_name::string as country_name,
    payload:country_code::string as country_code,
    payload:circuit_short_name::string as circuit_short_name,
    try_cast(payload:circuit_key::string as integer) as circuit_key,
    try_cast(payload:year::string as integer) as season_year,
    try_cast(payload:date_start::string as timestamp_tz) as meeting_started_at
from deduped
