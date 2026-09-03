-- models/staging/stg_sessions.sql
--
-- Batch-only, year-grain: raw_sessions is landed once per season (see
-- fetch_and_extract_year_dimensions in historical_race_batch.py), so its
-- outer key column is extract_year, not session_key -- the real session_key
-- natural key lives inside the payload itself.

with source as (
    select * from {{ source('raw_f1', 'raw_sessions') }}
),

parsed as (
    select parse_json(record_content) as payload
    from source
),

deduped as (
    -- COPY INTO is append-only (no truncate) so re-running a backfill over
    -- overlapping years re-lands the same sessions; keep one row each.
    select payload
    from parsed
    qualify row_number() over (
        partition by payload:session_key::string
        order by payload:session_key::string
    ) = 1
)

select
    try_cast(payload:session_key::string as integer) as session_key,
    try_cast(payload:meeting_key::string as integer) as meeting_key,
    payload:session_name::string as session_name,
    payload:session_type::string as session_type,
    try_cast(payload:year::string as integer) as season_year,
    payload:country_name::string as country_name,
    payload:country_code::string as country_code,
    payload:circuit_short_name::string as circuit_short_name,
    try_cast(payload:date_start::string as timestamp_tz) as session_started_at,
    try_cast(payload:date_end::string as timestamp_tz) as session_ended_at
from deduped
