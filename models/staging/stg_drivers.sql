-- models/staging/stg_drivers.sql
--
-- Batch-only: driver roster per session (no streaming equivalent -- OpenF1's
-- drivers endpoint isn't published to a Kafka topic).

with source as (
    select * from {{ source('raw_f1', 'raw_drivers') }}
),

parsed as (
    select
        try_cast(session_key as string) as session_key,
        parse_json(record_content) as payload
    from source
),

deduped as (
    -- COPY INTO is append-only (no truncate) so re-running a backfill over
    -- overlapping sessions re-lands the same roster; keep one row each.
    select session_key, payload
    from parsed
    qualify row_number() over (
        partition by session_key, payload:driver_number::string
        order by session_key
    ) = 1
)

select
    session_key,
    try_cast(payload:driver_number::string as integer) as driver_number,
    payload:full_name::string as full_name,
    payload:broadcast_name::string as broadcast_name,
    payload:name_acronym::string as name_acronym,
    payload:team_name::string as team_name,
    payload:team_colour::string as team_colour,
    payload:country_code::string as country_code,
    try_cast(payload:meeting_key::string as integer) as meeting_key
from deduped
