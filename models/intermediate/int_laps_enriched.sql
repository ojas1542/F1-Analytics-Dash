-- models/intermediate/int_laps_enriched.sql
--
-- Single source of truth for lap-grain analytics: joins stg_laps to its
-- dimensions (session, meeting, driver) and to stg_pit, and derives
-- lap-validity and stint boundaries once so downstream marts (race pace
-- and driver/season summary) don't duplicate this logic.
--
-- Grain: one row per session_key, driver_number, lap_number.

with laps_by_source as (
    -- A session that's both batch-backfilled and streaming-replayed can
    -- produce a 'batch' and a 'streaming' row for the same lap in
    -- stg_laps; keep one per lap, preferring streaming as the fresher
    -- source, so this model's stated grain actually holds.
    select *
    from {{ ref('stg_laps') }}
    qualify row_number() over (
        partition by session_key, driver_number, lap_number
        order by case source_system when 'streaming' then 0 else 1 end
    ) = 1
),

pit_by_lap as (
    select
        session_key,
        driver_number,
        lap_number,
        pit_duration_seconds
    from {{ ref('stg_pit') }}
    qualify row_number() over (
        partition by session_key, driver_number, lap_number
        order by case source_system when 'streaming' then 0 else 1 end
    ) = 1
),

enriched as (
    select
        l.session_key,
        l.driver_number,
        l.lap_number,
        l.lap_duration_seconds,
        l.s1_duration_seconds,
        l.s2_duration_seconds,
        l.s3_duration_seconds,
        l.is_pit_out_lap,
        l.lap_started_at,

        s.meeting_key,
        s.season_year,
        s.session_name,
        s.session_type,
        s.circuit_short_name,
        s.country_name,
        s.session_started_at,

        d.full_name as driver_full_name,
        d.team_name,

        p.pit_duration_seconds,
        (p.session_key is not null) as is_pit_lap

    from laps_by_source l
    left join {{ ref('stg_sessions') }} s on l.session_key = s.session_key
    left join {{ ref('stg_drivers') }} d
        on l.session_key = d.session_key
        and l.driver_number = d.driver_number
    left join pit_by_lap p
        on l.session_key = p.session_key
        and l.driver_number = p.driver_number
        and l.lap_number = p.lap_number
)

select
    *,
    (lap_duration_seconds is not null and not is_pit_out_lap and not is_pit_lap) as is_valid_lap,
    1 + sum(case when is_pit_out_lap then 1 else 0 end) over (
        partition by session_key, driver_number
        order by lap_number
        rows between unbounded preceding and current row
    ) as stint_number
from enriched
