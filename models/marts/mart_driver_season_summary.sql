-- models/marts/mart_driver_season_summary.sql
--
-- Driver/season summary: one row per driver per season, built on top of
-- fct_race_pace (not straight from staging) so pace/consistency logic is
-- computed exactly once.
--
-- Grain: driver_number, season_year.

with race_pace as (
    select * from {{ ref('fct_race_pace') }}
),

season_agg as (
    select
        driver_number,
        season_year,
        count(*) as sessions_count,
        -- Volume-weighted by valid_lap_count so a 60-lap race counts more
        -- than a 10-lap sprint toward the season average.
        sum(avg_lap_duration_seconds * valid_lap_count)
            / nullif(sum(valid_lap_count), 0) as avg_lap_duration_seconds,
        sum(lap_time_coefficient_of_variation * valid_lap_count)
            / nullif(sum(valid_lap_count), 0) as avg_lap_time_coefficient_of_variation,
        avg(degradation_slope_seconds_per_lap) as avg_degradation_slope_seconds_per_lap,
        sum(pit_stop_count) as total_pit_stops,
        sum(total_pit_duration_seconds) / nullif(sum(pit_stop_count), 0) as avg_pit_duration_seconds
    from race_pace
    group by driver_number, season_year
),

latest_identity as (
    -- Mid-season team changes: take the most recent session's driver
    -- name/team rather than averaging or picking arbitrarily.
    select
        driver_number,
        season_year,
        driver_full_name,
        team_name,
        row_number() over (
            partition by driver_number, season_year
            order by session_started_at desc
        ) as rn
    from race_pace
),

incidents as (
    -- Rough proxy, not a severity-classified incident count: every
    -- race-control event that names this driver, for this season.
    select
        rc.driver_number,
        s.season_year,
        count(*) as incident_event_count
    from {{ ref('stg_race_control') }} rc
    inner join {{ ref('stg_sessions') }} s on rc.session_key = s.session_key
    where rc.driver_number is not null
    group by rc.driver_number, s.season_year
)

select
    {{ dbt_utils.generate_surrogate_key(['a.driver_number', 'a.season_year']) }} as driver_season_key,
    a.driver_number,
    a.season_year,
    li.driver_full_name,
    li.team_name,
    a.sessions_count,
    a.avg_lap_duration_seconds,
    a.avg_lap_time_coefficient_of_variation,
    a.avg_degradation_slope_seconds_per_lap,
    a.total_pit_stops,
    a.avg_pit_duration_seconds,
    coalesce(i.incident_event_count, 0) as incident_event_count
from season_agg a
left join latest_identity li
    on a.driver_number = li.driver_number
    and a.season_year = li.season_year
    and li.rn = 1
left join incidents i
    on a.driver_number = i.driver_number
    and a.season_year = i.season_year
