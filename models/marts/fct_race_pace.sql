-- models/marts/fct_race_pace.sql
--
-- Race pace & consistency: one row per driver per session.
--
-- Grain: session_key, driver_number.

with base as (
    select * from {{ ref('int_laps_enriched') }}
),

aggregated as (
    select
        session_key,
        driver_number,

        any_value(season_year) as season_year,
        any_value(session_name) as session_name,
        any_value(session_type) as session_type,
        any_value(circuit_short_name) as circuit_short_name,
        any_value(country_name) as country_name,
        any_value(session_started_at) as session_started_at,
        any_value(driver_full_name) as driver_full_name,
        any_value(team_name) as team_name,

        -- Pace stats exclude in/out/pit laps (see int_laps_enriched.is_valid_lap)
        avg(case when is_valid_lap then lap_duration_seconds end) as avg_lap_duration_seconds,
        min(case when is_valid_lap then lap_duration_seconds end) as min_lap_duration_seconds,
        stddev(case when is_valid_lap then lap_duration_seconds end) as stddev_lap_duration_seconds,

        -- Linear-regression slope of lap time vs. lap number: positive =
        -- getting slower (degrading) over the session/stint, negative =
        -- getting faster (e.g. fuel burn-off outweighing tyre wear).
        regr_slope(
            case when is_valid_lap then lap_duration_seconds end,
            case when is_valid_lap then lap_number end
        ) as degradation_slope_seconds_per_lap,

        max(stint_number) as stint_count,
        count_if(is_valid_lap) as valid_lap_count,
        count(*) as total_lap_count,
        count_if(is_pit_lap) as pit_stop_count,
        sum(pit_duration_seconds) as total_pit_duration_seconds

    from base
    group by session_key, driver_number
)

select
    {{ dbt_utils.generate_surrogate_key(['session_key', 'driver_number']) }} as driver_session_key,
    session_key,
    driver_number,
    driver_full_name,
    team_name,
    season_year,
    session_name,
    session_type,
    circuit_short_name,
    country_name,
    session_started_at,
    avg_lap_duration_seconds,
    min_lap_duration_seconds,
    stddev_lap_duration_seconds,
    -- Consistency score: lower = more consistent lap times.
    stddev_lap_duration_seconds / nullif(avg_lap_duration_seconds, 0) as lap_time_coefficient_of_variation,
    degradation_slope_seconds_per_lap,
    stint_count,
    valid_lap_count,
    total_lap_count,
    pit_stop_count,
    total_pit_duration_seconds
from aggregated
