"""
Python client for the OpenF1 API (https://openf1.org).

Wraps every endpoint listed at https://openf1.org/docs/#api-endpoints:
  car_data, championship_drivers, championship_teams, drivers, intervals,
  laps, location, meetings, overtakes, pit, position, race_control,
  sessions, session_result, starting_grid, stints, team_radio, weather

All methods share one resilient base client (`OpenF1Client._get`) that
handles retries, rate-limit backoff, and OpenF1's comparison-operator
query syntax (e.g. `speed>=315`).
"""

from __future__ import annotations

import time
from typing import Any

import requests

BASE_URL = "https://api.openf1.org/v1"

# OpenF1 supports comparison operators on numeric/date fields via the query
# string, e.g. `speed>=315`. These suffixes let callers write Pythonic
# kwargs (`speed__gte=315`) instead of hand-building query strings.
_OPERATOR_SUFFIXES = {
    "__gte": ">=",
    "__lte": "<=",
    "__gt": ">",
    "__lt": "<",
    "__eq": "=",
}


class OpenF1Error(RuntimeError):
    """Raised when the OpenF1 API returns an error we can't recover from."""


class OpenF1Client:
    """
    Thin, resilient wrapper around every documented OpenF1 REST endpoint.

    Handles:
      - translating Pythonic filter kwargs into OpenF1's query-string syntax
      - retrying transient failures (5xx, connection errors) with backoff
      - respecting `Retry-After` on 429 rate-limit responses
      - raising a clear error on non-2xx responses instead of a bare
        requests exception

    Auth (optional): OpenF1's real-time endpoints require a bearer token
    from a paid sponsorship account. Historical data does not need one.
    Pass `api_token` to enable authenticated requests when you have one.
    """

    def __init__(
        self,
        base_url: str = BASE_URL,
        api_token: str | None = None,
        timeout: float = 10.0,
        max_retries: int = 3,
        backoff_seconds: float = 1.0,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.max_retries = max_retries
        self.backoff_seconds = backoff_seconds

        self._session = requests.Session()
        if api_token:
            self._session.headers["Authorization"] = f"Bearer {api_token}"

    # -- internals ---------------------------------------------------

    @staticmethod
    def _build_params(filters: dict[str, Any]) -> list[tuple[str, Any]]:
        """
        Turn `speed__gte=315` into the raw ('speed>=315', ...) query param
        OpenF1 expects, while passing plain kwargs (e.g. `driver_number=1`)
        through unchanged. Returns (key, value) tuples since `requests`
        would otherwise URL-encode operator characters like `>` in a key.
        """
        params: list[tuple[str, Any]] = []
        for key, value in filters.items():
            if value is None:
                continue
            matched = False
            for suffix, operator in _OPERATOR_SUFFIXES.items():
                if key.endswith(suffix):
                    field = key[: -len(suffix)]
                    params.append((f"{field}{operator}", value))
                    matched = True
                    break
            if not matched:
                params.append((key, value))
        return params

    def _get(self, endpoint: str, **filters: Any) -> list[dict[str, Any]]:
        url = f"{self.base_url}/{endpoint}"
        params = self._build_params(filters)

        last_error: Exception | None = None
        for attempt in range(1, self.max_retries + 1):
            try:
                response = self._session.get(url, params=params, timeout=self.timeout)
            except requests.exceptions.RequestException as exc:
                last_error = exc
                self._sleep_before_retry(attempt)
                continue

            # OpenF1 returns 404 with {"detail": "No results found."} when a time 
            # range or filter query contains no matching events.
            if response.status_code == 404:
                return []

            # Handle rate limiting (429) cleanly with backoff retry budget
            if response.status_code == 429:
                retry_after_hdr = response.headers.get("Retry-After")
                retry_after = (
                    float(retry_after_hdr) 
                    if retry_after_hdr and retry_after_hdr.isdigit() 
                    else self.backoff_seconds * (2 ** (attempt - 1))
                )
                last_error = OpenF1Error(f"Rate limited (429): {response.text[:200]}")
                time.sleep(retry_after)
                continue

            if 500 <= response.status_code < 600:
                last_error = OpenF1Error(f"{response.status_code} from OpenF1: {response.text[:200]}")
                self._sleep_before_retry(attempt)
                continue

            if not response.ok:
                raise OpenF1Error(
                    f"OpenF1 request failed ({response.status_code}) for {response.url}: "
                    f"{response.text[:300]}"
                )

            return response.json()

        raise OpenF1Error(f"OpenF1 request failed after {self.max_retries} attempts: {last_error}")
    
    def _sleep_before_retry(self, attempt: int) -> None:
        time.sleep(self.backoff_seconds * (2 ** (attempt - 1)))

    # -- car data ------------------------------------------------------

    def get_car_data(
        self,
        *,
        driver_number: int | None = None,
        session_key: int | str | None = None,
        meeting_key: int | str | None = None,
        **filters: Any,
    ) -> list[dict[str, Any]]:
        """Telemetry (speed, throttle, brake, gear, RPM, DRS) at ~3.7 Hz."""
        return self._get(
            "car_data",
            driver_number=driver_number,
            session_key=session_key,
            meeting_key=meeting_key,
            **filters,
        )

    # -- championships (beta) -------------------------------------------

    def get_championship_drivers(
        self,
        *,
        session_key: int | str | None = None,
        driver_number: int | None = None,
        **filters: Any,
    ) -> list[dict[str, Any]]:
        """Driver championship standings. Race sessions only."""
        return self._get(
            "championship_drivers",
            session_key=session_key,
            driver_number=driver_number,
            **filters,
        )

    def get_championship_teams(
        self,
        *,
        session_key: int | str | None = None,
        team_name: str | None = None,
        **filters: Any,
    ) -> list[dict[str, Any]]:
        """Team (constructor) championship standings. Race sessions only."""
        return self._get(
            "championship_teams",
            session_key=session_key,
            team_name=team_name,
            **filters,
        )

    # -- drivers ---------------------------------------------------------

    def get_drivers(
        self,
        *,
        driver_number: int | None = None,
        session_key: int | str | None = None,
        meeting_key: int | str | None = None,
        **filters: Any,
    ) -> list[dict[str, Any]]:
        """Driver bio/team info for a given session."""
        return self._get(
            "drivers",
            driver_number=driver_number,
            session_key=session_key,
            meeting_key=meeting_key,
            **filters,
        )

    # -- intervals ---------------------------------------------------------

    def get_intervals(
        self,
        *,
        session_key: int | str | None = None,
        driver_number: int | None = None,
        **filters: Any,
    ) -> list[dict[str, Any]]:
        """Gap-to-leader / interval-to-car-ahead. Races only, ~every 4s."""
        return self._get(
            "intervals",
            session_key=session_key,
            driver_number=driver_number,
            **filters,
        )

    # -- laps ---------------------------------------------------------

    def get_laps(
        self,
        *,
        session_key: int | str | None = None,
        driver_number: int | None = None,
        lap_number: int | None = None,
        **filters: Any,
    ) -> list[dict[str, Any]]:
        """Per-lap sector times, speeds, and pit-out flag."""
        return self._get(
            "laps",
            session_key=session_key,
            driver_number=driver_number,
            lap_number=lap_number,
            **filters,
        )

    # -- location ---------------------------------------------------------

    def get_location(
        self,
        *,
        session_key: int | str | None = None,
        driver_number: int | None = None,
        **filters: Any,
    ) -> list[dict[str, Any]]:
        """Approximate car (x, y, z) position on track, at ~3.7 Hz."""
        return self._get(
            "location",
            session_key=session_key,
            driver_number=driver_number,
            **filters,
        )

    # -- meetings ---------------------------------------------------------

    def get_meetings(
        self,
        *,
        year: int | None = None,
        country_name: str | None = None,
        meeting_key: int | str | None = None,
        **filters: Any,
    ) -> list[dict[str, Any]]:
        """Race weekends (a meeting = multiple sessions)."""
        return self._get(
            "meetings",
            year=year,
            country_name=country_name,
            meeting_key=meeting_key,
            **filters,
        )

    # -- overtakes ---------------------------------------------------------

    def get_overtakes(
        self,
        *,
        session_key: int | str | None = None,
        overtaking_driver_number: int | None = None,
        overtaken_driver_number: int | None = None,
        **filters: Any,
    ) -> list[dict[str, Any]]:
        """On-track and pit/penalty-driven position exchanges. Races only."""
        return self._get(
            "overtakes",
            session_key=session_key,
            overtaking_driver_number=overtaking_driver_number,
            overtaken_driver_number=overtaken_driver_number,
            **filters,
        )

    # -- pit ---------------------------------------------------------

    def get_pit(
        self,
        *,
        session_key: int | str | None = None,
        driver_number: int | None = None,
        **filters: Any,
    ) -> list[dict[str, Any]]:
        """Pit lane / pit stop durations."""
        return self._get(
            "pit",
            session_key=session_key,
            driver_number=driver_number,
            **filters,
        )

    # -- position ---------------------------------------------------------

    def get_position(
        self,
        *,
        meeting_key: int | str | None = None,
        session_key: int | str | None = None,
        driver_number: int | None = None,
        **filters: Any,
    ) -> list[dict[str, Any]]:
        """Race position over time, including initial placement."""
        return self._get(
            "position",
            meeting_key=meeting_key,
            session_key=session_key,
            driver_number=driver_number,
            **filters,
        )

    # -- race control ---------------------------------------------------------

    def get_race_control(
        self,
        *,
        session_key: int | str | None = None,
        driver_number: int | None = None,
        flag: str | None = None,
        **filters: Any,
    ) -> list[dict[str, Any]]:
        """Flags, safety car, and other race control messages."""
        return self._get(
            "race_control",
            session_key=session_key,
            driver_number=driver_number,
            flag=flag,
            **filters,
        )

    # -- sessions ---------------------------------------------------------

    def get_sessions(
        self,
        *,
        year: int | None = None,
        country_name: str | None = None,
        session_name: str | None = None,
        session_key: int | str | None = None,
        **filters: Any,
    ) -> list[dict[str, Any]]:
        """Practice/qualifying/sprint/race sessions within a meeting."""
        return self._get(
            "sessions",
            year=year,
            country_name=country_name,
            session_name=session_name,
            session_key=session_key,
            **filters,
        )

    # -- session result ---------------------------------------------------------

    def get_session_result(
        self,
        *,
        session_key: int | str | None = None,
        position__lte: int | None = None,
        **filters: Any,
    ) -> list[dict[str, Any]]:
        """Final standings for a session (available a few minutes after)."""
        return self._get(
            "session_result",
            session_key=session_key,
            position__lte=position__lte,
            **filters,
        )

    # -- starting grid ---------------------------------------------------------

    def get_starting_grid(
        self,
        *,
        session_key: int | str | None = None,
        **filters: Any,
    ) -> list[dict[str, Any]]:
        """Starting grid order for an upcoming race."""
        return self._get("starting_grid", session_key=session_key, **filters)

    # -- stints ---------------------------------------------------------

    def get_stints(
        self,
        *,
        session_key: int | str | None = None,
        driver_number: int | None = None,
        **filters: Any,
    ) -> list[dict[str, Any]]:
        """Tyre stints: compound, lap range, tyre age."""
        return self._get(
            "stints",
            session_key=session_key,
            driver_number=driver_number,
            **filters,
        )

    # -- team radio ---------------------------------------------------------

    def get_team_radio(
        self,
        *,
        session_key: int | str | None = None,
        driver_number: int | None = None,
        **filters: Any,
    ) -> list[dict[str, Any]]:
        """Driver/team radio recordings (partial, sparse from 2026 on)."""
        return self._get(
            "team_radio",
            session_key=session_key,
            driver_number=driver_number,
            **filters,
        )

    # -- weather ---------------------------------------------------------

    def get_weather(
        self,
        *,
        meeting_key: int | str | None = None,
        session_key: int | str | None = None,
        **filters: Any,
    ) -> list[dict[str, Any]]:
        """Track weather, updated roughly every minute."""
        return self._get(
            "weather",
            meeting_key=meeting_key,
            session_key=session_key,
            **filters,
        )


if __name__ == "__main__":
    client = OpenF1Client()

    sessions = client.get_sessions(year=2025, country_name="Netherlands", session_name="Race")
    for s in sessions:
        print(s["session_key"], s["session_name"], s["date_start"])

    # Example: drivers from the 2023 Singapore GP practice session
    drivers = client.get_drivers(session_key=s["session_key"])
    for d in drivers[:5]:
        print(f"#{d['driver_number']:>2}  {d['full_name']:<20}  {d['team_name']}")

    # Example: laps under 92s for one driver, using an operator filter
    laps = client.get_laps(session_key=s["session_key"], driver_number=63, lap_duration__lt=92)
    print(f"\n{len(laps)} laps under 92s for #63")

    # Example: high-speed telemetry samples
    fast_samples = client.get_car_data(session_key=s["session_key"], driver_number=55)
    print(f"{len(fast_samples)} samples at >=315 km/h for #55")