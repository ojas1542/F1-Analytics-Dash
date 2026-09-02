"""
Live F1 telemetry producer.

Continuously polls OpenF1's live session (`session_key="latest"`) and
publishes new car telemetry, lap, pit, and race control events to Kafka
as they appear. The Snowflake Kafka Connect sink consumes these same
topics (car_data, laps, pit, race_control) and streams them into
Snowflake via Snowpipe Streaming -- this script is the producer side of
that pipeline.

Each endpoint is polled on its own cadence (car telemetry updates far
more often than pit stops or race control messages) and only records
newer than the last one already published are re-fetched, via OpenF1's
`<field>__gt` filter, so this can run indefinitely against a live
session without re-sending data already in Snowflake.
"""

import json
import os
import time
from collections import deque
from datetime import datetime

from kafka import KafkaProducer

from telemetryIngester import OpenF1Client

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092").split(",")
OPENF1_API_TOKEN = os.environ.get("OPENF1_API_TOKEN")

# Set all three to poll a specific past session instead of the live one
# (useful for testing without waiting on a real session to be underway).
# Leave unset (the default) to stream whatever session is currently live.
SESSION_YEAR = os.environ.get("SESSION_YEAR")
SESSION_COUNTRY = os.environ.get("SESSION_COUNTRY")
SESSION_NAME = os.environ.get("SESSION_NAME")


class TieredRateLimiter:
    """Per-endpoint polling cadence layered under one global request-rate cap."""

    def __init__(self, global_max_sec: int = 6, global_max_min: int = 60):
        self.global_max_sec = global_max_sec
        self.global_max_min = global_max_min

        self.sec_timestamps = deque()
        self.min_timestamps = deque()

        self.last_execution = {}

    def is_type_due(self, request_type: str, interval_seconds: float) -> bool:
        now = time.time()
        last_time = self.last_execution.get(request_type, 0)
        return (now - last_time) >= interval_seconds

    def wait_and_record(self, request_type: str):
        while True:
            now = time.time()

            while self.sec_timestamps and self.sec_timestamps[0] <= now - 1.0:
                self.sec_timestamps.popleft()
            while self.min_timestamps and self.min_timestamps[0] <= now - 60.0:
                self.min_timestamps.popleft()

            sleep_needed = 0.0
            if len(self.min_timestamps) >= self.global_max_min:
                sleep_needed = max(sleep_needed, self.min_timestamps[0] + 60.0 - now)
            if len(self.sec_timestamps) >= self.global_max_sec:
                sleep_needed = max(sleep_needed, self.sec_timestamps[0] + 1.0 - now)

            if sleep_needed > 0:
                print(f"[GLOBAL LIMIT] Sleeping {sleep_needed:.2f}s...")
                time.sleep(sleep_needed)
                continue

            current_time = time.time()
            self.sec_timestamps.append(current_time)
            self.min_timestamps.append(current_time)
            self.last_execution[request_type] = current_time
            return


class IncrementalPoller:
    """
    Wraps one OpenF1 endpoint so repeated polls only return records newer
    than the last one already published, using OpenF1's `<field>__gt` filter.
    The first poll (no cursor yet) fetches the full backlog for the session
    so far; every poll after that is incremental.
    """

    def __init__(self, fetch, date_field: str):
        self._fetch = fetch
        self._date_field = date_field
        self._cursor = None

    def poll(self, **filters):
        if self._cursor is not None:
            filters[f"{self._date_field}__gt"] = self._cursor

        records = self._fetch(**filters)

        for record in records:
            value = record.get(self._date_field)
            if value and (self._cursor is None or value > self._cursor):
                self._cursor = value

        return records


def send_event(producer, topic, key, payload):
    producer.send(topic, key=key, value=payload)


def resolve_session(client: OpenF1Client) -> int:
    if SESSION_YEAR and SESSION_COUNTRY and SESSION_NAME:
        sessions = client.get_sessions(
            year=int(SESSION_YEAR),
            country_name=SESSION_COUNTRY,
            session_name=SESSION_NAME,
        )
    else:
        sessions = client.get_sessions(session_key="latest")

    if not sessions:
        raise ValueError("No live or matching session found.")

    return sessions[0]["session_key"]


def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: str(k).encode("utf-8") if k is not None else None,
        acks="all",
    )

    limiter = TieredRateLimiter(global_max_sec=6, global_max_min=60)
    client = OpenF1Client(api_token=OPENF1_API_TOKEN)

    print("Resolving session...")
    session_key = resolve_session(client)
    print(f"Streaming session #{session_key}")

    car_data_poller = IncrementalPoller(client.get_car_data, date_field="date")
    laps_poller = IncrementalPoller(client.get_laps, date_field="date_start")
    pit_poller = IncrementalPoller(client.get_pit, date_field="date")
    race_control_poller = IncrementalPoller(client.get_race_control, date_field="date")

    print("Starting live telemetry producer...\n")

    try:
        while True:
            # Car telemetry (high priority): every 1.5s -> ~40 API calls/min
            if limiter.is_type_due("car_data", interval_seconds=1.5):
                limiter.wait_and_record("car_data")
                records = car_data_poller.poll(session_key=session_key)
                for item in records:
                    send_event(producer, "car_data", item.get("driver_number"), item)
                print(f"[{datetime.now().strftime('%H:%M:%S')}] car_data: {len(records)} new")

            # Lap data (medium priority): every 15s -> 4 API calls/min
            if limiter.is_type_due("laps", interval_seconds=15.0):
                limiter.wait_and_record("laps")
                records = laps_poller.poll(session_key=session_key)
                for item in records:
                    send_event(producer, "laps", item.get("driver_number"), item)
                print(f"[{datetime.now().strftime('%H:%M:%S')}] laps: {len(records)} new")

            # Pit stops (medium priority): every 20s -> 3 API calls/min
            if limiter.is_type_due("pit", interval_seconds=20.0):
                limiter.wait_and_record("pit")
                records = pit_poller.poll(session_key=session_key)
                for item in records:
                    send_event(producer, "pit", item.get("driver_number"), item)
                print(f"[{datetime.now().strftime('%H:%M:%S')}] pit: {len(records)} new")

            # Race control (low priority): every 30s -> 2 API calls/min
            if limiter.is_type_due("race_control", interval_seconds=30.0):
                limiter.wait_and_record("race_control")
                records = race_control_poller.poll(session_key=session_key)
                for item in records:
                    send_event(producer, "race_control", "GLOBAL", item)
                print(f"[{datetime.now().strftime('%H:%M:%S')}] race_control: {len(records)} new")

            producer.flush()
            time.sleep(0.1)

    except KeyboardInterrupt:
        print("\nStopping live producer...")

    finally:
        producer.flush()
        producer.close()
        print("Producer closed successfully.")


if __name__ == "__main__":
    main()
