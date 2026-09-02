import json
import os
import time
from datetime import datetime, timedelta, timezone
from collections import deque
from kafka import KafkaProducer
from telemetryIngester import OpenF1Client

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092").split(",")


# ============================================================
# 1. Rate Limiter
# ============================================================

class TieredRateLimiter:
    def __init__(self, global_max_sec=6, global_max_min=60):
        self.global_max_sec = global_max_sec
        self.global_max_min = global_max_min
        self.sec_timestamps = deque()
        self.min_timestamps = deque()

    def wait_and_record(self):
        while True:
            now = time.time()

            while (
                self.sec_timestamps
                and self.sec_timestamps[0] <= now - 1.0
            ):
                self.sec_timestamps.popleft()

            while (
                self.min_timestamps
                and self.min_timestamps[0] <= now - 60.0
            ):
                self.min_timestamps.popleft()

            sleep_needed = 0.0

            if len(self.min_timestamps) >= self.global_max_min:
                sleep_needed = max(
                    sleep_needed,
                    self.min_timestamps[0] + 60.0 - now
                )

            if len(self.sec_timestamps) >= self.global_max_sec:
                sleep_needed = max(
                    sleep_needed,
                    self.sec_timestamps[0] + 1.0 - now
                )

            if sleep_needed > 0:
                time.sleep(sleep_needed)
                continue

            current_time = time.time()

            self.sec_timestamps.append(current_time)
            self.min_timestamps.append(current_time)

            return


# ============================================================
# 2. Helpers
# ============================================================

def parse_dt(value):
    if not value:
        return None

    if value.endswith("Z"):
        value = value.replace("Z", "+00:00")

    dt = datetime.fromisoformat(value)

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    return dt


def send_event(producer, topic, key, payload):
    producer.send(
        topic,
        key=key,
        value=payload
    )


# ============================================================
# 3. Kafka
# ============================================================

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: (
        str(k).encode("utf-8")
        if k is not None
        else None
    ),
    acks="all"
)


limiter = TieredRateLimiter(
    global_max_sec=6,
    global_max_min=60
)

client = OpenF1Client()


try:

    # ========================================================
    # 4. Find Session
    # ========================================================

    print("Fetching session details...")

    limiter.wait_and_record()

    sessions = client.get_sessions(
        year=2025,
        country_name="Belgium",
        session_name="Race"
    )

    if not sessions:
        raise ValueError(
            "Historical session not found."
        )

    session_data = sessions[0]

    session_key = session_data["session_key"]

    print(
        f"Found session #{session_key}"
    )


    # ========================================================
    # 5. Get Lap 1
    # ========================================================

    print("Fetching lap 1...")

    limiter.wait_and_record()

    lap_one = client.get_laps(
        session_key=session_key,
        lap_number=1
    )

    if not lap_one:
        raise ValueError(
            "Could not retrieve lap 1."
        )

    if lap_one[0].get("date_start"):
        start_time = parse_dt(
            lap_one[0]["date_start"]
        )
    else:
        start_time = parse_dt(
            session_data["date_start"]
        )

    end_time = parse_dt(
        session_data["date_end"]
    )


    # ========================================================
    # 6. Discover Drivers
    # ========================================================

    driver_numbers = sorted({
        record["driver_number"]
        for record in lap_one
        if record.get("driver_number") is not None
    })

    print(
        f"Drivers found: {driver_numbers}"
    )


    # ========================================================
    # 7. Download CAR DATA
    #
    # IMPORTANT:
    # No date filters.
    # Download once per driver.
    # ========================================================

    print("\n==============================")
    print("DOWNLOADING CAR DATA")
    print("==============================")

    car_events = []

    for i, driver_no in enumerate(
        driver_numbers,
        start=1
    ):

        print(
            f"[{i}/{len(driver_numbers)}] "
            f"Driver {driver_no}"
        )

        limiter.wait_and_record()

        records = client.get_car_data(
            session_key=session_key,
            driver_number=driver_no
        )

        count = 0

        for record in records:

            dt = parse_dt(
                record.get("date")
            )

            if dt is None:
                continue

            # Keep only race-session records
            if start_time <= dt <= end_time:

                car_events.append(
                    (dt, record)
                )

                count += 1

        print(
            f"    {count:,} records"
        )


    print(
        f"Total car telemetry: "
        f"{len(car_events):,}"
    )


    # ========================================================
    # 8. Download RACE CONTROL
    #
    # NO DATE FILTER.
    # ========================================================

    print("\n==============================")
    print("DOWNLOADING RACE CONTROL")
    print("==============================")

    limiter.wait_and_record()

    rc_records = client.get_race_control(
        session_key=session_key
    )

    rc_events = []

    for record in rc_records:

        dt = parse_dt(
            record.get("date")
        )

        if dt is None:
            continue

        if start_time <= dt <= end_time:

            rc_events.append(
                (dt, record)
            )


    print(
        f"Total race control events: "
        f"{len(rc_events):,}"
    )


    # ========================================================
    # 9. Download LAPS
    #
    # NO DATE FILTER.
    # ========================================================

    print("\n==============================")
    print("DOWNLOADING LAPS")
    print("==============================")

    limiter.wait_and_record()

    all_laps = client.get_laps(
        session_key=session_key
    )

    lap_events = []

    for record in all_laps:

        dt = parse_dt(
            record.get("date_start")
        )

        if dt is None:
            continue

        if start_time <= dt <= end_time:

            lap_events.append(
                (dt, record)
            )


    print(
        f"Total lap records: "
        f"{len(lap_events):,}"
    )


    # ========================================================
    # 10. Sort Everything
    # ========================================================

    print("\nSorting events...")

    car_events.sort(
        key=lambda x: x[0]
    )

    rc_events.sort(
        key=lambda x: x[0]
    )

    lap_events.sort(
        key=lambda x: x[0]
    )

    print("Sorting complete.")


    # ========================================================
    # 11. Playback Configuration
    # ========================================================

    PLAYBACK_SPEED = 1.0

    STEP_SECONDS = 1.0

    print("\n==============================")
    print("STARTING KAFKA PLAYBACK")
    print("==============================")

    print(
        f"Session: {session_key}"
    )

    print(
        f"Start: {start_time.isoformat()}"
    )

    print(
        f"End:   {end_time.isoformat()}"
    )

    print(
        f"Playback: {PLAYBACK_SPEED}x"
    )

    print(
        f"Step: {STEP_SECONDS}s"
    )

    print()


    # ========================================================
    # 12. Playback Indexes
    # ========================================================

    car_index = 0
    rc_index = 0
    lap_index = 0

    sim_current_time = start_time


    # ========================================================
    # 13. PLAYBACK LOOP
    #
    # NO API REQUESTS BELOW THIS POINT.
    # ========================================================

    while sim_current_time < end_time:

        step_start = time.time()

        sim_next_time = min(
            sim_current_time
            + timedelta(seconds=STEP_SECONDS),
            end_time
        )


        # ====================================================
        # CAR DATA
        # ====================================================

        car_count = 0

        while (
            car_index < len(car_events)
            and car_events[car_index][0] < sim_next_time
        ):

            event_time, record = car_events[car_index]

            if event_time >= sim_current_time:

                send_event(
                    producer,
                    "car_data",
                    record.get("driver_number"),
                    record
                )

                car_count += 1

            car_index += 1


        # ====================================================
        # RACE CONTROL
        # ====================================================

        rc_count = 0

        while (
            rc_index < len(rc_events)
            and rc_events[rc_index][0] < sim_next_time
        ):

            event_time, record = rc_events[rc_index]

            if event_time >= sim_current_time:

                send_event(
                    producer,
                    "race_control",
                    "GLOBAL",
                    record
                )

                rc_count += 1

            rc_index += 1


        # ====================================================
        # LAPS
        # ====================================================

        lap_count = 0

        while (
            lap_index < len(lap_events)
            and lap_events[lap_index][0] < sim_next_time
        ):

            event_time, record = lap_events[lap_index]

            if event_time >= sim_current_time:

                send_event(
                    producer,
                    "laps",
                    record.get("driver_number"),
                    record
                )

                lap_count += 1

            lap_index += 1


        # ====================================================
        # Flush Kafka periodically
        # ====================================================

        producer.flush()


        print(
            f"[{sim_current_time.strftime('%H:%M:%S')}] "
            f"car={car_count:,} "
            f"rc={rc_count} "
            f"laps={lap_count}"
        )


        # ====================================================
        # Advance virtual clock
        # ====================================================

        sim_current_time = sim_next_time


        # ====================================================
        # Realtime pacing
        # ====================================================

        elapsed = time.time() - step_start

        target_duration = (
            STEP_SECONDS / PLAYBACK_SPEED
        )

        sleep_time = (
            target_duration - elapsed
        )

        if sleep_time > 0:
            time.sleep(sleep_time)


    print("\nPlayback complete.")


except KeyboardInterrupt:

    print(
        "\nStopping historical stream..."
    )


finally:

    print(
        "Flushing Kafka..."
    )

    producer.flush()

    producer.close()

    print(
        "Producer closed successfully."
    )
