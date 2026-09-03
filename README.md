# F1-Analytics-Dash

An analytics pipeline and dashbaord capable of streaming live Formula 1 race data as well as providing analytics over long term inter-season metrics.

The pipeline consists of two paths: live-data and historical batch processing for analytics

                         F1 DATA PLATFORM
                              |
              +---------------+---------------+
              |                               |
        REAL-TIME PIPELINE               BATCH PIPELINE
              |                               |
        Live Race Feed                    OpenF1 API
              |                               |
              v                               v
           Kafka                           Airflow
              |                               |
              v                               v
     Prometheus Consumer                   Blob Storage
              |                               |
              v                               v
        Prometheus                        Snowflake
              |                               |
              v                               v
           Grafana                           dbt
                                              |
                                              v
                                      Snowflake Analytics

## Running the stack

```bash
cp .env.example .env        # fill in SNOWFLAKE_* values
cp <your key> secrets/snowflake_key.p8

docker compose up -d --build
```

This builds and starts, in dependency order: `airflow-postgres` → `airflow-init` (migrates the DB, creates the admin user) → `airflow-webserver`/`airflow-scheduler`, plus `zookeeper` → `kafka` → `kafka-connect` (Snowflake sink plugin baked in) → `connector-init` (registers the sink connector once Connect reports healthy), and `telemetry-consumer` + `prometheus`.

Check it's up:

- Airflow UI: `http://localhost:8080` (`AIRFLOW_ADMIN_USER`/`PASSWORD` from `.env`)
- Kafka Connect status: `curl http://localhost:8083/connectors/f1-telemetry-snowflake-sink/status`
- Prometheus: `http://localhost:9090`

### Feeding it data

These are profile-gated and don't start with the base `docker compose up`:

```bash
# historical backfill, via Airflow
docker compose exec airflow-webserver airflow dags trigger f1_historical_batch

# OR replay a specific historical race straight to Kafka
docker compose --profile producer up -d historic-producer

docker exec airflow-webserver airflow dags trigger f1_historical_batch \
  --conf '{"start_year": 2023, "end_year": 2025}'

# OR stream whatever F1 session is currently live
docker compose --profile live up -d live-producer
```
