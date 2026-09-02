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



