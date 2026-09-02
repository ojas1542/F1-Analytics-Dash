use axum::{Router, extract::State, routing::get};
use prometheus_client::{
    encoding::{EncodeLabelSet, text::encode},
    metrics::{counter::Counter, family::Family, gauge::Gauge},
    registry::Registry,
};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;
use serde::Deserialize;
use std::{
    error::Error,
    sync::{
        Arc, Mutex,
        atomic::{AtomicI64, AtomicU64},
    },
};

// ============================================================
// Struct Definitions
// ============================================================

#[derive(Debug, Deserialize)]
struct CarTelemetry {
    driver_number: Option<i32>,
    speed: Option<f64>,
    throttle: Option<f64>,
    brake: Option<f64>,
    gear: Option<i32>,
    rpm: Option<i32>,
    date: Option<String>,
}

#[derive(Debug, Deserialize)]
struct LapData {
    driver_number: Option<i32>,
    lap_number: Option<i32>,
    lap_duration: Option<f64>,
    duration_sector_1: Option<f64>,
    duration_sector_2: Option<f64>,
    duration_sector_3: Option<f64>,
}

#[derive(Debug, Deserialize)]
struct PitData {
    driver_number: Option<i32>,
    pit_duration: Option<f64>,
    lap_number: Option<i32>,
}

#[derive(Debug, Deserialize)]
struct RaceControlData {
    flag: Option<String>,
    message: Option<String>,
    scope: Option<String>,
    sector: Option<i32>,
}

// ============================================================
// Prometheus Labels
// ============================================================
//
// IMPORTANT:
// EncodeLabelSet is required by Family<DriverLabel, ...>
//

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct DriverLabel {
    driver: String,
}

// ============================================================
// Metrics
// ============================================================

struct F1Metrics {
    // --------------------------------------------------------
    // Car telemetry
    // --------------------------------------------------------
    //
    // prometheus-client 0.23.x needs AtomicU64 for f64 gauges.
    //
    speed: Family<DriverLabel, Gauge<f64, AtomicU64>>,
    throttle: Family<DriverLabel, Gauge<f64, AtomicU64>>,
    brake: Family<DriverLabel, Gauge<f64, AtomicU64>>,

    // Integer gauges use i64 + AtomicI64.
    gear: Family<DriverLabel, Gauge<i64, AtomicI64>>,
    rpm: Family<DriverLabel, Gauge<i64, AtomicI64>>,

    // --------------------------------------------------------
    // Lap data
    // --------------------------------------------------------
    lap_number: Family<DriverLabel, Gauge<i64, AtomicI64>>,
    lap_duration: Family<DriverLabel, Gauge<f64, AtomicU64>>,
    sector_1_duration: Family<DriverLabel, Gauge<f64, AtomicU64>>,
    sector_2_duration: Family<DriverLabel, Gauge<f64, AtomicU64>>,
    sector_3_duration: Family<DriverLabel, Gauge<f64, AtomicU64>>,

    // --------------------------------------------------------
    // Pit data
    // --------------------------------------------------------
    pit_stops: Family<DriverLabel, Counter>,
    pit_duration: Family<DriverLabel, Gauge<f64, AtomicU64>>,

    // --------------------------------------------------------
    // General consumer metrics
    // --------------------------------------------------------
    messages_received: Counter,
    kafka_errors: Counter,
    deserialization_errors: Counter,
}

impl F1Metrics {
    fn new() -> Self {
        Self {
            speed: Family::default(),
            throttle: Family::default(),
            brake: Family::default(),

            gear: Family::default(),
            rpm: Family::default(),

            lap_number: Family::default(),
            lap_duration: Family::default(),
            sector_1_duration: Family::default(),
            sector_2_duration: Family::default(),
            sector_3_duration: Family::default(),

            pit_stops: Family::default(),
            pit_duration: Family::default(),

            messages_received: Counter::default(),
            kafka_errors: Counter::default(),
            deserialization_errors: Counter::default(),
        }
    }

    fn register(&self, registry: &mut Registry) {
        registry.register(
            "f1_speed_kph",
            "Current car speed in km/h",
            self.speed.clone(),
        );

        registry.register(
            "f1_throttle_percent",
            "Current throttle percentage",
            self.throttle.clone(),
        );

        registry.register(
            "f1_brake_percent",
            "Current brake percentage",
            self.brake.clone(),
        );

        registry.register("f1_gear", "Current gear", self.gear.clone());

        registry.register("f1_rpm", "Current engine RPM", self.rpm.clone());

        registry.register(
            "f1_lap_number",
            "Current lap number",
            self.lap_number.clone(),
        );

        registry.register(
            "f1_lap_duration_seconds",
            "Latest lap duration in seconds",
            self.lap_duration.clone(),
        );

        registry.register(
            "f1_sector_1_duration_seconds",
            "Latest sector 1 duration in seconds",
            self.sector_1_duration.clone(),
        );

        registry.register(
            "f1_sector_2_duration_seconds",
            "Latest sector 2 duration in seconds",
            self.sector_2_duration.clone(),
        );

        registry.register(
            "f1_sector_3_duration_seconds",
            "Latest sector 3 duration in seconds",
            self.sector_3_duration.clone(),
        );

        registry.register(
            "f1_pit_stops_total",
            "Total pit stops observed",
            self.pit_stops.clone(),
        );

        registry.register(
            "f1_pit_duration_seconds",
            "Latest pit stop duration in seconds",
            self.pit_duration.clone(),
        );

        registry.register(
            "f1_messages_received_total",
            "Total Kafka messages received",
            self.messages_received.clone(),
        );

        registry.register(
            "f1_kafka_errors_total",
            "Total Kafka consumer errors",
            self.kafka_errors.clone(),
        );

        registry.register(
            "f1_deserialization_errors_total",
            "Total JSON deserialization errors",
            self.deserialization_errors.clone(),
        );
    }
}

// ============================================================
// Prometheus HTTP Handler
// ============================================================

type AppState = Arc<Mutex<Registry>>;

async fn metrics_handler(State(registry): State<AppState>) -> String {
    let registry = registry.lock().unwrap();

    let mut buffer = String::new();

    if let Err(e) = encode(&mut buffer, &registry) {
        eprintln!("Failed to encode Prometheus metrics: {:?}", e);
    }

    buffer
}

// ============================================================
// Main
// ============================================================

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    env_logger::init();

    let brokers = std::env::var("KAFKA_BROKERS").unwrap_or_else(|_| "localhost:9092".to_string());
    let metrics_port = std::env::var("METRICS_PORT").unwrap_or_else(|_| "9184".to_string());
    let group_id = "f1-telemetry-processor-group";

    let topics = ["car_data", "laps", "pit", "race_control"];

    println!("Initializing Rust Kafka Consumer...");

    // ========================================================
    // Prometheus setup
    // ========================================================

    let mut registry = Registry::default();

    let metrics = Arc::new(F1Metrics::new());

    metrics.register(&mut registry);

    let registry = Arc::new(Mutex::new(registry));

    // ========================================================
    // Start Prometheus HTTP server
    // ========================================================

    let metrics_registry = registry.clone();
    let metrics_bind_addr = format!("0.0.0.0:{metrics_port}");

    tokio::spawn(async move {
        let app = Router::new()
            .route("/metrics", get(metrics_handler))
            .with_state(metrics_registry);

        let listener = tokio::net::TcpListener::bind(&metrics_bind_addr)
            .await
            .unwrap_or_else(|e| panic!("Failed to bind metrics server on {metrics_bind_addr}: {e}"));

        println!("Prometheus metrics available at http://{metrics_bind_addr}/metrics");

        axum::serve(listener, app)
            .await
            .expect("Metrics server failed");
    });

    // ========================================================
    // Kafka Consumer
    // ========================================================

    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("group.id", group_id)
        .set("enable.auto.commit", "true")
        .set("auto.commit.interval.ms", "5000")
        .set("auto.offset.reset", "earliest")
        .create()?;

    consumer.subscribe(&topics)?;

    println!("Subscribed to topics: {:?}", topics);

    // ========================================================
    // Event Loop
    // ========================================================

    loop {
        match consumer.recv().await {
            Err(e) => {
                eprintln!("Kafka error receiving message: {:?}", e);

                metrics.kafka_errors.inc();
            }

            Ok(borrowed_message) => {
                let topic = borrowed_message.topic();

                metrics.messages_received.inc();

                // ------------------------------------------------
                // Kafka key
                // ------------------------------------------------

                let key = borrowed_message
                    .key()
                    .and_then(|k| std::str::from_utf8(k).ok())
                    .unwrap_or("UNKNOWN_KEY");

                // ------------------------------------------------
                // Payload
                // ------------------------------------------------

                if let Some(payload_bytes) = borrowed_message.payload() {
                    match topic {
                        // ====================================================
                        // CAR TELEMETRY
                        // ====================================================
                        "car_data" => {
                            match serde_json::from_slice::<CarTelemetry>(payload_bytes) {
                                Ok(data) => {
                                    let driver = data.driver_number.unwrap_or(0).to_string();

                                    let label = DriverLabel {
                                        driver: driver.clone(),
                                    };

                                    // Speed
                                    if let Some(value) = data.speed {
                                        metrics.speed.get_or_create(&label).set(value);
                                    }

                                    // Throttle
                                    if let Some(value) = data.throttle {
                                        metrics.throttle.get_or_create(&label).set(value);
                                    }

                                    // Brake
                                    if let Some(value) = data.brake {
                                        metrics.brake.get_or_create(&label).set(value);
                                    }

                                    // Gear
                                    if let Some(value) = data.gear {
                                        metrics.gear.get_or_create(&label).set(value as i64);
                                    }

                                    // RPM
                                    if let Some(value) = data.rpm {
                                        metrics.rpm.get_or_create(&label).set(value as i64);
                                    }

                                    println!(
                                        "[CAR_DATA] Driver #{} | Speed: {:?} km/h | Gear: {:?}",
                                        driver, data.speed, data.gear
                                    );
                                }

                                Err(e) => {
                                    eprintln!("Failed to deserialize car_data: {:?}", e);

                                    metrics.deserialization_errors.inc();
                                }
                            }
                        }

                        // ====================================================
                        // LAP DATA
                        // ====================================================
                        "laps" => {
                            match serde_json::from_slice::<LapData>(payload_bytes) {
                                Ok(data) => {
                                    let driver = data.driver_number.unwrap_or(0).to_string();

                                    let label = DriverLabel {
                                        driver: driver.clone(),
                                    };

                                    // Lap number
                                    if let Some(value) = data.lap_number {
                                        metrics.lap_number.get_or_create(&label).set(value as i64);
                                    }

                                    // Lap duration
                                    if let Some(value) = data.lap_duration {
                                        metrics.lap_duration.get_or_create(&label).set(value);
                                    }

                                    // Sector 1
                                    if let Some(value) = data.duration_sector_1 {
                                        metrics.sector_1_duration.get_or_create(&label).set(value);
                                    }

                                    // Sector 2
                                    if let Some(value) = data.duration_sector_2 {
                                        metrics.sector_2_duration.get_or_create(&label).set(value);
                                    }

                                    // Sector 3
                                    if let Some(value) = data.duration_sector_3 {
                                        metrics.sector_3_duration.get_or_create(&label).set(value);
                                    }

                                    println!(
                                        "[LAP_EVENT] Driver #{} | Lap {:?} | Time: {:?}s",
                                        driver, data.lap_number, data.lap_duration
                                    );
                                }

                                Err(e) => {
                                    eprintln!("Failed to deserialize laps: {:?}", e);

                                    metrics.deserialization_errors.inc();
                                }
                            }
                        }

                        // ====================================================
                        // PIT DATA
                        // ====================================================
                        "pit" => match serde_json::from_slice::<PitData>(payload_bytes) {
                            Ok(data) => {
                                let driver = data.driver_number.unwrap_or(0).to_string();

                                let label = DriverLabel {
                                    driver: driver.clone(),
                                };

                                if let Some(value) = data.pit_duration {
                                    metrics.pit_duration.get_or_create(&label).set(value);
                                }

                                metrics.pit_stops.get_or_create(&label).inc();

                                println!(
                                    "[PIT_STOP] Driver #{} | Duration: {:?}s | Lap {:?}",
                                    driver, data.pit_duration, data.lap_number
                                );
                            }

                            Err(e) => {
                                eprintln!("Failed to deserialize pit: {:?}", e);

                                metrics.deserialization_errors.inc();
                            }
                        },

                        // ====================================================
                        // RACE CONTROL
                        // ====================================================
                        "race_control" => {
                            match serde_json::from_slice::<RaceControlData>(payload_bytes) {
                                Ok(data) => {
                                    println!(
                                        "[RACE_CONTROL] Flag: {} | Message: {} | Scope: {} | Sector: {:?}",
                                        data.flag.as_deref().unwrap_or("NONE"),
                                        data.message.as_deref().unwrap_or(""),
                                        data.scope.as_deref().unwrap_or(""),
                                        data.sector
                                    );
                                }

                                Err(e) => {
                                    eprintln!("Failed to deserialize race_control: {:?}", e);

                                    metrics.deserialization_errors.inc();
                                }
                            }
                        }

                        // ====================================================
                        // Unknown topic
                        // ====================================================
                        _ => {
                            println!(
                                "Received message from unexpected topic: {} | Key: {}",
                                topic, key
                            );
                        }
                    }
                }
            }
        }
    }
}
