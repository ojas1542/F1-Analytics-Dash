# --- Build stage ---
FROM rust:1.85-bookworm AS builder

WORKDIR /app

# rdkafka builds librdkafka from source (cmake-build feature); it needs a
# toolchain plus the TLS/SASL/zlib headers librdkafka links against.
RUN apt-get update && apt-get install -y --no-install-recommends \
    cmake \
    pkg-config \
    libssl-dev \
    zlib1g-dev \
    libsasl2-dev \
    && rm -rf /var/lib/apt/lists/*

COPY Cargo.toml Cargo.lock ./
COPY src ./src

RUN cargo build --release

# --- Runtime stage ---
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    libssl3 \
    zlib1g \
    libsasl2-2 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/telemetryConsumer /usr/local/bin/telemetryConsumer

EXPOSE 9184

ENTRYPOINT ["/usr/local/bin/telemetryConsumer"]
