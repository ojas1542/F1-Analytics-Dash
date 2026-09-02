#!/usr/bin/env bash
# Registers (or updates) the Snowflake sink connector against a running
# Kafka Connect REST API. Runs once as a short-lived init container --
# the private key never touches disk outside the container and is never
# baked into an image or committed template.
set -euo pipefail

CONNECT_URL="${CONNECT_URL:-http://kafka-connect:8083}"
CONNECTOR_NAME="${CONNECTOR_NAME:-f1-telemetry-snowflake-sink}"
TEMPLATE_PATH="${TEMPLATE_PATH:-/connect/snowflake-sink-connector.json}"
KEY_PATH="${SNOWFLAKE_PRIVATE_KEY_PATH:-/secrets/snowflake_key.p8}"

echo "Waiting for Kafka Connect REST API at ${CONNECT_URL}..."
until curl -s -f -o /dev/null "${CONNECT_URL}/connectors"; do
  sleep 3
done
echo "Kafka Connect is up."

if [ ! -f "${KEY_PATH}" ]; then
  echo "Private key not found at ${KEY_PATH} -- mount it via the secrets volume." >&2
  exit 1
fi

# Snowflake's connector wants the key as base64 DER with no PEM
# header/footer/newlines.
export SNOWFLAKE_PRIVATE_KEY_INLINE
SNOWFLAKE_PRIVATE_KEY_INLINE="$(grep -v -- '-----' "${KEY_PATH}" | tr -d '\n')"

RENDERED="$(mktemp)"
RESPONSE_BODY="$(mktemp)"
trap 'rm -f "${RENDERED}" "${RESPONSE_BODY}"' EXIT
envsubst < "${TEMPLATE_PATH}" > "${RENDERED}"

echo "Registering connector '${CONNECTOR_NAME}'..."
STATUS="$(curl -s -o "${RESPONSE_BODY}" -w '%{http_code}' \
  -X PUT "${CONNECT_URL}/connectors/${CONNECTOR_NAME}/config" \
  -H "Content-Type: application/json" \
  -d @"${RENDERED}")"

if [ "${STATUS}" -ge 200 ] && [ "${STATUS}" -lt 300 ]; then
  echo "Connector registered (HTTP ${STATUS})."
else
  echo "Connector registration failed (HTTP ${STATUS}):" >&2
  cat "${RESPONSE_BODY}" >&2
  exit 1
fi
