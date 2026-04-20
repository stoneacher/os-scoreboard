#!/usr/bin/env sh
set -eu

INFLUX_URL="${INFLUX_URL:-http://localhost:8086}"
INFLUX_ORG="${INFLUX_ORG:-uni-scoreboard}"
INFLUX_BUCKET="${INFLUX_BUCKET:-scoreboard}"
INFLUX_TOKEN="${INFLUX_TOKEN:-}"

if [ -z "$INFLUX_TOKEN" ]; then
  echo "INFLUX_TOKEN is required" >&2
  exit 1
fi

echo "Checking InfluxDB at ${INFLUX_URL}"
influx ping --host "${INFLUX_URL}"

if influx bucket find --host "${INFLUX_URL}" --org "${INFLUX_ORG}" --token "${INFLUX_TOKEN}" --name "${INFLUX_BUCKET}" | grep -q "${INFLUX_BUCKET}"; then
  echo "Bucket ${INFLUX_BUCKET} already exists"
else
  influx bucket create --host "${INFLUX_URL}" --org "${INFLUX_ORG}" --token "${INFLUX_TOKEN}" --name "${INFLUX_BUCKET}"
  echo "Created bucket ${INFLUX_BUCKET}"
fi
