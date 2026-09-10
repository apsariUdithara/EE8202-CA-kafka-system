#!/usr/bin/env bash
# Create every topic the pipeline needs, explicitly.
#
# Auto-creation is disabled on the broker on purpose: a typo in a topic name
# should fail loudly rather than silently create a new single-partition topic.
set -euo pipefail

BOOTSTRAP_SERVERS="${BOOTSTRAP_SERVERS:-kafka:9092}"
ORDERS_TOPIC="${ORDERS_TOPIC:-orders}"
DLQ_TOPIC="${DLQ_TOPIC:-orders.DLQ}"
AGGREGATES_TOPIC="${AGGREGATES_TOPIC:-orders.aggregates}"
ORDERS_PARTITIONS="${ORDERS_PARTITIONS:-3}"
KAFKA_BIN="${KAFKA_BIN:-/opt/kafka/bin}"

create_topic() {
  local name="$1" partitions="$2" retention_ms="$3"
  echo "==> ${name} (partitions=${partitions}, retention=${retention_ms}ms)"
  "${KAFKA_BIN}/kafka-topics.sh" \
    --bootstrap-server "${BOOTSTRAP_SERVERS}" \
    --create --if-not-exists \
    --topic "${name}" \
    --partitions "${partitions}" \
    --replication-factor 1 \
    --config "retention.ms=${retention_ms}" \
    --config min.insync.replicas=1
}

echo "Waiting for ${BOOTSTRAP_SERVERS} ..."
for _ in $(seq 1 30); do
  if "${KAFKA_BIN}/kafka-broker-api-versions.sh" --bootstrap-server "${BOOTSTRAP_SERVERS}" >/dev/null 2>&1; then
    break
  fi
  sleep 2
done

# Orders: partitioned by orderId so a single order keeps its ordering.
create_topic "${ORDERS_TOPIC}" "${ORDERS_PARTITIONS}" 604800000    # 7 days
# DLQ: one partition keeps failures in strict arrival order, and they are kept
# far longer than the source topic - a dead letter is evidence, not throughput.
create_topic "${DLQ_TOPIC}" 1 2592000000                           # 30 days
# Aggregates: a short-lived stream of running-average snapshots.
create_topic "${AGGREGATES_TOPIC}" 1 86400000                      # 1 day

echo
"${KAFKA_BIN}/kafka-topics.sh" --bootstrap-server "${BOOTSTRAP_SERVERS}" --list
echo "Topics ready."
