#!/usr/bin/env bash
#
# Runs librdkafka's broker tests against the Kafka cluster of
# docker-compose.yml, next to this file: what tests/run-all-tests.sh does with
# a trivup cluster, without Java nor Kafka on the host but Docker.
#
#   .github/ci/kafka/run-tests.sh [run-test-batches.py arguments, e.g. -Q]
#
# The tests (make -C tests build) must be built. Environment:
#   KAFKA_VERSION   the Apache Kafka image version (4.3.0)
#   TESTS ...       as for tests/run-test-batches.py, e.g. TESTS=81-
#   KEEP_CLUSTER=1  leaves the cluster running, e.g. to run the tests again
#
# The tests get what trivup gives them: test.conf (RDKAFKA_TEST_CONF),
# BROKERS, the broker version, and KAFKA_PATH, Kafka's distribution, copied
# out of the image, for the tests running its scripts or Java classes.

set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
root="$(cd "$here/../../.." && pwd)"
export KAFKA_VERSION="${KAFKA_VERSION:-4.3.0}"
image="docker.io/apache/kafka:${KAFKA_VERSION}"
work="${KAFKA_TESTS_DIR:-${TMPDIR:-/tmp}/librdkafka-kafka-tests}"
brokers="localhost:9092,localhost:9093,localhost:9094"

# Empty means all, as the workflow passes it: run-test-batches.py takes a
# set TESTS as the list to run, and an empty one fails.
if [[ -z "${TESTS:-}" ]]; then
    unset TESTS
fi
compose=(docker compose --file "$here/docker-compose.yml")

cleanup() {
    if [[ -z "${KEEP_CLUSTER:-}" ]]; then
        "${compose[@]}" down --volumes --remove-orphans >/dev/null 2>&1 || true
    fi
}
trap cleanup EXIT

echo "Starting Apache Kafka ${KAFKA_VERSION}: ${brokers}"
if ! "${compose[@]}" up --detach --wait; then
    "${compose[@]}" logs --tail 100 >&2
    exit 1
fi

dist="$work/kafka-${KAFKA_VERSION}"
if [[ ! -x "$dist/bin/kafka-topics.sh" ]]; then
    echo "Copying Kafka's distribution out of $image into $dist"
    rm -rf "$dist"
    mkdir -p "$work"
    container="$(docker create "$image")"
    docker cp --quiet "$container:/opt/kafka" "$dist"
    docker rm "$container" >/dev/null
fi

mkdir -p "$work"
echo "bootstrap.servers=${brokers}" > "$work/test.conf"

export RDKAFKA_TEST_CONF="$work/test.conf"
export BROKERS="$brokers"
export KAFKA_PATH="$dist"
export TEST_KAFKA_VERSION="$KAFKA_VERSION"

cd "$root/tests"
python3 run-test-batches.py "$@"
