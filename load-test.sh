#!/usr/bin/env bash
#
# Convenience wrapper for the stream-processor stress/load test (load-test.py).
# Assumes the Docker stack (Kafka + Postgres) AND the processor are already running.
# Extra args pass straight through, e.g.:
#   ./load-test.sh --target-events-per-second 5000 --duration-seconds 60
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"

# Prefer the project venv (kafka-python==2.0.2); else plain python3 and let
# load-test.py fall back to a python:3.11-slim container.
if [[ -x "$ROOT/.venv/bin/python" ]]; then
    PYTHON="$ROOT/.venv/bin/python"
else
    PYTHON="python3"
fi

# Pre-flight: the load test needs Kafka and the processor already up.
port_open() { (exec 3<>"/dev/tcp/$1/$2") 2>/dev/null; }

if ! port_open localhost 9092; then
    echo "Kafka not reachable on localhost:9092."
    echo "  Start the stack:  docker compose up -d zookeeper kafka postgres"
    exit 1
fi
if ! port_open localhost 8081; then
    echo "Processor not reachable on localhost:8081."
    echo "  Start it:  mvn spring-boot:run -Dspring-boot.run.profiles=local"
    echo "  (or run the IntelliJ 'Stream Processor (host)' configuration)"
    exit 1
fi

echo "Running stress test via $PYTHON ..."
exec "$PYTHON" load-test.py "$@"
