#!/usr/bin/env bash
# End-to-end smoke test:
#   1. Bring up Kafka + Zookeeper + Postgres via docker compose.
#   2. Build the Spring Boot app and start it (running on the host JVM, talking
#      to the broker on localhost:9092 and to Postgres on localhost:5432).
#   3. Run the Python data generator to publish the 6-scenario test workload.
#   4. Publish synthetic "watermark advancer" events on every partition so
#      the joined watermark on each partition crosses the last real PV's fence
#      and forces a deterministic flush.
#   5. Query Postgres for attribution rows and compare against the expected set.
#   6. Dump the dead_letter Kafka topic for visual inspection.
#
# Exits 0 on PASS, non-zero on FAIL. The Spring Boot app is killed on exit but
# the Docker stack is left running so you can poke around with psql / kafka-ui.

set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
LOG_DIR="$ROOT/.smoke-test"
APP_LOG="$LOG_DIR/app.log"
VENV_DIR="$LOG_DIR/venv"
PY_DEPS_DIR="$LOG_DIR/python-site"
PYTHON="python3"
USE_DOCKER_PYTHON=false
DOCKER_NETWORK=""
SMOKE_GROUP="stream-processor-smoke-$(date +%s)"
mkdir -p "$LOG_DIR"

APP_PID=""
cleanup() {
    if [[ -n "$APP_PID" ]] && kill -0 "$APP_PID" 2>/dev/null; then
        echo
        echo "Stopping app (pid=$APP_PID)..."
        kill "$APP_PID" 2>/dev/null || true
        wait "$APP_PID" 2>/dev/null || true
    fi
    echo "App log preserved at: $APP_LOG"
    echo "Docker stack left running. Stop with: docker compose down"
}
trap cleanup EXIT INT TERM

step() { printf "\n==> %s\n" "$*"; }

# ----------------------------------------------------------------------- 0/8 --
step "0/8 ensuring Python dependencies (kafka-python from requirements.txt)"
if [[ -x "$ROOT/.venv/bin/python" ]]; then
    PYTHON="$ROOT/.venv/bin/python"
elif [[ -x "$VENV_DIR/bin/python" ]]; then
    PYTHON="$VENV_DIR/bin/python"
elif [[ -d "$PY_DEPS_DIR" ]]; then
    export PYTHONPATH="$PY_DEPS_DIR${PYTHONPATH:+:$PYTHONPATH}"
fi
if ! "$PYTHON" -c "import kafka; assert getattr(kafka, '__version__', None) == '2.0.2'; from kafka import KafkaProducer; from kafka.errors import NoBrokersAvailable" >/dev/null 2>&1; then
    echo "  creating local virtualenv..."
    rm -rf "$VENV_DIR"
    if python3 -m venv "$VENV_DIR" >/dev/null 2>&1; then
        PYTHON="$VENV_DIR/bin/python"
        echo "  installing pinned dependencies into local virtualenv..."
        "$PYTHON" -m pip install --quiet -r "$ROOT/requirements.txt"
    else
        echo "  python3-venv unavailable; installing into local target directory..."
        rm -rf "$PY_DEPS_DIR"
        pip3 install --quiet --break-system-packages --target "$PY_DEPS_DIR" -r "$ROOT/requirements.txt"
        export PYTHONPATH="$PY_DEPS_DIR${PYTHONPATH:+:$PYTHONPATH}"
    fi
fi

if ! "$PYTHON" -c "import kafka; assert getattr(kafka, '__version__', None) == '2.0.2'; from kafka import KafkaProducer; from kafka.errors import NoBrokersAvailable" >/dev/null 2>&1; then
    if command -v docker >/dev/null 2>&1; then
        USE_DOCKER_PYTHON=true
        echo "  host Python cannot import kafka-python==2.0.2; using python:3.11-slim container for generator steps"
    else
        echo "  kafka-python==2.0.2 is incompatible with this host Python, and Docker is unavailable for fallback"
        exit 1
    fi
fi

run_python() {
    if $USE_DOCKER_PYTHON; then
        if [[ -z "$DOCKER_NETWORK" ]]; then
            DOCKER_NETWORK=$(docker inspect -f '{{range $name, $_ := .NetworkSettings.Networks}}{{println $name}}{{end}}' "$(docker compose ps -q kafka)" | head -n 1)
        fi
        docker run --rm -i \
            --network "$DOCKER_NETWORK" \
            -e SMOKE_BOOTSTRAP_SERVERS=kafka:29092 \
            -v "$ROOT:/work" \
            -w /work \
            python:3.11-slim \
            sh -c 'pip install --quiet -r requirements.txt && exec python "$@"' sh "$@"
    else
        "$PYTHON" "$@"
    fi
}

# ----------------------------------------------------------------------- 1/8 --
step "1/8 bringing up Kafka, Zookeeper, Postgres"
docker compose up -d zookeeper kafka postgres >/dev/null

# ----------------------------------------------------------------------- 2/8 --
step "2/8 waiting for stack to become healthy"
for i in {1..90}; do
    kafka_ok=false
    pg_ok=false
    docker compose exec -T kafka kafka-broker-api-versions \
        --bootstrap-server kafka:29092 >/dev/null 2>&1 && kafka_ok=true
    docker compose exec -T postgres pg_isready \
        -U streamprocessor -d attribution >/dev/null 2>&1 && pg_ok=true
    if $kafka_ok && $pg_ok; then
        echo "  ready"
        break
    fi
    printf "."; sleep 1
    if [[ "$i" -eq 90 ]]; then echo; echo "  timed out waiting for stack"; exit 1; fi
done

# ----------------------------------------------------------------------- 3/8 --
step "3/8 resetting output state (truncate Postgres, recreate dead_letter topic)"
docker compose exec -T postgres psql -U streamprocessor -d attribution \
    -c "CREATE TABLE IF NOT EXISTS attributed_page_views (
            page_view_id TEXT PRIMARY KEY,
            user_id TEXT NOT NULL,
            event_time TIMESTAMPTZ NOT NULL,
            url TEXT NOT NULL,
            attributed_campaign_id TEXT,
            attributed_click_id TEXT,
            written_at TIMESTAMPTZ NOT NULL DEFAULT NOW())" >/dev/null
docker compose exec -T postgres psql -U streamprocessor -d attribution \
    -c "TRUNCATE TABLE attributed_page_views" >/dev/null

# Delete and recreate all three topics. Deleting the input topics too makes the
# smoke test truly idempotent — otherwise prior runs' data lingers and replays
# would arrive past their watermark fence and dead-letter en masse.
for topic in dead_letter ad_clicks page_views; do
    docker compose exec -T kafka kafka-topics --bootstrap-server kafka:29092 \
        --delete --topic "$topic" --if-exists >/dev/null 2>&1 || true
    docker compose exec -T kafka kafka-topics --bootstrap-server kafka:29092 \
        --create --topic "$topic" --partitions 3 --replication-factor 1 \
        >/dev/null 2>&1 || true
done

docker compose exec -T kafka kafka-consumer-groups --bootstrap-server kafka:29092 \
    --delete --group "$SMOKE_GROUP" >/dev/null 2>&1 || true

# ----------------------------------------------------------------------- 4/8 --
step "4/8 building app (mvn -DskipTests package)"
mvn -q -DskipTests package

# ----------------------------------------------------------------------- 5/8 --
step "5/8 starting Spring Boot app (logs: $APP_LOG)"
JAR=$(ls -1 "$ROOT"/target/stream-processor-*.jar | head -n 1)
# Run on the host JVM and override the kafka/postgres locators to point at the
# host-published ports of the compose stack.
java -jar "$JAR" \
    --kafka.bootstrap-servers=localhost:9092 \
    --kafka.consumer.group-id="$SMOKE_GROUP" \
    --postgres.jdbc-url=jdbc:postgresql://localhost:5433/attribution \
    > "$APP_LOG" 2>&1 &
APP_PID=$!
echo "  pid=$APP_PID"

for i in {1..120}; do
    if grep -q "Started StreamProcessorApplication" "$APP_LOG" 2>/dev/null; then
        echo "  app ready"; break
    fi
    if ! kill -0 "$APP_PID" 2>/dev/null; then
        echo "  app process died. Tail of log:"
        tail -40 "$APP_LOG"
        exit 1
    fi
    printf "."; sleep 1
    if [[ "$i" -eq 120 ]]; then echo; echo "  app failed to start"; exit 1; fi
done
echo "  giving consumers 3s to subscribe..."
sleep 3

# ----------------------------------------------------------------------- 6/8 --
step "6/8 running Python data generator"
run_python data_generator.py

# ----------------------------------------------------------------------- 7/8 --
step "7/8 publishing watermark-advancer events (one per partition, far-future event_time)"
run_python - <<'PY'
import json
import os
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=os.environ.get('SMOKE_BOOTSTRAP_SERVERS', 'localhost:9092'),
    value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Event_time 2 hours past the latest real test event; guarantees joined
# watermark on every partition crosses every real PV's lateness fence.
future = "2024-01-01T14:00:00"

for partition in range(3):
    producer.send(
        'ad_clicks',
        partition=partition,
        key=f'__advancer_p{partition}'.encode(),
        value={
            'user_id':     f'__advancer_p{partition}',
            'event_time':  future,
            'campaign_id': '__ADVANCER__',
            'click_id':    f'__adv_click_p{partition}',
        })
    producer.send(
        'page_views',
        partition=partition,
        key=f'__advancer_p{partition}'.encode(),
        value={
            'user_id':    f'__advancer_p{partition}',
            'event_time': future,
            'url':        f'/__advancer/p{partition}',
            'event_id':   f'__adv_pv_p{partition}',
        })

producer.flush()
print("  3 click+pv advancer pairs sent (one per partition)")
PY

echo "  waiting 10s for watermark-driven flush..."
sleep 10

# ----------------------------------------------------------------------- 8/8 --
step "8/8 verifying outputs"

rows=$(docker compose exec -T postgres psql -U streamprocessor -d attribution \
    -t -A -F'|' -c "
    SELECT page_view_id, COALESCE(attributed_campaign_id, 'NULL')
    FROM attributed_page_views
    WHERE page_view_id LIKE 'pv_%'
    ORDER BY page_view_id")

echo
echo "Attribution rows in Postgres:"
if [[ -z "$rows" ]]; then echo "  (none — flush did not complete)"; fi
echo "$rows" | sed 's/^/  /'
echo

# Expected: the deterministic subset. pv_5 is omitted because its attribution
# depends on processing-time ordering of click_5 vs. the configured 15-min
# allowed_lateness, which is acceptable per the README's "lateness handling"
# note but not an end-to-end invariant.
#
# Portable-with-bash-3.2 (= macOS's default /bin/bash): no associative arrays.
expected_for() {
    case "$1" in
        pv_1) echo campaign_A ;;
        pv_2) echo campaign_B ;;
        pv_3) echo campaign_D ;;
        pv_4) echo NULL ;;
        pv_6) echo NULL ;;
        *)    echo "" ;;  # not asserted
    esac
}

pass=true
seen=""  # space-separated list of pv ids we observed
while IFS='|' read -r pv campaign; do
    [[ -z "$pv" ]] && continue
    seen="$seen $pv "
    expected=$(expected_for "$pv")
    if [[ -z "$expected" ]]; then
        echo "  ? $pv → $campaign  (not asserted)"
    elif [[ "$campaign" == "$expected" ]]; then
        echo "  ✓ $pv → $campaign"
    else
        echo "  ✗ $pv → $campaign  (expected $expected)"
        pass=false
    fi
done <<< "$rows"

for pv in pv_1 pv_2 pv_3 pv_4 pv_6; do
    if [[ "$seen" != *" $pv "* ]]; then
        echo "  ✗ $pv MISSING from output"
        pass=false
    fi
done

echo
echo "Dead-letter Kafka topic (5s window):"
docker compose exec -T kafka kafka-console-consumer \
    --bootstrap-server kafka:29092 --topic dead_letter --from-beginning \
    --timeout-ms 5000 2>/dev/null | sed 's/^/  /' || true

echo
if $pass; then
    echo "✅ Smoke test PASS"
else
    echo "❌ Smoke test FAIL — see $APP_LOG"
    exit 1
fi
