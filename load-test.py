#!/usr/bin/env python3
"""
High-throughput load test for the stream processor.

Assumptions:
- Kafka, Postgres, and the Spring Boot processor are already running.
- Python environment has kafka-python==2.0.2 installed, or Docker is available
  so the script can rerun itself in python:3.11-slim.

The test publishes many events for 20 seconds at ~wall-clock event-time, covering
normal joins, out-of-order joins, multi-click latest-wins, and no-click page views.
It records a bounded sample of expected page-view outputs, then queries Postgres and
verifies those rows.

Event-times are stamped near wall-clock now (small sub-lateness offsets), so events
are neither future-rejected nor dropped as too-late. Page views are written on
arrival (commit-on-write), so no watermark-advancer events are needed.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, Iterable, List, Optional, Tuple


PARTITIONS = 3
TOPIC_CLICKS = "ad_clicks"
TOPIC_PAGE_VIEWS = "page_views"
NULL = "NULL"


@dataclass(frozen=True)
class ExpectedRow:
    campaign_id: Optional[str]
    click_id: Optional[str]

    def normalized(self) -> Tuple[str, str]:
        return self.campaign_id or NULL, self.click_id or NULL


@dataclass(frozen=True)
class DbConfig:
    host: Optional[str]
    port: int
    user: str
    password: str
    database: str


def iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%S")


def now_event_time() -> datetime:
    """Wall-clock now (second precision, naive) — events stamped here are not
    future-rejected and stay within the lateness window as the watermark advances."""
    return datetime.now(timezone.utc).replace(microsecond=0, tzinfo=None)


def kafka_import_error() -> Optional[BaseException]:
    try:
        from kafka import KafkaProducer as _KafkaProducer  # noqa: F401
        from kafka.errors import NoBrokersAvailable as _NoBrokersAvailable  # noqa: F401
        return None
    except BaseException as e:
        return e


def maybe_reexec_with_docker(args: argparse.Namespace) -> None:
    error = kafka_import_error()
    if error is None:
        return
    if os.environ.get("LOAD_TEST_IN_DOCKER") == "1":
        print(f"kafka-python import failed inside Docker fallback: {error}", file=sys.stderr)
        raise SystemExit(1)

    root = os.path.dirname(os.path.abspath(__file__))
    try:
        kafka_container = subprocess.check_output(
            ["docker", "compose", "ps", "-q", "kafka"],
            cwd=root,
            text=True,
        ).strip()
        if not kafka_container:
            raise RuntimeError("Kafka container is not running")
        network = subprocess.check_output(
            ["docker", "inspect", "-f", "{{range $name, $_ := .NetworkSettings.Networks}}{{println $name}}{{end}}", kafka_container],
            cwd=root,
            text=True,
        ).splitlines()[0]
    except Exception as docker_error:
        print(f"Host Python cannot import kafka-python: {error}", file=sys.stderr)
        print(f"Docker fallback failed: {docker_error}", file=sys.stderr)
        print("Install/use Python 3.11 with requirements.txt, or run under `newgrp docker` with the compose stack up.", file=sys.stderr)
        raise SystemExit(1)

    original_args = sys.argv[1:]
    docker_args = [
        "docker", "run", "--rm", "-i",
        "--network", network,
        "-e", "LOAD_TEST_IN_DOCKER=1",
        "-v", f"{root}:/work",
        "-w", "/work",
        "python:3.11-slim",
        "sh", "-c",
        "pip install --quiet -r requirements.txt psycopg2-binary==2.9.9 && exec python load-test.py \"$@\"",
        "sh",
        *original_args,
        "--bootstrap-servers", "kafka:29092",
        "--db-host", args.db_host or "postgres",
        "--db-port", str(args.db_port if args.db_host else 5432),
    ]
    print("Host Python cannot import kafka-python; rerunning load test in python:3.11-slim container")
    os.execvp("docker", docker_args)


def create_producer(bootstrap_servers: str, retries: int = 10):
    from kafka import KafkaProducer
    from kafka.errors import NoBrokersAvailable

    for attempt in range(retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v, separators=(",", ":")).encode("utf-8"),
                linger_ms=5,
                batch_size=64 * 1024,
                acks="all",
            )
            print(f"Connected to Kafka at {bootstrap_servers}")
            return producer
        except NoBrokersAvailable:
            if attempt == retries - 1:
                raise
            delay = min(2 ** attempt, 10)
            print(f"Kafka not ready; retrying in {delay}s ({attempt + 1}/{retries})")
            time.sleep(delay)
    raise RuntimeError("unreachable")


def send_click(
    producer,
    partition: int,
    user_id: str,
    event_time: datetime,
    campaign_id: str,
    click_id: str,
) -> int:
    producer.send(
        TOPIC_CLICKS,
        partition=partition,
        key=user_id.encode("utf-8"),
        value={
            "user_id": user_id,
            "event_time": iso(event_time),
            "campaign_id": campaign_id,
            "click_id": click_id,
        },
    )
    return 1


def send_page_view(
    producer,
    partition: int,
    user_id: str,
    event_time: datetime,
    event_id: str,
) -> int:
    producer.send(
        TOPIC_PAGE_VIEWS,
        partition=partition,
        key=user_id.encode("utf-8"),
        value={
            "user_id": user_id,
            "event_time": iso(event_time),
            "url": f"https://load.test/{event_id}",
            "event_id": event_id,
        },
    )
    return 1


def maybe_record(
    expected: Dict[str, ExpectedRow],
    event_id: str,
    row: ExpectedRow,
    sample_limit: int,
) -> None:
    if len(expected) < sample_limit:
        expected[event_id] = row


def seed_edge_cases(
    producer,
    run_id: str,
    expected: Dict[str, ExpectedRow],
    sample_limit: int,
) -> int:
    """Deterministic edge cases that all produce a row under commit-on-write.
    Event-times are ~now with small (<lateness) offsets, so nothing is dropped."""
    sent = 0

    for partition in range(PARTITIONS):
        t = now_event_time()

        # Regular: click then page view.
        user = f"{run_id}_edge_regular_p{partition}"
        click_id = f"{run_id}_edge_regular_click_p{partition}"
        campaign = f"{run_id}_campaign_regular_p{partition}"
        pv_id = f"{run_id}_edge_regular_pv_p{partition}"
        sent += send_click(producer, partition, user, t, campaign, click_id)
        sent += send_page_view(producer, partition, user, t + timedelta(seconds=1), pv_id)
        maybe_record(expected, pv_id, ExpectedRow(campaign, click_id), sample_limit)

        # Out of order: page view arrives first, click (same window) corrects it.
        user = f"{run_id}_edge_ooo_p{partition}"
        click_id = f"{run_id}_edge_ooo_click_p{partition}"
        campaign = f"{run_id}_campaign_ooo_p{partition}"
        pv_id = f"{run_id}_edge_ooo_pv_p{partition}"
        sent += send_page_view(producer, partition, user, t + timedelta(seconds=2), pv_id)
        sent += send_click(producer, partition, user, t + timedelta(seconds=1), campaign, click_id)
        maybe_record(expected, pv_id, ExpectedRow(campaign, click_id), sample_limit)

        # Multiple clicks: latest in-window click wins.
        user = f"{run_id}_edge_multi_p{partition}"
        older_click = f"{run_id}_edge_multi_old_click_p{partition}"
        latest_click = f"{run_id}_edge_multi_latest_click_p{partition}"
        older_campaign = f"{run_id}_campaign_multi_old_p{partition}"
        latest_campaign = f"{run_id}_campaign_multi_latest_p{partition}"
        pv_id = f"{run_id}_edge_multi_pv_p{partition}"
        sent += send_click(producer, partition, user, t, older_campaign, older_click)
        sent += send_click(producer, partition, user, t + timedelta(seconds=1), latest_campaign, latest_click)
        sent += send_page_view(producer, partition, user, t + timedelta(seconds=2), pv_id)
        maybe_record(expected, pv_id, ExpectedRow(latest_campaign, latest_click), sample_limit)

        # No click: null attribution (still a row).
        user = f"{run_id}_edge_no_click_p{partition}"
        pv_id = f"{run_id}_edge_no_click_pv_p{partition}"
        sent += send_page_view(producer, partition, user, t, pv_id)
        maybe_record(expected, pv_id, ExpectedRow(None, None), sample_limit)

    return sent


def send_load_group(
    producer,
    run_id: str,
    group_id: int,
    expected: Dict[str, ExpectedRow],
    sample_limit: int,
) -> int:
    partition = group_id % PARTITIONS
    scenario = group_id % 5
    t = now_event_time()
    user = f"{run_id}_u_{group_id}"
    sent = 0

    if scenario == 0:
        click_id = f"{run_id}_click_regular_{group_id}"
        campaign = f"{run_id}_campaign_regular_{group_id}"
        pv_id = f"{run_id}_pv_regular_{group_id}"
        sent += send_click(producer, partition, user, t, campaign, click_id)
        sent += send_page_view(producer, partition, user, t + timedelta(seconds=5), pv_id)
        if group_id % 100 == 0:
            maybe_record(expected, pv_id, ExpectedRow(campaign, click_id), sample_limit)
    elif scenario == 1:
        click_id = f"{run_id}_click_ooo_{group_id}"
        campaign = f"{run_id}_campaign_ooo_{group_id}"
        pv_id = f"{run_id}_pv_ooo_{group_id}"
        sent += send_page_view(producer, partition, user, t + timedelta(seconds=5), pv_id)
        sent += send_click(producer, partition, user, t + timedelta(seconds=1), campaign, click_id)
        if group_id % 100 == 1:
            maybe_record(expected, pv_id, ExpectedRow(campaign, click_id), sample_limit)
    elif scenario == 2:
        older_click = f"{run_id}_click_multi_old_{group_id}"
        latest_click = f"{run_id}_click_multi_latest_{group_id}"
        older_campaign = f"{run_id}_campaign_multi_old_{group_id}"
        latest_campaign = f"{run_id}_campaign_multi_latest_{group_id}"
        pv_id = f"{run_id}_pv_multi_{group_id}"
        sent += send_click(producer, partition, user, t, older_campaign, older_click)
        sent += send_click(producer, partition, user, t + timedelta(seconds=2), latest_campaign, latest_click)
        sent += send_page_view(producer, partition, user, t + timedelta(seconds=6), pv_id)
        if group_id % 100 == 2:
            maybe_record(expected, pv_id, ExpectedRow(latest_campaign, latest_click), sample_limit)
    elif scenario == 3:
        pv_id = f"{run_id}_pv_no_click_{group_id}"
        sent += send_page_view(producer, partition, user, t + timedelta(seconds=5), pv_id)
        if group_id % 100 == 3:
            maybe_record(expected, pv_id, ExpectedRow(None, None), sample_limit)
    else:
        # Non-sampled background traffic. Useful for throughput without making
        # verification dependent on every single generated row.
        click_id = f"{run_id}_click_bg_{group_id}"
        campaign = f"{run_id}_campaign_bg_{group_id}"
        pv_id = f"{run_id}_pv_bg_{group_id}"
        sent += send_click(producer, partition, user, t, campaign, click_id)
        sent += send_page_view(producer, partition, user, t + timedelta(seconds=4), pv_id)

    return sent


def sql_quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def query_rows(page_view_ids: Iterable[str], db: DbConfig) -> Dict[str, Tuple[str, str]]:
    ids = list(page_view_ids)
    if not ids:
        return {}
    sql = """
        SELECT page_view_id,
               COALESCE(attributed_campaign_id, 'NULL'),
               COALESCE(attributed_click_id, 'NULL')
        FROM attributed_page_views
        WHERE page_view_id IN ({ids})
    """.format(ids=", ".join(sql_quote(i) for i in ids))
    if db.host:
        import psycopg2

        rows: Dict[str, Tuple[str, str]] = {}
        with psycopg2.connect(
            host=db.host,
            port=db.port,
            user=db.user,
            password=db.password,
            dbname=db.database,
        ) as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                for page_view_id, campaign_id, click_id in cur.fetchall():
                    rows[page_view_id] = (campaign_id, click_id)
        return rows

    cmd = [
        "docker", "compose", "exec", "-T", "postgres",
        "psql", "-U", db.user, "-d", db.database,
        "-t", "-A", "-F", "|", "-c", sql,
    ]
    result = subprocess.run(
        cmd,
        check=True,
        text=True,
        capture_output=True,
        cwd=os.path.dirname(os.path.abspath(__file__)),
    )
    rows: Dict[str, Tuple[str, str]] = {}
    for line in result.stdout.splitlines():
        if not line.strip():
            continue
        page_view_id, campaign_id, click_id = line.split("|", 2)
        rows[page_view_id] = (campaign_id, click_id)
    return rows


def wait_for_rows(page_view_ids: Iterable[str], timeout_seconds: int, label: str, db: DbConfig) -> Dict[str, Tuple[str, str]]:
    ids = list(page_view_ids)
    deadline = time.monotonic() + timeout_seconds
    last_rows: Dict[str, Tuple[str, str]] = {}
    while time.monotonic() < deadline:
        last_rows = query_rows(ids, db)
        if len(last_rows) == len(ids):
            return last_rows
        missing = len(ids) - len(last_rows)
        print(f"Waiting for {missing} {label} rows...")
        time.sleep(2)
    return last_rows


def verify(expected: Dict[str, ExpectedRow], actual: Dict[str, Tuple[str, str]]) -> bool:
    ok = True
    missing = sorted(set(expected) - set(actual))
    for page_view_id in missing[:20]:
        print(f"MISSING {page_view_id}: expected {expected[page_view_id].normalized()}")
        ok = False
    if len(missing) > 20:
        print(f"... plus {len(missing) - 20} more missing rows")

    mismatches = []
    for page_view_id, expected_row in expected.items():
        if page_view_id not in actual:
            continue
        expected_tuple = expected_row.normalized()
        if actual[page_view_id] != expected_tuple:
            mismatches.append((page_view_id, expected_tuple, actual[page_view_id]))

    for page_view_id, expected_tuple, actual_tuple in mismatches[:20]:
        print(f"MISMATCH {page_view_id}: expected {expected_tuple}, actual {actual_tuple}")
        ok = False
    if len(mismatches) > 20:
        print(f"... plus {len(mismatches) - 20} more mismatches")
    return ok and not missing and not mismatches


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load test the stream processor for 20 seconds by default.")
    parser.add_argument("--bootstrap-servers", default="localhost:9092",
                        help="Kafka bootstrap servers (default: localhost:9092)")
    parser.add_argument("--duration-seconds", type=int, default=20,
                        help="How long to publish high-rate load before verification (default: 20)")
    parser.add_argument("--target-events-per-second", type=int, default=1000,
                        help="Approximate publish pace (default: 1000 events/s)")
    parser.add_argument("--sample-limit", type=int, default=200,
                        help="Max expected rows kept in memory and verified (default: 200)")
    parser.add_argument("--wait-timeout-seconds", type=int, default=60,
                        help="How long to wait for expected rows to appear in Postgres (default: 60)")
    parser.add_argument("--db-host", default=None,
                        help="Postgres host for direct verification. If omitted, uses docker compose exec postgres psql.")
    parser.add_argument("--db-port", type=int, default=5433,
                        help="Postgres port for direct verification (default: 5433 on host; Docker fallback overrides to 5432)")
    parser.add_argument("--db-user", default="streamprocessor",
                        help="Postgres user (default: streamprocessor)")
    parser.add_argument("--db-password", default="streamprocessor",
                        help="Postgres password for direct verification (default: streamprocessor)")
    parser.add_argument("--db-name", default="attribution",
                        help="Postgres database (default: attribution)")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    maybe_reexec_with_docker(args)

    db = DbConfig(
        host=args.db_host,
        port=args.db_port,
        user=args.db_user,
        password=args.db_password,
        database=args.db_name,
    )
    run_id = "lt_" + str(int(time.time()))
    expected: Dict[str, ExpectedRow] = {}

    producer = create_producer(args.bootstrap_servers)
    sent = 0
    group_id = 0

    print(f"Run id: {run_id}")
    print("Seeding deterministic edge cases...")
    sent += seed_edge_cases(producer, run_id, expected, args.sample_limit)
    producer.flush()

    print(f"Publishing high-rate load for {args.duration_seconds}s...")
    start = time.monotonic()
    next_report = start + 5
    while time.monotonic() - start < args.duration_seconds:
        sent += send_load_group(producer, run_id, group_id, expected, args.sample_limit)
        group_id += 1
        if sent % 5000 < 5:
            producer.flush()

        elapsed = time.monotonic() - start
        target_sent = args.target_events_per_second * elapsed
        if sent > target_sent:
            time.sleep(min((sent - target_sent) / max(args.target_events_per_second, 1), 0.05))

        if time.monotonic() >= next_report:
            print(f"  sent={sent} groups={group_id} sampled_expected={len(expected)}")
            next_report += 5

    producer.flush()
    print(f"Load publish complete: sent={sent}, groups={group_id}, sampled_expected={len(expected)}")

    # Page views are written on arrival (commit-on-write) and corrections apply as
    # clicks land, so the sampled rows appear without any watermark advancement.
    print("Querying sampled expected rows from Postgres...")
    actual = wait_for_rows(expected.keys(), args.wait_timeout_seconds, "expected", db)
    passed = verify(expected, actual)

    print()
    print(f"Events sent: {sent}")
    print(f"Sampled rows expected: {len(expected)}")
    print(f"Sampled rows found: {len(actual)}")
    if passed:
        print("LOAD TEST PASS")
        return 0
    print("LOAD TEST FAIL")
    return 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except subprocess.CalledProcessError as e:
        print("Command failed:", " ".join(e.cmd), file=sys.stderr)
        print(e.stderr, file=sys.stderr)
        raise SystemExit(1)
