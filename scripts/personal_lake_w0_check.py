#!/usr/bin/env python3
"""Verify the personal lake W0 boot gate.

The check intentionally requires explicit config, host-side DB path, base URL,
and API token environment name. Docker configs use container paths, so guessing
the host volume path would hide deployment mistakes.
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any


EXPECTED_ROUTING_ORDER = "year_month_source_container_published"
EXPECTED_ROUTING_AXES = [
    "coarse_year",
    "coarse_month",
    "source",
    "container",
    "fine_published",
]
EXPECTED_ROUTING_VERSION = "routing-keyspec/v1"
EXPECTED_IDENTITY_VERSION = "identity-keyspec/v1"


def main() -> int:
    if sys.version_info < (3, 11):
        fail("Python 3.11 or newer is required for tomllib")

    import tomllib

    args = parse_args()
    config_path = args.config.resolve(strict=True)
    db_path = args.db.resolve(strict=True)
    token = required_env(args.api_token_env)

    with config_path.open("rb") as handle:
        config = tomllib.load(handle)

    verify_config(config, config_path)
    health = verify_deep_health(args.base_url, token, args.timeout_seconds)
    partition = verify_partition_log(db_path)

    print(
        json.dumps(
            {
                "status": "ok",
                "health_status": health["status"],
                "storage_dependency": "ok",
                "database_path": str(db_path),
                "partition_initialize_event_seq": partition["event_seq"],
                "routing_axes": partition["routing_axes"],
                "root_leaf_id": partition["root_leaf_id"],
            },
            indent=2,
        )
    )
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument("--base-url", required=True)
    parser.add_argument("--api-token-env", required=True)
    parser.add_argument("--timeout-seconds", type=float, default=10.0)
    return parser.parse_args()


def required_env(name: str) -> str:
    try:
        value = os.environ[name]
    except KeyError:
        fail(f"missing environment variable {name}")
    if not value.strip():
        fail(f"environment variable {name} must not be blank")
    return value


def verify_config(config: dict[str, Any], config_path: Path) -> None:
    routing = required_table(config, "routing", config_path)
    key_order = required_value(routing, "key_order", config_path)
    if key_order != EXPECTED_ROUTING_ORDER:
        fail(
            f"{config_path}: routing.key_order is {key_order!r}; "
            f"expected {EXPECTED_ROUTING_ORDER!r}"
        )

    sources = required_table(config, "sources", config_path)
    for source_key in ("slack", "google_slides"):
        value = required_value(sources, source_key, config_path)
        if value != []:
            fail(f"{config_path}: sources.{source_key} must be an empty array")


def verify_deep_health(
    base_url: str, token: str, timeout_seconds: float
) -> dict[str, Any]:
    if timeout_seconds <= 0:
        fail("--timeout-seconds must be positive")
    url = f"{base_url.rstrip('/')}/health/deep"
    request = urllib.request.Request(
        url,
        headers={"Authorization": f"Bearer {token}"},
        method="GET",
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
            status_code = response.status
            body = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as error:
        detail = error.read().decode("utf-8", errors="replace")
        fail(f"{url} returned HTTP {error.code}: {detail}")
    except urllib.error.URLError as error:
        fail(f"{url} request failed: {error}")

    if status_code != 200:
        fail(f"{url} returned HTTP {status_code}")
    if body.get("status") != "ok":
        fail(f"{url} status is {body.get('status')!r}; expected 'ok'")

    dependencies = body.get("dependencies")
    if not isinstance(dependencies, list):
        fail(f"{url} response dependencies must be a list")
    storage = [item for item in dependencies if item.get("name") == "storage"]
    if len(storage) != 1:
        fail(f"{url} response must contain exactly one storage dependency")
    if storage[0].get("status") != "ok":
        fail(f"{url} storage dependency status is {storage[0].get('status')!r}")

    return body


def verify_partition_log(db_path: Path) -> dict[str, Any]:
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """
            SELECT event_seq, leaf_id, routing_keyspec_json,
                   identity_keyspec_json, event_json
            FROM partition_log
            WHERE event_type = 'initialize'
            """
        ).fetchall()
        if len(rows) != 1:
            fail(f"{db_path}: expected exactly one partition initialize event, got {len(rows)}")

        row = rows[0]
        routing = json.loads(row["routing_keyspec_json"])
        identity = json.loads(row["identity_keyspec_json"])
        event = json.loads(row["event_json"])

        verify_routing_keyspec(db_path, routing)
        verify_identity_keyspec(db_path, identity)
        verify_initialize_event(db_path, row["leaf_id"], event)
        verify_partition_invariants(db_path, conn)

        return {
            "event_seq": row["event_seq"],
            "routing_axes": [axis["name"] for axis in routing["axes"]],
            "root_leaf_id": event["root_leaf_id"],
        }
    finally:
        conn.close()


def verify_routing_keyspec(db_path: Path, routing: dict[str, Any]) -> None:
    if routing.get("version") != EXPECTED_ROUTING_VERSION:
        fail(f"{db_path}: unexpected routing keyspec version {routing.get('version')!r}")
    axes = routing.get("axes")
    if not isinstance(axes, list):
        fail(f"{db_path}: routing keyspec axes must be a list")
    axis_names = [axis.get("name") for axis in axes]
    if axis_names != EXPECTED_ROUTING_AXES:
        fail(f"{db_path}: routing axes are {axis_names!r}; expected {EXPECTED_ROUTING_AXES!r}")


def verify_identity_keyspec(db_path: Path, identity: dict[str, Any]) -> None:
    if identity.get("version") != EXPECTED_IDENTITY_VERSION:
        fail(f"{db_path}: unexpected identity keyspec version {identity.get('version')!r}")
    if identity.get("structure") != "source:object_id:sha256(canonical_content)":
        fail(f"{db_path}: unexpected identity keyspec structure {identity.get('structure')!r}")


def verify_initialize_event(db_path: Path, leaf_id: str, event: dict[str, Any]) -> None:
    root_leaf_id = event.get("root_leaf_id")
    if root_leaf_id != leaf_id:
        fail(f"{db_path}: initialize event root_leaf_id does not match partition_log.leaf_id")
    if event.get("routing_keyspec_version") != EXPECTED_ROUTING_VERSION:
        fail(f"{db_path}: initialize event routing keyspec version mismatch")
    if event.get("identity_keyspec_version") != EXPECTED_IDENTITY_VERSION:
        fail(f"{db_path}: initialize event identity keyspec version mismatch")


def verify_partition_invariants(db_path: Path, conn: sqlite3.Connection) -> None:
    indexes = {
        row["name"]
        for row in conn.execute(
            """
            SELECT name
            FROM sqlite_master
            WHERE type = 'index'
              AND name = 'partition_log_single_initialize'
            """
        )
    }
    if indexes != {"partition_log_single_initialize"}:
        fail(f"{db_path}: partition_log_single_initialize index is missing")

    triggers = {
        row["name"]
        for row in conn.execute(
            """
            SELECT name
            FROM sqlite_master
            WHERE type = 'trigger'
              AND name IN ('partition_log_no_update', 'partition_log_no_delete')
            """
        )
    }
    expected = {"partition_log_no_update", "partition_log_no_delete"}
    if triggers != expected:
        fail(f"{db_path}: partition_log append-only triggers are {sorted(triggers)!r}")


def required_table(config: dict[str, Any], key: str, config_path: Path) -> dict[str, Any]:
    value = required_value(config, key, config_path)
    if not isinstance(value, dict):
        fail(f"{config_path}: {key} must be a TOML table")
    return value


def required_value(config: dict[str, Any], key: str, config_path: Path) -> Any:
    if key not in config:
        fail(f"{config_path}: missing required key {key}")
    return config[key]


def fail(message: str) -> None:
    raise SystemExit(f"error: {message}")


if __name__ == "__main__":
    raise SystemExit(main())
