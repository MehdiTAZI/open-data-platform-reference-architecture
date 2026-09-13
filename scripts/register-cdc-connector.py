#!/usr/bin/env python3
import argparse
import json
import os
import time
from urllib import error, request

CONNECTOR_NAME = "orders-postgres-cdc"


def request_json(method: str, url: str, payload=None):
    body = None if payload is None else json.dumps(payload).encode()
    req = request.Request(url, data=body, method=method)
    if body is not None:
        req.add_header("Content-Type", "application/json")
    with request.urlopen(req, timeout=10) as response:
        data = response.read()
        return json.loads(data) if data else None


def wait_ready(base_url: str, attempts: int = 60):
    for _ in range(attempts):
        try:
            request_json("GET", f"{base_url}/connectors")
            return
        except Exception:
            time.sleep(1)
    raise RuntimeError("Debezium Connect REST API did not become ready")


def connector_config():
    user = os.environ["POSTGRES_USER"]
    password = os.environ["POSTGRES_PASSWORD"]
    return {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "tasks.max": "1",
        "database.hostname": "postgres.odp-data.svc.cluster.local",
        "database.port": "5432",
        "database.user": user,
        "database.password": password,
        "database.dbname": "platform",
        "topic.prefix": "odp-commerce",
        "schema.include.list": "source",
        "table.include.list": "source.orders",
        "plugin.name": "pgoutput",
        "slot.name": "odp_orders_slot",
        "publication.name": "odp_orders_pub",
        "publication.autocreate.mode": "filtered",
        "snapshot.mode": "initial",
        "tombstones.on.delete": "false",
        "decimal.handling.mode": "string",
        "time.precision.mode": "connect",
        "key.converter": "org.apache.kafka.connect.json.JsonConverter",
        "key.converter.schemas.enable": "false",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter.schemas.enable": "false",
        "provide.transaction.metadata": "true",
    }


def wait_running(base_url: str, attempts: int = 90):
    last = None
    for _ in range(attempts):
        try:
            last = request_json("GET", f"{base_url}/connectors/{CONNECTOR_NAME}/status")
            connector_running = last.get("connector", {}).get("state") == "RUNNING"
            tasks = last.get("tasks", [])
            tasks_running = bool(tasks) and all(task.get("state") == "RUNNING" for task in tasks)
            if connector_running and tasks_running:
                print(json.dumps(last, indent=2, sort_keys=True))
                return
        except Exception:
            pass
        time.sleep(1)
    raise RuntimeError(f"Debezium connector did not become RUNNING: {last}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://127.0.0.1:18083")
    args = parser.parse_args()
    base_url = args.url.rstrip("/")
    wait_ready(base_url)

    payload = {"name": CONNECTOR_NAME, "config": connector_config()}
    try:
        request_json("POST", f"{base_url}/connectors", payload)
        print(f"Created Debezium connector {CONNECTOR_NAME}")
    except error.HTTPError as exc:
        if exc.code != 409:
            raise
        request_json("PUT", f"{base_url}/connectors/{CONNECTOR_NAME}/config", payload["config"])
        print(f"Updated Debezium connector {CONNECTOR_NAME}")

    wait_running(base_url)


if __name__ == "__main__":
    main()
