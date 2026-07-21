#!/usr/bin/env python3
"""
EDA Event Generator

Generate demo events and deliver them by:
- HTTP webhook
- UDP/TCP syslog
- SNMP v2c trap
- Kafka

Event definitions are stored in events.yml.
"""

from __future__ import annotations

import argparse
import json
import logging
import logging.handlers
import socket
import sys
import time
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml
import requests

try:
    from confluent_kafka import Producer
except ImportError:
    Producer = None

try:
    from pysnmp.hlapi import (
        CommunityData,
        ContextData,
        NotificationType,
        ObjectIdentity,
        ObjectType,
        SnmpEngine,
        UdpTransportTarget,
        sendNotification,
    )
except ImportError:
    CommunityData = None


DEFAULT_CONFIG = Path(__file__).with_name("events.yml")


def load_config(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Configuration file not found: {path}")

    with path.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle) or {}

    if "events" not in data or not isinstance(data["events"], dict):
        raise ValueError("Configuration must contain an 'events' mapping.")

    return data


def utc_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat()


def render_value(value: Any, variables: dict[str, Any]) -> Any:
    if isinstance(value, str):
        rendered = value
        for key, replacement in variables.items():
            rendered = rendered.replace(f"{{{{ {key} }}}}", str(replacement))
            rendered = rendered.replace(f"{{{{{key}}}}}", str(replacement))
        return rendered

    if isinstance(value, dict):
        return {key: render_value(item, variables) for key, item in value.items()}

    if isinstance(value, list):
        return [render_value(item, variables) for item in value]

    return value


def build_event(
    name: str,
    definition: dict[str, Any],
    overrides: dict[str, str],
) -> dict[str, Any]:
    event = deepcopy(definition.get("payload", {}))

    variables = {
        "timestamp": utc_timestamp(),
        "event_name": name,
        **definition.get("variables", {}),
        **overrides,
    }

    rendered = render_value(event, variables)

    rendered.setdefault("event_name", name)
    rendered.setdefault("timestamp", variables["timestamp"])
    rendered.setdefault("source", "eda-event-generator")

    return rendered


def send_webhook(target: dict[str, Any], payload: dict[str, Any]) -> None:
    url = target["url"]
    headers = target.get("headers", {"Content-Type": "application/json"})
    timeout = int(target.get("timeout", 10))
    verify_tls = bool(target.get("verify_tls", True))

    response = requests.post(
        url,
        headers=headers,
        json=payload,
        timeout=timeout,
        verify=verify_tls,
    )
    response.raise_for_status()
    print(f"Webhook delivered: HTTP {response.status_code} -> {url}")


def send_syslog(target: dict[str, Any], payload: dict[str, Any]) -> None:
    host = target["host"]
    port = int(target.get("port", 514))
    protocol = target.get("protocol", "udp").lower()
    facility_name = target.get("facility", "local0").lower()
    level_name = target.get("level", "info").lower()
    message_template = target.get("message", "{{ event_name }} {{ payload }}")

    variables = {
        "event_name": payload.get("event_name", "event"),
        "payload": json.dumps(payload, separators=(",", ":")),
        **payload,
    }
    message = render_value(message_template, variables)

    facility = getattr(logging.handlers.SysLogHandler, f"LOG_{facility_name.upper()}", None)
    if facility is None:
        raise ValueError(f"Unsupported syslog facility: {facility_name}")

    socktype = socket.SOCK_DGRAM if protocol == "udp" else socket.SOCK_STREAM
    handler = logging.handlers.SysLogHandler(
        address=(host, port),
        facility=facility,
        socktype=socktype,
    )

    logger = logging.getLogger(f"eda-event-generator-{time.time_ns()}")
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()
    logger.propagate = False
    logger.addHandler(handler)

    level = getattr(logging, level_name.upper(), logging.INFO)
    logger.log(level, message)
    handler.close()

    print(f"Syslog delivered via {protocol.upper()} -> {host}:{port}")


def send_snmp(target: dict[str, Any], payload: dict[str, Any]) -> None:
    if CommunityData is None:
        raise RuntimeError(
            "SNMP support requires pysnmp. Install dependencies with: "
            "python3 -m pip install -r requirements.txt"
        )

    host = target["host"]
    port = int(target.get("port", 162))
    community = target.get("community", "public")
    trap_oid = target["trap_oid"]

    varbinds = []
    for item in target.get("varbinds", []):
        oid = item["oid"]
        value_template = item.get("value", "")
        value = render_value(value_template, payload)
        varbinds.append(ObjectType(ObjectIdentity(oid), str(value)))

    iterator = sendNotification(
        SnmpEngine(),
        CommunityData(community, mpModel=1),
        UdpTransportTarget((host, port)),
        ContextData(),
        "trap",
        NotificationType(ObjectIdentity(trap_oid)).addVarBinds(*varbinds),
    )

    error_indication, error_status, error_index, _ = next(iterator)

    if error_indication:
        raise RuntimeError(str(error_indication))
    if error_status:
        raise RuntimeError(
            f"{error_status.prettyPrint()} at index {error_index}"
        )

    print(f"SNMP trap delivered -> {host}:{port}")


def send_kafka(target: dict[str, Any], payload: dict[str, Any]) -> None:
    if Producer is None:
        raise RuntimeError(
            "Kafka support requires confluent-kafka. Install dependencies with: "
            "python3 -m pip install -r requirements.txt"
        )

    servers = target["bootstrap_servers"]
    topic = target["topic"]
    key = str(payload.get(target.get("key_field", "hostname"), ""))

    producer = Producer({"bootstrap.servers": servers})

    delivery_errors: list[str] = []

    def delivery_callback(error, message):
        if error is not None:
            delivery_errors.append(str(error))
        else:
            print(
                f"Kafka event delivered -> {message.topic()} "
                f"partition={message.partition()} offset={message.offset()}"
            )

    producer.produce(
        topic=topic,
        key=key.encode("utf-8") if key else None,
        value=json.dumps(payload).encode("utf-8"),
        callback=delivery_callback,
    )
    producer.flush(10)

    if delivery_errors:
        raise RuntimeError("; ".join(delivery_errors))


SENDERS = {
    "webhook": send_webhook,
    "syslog": send_syslog,
    "snmp": send_snmp,
    "kafka": send_kafka,
}


def parse_overrides(items: list[str]) -> dict[str, str]:
    result: dict[str, str] = {}
    for item in items:
        if "=" not in item:
            raise ValueError(f"Override must be key=value: {item}")
        key, value = item.split("=", 1)
        result[key] = value
    return result


def list_events(config: dict[str, Any]) -> None:
    print("\nAvailable events:\n")
    for name, definition in config["events"].items():
        description = definition.get("description", "")
        transports = ", ".join(definition.get("targets", []))
        print(f"  {name:24} {description}")
        print(f"  {'':24} targets: {transports or 'none'}")
    print()


def choose_event(config: dict[str, Any]) -> str:
    names = list(config["events"])
    print("\nEDA Event Generator\n")
    for index, name in enumerate(names, start=1):
        description = config["events"][name].get("description", "")
        print(f"{index:2}. {name:24} {description}")

    while True:
        selected = input("\nSelect an event number: ").strip()
        try:
            number = int(selected)
            if 1 <= number <= len(names):
                return names[number - 1]
        except ValueError:
            pass
        print("Invalid selection.")


def run_event(
    config: dict[str, Any],
    event_name: str,
    target_filter: str | None,
    overrides: dict[str, str],
    dry_run: bool,
) -> None:
    definition = config["events"].get(event_name)
    if definition is None:
        raise KeyError(f"Unknown event: {event_name}")

    payload = build_event(event_name, definition, overrides)

    print("\nGenerated payload:")
    print(json.dumps(payload, indent=2))

    if dry_run:
        print("\nDry run enabled. No event was sent.")
        return

    target_names = definition.get("targets", [])
    if target_filter:
        target_names = [name for name in target_names if name == target_filter]

    if not target_names:
        raise ValueError("No matching targets configured for this event.")

    for target_name in target_names:
        target = config.get("targets", {}).get(target_name)
        if target is None:
            raise KeyError(f"Target '{target_name}' is not defined.")

        transport = target.get("type")
        sender = SENDERS.get(transport)
        if sender is None:
            raise ValueError(f"Unsupported target type: {transport}")

        sender(target, payload)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Generate events for Event-Driven Ansible demonstrations."
    )
    parser.add_argument(
        "-c",
        "--config",
        type=Path,
        default=DEFAULT_CONFIG,
        help="Path to the YAML configuration file.",
    )
    parser.add_argument(
        "-e",
        "--event",
        help="Event name to generate. Omit for an interactive menu.",
    )
    parser.add_argument(
        "-t",
        "--target",
        help="Send only to this named target.",
    )
    parser.add_argument(
        "--set",
        dest="overrides",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="Override an event variable. May be used multiple times.",
    )
    parser.add_argument(
        "--repeat",
        type=int,
        default=1,
        help="Number of times to send the event.",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=1.0,
        help="Seconds between repeated events.",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="List configured events and exit.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show the payload without sending it.",
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    try:
        config = load_config(args.config)

        if args.list:
            list_events(config)
            return 0

        event_name = args.event or choose_event(config)
        overrides = parse_overrides(args.overrides)

        for index in range(args.repeat):
            if args.repeat > 1:
                print(f"\n--- Event {index + 1} of {args.repeat} ---")
            run_event(
                config=config,
                event_name=event_name,
                target_filter=args.target,
                overrides=overrides,
                dry_run=args.dry_run,
            )
            if index < args.repeat - 1:
                time.sleep(args.interval)

        return 0

    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
