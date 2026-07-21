# EDA Event Generator

A reusable command-line utility for generating demonstration events for
Event-Driven Ansible.

## Supported delivery methods

- HTTP webhook
- UDP or TCP syslog
- SNMP v2c traps
- Kafka

## Installation

```bash
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install --upgrade pip
python3 -m pip install -r requirements.txt
```

For webhook-only or syslog-only demos, the optional Kafka and SNMP libraries
can be removed from `requirements.txt`.

## Configure the targets

Edit `events.yml`.

The default webhook target is:

```yaml
eda_webhook:
  type: webhook
  url: http://127.0.0.1:5000/endpoint
```

Change the hostname or IP address to the system running the activation or
local `ansible-rulebook` process.

## Start the sample rulebook

Install the EDA collection:

```bash
ansible-galaxy collection install ansible.eda
```

Run:

```bash
ansible-rulebook \
  --rulebook rulebooks/demo-rulebook.yml \
  --inventory localhost, \
  --verbose
```

## Interactive mode

```bash
python3 eda_event_generator.py
```

## List available events

```bash
python3 eda_event_generator.py --list
```

## Generate one event

```bash
python3 eda_event_generator.py \
  --event cisco_interface_down
```

## Preview without sending

```bash
python3 eda_event_generator.py \
  --event palo_alto_threat \
  --dry-run
```

## Override event values

```bash
python3 eda_event_generator.py \
  --event cisco_interface_down \
  --set hostname=branch-switch-23 \
  --set interface=GigabitEthernet1/0/48
```

## Send only to one target

```bash
python3 eda_event_generator.py \
  --event high_cpu \
  --target eda_webhook
```

## Generate an event storm

```bash
python3 eda_event_generator.py \
  --event failed_login \
  --repeat 50 \
  --interval 0.25
```

## Add a custom event

Add another entry under `events` in `events.yml`:

```yaml
events:
  disk_full:
    description: Linux filesystem exceeds threshold
    targets:
      - eda_webhook
    variables:
      hostname: linux01
      filesystem: /var
      usage_percent: "95"
    payload:
      hostname: "{{ hostname }}"
      event_type: disk_full
      filesystem: "{{ filesystem }}"
      usage_percent: "{{ usage_percent }}"
      severity: critical
      message: "{{ filesystem }} is {{ usage_percent }} percent full"
      timestamp: "{{ timestamp }}"
```

## AAP activation notes

For an AAP EDA activation:

1. Store this project in Git.
2. Create an EDA project that points to the repository.
3. Create a rulebook activation using the rulebook.
4. Make sure TCP port 5000 is reachable from the event generator.
5. Update the webhook URL in `events.yml` to the activation endpoint.

For production designs, use TLS, authentication, a supported event source,
and an intermediary such as Kafka, an observability platform, or an API gateway.
This utility is intended for demonstrations and workshops.
