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

## Authenticate to EDA with an OAuth token

The webhook target and the sample rulebook are configured to require an
OAuth/bearer token so EDA only accepts authenticated events. The generator
sends the token as an HTTP header:

```
Authorization: Bearer <token>
```

and the `ansible.eda.webhook` source rejects any request that is missing or
carries the wrong token with `HTTP 401`.

The token is **never stored in git**. Both sides read it from the
`EDA_WEBHOOK_TOKEN` environment variable, defined once in `events.yml`:

```yaml
eda_webhook:
  type: webhook
  url: http://127.0.0.1:5000/endpoint
  auth:
    type: bearer
    token_env: EDA_WEBHOOK_TOKEN
```

### Local testing

Export the same token in both terminals:

```bash
export EDA_WEBHOOK_TOKEN='choose-a-long-random-value'
```

Start the rulebook, importing the variable so `{{ EDA_WEBHOOK_TOKEN }}`
resolves:

```bash
ansible-rulebook \
  --rulebook rulebooks/demo-rulebook.yml \
  --inventory localhost, \
  --env-vars EDA_WEBHOOK_TOKEN \
  --verbose
```

Then send an authenticated event:

```bash
python3 eda_event_generator.py --event cisco_interface_down
```

You can also pass the token explicitly instead of exporting it. `--token`
overrides the environment variable:

```bash
python3 eda_event_generator.py --event high_cpu --token 'choose-a-long-random-value'
```

### Token precedence (generator side)

1. `--token` command-line flag
2. the environment variable named by `auth.token_env` (recommended)
3. an inline `auth.token` value in `events.yml` (local testing only — keep it
   out of git)

If an `auth` block is present but no token can be resolved, the generator
fails loudly rather than sending an unauthenticated request.

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
6. Set the same `EDA_WEBHOOK_TOKEN` value on the activation (so the rulebook's
   `{{ EDA_WEBHOOK_TOKEN }}` resolves) and in the shell running the event
   generator CLI.

For production designs, use TLS, authentication, a supported event source,
and an intermediary such as Kafka, an observability platform, or an API gateway.
This utility is intended for demonstrations and workshops.
