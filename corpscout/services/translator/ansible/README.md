# Translator Ansible deployment

This package builds and deploys `translator-api` as a system-level systemd
service. The default inventory connects to `dagster` over SSH as `graovic`,
uses passwordless sudo for system changes, and runs the service itself as the
dedicated unprivileged `translator` account.

The deployment installs:

- `/opt/companycollect/corpscout/translator/bin/translator-api`;
- `/etc/corpscout-translator/translator.json`;
- `/etc/corpscout-translator/translator.env` (root-only);
- `/etc/systemd/system/translator-api.service`; and
- the preserved SQLite queue under
  `/opt/companycollect/corpscout/translator/data/translator`.

The canonical `../config/translator.json` is copied without creating a second
JSON configuration model. Ansible supplies only deployment-specific overrides:
the localhost API binding, the Temporal address, the absolute config path, and
the ClickHouse password.

## Prerequisites

- `dagster` resolves through SSH configuration and is reachable as `graovic`.
- `graovic` has passwordless sudo on the target.
- The control machine has Ansible and Go 1.26.1 or newer.
- `../dagster_v3/.env` contains the working `CLICKHOUSE_PASSWORD`.
- ClickHouse, Temporal, and the configured OpenAI-compatible endpoint are
  reachable from `dagster`.

Only Ansible built-in modules are used; no Galaxy collection is required.

## Deploy

Export the ClickHouse password into the Ansible process, review a dry run, and
then deploy. The command below reads only that key; the complete Dagster
`.env` is not loaded into the deployment process.

```bash
cd translator/ansible
export CLICKHOUSE_PASSWORD="$(sed -n 's/^CLICKHOUSE_PASSWORD=//p' ../../dagster_v3/.env | tail -n 1)"
export LC_ALL=en_US.UTF-8
export LANG=en_US.UTF-8

ansible-playbook site.yml --check --diff
ansible-playbook site.yml
```

The locale exports avoid Ansible's unsupported `C.UTF-8` startup error in the
current macOS control environment.

The dry run still executes the local Go tests and Linux/AMD64 build, but it
does not stop or start anything on `dagster`. On a completely new target it
predicts creation of the account and directories; artifact copies that need
those not-yet-created directories occur during the real run.

The playbook refuses to continue before any service cutover when
`CLICKHOUSE_PASSWORD` is absent or contains control characters. The password
is written only to the target's root-owned `0600` environment file; the task
uses both Ansible output suppression and disabled template diffs so the value
does not appear in deployment output.

## First deployment from tmux

The existing manual service runs in the root tmux session named `translator`.
On the first real deployment, after the binary, configuration, environment,
and unit are ready, the playbook:

1. confirms the tmux process working directory, configuration, and SQLite
   queue path match the systemd deployment;
2. captures the current queue state and sends `Ctrl-C` so the Go process
   receives its normal graceful shutdown signal;
3. waits for that exact process to exit, including deferred Temporal and
   SQLite cleanup, before removing only the `translator` tmux session;
4. preserves, re-owns, and makes private the complete SQLite queue directory,
   including WAL, SHM, and failed-item state; and
5. enables and starts `translator-api.service`, then proves the unit owns port
   8080 and still exposes the preserved queue state.

It never kills the tmux server or the separate `dagster` session. Every run
checks the named legacy session and rejects any remaining `translator-api`
process that is not the systemd unit's `MainPID`, making retries safe after a
partially completed deployment.

## Operations

The API listens only on `127.0.0.1:8080`, matching Dagster's default
`TRANSLATOR_API_URL`. Inspect the service and its JSON logs with:

```bash
ssh dagster 'sudo systemctl status translator-api --no-pager'
ssh dagster 'sudo journalctl -u translator-api -n 100 -f'
```

Check the API and queue locally on the target:

```bash
ssh dagster 'curl -fsS http://127.0.0.1:8080/healthz'
ssh dagster 'curl -fsS http://127.0.0.1:8080/v1/queue/stats'
```

Each subsequent playbook run tests and rebuilds the local source, copies only
changed artifacts, and restarts the service once when the binary,
configuration, environment, or unit changed.

To stop or restart it explicitly:

```bash
ssh dagster 'sudo systemctl stop translator-api'
ssh dagster 'sudo systemctl restart translator-api'
```
