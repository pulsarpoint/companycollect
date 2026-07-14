# Dagster v3 Ansible development deployment

This package keeps the current host-mode Dagster development topology while
giving it repeatable deployment and systemd supervision. It rsyncs the local
`services/dagster_v3` source tree to the existing stable path
`/opt/companycollect/corpscout/dagster_v3` on `dagster`, reconciles its Linux
environment with `uv sync --frozen`, then runs `.venv/bin/dg dev` as
`corpscout-dagster-dev.service`.

There is no `dev_run_script` entry point in this project. The supported
development launcher is:

```bash
uv run scripts/dagster-dev.sh -h 0.0.0.0
```

Systemd invokes the lock-synchronized `dg` binary directly so service restarts
cannot mutate the environment and signals reach the Dagster supervisor. This
is the same development webserver-and-daemon topology as the wrapper. Its pool
overflow is the `dagster_db_pool_max_overflow` Ansible variable (default 100);
the wrapper's `DAGSTER_DB_POOL_MAX_OVERFLOW` remains for manual local runs.

The default inventory connects to `dagster` as `graovic` and uses passwordless
sudo for the root-owned deployment tree and systemd unit.

## What rsync preserves

The role uses `--delete-after`, but an audited exclusion file protects all
host-specific and runtime state, notably:

- `.env` and `.venv`;
- `data/` (currently about 259 GiB);
- `storage/`, `logs/`, `.logs_queue/`, and `.dagster_home/`;
- `scripts/data/` and its legacy DuckDB queue;
- dlt, dbt, Python, test, telemetry, and tool caches.

The remote `.env` is never copied from the workstation. Ansible requires it to
exist, preserves its checksum across rsync, and restricts it to root mode
`0600`. This also preserves the live PgBouncer-backed Dagster metadata DSN.
The Linux `.venv` is synchronized from the committed `uv.lock` with
`uv sync --frozen`; the dev dependency group remains installed because `dg`
and `dagster-webserver` are the runtime for this transitional deployment.

## Deploy

The current macOS control environment needs a valid UTF-8 locale for Ansible:

```bash
cd services/dagster_v3/ansible
export LC_ALL=en_US.UTF-8
export LANG=en_US.UTF-8

ansible-playbook site.yml --check --diff
ansible-playbook site.yml
```

The controller needs `uv`, Ansible, and rsync. The target needs systemd,
rsync, `lsof`, `ss`, executable `/root/.local/bin/uv`, and a manually
provisioned `.env`. The role checks these prerequisites before stopping the
running process.

Before a stopped deployment, the role:

1. previews rsync and checks the target virtual environment;
2. runs `uv run --frozen --no-sync dg check defs` locally when source changed;
3. applies a best-effort gate for `STARTING` or `STARTED` Dagster runs
   immediately before shutdown;
4. gracefully retires only the exact root tmux session `dagster`, if present;
5. rsyncs source without touching runtime state, runs frozen `uv sync`, and
   validates the synchronized definitions; and
6. starts systemd and verifies the webserver, `dagster_v3` code location,
   daemon heartbeats, listener address, and service cgroup.

Queued runs remain in Postgres and resume under the daemon. Runs already in
`CANCELING` are reported but not rewritten or relaunched. A no-op playbook run
does not stop or restart Dagster.

The remote `.env` checksum is embedded in the rendered unit, so rerunning
Ansible after a server-side environment edit validates and restarts the
service. For a restart with unchanged files, use
`ansible-playbook site.yml -e dagster_force_restart=true`.

For a busy deployment, pause new launches or schedules before running the
playbook; the GraphQL run check and process shutdown cannot be one atomic
operation.

## Operations

```bash
ssh dagster 'sudo systemctl status corpscout-dagster-dev --no-pager'
ssh dagster 'sudo journalctl -u corpscout-dagster-dev -n 200 -f'
ssh dagster 'sudo systemctl restart corpscout-dagster-dev'
```

The UI intentionally retains the current unauthenticated
`0.0.0.0:3000` development binding. Access should remain restricted by the
host firewall/Tailscale network.

## Deliberate production gaps

This is not the final Dagster deployment model. It intentionally keeps `dg
dev`, an in-place mutable checkout, local compute logs, and the existing root
runtime identity. Root parity avoids recursively changing ownership across
more than 260 GiB of live DuckDB and compute state, plus root-scoped uv/Python
and dlt caches, during this migration.

A production design should split webserver, daemon, and code server; use an
unprivileged service identity and explicit runtime volumes; build an immutable
artifact or container; put authentication/reverse proxying in front of the UI;
and move compute logs to durable object storage.
