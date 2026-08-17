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

ansible-playbook sync.yml --check --diff
ansible-playbook sync.yml
```

`sync.yml` is the canonical full deployment playbook. The previous `site.yml`
entry point remains as a compatibility alias and runs the same full deployment.

The controller needs `uv`, Ansible, and rsync. The target needs systemd,
rsync, `lsof`, `ss`, executable `/root/.local/bin/uv`, an APT-compatible Linux
package manager, outbound HTTPS access, several GiB of free disk, and a manually
provisioned `.env`. The role checks these prerequisites before stopping the
running process. When the pinned libpostal runtime or parser model is absent,
the role builds it from its checksum-verified source archive while Dagster stays
online, then installs and verifies it inside the normal active-run-gated
deployment. After synchronizing the Python environment, the role verifies the
pypostal binding against a Swedish apartment address, then installs and verifies
the Chromium shared libraries required by CloakBrowser; the
browser binary itself remains managed by CloakBrowser under
`/root/.cloakbrowser`.

## Hot-sync asset code without restarting active runs

For Python asset and definition changes that do not modify dependencies, the
lockfile, the systemd unit, scripts, or deployment configuration, use the
content-only playbook:

```bash
cd services/dagster_v3/ansible
export LC_ALL=en_US.UTF-8
export LANG=en_US.UTF-8

ansible-playbook light_sync.yml --check --diff
ansible-playbook light_sync.yml
```

`light_sync.yml` validates the local definitions, rsyncs only
`src/dagster_v3/`, and validates the synchronized definitions on the host. It
does not run `uv sync`,
touch the systemd unit, query or block active runs, or stop/restart Dagster. The
running `dg dev` supervisor observes the source changes and reloads its code
location. The playbook explicitly reloads only the `dagster_v3` code location,
waits for it to become healthy, and asserts that the systemd `MainPID` and
restart count stayed unchanged.

The Pythonic dbt definitions read build-time manifests from their project
`target/manifest.json` files. The dbt components read generated projects from
`src/dagster_v3/defs/.local_defs_state/`. Runtime definition loads never
generate or refresh these shared artifacts. After changing a dbt project,
prepare every affected artifact in the local release tree before deployment:

```bash
cd services/dagster_v3
uv run --frozen --no-sync dbt parse \
  --project-dir src/dagster_v3/defs/finland_ytj/dbt \
  --profiles-dir src/dagster_v3/defs/finland_ytj/dbt
uv run --frozen --no-sync dbt parse \
  --project-dir src/dagster_v3/defs/exchange_rates_v2/dbt \
  --profiles-dir src/dagster_v3/defs/exchange_rates_v2/dbt
uv run --frozen --no-sync dg utils refresh-defs-state
```

Run only the commands for projects whose dbt sources changed, plus the component
refresh when a component project changed. Do not generate or refresh dbt state
against the live server checkout. Both deploy paths require every generated dbt
project and manifest before validation, then promote the complete definitions
tree as one release. They ship the root manifests but omit dbt invocation
directories and logs.

Use the full `sync.yml` deployment whenever `pyproject.toml`, `uv.lock`, `.env`,
the Ansible role, service configuration, or non-package runtime files changed.
The hot-sync preserves already-running processes, but code loaded by a future
step or subprocess may use the new implementation; avoid changing the contract
of an in-flight multi-step job.

Before a stopped deployment, the role:

1. previews rsync and checks the target virtual environment;
2. runs `uv run --frozen --no-sync dg check defs` locally when source changed;
3. applies a best-effort gate for `STARTING` or `STARTED` Dagster runs
   immediately before shutdown;
4. gracefully retires only the exact root tmux session `dagster`, if present;
5. installs a prepared libpostal build when required, rsyncs source without
   touching runtime state, runs frozen `uv sync`, installs the
   CloakBrowser/Chromium system libraries, and validates the synchronized
   definitions; and
6. starts systemd and verifies the webserver, `dagster_v3` code location,
   daemon heartbeats, listener address, and service cgroup.

Queued runs remain in Postgres and resume under the daemon. Runs already in
`CANCELING` are reported but not rewritten or relaunched. A no-op playbook run
does not stop or restart Dagster.

The remote `.env` checksum is embedded in the rendered unit, so rerunning
Ansible after a server-side environment edit validates and restarts the
service. For a restart with unchanged files, use
`ansible-playbook sync.yml -e dagster_force_restart=true`.

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
