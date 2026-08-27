# Ratsit process deployment

This playbook owns the single `ratsit-process.service` systemd user unit. It
deploys the Python 3.14 crawler, installs CloakBrowser and Chromium runtime
dependencies, copies protected runtime configuration, and runs the service as
UID 1000 in the active graphical session.

The process launches all configured browsers itself. There is no CDP server,
CDP port, or separate browser deployment.

## Prerequisites

- Linux on x86-64 or arm64.
- `graovic` is UID 1000, or all service-user variables are changed together.
- The UID 1000 systemd user manager has an active `graphical-session.target`,
  `DISPLAY`, and `XAUTHORITY`.
- `uv` is available in the SSH user's login environment.
- The deployment user has passwordless sudo.
- The host can reach Temporal, S3/RustFS, and ClickHouse.
- The ClickHouse Ratsit result migration is applied.

## Configure

```shell
cd crawler_ratsit/ansible
cp inventory.example.ini inventory.ini
cp worker-environment.example worker-environment
cp process-config.toml.example process-config.toml
chmod 0600 process-config.toml
$EDITOR inventory.ini worker-environment process-config.toml
```

`worker-environment` contains service credentials and connection settings.
`process-config.toml` contains browser contexts, optional fixed proxy URLs, and
both Temporal activity rates. Both installed files use mode `0600`; the TOML
source must also be private or validation fails.

Omit `proxy_url` for a direct browser. Add one `[[browsers]]` entry per proxy.
Every entry receives a unique persistent profile beneath
`/var/lib/ratsit-process`.

## Deploy

```shell
export LC_ALL=en_US.UTF-8
export LANG=en_US.UTF-8
ansible-playbook site.yml --syntax-check
ansible-playbook site.yml --check --diff
ansible-playbook site.yml
```

To deploy code and configuration while preventing all Temporal polling, enable
maintenance mode for both the check and real run:

```shell
ansible-playbook site.yml --check --diff \
  -e crawler_ratsit_maintenance_mode=true
ansible-playbook site.yml \
  -e crawler_ratsit_maintenance_mode=true
```

Maintenance mode stops and disables `ratsit-process` after applying changes,
including when a changed file notifies the restart handler. A later normal
deployment starts and enables the unit again.

Before changing the host, the role validates both configuration files and runs
the non-integration test suite. During cutover it stops, disables, and removes
the former UID 1000 `ratsit-worker` and `ratsit-cdp` user units, then enables
`ratsit-process` under `graphical-session.target`. Existing old browser state is
left in place and can be removed separately after the new service is verified.

## Operations

Because this is a user unit, query it through the UID 1000 user manager:

```shell
ssh graovic@192.168.88.149 \
  'XDG_RUNTIME_DIR=/run/user/1000 systemctl --user status ratsit-process --no-pager'

ssh graovic@192.168.88.149 \
  'XDG_RUNTIME_DIR=/run/user/1000 journalctl --user -u ratsit-process -f -o short-iso'
```

Useful filtered views on the remote host:

```shell
# Completed crawls during the last hour.
XDG_RUNTIME_DIR=/run/user/1000 \
  journalctl --user -u ratsit-process --since "1 hour ago" --no-pager \
    -g 'event=ratsit_crawl_completed' -o cat

# Rate limits during the current day.
XDG_RUNTIME_DIR=/run/user/1000 \
  journalctl --user -u ratsit-process --since today --no-pager \
    -g 'event=ratsit_crawl_rate_limited' -o cat
```

From a root shell, run the same commands through the service user:

```shell
sudo -u graovic env XDG_RUNTIME_DIR=/run/user/1000 \
  journalctl --user -u ratsit-process -f -o short-iso
```

Crawl events use searchable `key=value` fields for browser ID, hashed identity
reference and kind, batch ID, outcome, HTTP status, content bytes, duration,
attempt, S3 disposition, and Temporal run ID. Full identity numbers, workflow
IDs containing identity numbers, proxy URLs, and credentials are not logged.
