# Ratsit Temporal worker deployment

This playbook owns only `ratsit-worker.service`. It deploys the crawler source,
an independent locked Python 3.14 environment, protected runtime configuration,
and a systemd user unit. It does not install or manage CloakBrowser.

The worker defaults to UID 1000 user `graovic`, matching the current CDP host,
but has its own inventory and service variables. Its unit is enabled under
`default.target`, not `graphical-session.target`, so the worker does not require
a desktop session. Ansible enables systemd lingering for the service user.

## Prerequisites

- The target is Linux on x86-64 or arm64.
- `graovic` is UID 1000; change the worker variables together if production
  uses another service account.
- `uv` is available in the target SSH user's login environment. The playbook
  discovers it automatically; `crawler_ratsit_uv_binary` can override the path.
- The SSH deployment user has passwordless sudo.
- The target can reach CDP, Temporal, S3/RustFS, and ClickHouse.
- The ClickHouse migration for `se_company_ratsit_crawl_results` is applied.

Only Ansible built-in modules are used.

## Configure

Create the independent inventory. For the current same-host deployment, use
the same address and SSH user as the CDP server inventory:

```bash
cd crawler_ratsit/ansible
cp inventory.example.ini inventory.ini
```

Create the ignored environment file and replace every placeholder:

```bash
cp worker-environment.example worker-environment
$EDITOR inventory.ini worker-environment
```

For the current topology, keep:

```dotenv
RATSIT_CDP_URL=http://127.0.0.1:9222
```

When the worker moves to another host, change only this worker inventory and
`RATSIT_CDP_URL`. The CDP endpoint must remain protected; use a private tunnel
or private network policy rather than exposing Chrome DevTools publicly.

## Deploy

```bash
export LC_ALL=en_US.UTF-8
export LANG=en_US.UTF-8
ansible-playbook site.yml --syntax-check
ansible-playbook site.yml --check --diff
ansible-playbook site.yml
```

The playbook validates the environment through `WorkerSettings`, rejects
placeholder credentials, copies it to
`~/.config/crawler-ratsit/environment` with mode `0600`, runs the worker tests,
installs the worker in `/opt/companycollect/corpscout/crawler-ratsit`, and
requires the Temporal worker to remain active after deployment.

## Operations

```bash
ssh ratsit-worker 'systemctl --user status ratsit-worker --no-pager'
ssh ratsit-worker 'journalctl --user -u ratsit-worker -n 100 -f'
```

Re-run only this worker playbook after changing crawler code or
`worker-environment`. Browser deployments and restarts remain independent.
