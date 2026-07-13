# cc-dns-axfr Ansible deployment

This package deploys only the Common Crawl AXFR scanner to the `cc_dns_axfr` inventory group. It
builds `../../cc-dns-axfr/cmd/cc-dns-axfr` on the control machine and installs:

- the binary under `/opt/companycollect/corpscout/commoncrawl/cc-dns-axfr/bin`;
- the ClickHouse environment file at
  `/opt/companycollect/corpscout/commoncrawl/cc-dns-axfr/.env`;
- `cc-dns-axfr.service`.

The scanner uses `/opt/companycollect/corpscout/commoncrawl/cc-dns-worker` only as a compatibility
state directory. Existing `axfr-cycle-state.json`, `axfr-scan-*.db`, and SQLite WAL files remain in
place, allowing an interrupted AXFR cycle to resume. The service runs 500 workers with a claim batch
of 1000.

This package does not deploy or operate the DNS scanner, Unbound, or host OS tuning. It uses only
Ansible built-in modules and has no collection dependencies.

## Prerequisites

- The control machine has Ansible and Go installed and this repository checked out.
- `hetzner01` is reachable over Tailscale/SSH and can reach the ClickHouse host `companycollect`.
- The required ClickHouse migrations have been applied.
- The independent DNS package at `../cc_dns_scan` has completed the initial cutover.
- No legacy combined `cc-dns-worker` process is running.

The final two conditions are important: the legacy worker and `cc-dns-axfr` would otherwise operate
the same AXFR state and workload. A read-only preflight checks the exact process name
`cc-dns-worker` and stops this play before it changes the host if the legacy process is active. The
preflight does not stop or alter `cc-dns-scan.service`.

## Vault

The preserved encrypted values live in `group_vars/cc_dns_axfr/vault.yml`. This package deliberately
reuses the existing external vault password file configured by the DNS deployment:

```text
~/.config/ansible/cc-dns-scan
```

Do not create a second password file or re-encrypt the copied vault for this package. Confirm access
before deploying:

```bash
cd corpscout/commoncrawl/deploy/cc_dns_axfr
ansible-vault view group_vars/cc_dns_axfr/vault.yml
```

To rotate the shared vault password, coordinate the change with the DNS package and rekey both
encrypted vault files.

## First deployment

Deploy the DNS package first. Then enter this package and perform a dry run:

```bash
cd corpscout/commoncrawl/deploy/cc_dns_axfr
ansible-playbook site.yml --check --diff
```

Deploy AXFR:

```bash
ansible-playbook site.yml
```

The play stops the current `cc-dns-axfr.service` during replacement and also stops, disables, and
removes the stale interim `cc-axfr-scan.service` if present. It never stops or changes
`cc-dns-scan.service`.

Deployment intentionally leaves `cc-dns-axfr.service` stopped and disabled. Verify the unit and
compatibility state before starting it:

```bash
ssh root@hetzner01 'systemctl status cc-dns-axfr --no-pager || true'
ssh root@hetzner01 'ls -la /opt/companycollect/corpscout/commoncrawl/cc-dns-worker'
```

After verifying the installed unit, enable and start it, then follow the expected resumed AXFR cycle:

```bash
ssh root@hetzner01 'systemctl enable --now cc-dns-axfr'
ssh root@hetzner01 'journalctl -u cc-dns-axfr -n 100 -f'
```

Use the same dry-run and deploy commands for later AXFR releases. Each deployment again leaves the
service stopped and disabled, requiring the same explicit `systemctl enable --now` after verification.
`cc-axfr-scan` is the obsolete pre-split unit name and is wrong for standalone AXFR logs; use
`journalctl -u cc-dns-axfr`.

## Rollback

To stop the independent AXFR scanner without affecting DNS:

```bash
ssh root@hetzner01 'systemctl disable --now cc-dns-axfr'
```

To restore an earlier independent AXFR release, check out the desired source and deployment revision,
rerun this playbook, verify the logs, and explicitly start and enable the service.

To roll all the way back to the former combined worker, first stop and disable both split services,
restore the previous combined `cc-dns-scan.service` and `cc-dns-worker` binary, and start only that
combined service. Never run the combined worker and `cc-dns-axfr` concurrently. The compatibility
state directory does not need to be copied or renamed for either rollback path.

## Layout

```text
deploy/cc_dns_axfr/
├── ansible.cfg
├── inventory.ini
├── site.yml
├── group_vars/cc_dns_axfr/
│   ├── vars.yml
│   └── vault.yml
└── roles/cc_dns_axfr/
    ├── tasks/main.yml
    └── templates/
        ├── cc-dns-axfr.service.j2
        └── env.j2
```
