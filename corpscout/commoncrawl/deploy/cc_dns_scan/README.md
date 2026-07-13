# Common Crawl DNS scanner deployment

This package deploys only the `cc-dns-scan` project to the `cc_dns_scan` Ansible host group. It:

- builds `../../cc-dns-scan/cmd/cc-dns-scan` for Linux/AMD64 on the control machine;
- installs the binary and `cc-dns-scan.service` under
  `/opt/companycollect/corpscout/commoncrawl/cc-dns-scan`;
- applies DNS host tuning and configures the local Unbound resolver; and
- leaves `cc-dns-scan.service` stopped and disabled after every deployment.

The scanner continues to use `/opt/companycollect/corpscout/commoncrawl/cc-dns-worker` as its state
directory. Keeping `dns-cycle-state.json` and `dns-scan-*.db` there lets the new DNS-only process
resume the existing scan without copying or renaming state.

## Prerequisites

- The target host is reachable over SSH from the control machine.
- The target can reach the ClickHouse host over the configured private network.
- The Corpscout ClickHouse migrations have already been applied.
- Ansible and Go are installed on the control machine, with `cc-dns-scan` checked out beside the
  `deploy` directory.

All roles use modules included with Ansible. No Ansible Galaxy collection is required.

## One-time vault setup

Secrets are encrypted in `group_vars/cc_dns_scan/vault.yml`. Ansible reads the vault password from
`~/.config/ansible/cc-dns-scan`, as configured in `ansible.cfg`.

Create that external password file once:

```bash
mkdir -p ~/.config/ansible
openssl rand -base64 48 > ~/.config/ansible/cc-dns-scan
chmod 600 ~/.config/ansible/cc-dns-scan
```

To create or replace the encrypted ClickHouse value:

```bash
cd corpscout/commoncrawl/deploy/cc_dns_scan
printf 'vault_clickhouse_password: "<real password>"\n' > group_vars/cc_dns_scan/vault.yml
ansible-vault encrypt group_vars/cc_dns_scan/vault.yml
```

Inspect or rotate it with:

```bash
ansible-vault view group_vars/cc_dns_scan/vault.yml
ansible-vault rekey group_vars/cc_dns_scan/vault.yml
```

## First split-project cutover

Run this DNS package before the standalone AXFR deployment package. The existing service is the
legacy combined DNS/AXFR process and has the same `cc-dns-scan.service` name. This playbook stops it,
replaces its unit and binary path with the DNS-only service, and leaves the replacement stopped and
disabled. Running DNS first therefore removes the legacy AXFR process before standalone AXFR can be
deployed safely.

Review a dry run, then deploy:

```bash
cd corpscout/commoncrawl/deploy/cc_dns_scan
ansible-playbook site.yml --check --diff
ansible-playbook site.yml
```

The local Go build still runs during `--check` so Ansible can evaluate the binary copy task.

Verify that the installed unit points to the new project and that no legacy combined worker remains:

```bash
ssh root@hetzner01 'systemctl show cc-dns-scan -p ExecStart'
ssh root@hetzner01 'systemctl is-active cc-dns-scan; systemctl is-enabled cc-dns-scan'
ssh root@hetzner01 'pgrep -af "cc-dns-worker|cc-dns-scan" || true'
ssh root@hetzner01 'ls -la /opt/companycollect/corpscout/commoncrawl/cc-dns-worker'
```

After verifying the installed unit, enable and start the DNS-only scanner, then follow its resumed
cycle:

```bash
ssh root@hetzner01 'systemctl enable --now cc-dns-scan'
ssh root@hetzner01 'journalctl -u cc-dns-scan -n 100 -f'
```

The standalone AXFR package can then be deployed independently. Its playbook also leaves the service
stopped and disabled; activate and follow the new unit with:

```bash
ssh root@hetzner01 'systemctl enable --now cc-dns-axfr'
ssh root@hetzner01 'journalctl -u cc-dns-axfr -n 100 -f'
```

`cc-axfr-scan` is obsolete and is not the unit to use for AXFR logs.

## Later deployments

Run this package directly; its playbook contains no AXFR role or configuration:

```bash
cd corpscout/commoncrawl/deploy/cc_dns_scan
ansible-playbook site.yml
```

Each deployment stops and disables `cc-dns-scan.service`. Validate the installed unit, then run the
same `systemctl enable --now` and `journalctl` commands above.

## Rollback

1. Stop and disable the DNS-only service:

   ```bash
   ssh root@hetzner01 'systemctl disable --now cc-dns-scan'
   ```

2. Stop the standalone `cc-dns-axfr.service` if it has already been deployed. The restored combined
   process and standalone AXFR must never run concurrently.
3. Restore the previous combined `cc-dns-scan.service` and `cc-dns-worker` binary from the prior
   release or repository revision.
4. Confirm standalone AXFR is stopped, then start the restored combined service.

The compatibility state directory remains unchanged during rollback.

## Layout

```text
deploy/cc_dns_scan/
├── ansible.cfg
├── inventory.ini
├── site.yml
├── group_vars/cc_dns_scan/
│   ├── vars.yml
│   └── vault.yml
└── roles/
    ├── cc_dns_scan/
    ├── os_tuning/
    └── unbound/
```
