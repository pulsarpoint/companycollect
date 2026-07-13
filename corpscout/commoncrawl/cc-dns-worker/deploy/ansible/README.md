# Common Crawl DNS workers Ansible deploy

Deploys two independent binaries and services to `hetzner01`:

- `cc_dns_worker` installs the DNS-only `cc-dns-worker` and `cc-dns-scan.service`.
- `cc_axfr_worker` installs `cc-axfr-worker` and `cc-axfr-scan.service`.

The play also applies OS tuning and configures local Unbound for the DNS worker. The services share
ClickHouse credentials and a deployment directory, but they have separate processes, flags, state
files, SQLite databases, and restart lifecycles.

## Prerequisites

- The target host (`hetzner01`) is reachable over Tailscale/ssh from the control
  machine, and can in turn reach `companycollect` (the ClickHouse host) on its
  Tailscale network.
- ClickHouse migrations for `corpscout` have already been applied on
  `companycollect`.
- The control machine has:
  - Ansible installed (`ansible-playbook`, `ansible-vault`).
  - Go installed (used to build both worker binaries before they are shipped to the target).
  - This repository checked out.
  - Required Ansible collections installed:

    ```bash
    cd deploy/ansible
    ansible-galaxy collection install -r requirements.yml
    ```

## One-time vault setup

Secrets (currently just the ClickHouse password) are stored encrypted in
`group_vars/cc_dns/vault.yml` and decrypted automatically via a vault password
file so that runs never prompt interactively.

1. Generate the vault password file (once, outside the repo):

   ```bash
   mkdir -p ~/.config/ansible
   openssl rand -base64 48 > ~/.config/ansible/cc-dns-scan
   chmod 600 ~/.config/ansible/cc-dns-scan
   ```

2. Encrypt the ClickHouse password into `vault.yml`:

   ```bash
   cd deploy/ansible
   printf 'vault_clickhouse_password: "<real password>"\n' > group_vars/cc_dns/vault.yml
   ansible-vault encrypt group_vars/cc_dns/vault.yml   # uses vault_password_file from ansible.cfg
   ```

   The encrypted `vault.yml` is safe to commit. The password file at
   `~/.config/ansible/cc-dns-scan` must never be committed — it lives outside
   the repo.

3. To rotate the vault password: re-run `openssl rand` into the file, then
   `ansible-vault rekey group_vars/cc_dns/vault.yml`.

To inspect the decrypted contents at any time:

```bash
ansible-vault view group_vars/cc_dns/vault.yml
```

## Running

### Deployment

```bash
cd deploy/ansible
ansible-playbook site.yml
```

The first split-service deployment must use the complete play. `cc_dns_worker` runs first, stopping
the legacy combined `cc-dns-scan.service` before its binary is replaced with the DNS-only build. The
AXFR role then installs its binary and unit. Both services are left stopped and disabled.

No state migration is needed. The DNS service resumes `dns-cycle-state.json`/`dns-scan-*.db`; the
AXFR service resumes `axfr-cycle-state.json`/`axfr-scan-*.db` from the existing deployment directory.
Deployment never deletes or moves those files.

Start each service explicitly after checking its state:

```bash
ssh hetzner01 'systemctl start cc-dns-scan'
ssh hetzner01 'systemctl start cc-axfr-scan'
```

The deployment is non-interactive — the vault password is read from
`~/.config/ansible/cc-dns-scan` via `ansible.cfg`.

Dry run (no changes applied):

```bash
ansible-playbook site.yml --check --diff
```

After the initial full cutover, deploy either worker independently with its role tag:

```bash
ansible-playbook site.yml --tags cc_dns_worker
ansible-playbook site.yml --tags cc_axfr_worker
```

Do not run an AXFR-only tagged deployment as the initial cutover while the legacy combined binary is
still running. That would leave two AXFR supervisors operating on the same state.

## Layout

```
deploy/ansible/
├── ansible.cfg              # inventory + vault_password_file config
├── inventory.ini            # [cc_dns] hetzner01
├── site.yml                 # DNS worker first, then OS/Unbound and AXFR worker
├── group_vars/cc_dns/
│   ├── vars.yml              # deploy layout, worker run flags, unbound/OS tuning, CH connection
│   └── vault.yml              # encrypted: vault_clickhouse_password
└── roles/
    ├── cc_dns_worker/
    ├── cc_axfr_worker/
    ├── os_tuning/
    └── unbound/
```
