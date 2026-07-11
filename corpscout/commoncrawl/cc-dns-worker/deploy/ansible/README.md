# cc-dns-worker Ansible deploy

Deploys `cc-dns-worker` to `hetzner01`: OS tuning (`os_tuning`), a local recursive
resolver (`unbound`), and the worker service itself (`cc_dns_worker`).

## Production feature defaults

The binary starts independent DNS and AXFR scanner goroutines by default. They
use separate state files, SQLite databases, worker pools, and retry loops. The
production command intentionally omits `--dns`, `--axfr`, and `--host-enrich`
because their defaults are `true`. Add a `=false` override only for an explicit
temporary opt-out.

## Prerequisites

- The target host (`hetzner01`) is reachable over Tailscale/ssh from the control
  machine, and can in turn reach `companycollect` (the ClickHouse host) on its
  Tailscale network.
- ClickHouse migrations for `corpscout` have already been applied on
  `companycollect`.
- The control machine has:
  - Ansible installed (`ansible-playbook`, `ansible-vault`).
  - Go installed (used to build `cc-dns-worker` before it's shipped to the
    target, per the `cc_dns_worker` role).
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

The role builds a CGO-free Linux/AMD64 binary on the control machine and copies
it to the target. If `cc-dns-scan` already exists and is running, the role stops
it before replacing the deployed files. Deployment leaves the service stopped
and disabled. It does not modify or retire worker state; any required state
migration or cleanup must be performed manually before running the playbook.

The independent supervisors use `dns-cycle-state.json`/`dns-scan-*.db` and
`axfr-cycle-state.json`/`axfr-scan-*.db`. They do not resume the coupled legacy
`orchestrator-state.json`/`scan-*.db`; inspect and handle those files manually
before the first start.

Start the deployed worker explicitly only after checking its state:

```bash
ssh hetzner01 'systemctl start cc-dns-scan'
```

The deployment is non-interactive — the vault password is read from
`~/.config/ansible/cc-dns-scan` via `ansible.cfg`.

Dry run (no changes applied):

```bash
ansible-playbook site.yml --check --diff
```

Apply a single role (roles are tagged with their own name):

```bash
ansible-playbook site.yml --tags unbound
```

## Layout

```
deploy/ansible/
├── ansible.cfg              # inventory + vault_password_file config
├── inventory.ini            # [cc_dns] hetzner01
├── site.yml                 # play: os_tuning, unbound, cc_dns_worker
├── group_vars/cc_dns/
│   ├── vars.yml              # deploy layout, worker run flags, unbound/OS tuning, CH connection
│   └── vault.yml              # encrypted: vault_clickhouse_password
└── roles/
    ├── os_tuning/
    ├── unbound/
    └── cc_dns_worker/
```
