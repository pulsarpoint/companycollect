# cc-dns-worker Ansible deploy

Deploys `cc-dns-worker` to `hetzner01`: OS tuning (`os_tuning`), a local recursive
resolver (`unbound`), and the worker service itself (`cc_dns_worker`).

## Feature containment (temporary)

`--axfr` and `--host-enrich` are **disabled** in the production flag set
(`group_vars/cc_dns/vars.yml`) until their release gates in
[`../../docs/superpowers/plans/2026-07-10-cc-dns-worker-correctness-hardening.md`](../../docs/superpowers/plans/2026-07-10-cc-dns-worker-correctness-hardening.md)
pass — AXFR needs Tasks 2–6, host enrichment needs Task 11. This is temporary
containment, not feature removal: re-enable by appending `--axfr --host-enrich`
to `cc_dns_run_flags`. Rollback of the containment is that single variable change.

Note: the **base resolver** still requires Task 1 (public-target dial filtering)
before it is production-safe, because authoritative DNS queries dial untrusted NS
addresses too — not only AXFR.

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

### One-time raw-observation cutover

The first deployment containing the retry-safe raw DNS observation writer must start a new scan ID.
The role refuses to restart an existing cycle until an operator makes that boundary explicit.

To stop the worker, preserve its current state file as a pre-cutover backup, and deliberately abandon
that cycle before the new binary starts:

```bash
ansible-playbook site.yml -e cc_dns_observation_cutover_mode=abandon_active_cycle
```

The SQLite scan DB is left untouched (and remains subject to the normal `--keep-dbs` pruning policy),
but its state file is retired so the new writer mints a fresh cycle. If the cutover was completed
independently before this guard was deployed, verify that no scan ID contributed to both the legacy
aggregate and raw observations, then record that fact with:

```bash
ansible-playbook site.yml -e cc_dns_observation_cutover_mode=already_complete
```

Both modes create `.dns-record-observation-cutover-complete` on the target. Later deployments require no
extra variable and cannot accidentally repeat the one-time abandonment.

### Normal deployment

```bash
cd deploy/ansible
ansible-playbook site.yml
```

This is non-interactive — the vault password is read from
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
