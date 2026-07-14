# Deploying pulsarprotectctlog with Ansible

This is the only supported deployment path. One idempotent `ansible-playbook`
run converges a host to the declared state.

## Where everything is defined

Deliberately flat — no role defaults, no host_vars, no layered precedence:

| File | Holds |
|---|---|
| `group_vars/ctlog_hosts/vars.yml` | **ALL configuration** — the only file you edit |
| `group_vars/ctlog_hosts/vault.yml` | **secrets only**, encrypted with ansible-vault |
| `inventory.ini` | which hosts are in the group |

`vars.yml` references secrets via `{{ vault_… }}` indirection, so it stays
greppable while the actual values are encrypted.

## What it does

For each host in `[ctlog_hosts]` (`roles/ctlog`):

1. Creates the `ctlog` system user/group.
2. Lays the linux binary into `releases/<version>/` and points `current` at it
   (atomic switch; rollback = change `ctlog_version` and re-run).
3. Renders `/opt/pulsarprotectctlog/ctlog.env` (root:0600) — the **single**
   `EnvironmentFile` holding all config including the vaulted password.
4. Renders `sources.json` from `ctlog_sources`.
5. Installs the templated `ctlog@.service` / `ctlog@.timer` units, `daemon-reload`.
6. Enables + starts one `ctlog@<source>.timer` per declared source.

State (`data/`, `sources.json`) lives at the deploy root and is shared across
versions, so upgrades/rollbacks never lose the SQLite cursor.

## One-time setup

```bash
cd ansible
make -C .. build-linux              # produces ../bin/ctlog-linux-amd64
```

Set up the vault (once):

```bash
# 1. Create the vault key OUTSIDE the repo (one line of random text):
mkdir -p ~/.config/ansible
openssl rand -base64 32 | install -m600 /dev/stdin ~/.config/ansible/ctlog-vault-pass
# ansible.cfg already points vault_password_file at it.

# 2. Put the real password into vault.yml, then encrypt the file:
ansible-vault edit group_vars/ctlog_hosts/vault.yml   # if already encrypted
#   — or, first time, edit it in your editor and then:
ansible-vault encrypt group_vars/ctlog_hosts/vault.yml
```

`git diff` should then show `$ANSIBLE_VAULT;1.1;AES256` gibberish for vault.yml —
never a plaintext password.

## Vault rules (best practice)

- **Only `vault.yml` is encrypted**; every var in it is prefixed `vault_` and
  referenced from `vars.yml` — config stays readable/greppable.
- **The vault key never enters the repo** (`~/.config/ansible/ctlog-vault-pass`,
  mode 600). Back it up in your password manager; share with teammates via a
  secret channel, never git.
- **Rotate the DB password** = `ansible-vault edit vault.yml`, change it, re-run
  the playbook (next timer run picks it up — oneshot services re-read the env
  file on every run). **Rotate the vault key** = `ansible-vault rekey vault.yml`.
- Never pass secrets with `-e password=…` (shell history) and don't paste them
  into `vars.yml` "temporarily".
- The env-file task sets `diff: false` so `--diff` output can't leak the secret.
- Caveat to know: once an encrypted secret is committed, that ciphertext is in
  git history forever — if the vault key ever leaks, rotate the *database*
  password, not just the key.

## Run

```bash
ansible-playbook site.yml --check --diff        # dry run: shows what WOULD change
ansible-playbook site.yml                        # apply
```

## Common operations

All edits happen in `group_vars/ctlog_hosts/vars.yml`, then `ansible-playbook site.yml`:

| Goal | Change |
|---|---|
| Ship a new version | `ctlog_version:` |
| Roll back | set `ctlog_version:` to the prior value |
| Add a source | append to `ctlog_sources` |
| Change cadence | `ctlog_drain_interval:` |
| Change DB password | `ansible-vault edit group_vars/ctlog_hosts/vault.yml` |
| Add a host | new line under `[ctlog_hosts]` in `inventory.ini` |

## Verify on the host

```bash
systemctl list-timers 'ctlog@*'
journalctl -u ctlog@le-sycamore.service -f
```

## Notes / next steps

- **CI**: in a pipeline, replace `ctlog_binary_src` with a `get_url` of a
  released asset so the control machine doesn't need a local build.
- **Layout**: this role owns only ctlog. A shared `common` role (base packages,
  users, hardening) would sit alongside it and be applied first in `site.yml`
  as the fleet grows — the same pattern extends to the other PulsarPoint apps.
