# nats — Ansible deployment

Installs [NATS](https://nats.io) with JetStream as a native systemd service on
the `nats` VM (`192.168.88.129`, Ubuntu 26.04, KVM). Single node, no Docker:
pinned release binary in `/usr/local/bin`, config in `/etc/nats`, JetStream data
on its own XFS logical volume.

## Layout

| File | Purpose |
|---|---|
| `install.yml` | Full install: volume, user, binaries, config, service, verification |
| `vars.yml` | Pinned versions + checksums, volume, ports, JetStream limits, accounts |
| `secrets.yml` | Git-ignored passwords; create from `secrets.yml.example` |
| `templates/nats-server.conf.j2` | → `/etc/nats/nats-server.conf` |
| `templates/nats.service.j2` | → `/etc/systemd/system/nats.service` |

## Usage

```bash
cd corpscout/infra/nats
cp secrets.yml.example secrets.yml   # new machine only; set real passwords
ansible-playbook install.yml
```

Idempotent — safe to re-run for installation and configuration reconciliation.
A changed config, unit, or binary restarts the service (about a second; clients
reconnect on their own and JetStream data persists). The config is checked with
`nats-server -t` before it replaces the live file.

The run ends with a real check, not just a port probe: JetStream health, then a
round trip through the application account — create a temporary file-backed
stream, publish and wait for the ack, read the count back, remove the stream —
and a `nats server ping` through the system account.

## Connecting

- Client: `nats://corpscout:<nats_app_password>@192.168.88.129:4222`
- Monitoring (HTTP, read-only, unauthenticated): `http://192.168.88.129:8222`
  — `/healthz`, `/varz`, `/jsz`, `/connz`

| Account | User | Purpose |
|---|---|---|
| `CORPSCOUT` | `corpscout` | Applications. JetStream enabled; every stream lives here. |
| `SYS` | `sys` | System account behind `nats server ...` admin commands. No JetStream. |

Streams belong to an account: a stream created by `corpscout` is stored under
`/var/lib/nats/jetstream/CORPSCOUT` and is invisible to any other account.

Passwords sit in plaintext in `/etc/nats/nats-server.conf` (root:nats, 0640),
so every start logs `[WRN] Plaintext passwords detected`. That is deliberate:
bcrypt is slow by design and the server pays that cost on every connect, which
adds up for workers that reconnect often. Switch the config to bcrypt (`nats
server passwd`) or nkeys if the file's exposure ever changes.

## Operations

The `nats` CLI is installed on the server:

```bash
export NATS_URL=nats://127.0.0.1:4222 NATS_USER=corpscout NATS_PASSWORD=...
nats stream ls
nats stream report

export NATS_USER=sys NATS_PASSWORD=...      # system account
nats server report jetstream
nats server report connections
```

```bash
sudo systemctl status nats
sudo journalctl -u nats -f
sudo systemctl reload nats     # SIGHUP: re-read accounts/users without dropping clients
```

Prefer re-running the playbook over `reload` for config changes: a SIGHUP that
touches an option which cannot be hot-reloaded is refused and only logged,
leaving the old config live, whereas the playbook's restart always applies the
whole file.

The unit stops the server with SIGINT — nats-server exits 1 on SIGTERM, which
systemd would record as a failed stop.

## Storage

JetStream writes to `/var/lib/nats`, a 100 GiB XFS logical volume
(`ubuntu-vg/nats-data`) separate from the root filesystem, with
`max_file_store` at 90 GiB and `max_memory_store` at 4 GiB. The unit declares
`RequiresMountsFor=/var/lib/nats`, so the server never starts against the bare
mount point.

To grow (about 98 GiB remain free in the volume group): raise `nats_lv_size` and
`nats_max_file_store` in `vars.yml` and re-run `install.yml`. The volume and
filesystem grow online; the playbook never shrinks a volume.

## Upgrading

Set `nats_version` and `nats_tarball_sha256` in `vars.yml` (checksums are in the
release's `SHA256SUMS`), then re-run `install.yml`. Each release unpacks into
its own `/usr/local/lib/nats-server/<version>/`, so rolling back is restoring
the previous two values and re-running. The `nats` CLI is pinned the same way
through `natscli_version` / `natscli_zip_sha256`.
