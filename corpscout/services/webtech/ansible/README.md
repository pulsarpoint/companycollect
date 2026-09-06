# Webtech Ansible deployment

This playbook deploys the Webtech scanner to `graovic@192.168.88.149` and
manages its existing user-level `webtech.service` unit. It validates the local
Python and JavaScript source, synchronizes the project, reconciles the Linux
virtual environment with the committed `uv.lock`, and verifies `/healthz`.

The target owns `/opt/companycollect/corpscout/webtech/.env`. Ansible requires
that file to exist as `graovic` with mode `0600` and never copies it from the
control machine. Synchronization also preserves `.venv`, `output`, and
`.cloakbrowser-profile`.

The playbook queries `/healthz` before changing files. It refuses to deploy
while a scan is active, because both a service restart and an extension source
change could alter in-flight browser work. When no source or unit files change,
the playbook leaves the running service untouched.

## Deploy

The macOS control environment needs a supported UTF-8 locale:

```bash
cd corpscout/services/webtech/ansible
export LC_ALL=en_US.UTF-8
export LANG=en_US.UTF-8

ansible-playbook site.yml --check --diff
ansible-playbook site.yml
```

The controller needs Ansible, `uv`, `rsync`, and Node.js. The target needs
Python 3, `uv`, `rsync`, systemd user services, the existing CloakBrowser
runtime, and the manually provisioned `.env`.

## Operations

### Recursive DNS on the scanner host

`dns.yml` installs a loopback-only Unbound resolver on the scanner machine.
It caches DNS answers and resolves cache misses directly through root, TLD,
and authoritative DNS servers. There are no forwarding zones or public DNS
forwarders. The packaged `dns-root-data` supplies root hints and the initial
DNSSEC trust anchor; Unbound maintains its writable trust anchor afterward.

The host's existing `systemd-resolved` stub routes public names to
`127.0.0.1:53`. Its more-specific Tailscale routes still handle internal names.
Unbound owns the cache (64 MiB message cache, 128 MiB RRset cache, two threads);
the systemd stub's duplicate cache is disabled. Nothing listens on the LAN.
This requires direct outbound UDP and TCP port 53 to authoritative servers.

Apply DNS configuration without redeploying or restarting an active scanner:

```bash
ansible-playbook dns.yml --check --diff
ansible-playbook dns.yml
```

The normal `site.yml` deployment includes the DNS playbook. It requires sudo
for resolver installation/configuration. DNS routing is changed only after
Unbound returns a DNSSEC-validated answer and confirms no forwarding zones;
failed public/internal resolution after the switch restores previous routing.
On a first-install dry run, package-dependent checks wait for a real deployment.

```bash
ssh 192.168.88.149 'sudo unbound-control stats_noreset'
ssh 192.168.88.149 'sudo unbound-control list_forwards'
ssh 192.168.88.149 'resolvectl status'
ssh 192.168.88.149 'dig @127.0.0.1 example.com; dig @127.0.0.1 example.com'
```

### Scanner API

```bash
ssh 192.168.88.149 'systemctl --user status webtech.service --no-pager'
ssh 192.168.88.149 'journalctl --user -u webtech.service -n 200 -f'
ssh 192.168.88.149 'curl -fsS http://127.0.0.1:8088/healthz'
```
