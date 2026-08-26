# Ratsit CDP server deployment

This playbook owns only the headed CloakBrowser CDP server. It deploys
`ratsit-cdp.service` as a UID 1000 systemd user unit on a graphical Linux host.
The browser profile survives restarts in `/var/lib/ratsit-server/profile`, while
downloaded CloakBrowser binaries live in `/var/cache/ratsit-server`.

The unit is attached to `graphical-session.target`. It starts only while the
configured desktop user has an active graphical login, inheriting the current
`DISPLAY` and `XAUTHORITY` from that user's systemd manager. This intentionally
does not use Xvfb.

The Temporal worker has an independent deployment under
[`crawler_ratsit/ansible`](../../crawler_ratsit/ansible/README.md). The two
inventories may point to the same host today and different hosts later.

## Prerequisites

- The target is Debian/Ubuntu Linux on x86-64 or arm64.
- `graovic` is UID 1000 and has an active graphical desktop session.
- `uv` is available in the target SSH user's login environment. The playbook
  discovers it automatically; `ratsit_server_uv_binary` can override the path.
- The SSH deployment user has passwordless sudo.
- The control machine has Ansible and `uv`.

Only Ansible built-in modules are used.

## Deploy

Create the real inventory and edit its host and SSH user:

```bash
cd ratsit_server/ansible
cp inventory.example.ini inventory.ini
export LC_ALL=en_US.UTF-8
export LANG=en_US.UTF-8
```

Review the variables, then validate and deploy:

```bash
ansible-playbook site.yml --syntax-check
ansible-playbook site.yml --check --diff
ansible-playbook site.yml
```

The deployment verifies the lockfile, runs the focused server tests, installs
Python 3.14 through `uv` when necessary, verifies the GeoIP dependency,
installs Chromium's Linux libraries, downloads the signed CloakBrowser binary,
migrates away from the former system-level/Xvfb unit, installs the user unit,
and checks `/json/version` after startup.

## Operations

```bash
ssh ratsit 'systemctl --user status ratsit-cdp --no-pager'
ssh ratsit 'journalctl --user -u ratsit-cdp -n 100 -f'
ssh ratsit 'curl -fsS http://127.0.0.1:9222/json/version'
```

CDP gives complete control of the browser, so the service binds to loopback by
default. Reach it from another machine only through a protected tunnel:

```bash
ssh -N -L 9222:127.0.0.1:9222 ratsit
```

If a CloakBrowser license or custom binary setting is needed, create
`~/.config/ratsit-server/environment` on the target as a `graovic`-owned `0600`
file. The user unit reads it without Ansible copying secrets from the
repository.
