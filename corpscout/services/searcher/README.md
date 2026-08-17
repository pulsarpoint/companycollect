# CloakBrowser CDP service

Runs a persistent CloakBrowser profile and exposes its Chrome DevTools Protocol
endpoint on `127.0.0.1`. Cookies, local storage, IndexedDB, cache, and other
browser state remain in the profile directory across service restarts.

Use one profile directory for one browser instance. Chromium does not support
two running processes sharing the same profile.

## Run locally

```bash
uv sync --frozen
uv run main.py \
  --cdp-port 9222 \
  --profile-dir .local/cloakbrowser-9222 \
  --headed
```

Log in to Gmail, LinkedIn, or another site through that browser. Stop the
process with Ctrl+C and run the same command later with the same
`--profile-dir`; the browser will reuse the saved session unless the site has
expired or revoked it.

Verify the endpoint from another terminal:

```bash
curl http://127.0.0.1:9222/json/version
```

Use `--headless` on a server without a graphical display.

## Install as a systemd service

The provided template assumes the project is installed at `/opt/searcher` and
runs under a dedicated `cloakbrowser` system user. Adjust `WorkingDirectory`
and `ExecStart` in the unit if you use another path.

```bash
sudo useradd \
  --system \
  --home-dir /var/lib/cloakbrowser \
  --shell /usr/sbin/nologin \
  cloakbrowser

sudo install -d -o cloakbrowser -g cloakbrowser /opt/searcher
sudo cp -a . /opt/searcher/
sudo chown -R cloakbrowser:cloakbrowser /opt/searcher
sudo -u cloakbrowser uv sync --frozen --project /opt/searcher

sudo install -m 0644 \
  deploy/systemd/cloakbrowser@.service \
  /etc/systemd/system/cloakbrowser@.service
sudo systemctl daemon-reload
sudo systemctl enable --now cloakbrowser@9222.service
```

Inspect the service and endpoint:

```bash
systemctl status cloakbrowser@9222.service
journalctl -u cloakbrowser@9222.service -f
curl http://127.0.0.1:9222/json/version
```

The template instance name is the CDP port. A second browser therefore uses a
different port and receives its own persistent profile:

```bash
sudo systemctl enable --now cloakbrowser@9223.service
```

The profiles are stored under `/var/lib/cloakbrowser-PORT/profile`. Protect and
back up these directories as credentials: anyone who can read a profile may be
able to reuse its authenticated sessions.

Stop an instance with:

```bash
sudo systemctl disable --now cloakbrowser@9222.service
```

Each endpoint is bound only to localhost. Do not expose a CDP endpoint directly
to a public or untrusted network because it grants full control over the
browser. Use an SSH tunnel when access from another machine is required.
