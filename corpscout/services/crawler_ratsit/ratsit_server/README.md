# Ratsit browser server

`main.py` runs one persistent CloakBrowser instance and exposes its Chrome
DevTools Protocol endpoint. It has no queue or crawl logic; the Temporal worker
connects to this endpoint and owns crawl execution.

Run it locally with a visible browser:

```bash
uv run ratsit-server --profile-dir ./profile --headed
```

Or run it with Chromium's headless mode:

```bash
uv run ratsit-server --profile-dir ./profile --headless
```

Verify the endpoint with:

```bash
curl -fsS http://127.0.0.1:9222/json/version
```

`CLOAKBROWSER_LICENSE_KEY` is optional and is read from the process
environment. Keep local values in the ignored `.env` file and export them
before starting the command; the launcher does not parse dotenv files itself.

For remote installation as a systemd service, follow the
[Ansible deployment procedure](ansible/README.md). The deployed user service
runs headed Chromium on UID 1000's active graphical session.
