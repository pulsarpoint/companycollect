# Company crawler systemd deployment

Deploys the service to `192.168.88.132` as the system unit
`company-research.service`. The default configuration enables REST on port 8080
and a durable JetStream consumer against the NATS URL supplied in secrets.
The existing Corpscout broker is `192.168.88.129:4222`.

The target must be Debian/Ubuntu with systemd, SSH access and sudo privileges.
Set the SSH user/key in `inventory.ini`, or override with Ansible's
`-e ansible_user=... --private-key ...`. The controller needs Ansible and `uv`.
SSH host-key verification stays enabled.

## Deploy

From this directory:

```sh
cp secrets.yml.example secrets.yml  # only if secrets.yml does not already exist
chmod 600 secrets.yml
# Fill in the model keys, API token and authenticated NATS URL.
ansible-playbook site.yml
```

`secrets.yml` is ignored by Git. Alternatively export `DEEPSEEK`,
`OPENROUTER_API_KEY`, `CRAWL_API_TOKEN` and `NATS_URL` on the controller.
The default DeepSeek key is required; OpenRouter is optional. Use a random API
token of at least 32 characters. The secrets file takes precedence over environment
variables. Credentials are copied with mode `0640`, readable by root and the
service group, and suppressed from Ansible output/diffs.

Use `--ask-become-pass` if sudo needs a password; use `--ask-pass` if the target
requires SSH password authentication. On macOS, if Ansible reports an unsupported
locale, prefix the command with `LC_ALL=en_US.UTF-8`.

All non-secret deployment settings are in [vars.yml](vars.yml). The default
JetStream stream is `COMPANY_CRAWL`, subject `company.crawl.requests`, and durable
consumer `company-crawl-132`. The playbook allows the service to create a missing
stream; it does not change an existing stream's configuration or install another
NATS server. Set `company_research_transport: rest` for REST only.

## Installed layout

| Path | Purpose |
| --- | --- |
| `/opt/companycollect/corpscout/company_research/releases/<hash>` | Application wheel and dedicated Python environment |
| `/opt/companycollect/corpscout/company_research/current` | Symlink to the prepared release |
| `/etc/company-research/company-research.env` | Model keys, REST token and NATS connection URL |
| `/var/lib/company-research/results` | Persistent request, job and result JSON, plus HTML captures |
| `/var/cache/company-research` | Browser downloads and runtime caches |
| `/etc/systemd/system/company-research.service` | System unit, enabled at boot |

The controller builds a wheel and exports hashed service dependencies from
`uv.lock`. A release is identified by the wheel, dependencies and Python version.
The target prepares its environment and launches Chromium as the unprivileged
service account before selecting that release. Local datasets, experiments,
virtual environments and `.env` files are not packaged or uploaded.

Rerunning the playbook reuses a prepared release and restarts the service only
when its release, configuration or unit changes. Previous releases and JSON
results are retained. An interrupted crawl is recovered by the service on restart.
The system unit has automatic restart, journald logs and write access limited to
its state/cache directories plus private temporary storage.

## Verify and operate

The playbook checks the systemd unit and waits for `/healthz`; with both inputs
enabled, health requires REST workers and the JetStream consumer to be ready.

```sh
ssh graovic@192.168.88.132 'sudo systemctl status company-research --no-pager'
ssh graovic@192.168.88.132 'sudo journalctl -u company-research -n 50 --no-pager'
curl --fail http://192.168.88.132:8080/healthz
```

With `CRAWL_API_TOKEN` set in your local shell, submit a real one-page crawl:

```sh
curl --fail http://192.168.88.132:8080/v1/crawls \
  -H "Authorization: Bearer $CRAWL_API_TOKEN" \
  -H 'Content-Type: application/json' \
  --data '{"request_id":"novelic-deploy-check-001","url":"https://www.novelic.com/","pages":["https://www.novelic.com/careers/"],"config":{"max_pages":1}}'
curl --fail http://192.168.88.132:8080/v1/crawls/novelic-deploy-check-001 \
  -H "Authorization: Bearer $CRAWL_API_TOKEN"
curl --fail http://192.168.88.132:8080/v1/crawls/novelic-deploy-check-001/result \
  -H "Authorization: Bearer $CRAWL_API_TOKEN"
```

Use a new request ID for a new crawl. This explicit-page example makes no model
calls. See [the service contract](../SERVICE.md) for site-info, custom instructions,
JetStream publishing and result fields.

The standalone CLI is also installed. On the server, use a separate output
folder and the same service identity:

```sh
sudo -u company-research env \
  HOME=/var/lib/company-research \
  CLOAKBROWSER_CACHE_DIR=/var/cache/company-research/cloakbrowser \
  CLOAKBROWSER_AUTO_UPDATE=false \
  /opt/companycollect/corpscout/company_research/current/.venv/bin/company-research-crawl \
  https://www.novelic.com/ --site-info \
  --env-file /etc/company-research/company-research.env \
  --output-dir /var/lib/company-research/cli-novelic-info
```

To inspect the build without connecting to the server:

```sh
ansible-playbook site.yml --syntax-check
ansible-playbook site.yml --tags build
```

`--check` also validates credentials and target platform, but deliberately skips
remote installation and activation; it is not a complete first-install simulation.
No deployment success is implied by a build-only or syntax check.

Dependency export follows the [uv command reference](https://docs.astral.sh/uv/reference/cli/).
Unit filesystem controls follow [systemd.exec](https://www.freedesktop.org/software/systemd/man/latest/systemd.exec.html).
