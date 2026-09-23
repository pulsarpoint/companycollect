# Company crawler systemd deployment

Deploys the service to the `crawler` VM (Tailscale MagicDNS name; its LAN
address is DHCP-assigned) as the system unit `company-research.service`. The
service exposes only its REST API on port 8080, like the browser service.

The crawler uses the independent [browser service](../../browser_service/README.md).
Deploy that service first, then configure `company_research_browser_api_url` and
`company_research_browser_api_token`. This playbook no longer installs Chromium,
Playwright, Xvfb or desktop tools. It preserves crawl results and queues.

Human assistance remains enabled by default and requires S3 settings. All scans use
an externally reserved profile. Normal REST requests have two worker
slots, configured with `company_research_concurrency`, matching the two browsers
on this deployment. Up to `company_research_max_pending` further requests wait in
the service's local queue; beyond that REST submissions are rejected.
Manual retries have two worker slots by default,
configured with `company_research_manual_concurrency`; browser capacity is external.

Set the SSH user/key in `inventory.ini`, or override with Ansible's
`-e ansible_user=... --private-key ...`. The controller needs Ansible and `uv`.
SSH host-key verification stays enabled.

## Deploy

From this directory:

```sh
cp secrets.yml.example secrets.yml  # only if secrets.yml does not already exist
chmod 600 secrets.yml
# Fill in the model keys and API token.
ansible-playbook site.yml
```

`secrets.yml` is ignored by Git. Alternatively export `DEEPSEEK`,
`OPENROUTER_API_KEY` and `CRAWL_API_TOKEN` on the controller.
The default DeepSeek key is required; OpenRouter is optional. Use a random API
token of at least 32 characters. The secrets file takes precedence over environment
variables. Credentials are copied with mode `0640`, readable by root and the
service group, and suppressed from Ansible output/diffs.

Use `--ask-become-pass` if sudo needs a password; use `--ask-pass` if the target
requires SSH password authentication. On macOS, if Ansible reports an unsupported
locale, prefix the command with `LC_ALL=en_US.UTF-8`.

All non-secret deployment settings are in [vars.yml](vars.yml).

To store failed attempts and results in S3, add these
settings to the ignored `secrets.yml` (the bucket must already exist):

```yaml
company_research_s3_bucket: crawls
company_research_s3_endpoint_url: http://rustfs:9000
company_research_s3_access_key: replace-me
company_research_s3_secret_key: replace-me
```

The default object prefix is `company-crawls`. Override
`company_research_s3_prefix` in deployment variables if needed. The environment
file carries S3 credentials with the same permissions and output suppression as
the other secrets. Leave the endpoint empty for AWS S3; the AWS credential provider
chain can supply credentials instead. Controller environment equivalents are
`CRAWL_S3_BUCKET`, `CRAWL_S3_ENDPOINT_URL`, `AWS_ACCESS_KEY_ID`,
`AWS_SECRET_ACCESS_KEY` and optional `AWS_SESSION_TOKEN`.

Local job state, results and delivery receipts stay under the persistent output folder.
See [delivery and retry behavior](../SERVICE.md#s3-results-and-completion-events).

## Installed layout

| Path | Purpose |
| --- | --- |
| `/opt/companycollect/corpscout/company_research/releases/<hash>` | Application wheel and dedicated Python environment |
| `/opt/companycollect/corpscout/company_research/current` | Symlink to the prepared release |
| `/etc/company-research/company-research.env` | Model keys and REST token |
| `/var/lib/company-research/results` | Persistent request, job and result JSON, plus HTML captures |
| `/var/lib/company-research/results/crawl-history.sqlite3` | Searchable attempt history and durable status events |
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
`KillMode=mixed` lets the main service cancel browser recovery and flush profiles
before systemd terminates any remaining children. The existing 90-second shutdown
deadline still bounds cleanup.

## Verify and operate

The playbook checks the systemd unit and waits for `/healthz`, which requires the
REST workers to be ready.

```sh
ssh graovic@crawler 'sudo systemctl status company-research --no-pager'
ssh graovic@crawler 'sudo journalctl -u company-research -n 50 --no-pager'
curl --fail http://crawler:8080/healthz
```

With `CRAWL_API_TOKEN` set in your local shell, submit a real one-page crawl:

```sh
curl --fail http://crawler:8080/v1/crawls \
  -H "Authorization: Bearer $CRAWL_API_TOKEN" \
  -H 'Content-Type: application/json' \
  --data '{"request_id":"novelic-deploy-check-001","url":"https://www.novelic.com/","pages":["https://www.novelic.com/careers/"],"config":{"max_pages":1}}'
curl --fail http://crawler:8080/v1/crawls/novelic-deploy-check-001 \
  -H "Authorization: Bearer $CRAWL_API_TOKEN"
curl --fail http://crawler:8080/v1/crawls/novelic-deploy-check-001/result \
  -H "Authorization: Bearer $CRAWL_API_TOKEN"
```

Use a new request ID for a new crawl. This explicit-page example makes no model
calls. See [the service contract](../SERVICE.md) for site-info, custom instructions,
S3 delivery and result fields.

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

Detailed crawl artifacts are enabled by default while developing. Add
`--no-save-artifacts` to a crawl command, or `"save_artifacts": false` to a REST
request, to retain just the bundled result in that crawl directory.
The service's request/job files still persist for recovery.

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
