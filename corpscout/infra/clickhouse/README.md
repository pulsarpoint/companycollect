# ClickHouse Runtime

This folder owns the standalone ClickHouse server runtime for Corpscout. It
runs on the `companycollect` host, deployed with Ansible.

## Deploy

```bash
cd corpscout/infra/clickhouse
ansible-playbook install.yml
```

The playbook copies `docker-compose.yml`, renders `.env` from `vars.yml`, and
installs `clickhouse-compose.service` — a oneshot systemd unit that runs
`docker compose up -d` on boot (after `docker.service`) and `docker compose
down` on stop. Crash restarts are still handled by docker's
`restart: unless-stopped`; systemd owns boot ordering and start/stop:

```bash
sudo systemctl status clickhouse-compose
sudo systemctl stop clickhouse-compose    # compose down
sudo systemctl start clickhouse-compose   # compose up -d
```

Config changes (compose file or `.env`) are applied by re-running the
playbook; compose only recreates the container when its effective
configuration changed, so an unchanged run never restarts ClickHouse.

Data lives on the host at `/opt/clickhouse/data/clickhouse`.

## Local development

```bash
cd corpscout/infra/clickhouse
cp .env.example .env   # then set a real password
docker compose up -d
```

## Connectivity

Corpscout migrations and imports connect to ClickHouse through
`CLICKHOUSE_MIGRATE_URL` and `CLICKHOUSE_NATIVE_URL` from
`companycollect/corpscout/.env`.

`CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1` is required because some migrations
provision least-privilege service users, such as the localhost-only reader used
by the RDAP IP-trie dictionary. The `default` account remains protected by
`CLICKHOUSE_PASSWORD`.

After enabling access management on an existing deployment, recreate the
ClickHouse container so the image entrypoint regenerates its user configuration:

```bash
docker compose up -d --force-recreate clickhouse
```

## Migration recovery

If migration 126 previously failed at `CREATE USER`, golang-migrate left version
126 dirty after dropping the old dictionary. Reset it to the last successful
version and rerun it from the Corpscout checkout:

```bash
make clickhouse-migrate-force VERSION=125
make clickhouse-migrate-up
```
