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

## Backups

A `clickhouse-backup` sidecar (Altinity, watch mode) continuously backs up
all databases to the Backblaze B2 bucket `main-ch-backup`: a full backup
every 7 days, an incremental every 24 h, keeping the 28 most recent remote
backups (~4 weeks of daily restore points). It is enabled by
`COMPOSE_PROFILES=backup` in the server `.env`; local dev without that
profile never starts it.

Check backup health:

```bash
docker exec clickhouse-clickhouse-backup-1 clickhouse-backup list remote
docker logs --tail 50 clickhouse-clickhouse-backup-1
```

Restore (incrementals resolve their base chain automatically):

```bash
# Full restore of everything, e.g. on a fresh host after install.yml:
docker exec clickhouse-clickhouse-backup-1 clickhouse-backup restore_remote <backup_name>

# Single table into a scratch database (used for periodic restore drills):
docker exec clickhouse-clickhouse-backup-1 clickhouse-backup restore_remote \
  --tables="corpscout.<table>" \
  --restore-database-mapping="corpscout:backup_verify" <backup_name>
```

Note: backup names carry a literal `shard{shard}-` prefix (the {shard} macro is not set on this host), so names look like `shard{shard}-full-20260714142423` as listed by `list remote`. Watch's backup chain restarts (new full) if the sidecar container is
recreated; a host reboot therefore triggers an off-schedule full upload.

## Local development

```bash
cd corpscout/infra/clickhouse
cp .env.example .env   # then set a real password
docker compose up -d
```

## Connectivity

Corpscout migrations connect to ClickHouse through `CLICKHOUSE_MIGRATE_URL` from
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
