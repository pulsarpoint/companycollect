# ClickHouse → Backblaze B2 Backup Design

**Date:** 2026-07-14
**Status:** Approved

## Problem

The corpscout ClickHouse on `companycollect` (~456 GiB active data: `ctlogs`
232 GiB, `corpscout` 191 GiB, ClickHouse 26.5.1, nearly all
MergeTree-family engines) has no backups. Backblaze B2 credentials are
already staged in `/opt/clickhouse/.env`, and the B2 bucket
`main-ch-backup` exists (endpoint `s3.us-east-005.backblazeb2.com`).

## Decisions

- **Tool:** Altinity `clickhouse-backup` (battle-tested standard for
  self-hosted ClickHouse; native S3-compatible upload, incremental remote
  backups, built-in retention, consistent `FREEZE`-based snapshots with no
  downtime). Rejected: native `BACKUP ... TO S3()` (retention/scheduling
  becomes hand-rolled scripting; pickier about B2 S3 quirks) and
  filesystem-level copies (consistency requires reimplementing option 1).
- **Scope:** everything (both `ctlogs` and `corpscout`).
- **Schedule:** weekly full + daily incremental, via clickhouse-backup's
  built-in `watch` mode.
- **Retention:** `BACKUPS_TO_KEEP_REMOTE=28` (~4 weeks of daily restore
  points).
- **Estimated cost:** ~1–1.5 TB on B2 ≈ $6–9/month.

## Architecture

Extend the existing `corpscout/infra/clickhouse/` deployment; no new host
components.

1. **Sidecar service** `clickhouse-backup` in the same
   `docker-compose.yml`:
   - Image `altinity/clickhouse-backup:2.7.4` (pinned; latest as of
     2026-07-05).
   - Shares the ClickHouse data bind mount (`./data/clickhouse:/var/lib/clickhouse`)
     and reaches the server over the compose network.
   - Command: `watch --watch-interval=24h --full-interval=168h`
     → full every 7 days, incremental every 24 h; watch inherently deletes
     the local backup copy after each upload.
   - `restart: unless-stopped`, same lifecycle as the server via the
     existing `clickhouse-compose.service` systemd unit.
2. **Configuration** entirely via environment from the ansible-rendered
   `.env`:
   - `REMOTE_STORAGE=s3`, `S3_BUCKET=main-ch-backup`,
     `S3_ENDPOINT=https://s3.us-east-005.backblazeb2.com`,
     `S3_ACCESS_KEY`/`S3_SECRET_KEY` = staged Backblaze keys,
     `S3_FORCE_PATH_STYLE=true`, `BACKUPS_TO_KEEP_REMOTE=28`,
     `CLICKHOUSE_HOST=clickhouse`, `CLICKHOUSE_PASSWORD` as for the server.
3. **Ansible:** the existing `install.yml` rolls it out (compose copy +
   `.env` template gain the new keys; `vars.yml` gains bucket/retention
   vars). The handler (`docker compose up -d`) creates only the new sidecar;
   the ClickHouse container is not recreated.
4. **Restore (documented in README):**
   `docker compose exec clickhouse-backup clickhouse-backup restore_remote <name>`
   (incrementals resolve their base chain automatically); health check via
   `list remote`.

## Error handling / observability

Watch mode logs failures to container logs
(`docker compose logs clickhouse-backup`). No alerting in this iteration
(YAGNI); clickhouse-backup exposes a REST/metrics server if alerting is
wanted later.

## Verification

1. Deploy via `ansible-playbook install.yml`; confirm ClickHouse container
   uptime unchanged.
2. Trigger first full manually: `create_remote`; confirm it in
   `list remote` and objects in the `main-ch-backup` bucket.
3. Prove the round trip: restore one small table into a scratch database
   name and compare row counts, then drop the scratch database.
4. Let watch mode take over; after 24 h confirm an incremental appears.
