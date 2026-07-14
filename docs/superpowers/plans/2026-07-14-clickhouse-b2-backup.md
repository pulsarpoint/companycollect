# ClickHouse → Backblaze B2 Backup Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an Altinity clickhouse-backup sidecar to the corpscout ClickHouse compose stack that continuously backs up all databases to the Backblaze B2 bucket `main-ch-backup` (weekly full + daily incremental, 28 remote backups retained).

**Architecture:** A `clickhouse-backup` container joins the existing `corpscout/infra/clickhouse/docker-compose.yml`, sharing the ClickHouse data bind mount and running the tool's built-in `watch` scheduler as its main process. Configuration comes from the ansible-rendered `/opt/clickhouse/.env`. The sidecar is gated behind a compose profile (`backup`) so local dev `docker compose up` is unaffected; the server's `.env` enables the profile. The existing Ansible playbook (`install.yml`) deploys everything unchanged in structure.

**Tech Stack:** docker compose, Ansible, `altinity/clickhouse-backup:2.7.4`, Backblaze B2 S3 API.

**Spec:** `docs/superpowers/specs/2026-07-14-clickhouse-b2-backup-design.md`

## Global Constraints

- Image pinned to `altinity/clickhouse-backup:2.7.4`.
- Bucket `main-ch-backup`, endpoint `s3.us-east-005.backblazeb2.com`, region `us-east-005`, path-style addressing.
- Watch mode: `--watch-interval=24h --full-interval=168h`; retention `BACKUPS_TO_KEEP_REMOTE=28`.
- The running `clickhouse-clickhouse-1` container must NOT be recreated or restarted by this rollout (its compose service definition must not change).
- Secrets live in plaintext `vars.yml` (established pattern); `.env` files stay gitignored.
- All work happens in `/Users/graovic/pulsarpoint/ppoint/companycollect` (git repo root); infra folder is `corpscout/infra/clickhouse/`.
- The server is reached as ssh host `companycollect`; playbook runs from the infra folder with `ansible-playbook install.yml`.

---

### Task 1: Add the backup sidecar to compose + env plumbing

**Files:**
- Modify: `corpscout/infra/clickhouse/docker-compose.yml`
- Modify: `corpscout/infra/clickhouse/vars.yml`
- Modify: `corpscout/infra/clickhouse/templates/clickhouse.env.j2`
- Modify: `corpscout/infra/clickhouse/.env.example`

**Interfaces:**
- Consumes: existing `.env` vars `BACKBLAZE_KEY_ID`, `BACKBLAZE_APP_KEY`, `BACKBLAZE_ENDPOINT`, `CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD`.
- Produces: compose service `clickhouse-backup` (profile `backup`); new `.env` keys `COMPOSE_PROFILES`, `BACKUP_S3_BUCKET`, `BACKUP_S3_REGION`, `BACKUPS_TO_KEEP_REMOTE` that Task 2's deploy relies on.

- [ ] **Step 1: Append the sidecar service to `docker-compose.yml`**

The existing `clickhouse:` service block stays byte-for-byte identical (constraint: no recreate). Full new file content:

```yaml
services:
  clickhouse:
    image: clickhouse/clickhouse-server:26.5
    environment:
      CLICKHOUSE_DB: ${CLICKHOUSE_DB:-corpscout_sources}
      CLICKHOUSE_USER: ${CLICKHOUSE_USER:-default}
      CLICKHOUSE_PASSWORD: ${CLICKHOUSE_PASSWORD:-change-me}
      CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT: ${CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT:-1}
    ports:
      - "${CLICKHOUSE_HTTP_PORT:-8123}:8123"
      - "${CLICKHOUSE_NATIVE_PORT:-9002}:9000"
    volumes:
      - ./data/clickhouse:/var/lib/clickhouse
    healthcheck:
      test: ["CMD", "clickhouse-client", "--query", "SELECT 1"]
      interval: 5s
      timeout: 3s
      retries: 20
      start_period: 10s
    restart: unless-stopped

  # Continuous backups to S3-compatible storage (Backblaze B2). Enabled via
  # COMPOSE_PROFILES=backup in .env; local dev without it never starts this.
  clickhouse-backup:
    image: altinity/clickhouse-backup:2.7.4
    profiles: ["backup"]
    command: watch --watch-interval=24h --full-interval=168h
    depends_on:
      clickhouse:
        condition: service_healthy
    environment:
      CLICKHOUSE_HOST: clickhouse
      CLICKHOUSE_PORT: 9000
      CLICKHOUSE_USERNAME: ${CLICKHOUSE_USER:-default}
      CLICKHOUSE_PASSWORD: ${CLICKHOUSE_PASSWORD:-change-me}
      REMOTE_STORAGE: s3
      S3_BUCKET: ${BACKUP_S3_BUCKET:-main-ch-backup}
      S3_ENDPOINT: https://${BACKBLAZE_ENDPOINT:-s3.us-east-005.backblazeb2.com}
      S3_REGION: ${BACKUP_S3_REGION:-us-east-005}
      S3_ACCESS_KEY: ${BACKBLAZE_KEY_ID:-}
      S3_SECRET_KEY: ${BACKBLAZE_APP_KEY:-}
      S3_FORCE_PATH_STYLE: "true"
      BACKUPS_TO_KEEP_REMOTE: ${BACKUPS_TO_KEEP_REMOTE:-28}
    volumes:
      - ./data/clickhouse:/var/lib/clickhouse
    restart: unless-stopped
```

- [ ] **Step 2: Add backup vars to `vars.yml`**

Append to `corpscout/infra/clickhouse/vars.yml`:

```yaml

# Backups — clickhouse-backup sidecar (watch mode) uploading to Backblaze B2.
backup_s3_bucket: main-ch-backup
backup_s3_region: us-east-005
backups_to_keep_remote: 28
```

- [ ] **Step 3: Add backup keys to the .env template**

Append to `corpscout/infra/clickhouse/templates/clickhouse.env.j2`:

```jinja

COMPOSE_PROFILES=backup
BACKUP_S3_BUCKET={{ backup_s3_bucket }}
BACKUP_S3_REGION={{ backup_s3_region }}
BACKUPS_TO_KEEP_REMOTE={{ backups_to_keep_remote }}
```

- [ ] **Step 4: Mirror the keys in `.env.example`**

Append to `corpscout/infra/clickhouse/.env.example` (documenting local-dev opt-in):

```bash

# Uncomment to run the clickhouse-backup sidecar (see docker-compose.yml).
# COMPOSE_PROFILES=backup
# BACKUP_S3_BUCKET=main-ch-backup
# BACKUP_S3_REGION=us-east-005
# BACKUPS_TO_KEEP_REMOTE=28
```

- [ ] **Step 5: Validate compose renders with and without the profile**

Run from `corpscout/infra/clickhouse/`:

```bash
docker compose config --services
COMPOSE_PROFILES=backup docker compose config --services
```

Expected: first command prints only `clickhouse`; second prints `clickhouse` and `clickhouse-backup`. (Local `.env` has no `COMPOSE_PROFILES`, so the first form exercises the local-dev path.)

- [ ] **Step 6: Commit**

```bash
git add corpscout/infra/clickhouse/docker-compose.yml corpscout/infra/clickhouse/vars.yml corpscout/infra/clickhouse/templates/clickhouse.env.j2 corpscout/infra/clickhouse/.env.example
git commit -m "feat(corpscout/infra): clickhouse-backup sidecar with B2 watch-mode backups"
```

---

### Task 2: Deploy to companycollect and verify the first full backup starts

**Files:**
- No repo changes — runs `ansible-playbook install.yml` from `corpscout/infra/clickhouse/`.

**Interfaces:**
- Consumes: Task 1's compose service + `.env` keys.
- Produces: running `clickhouse-clickhouse-backup-1` container on the server; first full backup uploading to `main-ch-backup`. Task 3 needs the backup name format `{type}-{timestamp}` from `clickhouse-backup list remote`.

- [ ] **Step 1: Record pre-deploy state**

```bash
ssh companycollect 'docker inspect clickhouse-clickhouse-1 --format "{{.State.StartedAt}}"'
```

Save the timestamp — it must be identical after deploy.

- [ ] **Step 2: Run the playbook**

From `corpscout/infra/clickhouse/`:

```bash
ansible-playbook install.yml
```

Expected: `failed=0`; changed tasks are `Copy docker-compose.yml`, `Write .env`, and the `Apply compose` handler. The handler's `docker compose up -d` creates only the new sidecar (profile enabled by the freshly written `.env`).

- [ ] **Step 3: Verify ClickHouse was not restarted**

```bash
ssh companycollect 'docker inspect clickhouse-clickhouse-1 --format "{{.State.StartedAt}} {{.State.Health.Status}}"'
```

Expected: same `StartedAt` as Step 1, status `healthy`.

- [ ] **Step 4: Verify the sidecar is running watch and started a full backup**

```bash
ssh companycollect 'docker ps --filter name=clickhouse-backup --format "{{.Names}} {{.Status}}"; docker logs --tail 30 clickhouse-clickhouse-backup-1'
```

Expected: container `Up`; logs show `watch` starting and a `create_remote` / `create backup` operation for a backup named like `full-20260714...` with no auth/S3 errors. If logs show S3 403/404: check bucket name and that `.env` on the server contains the `BACKUP_*` keys.

- [ ] **Step 5: Verify objects are landing in B2**

```bash
AWS_ACCESS_KEY_ID=<backblaze_key_id from vars.yml> AWS_SECRET_ACCESS_KEY=<backblaze_app_key from vars.yml> \
  aws s3 ls s3://main-ch-backup/ --recursive --summarize --endpoint-url https://s3.us-east-005.backblazeb2.com | tail -3
```

Expected: object count and total size > 0 and growing on re-run. The full upload is ~456 GiB and may take hours — do NOT wait for completion here; that's Task 3's entry condition.

- [ ] **Step 6: Idempotency check**

```bash
ansible-playbook install.yml
```

Expected: `changed=0`.

---

### Task 3: Restore round-trip verification (after first full completes)

**Files:**
- No repo changes — server-side verification only.

**Interfaces:**
- Consumes: completed full backup from Task 2 (check with `list remote`).
- Produces: proven restore path; the exact commands land in Task 4's README section.

- [ ] **Step 1: Confirm the first full backup finished**

```bash
ssh companycollect 'docker exec clickhouse-clickhouse-backup-1 clickhouse-backup list remote'
```

Expected: one line with a `full-...` backup name, its size, and no `in progress` marker. If still uploading, stop and retry later (check progress via `docker logs`).

- [ ] **Step 2: Pick a small non-empty table for the round trip**

```bash
ssh companycollect 'docker exec clickhouse-clickhouse-1 clickhouse-client --password password123 --query "SELECT database, name, total_rows FROM system.tables WHERE database = '"'"'corpscout'"'"' AND engine LIKE '"'"'%MergeTree%'"'"' AND total_rows > 0 ORDER BY total_bytes ASC LIMIT 1 FORMAT TSV"'
```

Save `<db>.<table>` and its `total_rows`.

- [ ] **Step 3: Restore that table into a scratch database**

```bash
ssh companycollect 'docker exec clickhouse-clickhouse-backup-1 clickhouse-backup restore_remote --tables="corpscout.<table>" --restore-database-mapping="corpscout:backup_verify" <backup_name>'
```

Expected: exit 0, log line `done` for the restore. (If it fails with "database does not exist", create it first: `docker exec clickhouse-clickhouse-1 clickhouse-client --password password123 --query "CREATE DATABASE backup_verify"` and re-run.)

- [ ] **Step 4: Compare row counts, then clean up**

```bash
ssh companycollect 'docker exec clickhouse-clickhouse-1 clickhouse-client --password password123 --query "SELECT (SELECT count() FROM corpscout.<table>) AS src, (SELECT count() FROM backup_verify.<table>) AS restored"'
ssh companycollect 'docker exec clickhouse-clickhouse-1 clickhouse-client --password password123 --query "DROP DATABASE backup_verify"'
```

Expected: `src` == `restored` (source count may be slightly higher if rows arrived after the backup — restored must equal the count as of backup time; equal-or-slightly-lower restored is a pass for an actively-written table, identical for a static one).

---

### Task 4: Document backup operations in the README

**Files:**
- Modify: `corpscout/infra/clickhouse/README.md`

**Interfaces:**
- Consumes: verified commands from Tasks 2–3.
- Produces: operator documentation.

- [ ] **Step 1: Add a Backups section to the README**

Insert after the "Deploy" section of `corpscout/infra/clickhouse/README.md`:

```markdown
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

Note: watch's backup chain restarts (new full) if the sidecar container is
recreated; a host reboot therefore triggers an off-schedule full upload.
```

- [ ] **Step 2: Commit**

```bash
git add corpscout/infra/clickhouse/README.md
git commit -m "docs(corpscout/infra): clickhouse backup operations runbook"
```

---

## Self-Review Notes

- Spec coverage: sidecar + pinned image (Task 1), env-driven config (Task 1), ansible rollout with no ClickHouse restart (Task 2), first-full + B2 object verification (Task 2), restore round trip (Task 3), README restore/monitoring docs (Task 4). Retention is config-only (`BACKUPS_TO_KEEP_REMOTE=28`, enforced by watch) — no separate task needed.
- Deviation from spec: the spec said "trigger the first full manually (`create_remote`)"; watch mode creates the first full automatically on container start, and running a manual `create_remote` concurrently could collide with it. Task 2 verifies watch's own first full instead — same outcome, no concurrency risk.
- Types/names consistent: `clickhouse-backup` service name → container `clickhouse-clickhouse-backup-1` (compose project `clickhouse`); env keys match between compose, template, and vars.
