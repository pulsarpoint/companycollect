# provider-recon ClickHouse mapping and Dagster assets Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** ClickHouse maps the `provider-recon` bucket through an S3-engine table. A daily Dagster job then:
1. triggers the provider-recon service (`provider_recon_documents`), and
2. upserts the documents into normalised ClickHouse tables (`provider_recon_clickhouse`).

Those tables form the permanent range and rule timeline that historical detection will join against.

**Architecture:**
- **Migration:**
  - `provider_recon_documents_s3` (S3 engine over the `provider_recon` named collection, `JSONAsString`)
  - three ReplacingMergeTree tables versioned by `loaded_at`: `provider_services`, `provider_ip_ranges` (with pre-computed IPv6 `range_start`/`range_end`), `provider_rules`
  - the view `provider_ip_ranges_current`
- **Loader:** pure SQL (`INSERT … SELECT` with `ARRAY JOIN` over the JSON), built by `defs/provider_recon/sql.py` and tested against a real ClickHouse engine through `tests/clickhouse_local.py`.
- **Service client:** `ProviderReconResource` (plain `requests`, like the translator resource) starts a collect and polls it.
- **Scheduling:** job `provider_recon_job`, schedule `provider_recon_daily` (03:12 UTC, created stopped).

**Tech Stack:** Python 3.14, Dagster (`dg`), dagster_clickhouse, `requests`, pytest, ClickHouse 26.5 (`clickhouse-local` via Docker for tests).

**Spec:** `docs/superpowers/specs/2026-09-27-provider-recon-service-design.md`, section "Stage 3: service, ClickHouse mapping and Dagster".

**Depends on:** `docs/superpowers/plans/2026-09-28-provider-recon-service.md`, which must be executed and deployed first. It creates the named collection `provider_recon` (without it the migration fails) and runs the service the first asset calls.

## Global Constraints

- Work in `corpscout/services/dagster_v3` using `uv run` (`uv run pytest …`, `uv run dg check defs`). Follow `services/dagster_v3/CLAUDE.md`:
  - The migration owns the schema; the code asserts the tables exist.
  - No `from __future__ import annotations` in asset modules.
  - Register the migration in `EXPECTED_MIGRATIONS`.
  - Every migration has a `.down.sql`.
  - `ORDER BY` columns are never Nullable.
- **Migration number:** the next free number after checking both `main` and the prod ledger (`schema_migrations`); memory: collisions happened before. This plan writes `000460`; renumber if taken.
- **Load semantics:** upsert only (`INSERT`), never `EXCHANGE`/`TRUNCATE`/`DROP PARTITION`. Rows for instances that left `latest.json` must survive. The loader refuses to run when the S3 table has zero documents.
- **Keys:**
  - `provider_services` (provider_slug, service_key)
  - `provider_ip_ranges` (provider_slug, service_key, cidr, collector, first_seen)
  - `provider_rules` (provider_slug, service_key, kind, rule_key, first_seen)

  `rule_key` mirrors the Go `Key()`:
  - dns: `record_type match_field matcher_type pattern`
  - http: `http_part header_name matcher_type pattern`
  - ptr: `matcher_type pattern`
  - certificate: `identity_type identity_value`
  - asn: `AS<n>`
- **Addresses:** `range_start`/`range_end` are `IPv6`, with IPv4 mapped as `::ffff:a.b.c.d`.
- **Service:** reached at `http://companycollect.taileb086.ts.net:8095` (full hostname). The token comes from `PROVIDER_RECON_API_TOKEN` in the Dagster server's `.env`.
- **Schedule:** `provider_recon_daily`, cron `12 3 * * *` UTC, `DefaultScheduleStatus.STOPPED`. Start it on the instance after the first verified run.
- Commit by explicit path from the `corpscout` root (the tree carries unrelated in-progress work). Conventional Commits, each ending with `Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>`.
- Deploys and one-off launches follow `docs/deployment-runbook.md` and the memory recipes (worktree deploy recipe, the server one-off `dg launch` recipe, the deploy lock). Order: **migrate → deploy code → materialize.**

## Review Focus

1. **A range instance that disappears from `latest.json`** (purged after 90 days). Expected: its last loaded row stays in `provider_ip_ranges`; nothing deletes it. *(Task 2: `test_load_keeps_rows_absent_from_later_documents`.)*
2. **Re-loading identical documents.** Expected: after `FINAL`, still one row per key, with the newest `loaded_at`. *(Task 2: `test_reload_is_idempotent_after_final`.)*
3. **IPv4 and IPv6 CIDRs, and a removed instance plus its re-added successor with the same CIDR.** Expected: correct mapped ranges; two rows keyed by different `first_seen`. *(Task 2: fixture covers both.)*
4. **A service run that fails, the service busy (409), or a run that never finishes.** Expected: the documents asset fails with the service's error; no load runs. *(Task 3 resource tests; Task 4: `test_documents_asset_fails_when_run_fails`.)*
5. **An empty or unreachable S3 mapping.** Expected: the ClickHouse asset refuses to load and reports why. *(Task 4: `test_clickhouse_asset_refuses_empty_mapping`.)*

---

## File Structure

```
clickhouse/migrations/000460_corpscout_provider_recon.up.sql / .down.sql
services/dagster_v3/tests/test_clickhouse_migrations.py          + EXPECTED_MIGRATIONS entry
services/dagster_v3/src/dagster_v3/defs/provider_recon/__init__.py
services/dagster_v3/src/dagster_v3/defs/provider_recon/tables.py  table names + column tuples
services/dagster_v3/src/dagster_v3/defs/provider_recon/sql.py     loader SQL builders
services/dagster_v3/src/dagster_v3/defs/provider_recon/resource.py
services/dagster_v3/src/dagster_v3/defs/provider_recon/assets.py  assets, check, job, schedule, defs
services/dagster_v3/tests/fixtures/provider_recon/aws_latest.json
services/dagster_v3/tests/test_provider_recon_sql.py              clickhouse-local integration
services/dagster_v3/tests/test_provider_recon_resource.py
services/dagster_v3/tests/test_provider_recon_assets.py
```

---

### Task 1: Migration

**Files:**
- Create: `clickhouse/migrations/000460_corpscout_provider_recon.up.sql`, `.down.sql`
- Modify: `services/dagster_v3/tests/test_clickhouse_migrations.py` (the `EXPECTED_MIGRATIONS` tuple, plus one test)

- [ ] **Step 1: Confirm the number**

```bash
ls clickhouse/migrations | tail -2
ssh companycollect "docker exec clickhouse-clickhouse-1 clickhouse-client -q \"SELECT max(version) FROM corpscout.schema_migrations\""
```
Use the next number above both. If it isn't 000460, replace `000460` everywhere in this plan's files.

- [ ] **Step 2: Write the failing test** (append to `tests/test_clickhouse_migrations.py`)

```python
def test_provider_recon_migration_maps_s3_without_credentials() -> None:
    up = (MIGRATIONS_DIR / "000460_corpscout_provider_recon.up.sql").read_text()
    assert "ENGINE = S3(provider_recon, filename = 'providers/*/latest.json', format = 'JSONAsString')" in up
    for secret_marker in ("access_key", "secret", "aws_", "http://", "https://"):
        assert secret_marker not in up.lower(), secret_marker
    for table in ("provider_services", "provider_ip_ranges", "provider_rules"):
        assert f"CREATE TABLE IF NOT EXISTS corpscout.{table}" in up
        assert "ENGINE = ReplacingMergeTree(loaded_at)" in up
    assert "CREATE VIEW IF NOT EXISTS corpscout.provider_ip_ranges_current" in up
```

If the test module names the migrations directory differently than `MIGRATIONS_DIR`, use the existing constant. Check the top of the file.

Add `"000460_corpscout_provider_recon",` after `"000459_corpscout_website_company_lookup_results",` in `EXPECTED_MIGRATIONS`.

- [ ] **Step 3: Run to verify failure**

Run: `uv run pytest tests/test_clickhouse_migrations.py -q`
Expected: FAIL (the migration file is missing).

- [ ] **Step 4: Write the migration**

`clickhouse/migrations/000460_corpscout_provider_recon.up.sql`:
```sql
CREATE DATABASE IF NOT EXISTS corpscout;

-- provider-recon documents read live from the provider-recon bucket, one row
-- per provider (providers/<slug>/latest.json). Credentials and the endpoint
-- live in the operator-owned named collection provider_recon, created by
-- services/provider_recon/ansible from stdin; nothing secret is in this file.
CREATE TABLE IF NOT EXISTS corpscout.provider_recon_documents_s3
(
    json String
)
ENGINE = S3(provider_recon, filename = 'providers/*/latest.json', format = 'JSONAsString');

-- Providers and their services as last published. Upserted by the Dagster
-- asset provider_recon_clickhouse; read with FINAL.
CREATE TABLE IF NOT EXISTS corpscout.provider_services
(
    provider_slug LowCardinality(String),
    provider_name String,
    provider_category LowCardinality(String),
    provider_country LowCardinality(String),
    provider_website String,
    provider_keys Array(String),
    service_key String,
    service_name String,
    service_types Array(LowCardinality(String)),
    traits Array(LowCardinality(String)),
    removed_at Nullable(Date),
    collected_at DateTime64(3, 'UTC'),
    content_hash String,
    loaded_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(loaded_at)
ORDER BY (provider_slug, service_key);

-- The permanent range timeline: one row per range instance (a removed range
-- that returns is a new instance with its own first_seen). Loads upsert and
-- never delete, so instances purged from latest.json after 90 days keep their
-- last state here. range_start/range_end are IPv6 with IPv4 mapped
-- (::ffff:a.b.c.d) for interval joins against DNS seen-windows.
CREATE TABLE IF NOT EXISTS corpscout.provider_ip_ranges
(
    provider_slug LowCardinality(String),
    service_key String,
    cidr String,
    ip_family UInt8,
    range_start IPv6,
    range_end IPv6,
    collector LowCardinality(String),
    source LowCardinality(String),
    feed_tag LowCardinality(String),
    region LowCardinality(String),
    confidence Float32,
    status LowCardinality(String),
    first_seen Date,
    last_seen Date,
    missing_since Nullable(Date),
    removed_at Nullable(Date),
    removal_action LowCardinality(String),
    restored_at Nullable(Date),
    source_url String,
    source_version String,
    loaded_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(loaded_at)
ORDER BY (provider_slug, service_key, cidr, collector, first_seen);

-- Non-range evidence (DNS / HTTP / PTR / certificate / ASN) with the same
-- lifecycle. kind + rule_key mirror the Go model's Key() per kind.
CREATE TABLE IF NOT EXISTS corpscout.provider_rules
(
    provider_slug LowCardinality(String),
    service_key String,
    kind LowCardinality(String),
    rule_key String,
    record_type LowCardinality(String),
    match_field LowCardinality(String),
    http_part LowCardinality(String),
    header_name String,
    matcher_type LowCardinality(String),
    pattern String,
    case_sensitive UInt8,
    path_scope String,
    identity_type LowCardinality(String),
    asn UInt32,
    confidence Float32,
    priority Int32,
    note String,
    source LowCardinality(String),
    source_url String,
    status LowCardinality(String),
    first_seen Date,
    last_seen Date,
    missing_since Nullable(Date),
    removed_at Nullable(Date),
    removal_action LowCardinality(String),
    restored_at Nullable(Date),
    loaded_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(loaded_at)
ORDER BY (provider_slug, service_key, kind, rule_key, first_seen);

-- Ranges usable for present-day lookups: active and missing (still within
-- grace) instances, latest loaded state.
CREATE VIEW IF NOT EXISTS corpscout.provider_ip_ranges_current AS
SELECT *
FROM corpscout.provider_ip_ranges FINAL
WHERE status != 'removed';
```

`clickhouse/migrations/000460_corpscout_provider_recon.down.sql`:
```sql
DROP VIEW IF EXISTS corpscout.provider_ip_ranges_current;
DROP TABLE IF EXISTS corpscout.provider_rules;
DROP TABLE IF EXISTS corpscout.provider_ip_ranges;
DROP TABLE IF EXISTS corpscout.provider_services;
DROP TABLE IF EXISTS corpscout.provider_recon_documents_s3;
```

- [ ] **Step 5: Run to verify pass**

Run: `uv run pytest tests/test_clickhouse_migrations.py -q`
Expected: PASS. This includes the existing guards: comments have no semicolons, databases and tables are created, and down files exist.

- [ ] **Step 6: Commit**

```bash
git add clickhouse/migrations/000460_corpscout_provider_recon.up.sql clickhouse/migrations/000460_corpscout_provider_recon.down.sql services/dagster_v3/tests/test_clickhouse_migrations.py
git commit -m "feat(clickhouse): provider-recon S3 mapping and normalised range/rule timeline tables (000460)"
```

---

### Task 2: Loader SQL, tested against a real engine

**Files:**
- Create:
  - `src/dagster_v3/defs/provider_recon/__init__.py` (empty)
  - `src/dagster_v3/defs/provider_recon/tables.py`
  - `src/dagster_v3/defs/provider_recon/sql.py`
  - `tests/fixtures/provider_recon/aws_latest.json`
  - `tests/test_provider_recon_sql.py`

**Interfaces:**
- Produces (module `dagster_v3.defs.provider_recon.sql`):
  - `documents_count_sql(database: str) -> str`
  - `load_statements(database: str, source_table: str = DOCUMENTS_S3_TABLE) -> list[tuple[str, str]]`, returning (target table, INSERT … SELECT) pairs. Every statement takes the `%(loaded_at)s` parameter.
- Produces (`tables.py`): `DOCUMENTS_S3_TABLE`, `SERVICES_TABLE`, `IP_RANGES_TABLE`, `RULES_TABLE`, `CURRENT_VIEW`, `ALL_TABLES`.

- [ ] **Step 1: Write the fixture** — `tests/fixtures/provider_recon/aws_latest.json`

```json
{
  "version": "provider-recon/v1",
  "slug": "aws",
  "display_name": "Amazon Web Services",
  "category": "cloud",
  "website": "https://aws.amazon.com",
  "country": "US",
  "aliases": ["AWS"],
  "provider_keys": ["amazonaws.com", "cloudfront.net"],
  "services": [
    {
      "service_key": "aws.cloudfront",
      "display_name": "Amazon CloudFront",
      "service_types": ["cdn"],
      "traits": ["origin_obscured", "shared_infrastructure"],
      "evidence": {
        "ip_ranges": [
          {"cidr": "52.84.0.0/15", "feed_tag": "CLOUDFRONT", "region": "GLOBAL", "confidence": 1, "source": "official_feed", "collector": "aws_ip_ranges", "source_url": "https://ip-ranges.amazonaws.com/ip-ranges.json", "source_version": "syncToken=1", "status": "active", "first_seen": "2026-09-27", "last_seen": "2026-09-28"},
          {"cidr": "2600:9000::/28", "feed_tag": "CLOUDFRONT", "confidence": 1, "source": "official_feed", "collector": "aws_ip_ranges", "status": "missing", "first_seen": "2026-09-27", "last_seen": "2026-09-27", "missing_since": "2026-09-28"},
          {"cidr": "13.32.0.0/15", "feed_tag": "CLOUDFRONT", "confidence": 1, "source": "official_feed", "collector": "aws_ip_ranges", "status": "removed", "first_seen": "2026-06-01", "last_seen": "2026-06-10", "missing_since": "2026-06-11", "removed_at": "2026-06-18", "removal_action": "grace_expired"},
          {"cidr": "13.32.0.0/15", "feed_tag": "CLOUDFRONT", "confidence": 1, "source": "official_feed", "collector": "aws_ip_ranges", "status": "active", "first_seen": "2026-07-01", "last_seen": "2026-09-28", "restored_at": ""}
        ],
        "asns": [{"asn": 16509, "confidence": 0.6, "source": "curated", "status": "active", "first_seen": "2026-09-27", "last_seen": "2026-09-28"}],
        "dns_rules": [{"record_type": "CNAME", "match_field": "target", "matcher_type": "suffix", "pattern": "cloudfront.net", "confidence": 1, "priority": 100, "source": "curated", "status": "active", "first_seen": "2026-09-27", "last_seen": "2026-09-28"}],
        "http_rules": [{"http_part": "header", "header_name": "x-amz-cf-id", "matcher_type": "exists", "pattern": "", "confidence": 1, "priority": 100, "source": "curated", "status": "removed", "first_seen": "2026-09-27", "last_seen": "2026-09-27", "removed_at": "2026-09-28", "removal_action": "definition_removed"}],
        "ptr_rules": [{"matcher_type": "suffix", "pattern": "cloudfront.net", "confidence": 0.9, "source": "curated", "status": "active", "first_seen": "2026-09-27", "last_seen": "2026-09-28"}],
        "certificate_identities": []
      }
    }
  ],
  "collection": {"collected_at": "2026-09-28T03:12:05Z", "collectors": {}, "content_hash": "sha256:abc"},
  "candidates": []
}
```

- [ ] **Step 2: Write the failing tests** — `tests/test_provider_recon_sql.py`

```python
"""The provider-recon loader SQL, run against a real ClickHouse engine."""

import json
import re
import subprocess
from datetime import datetime
from pathlib import Path

from dagster_v3.defs.provider_recon import sql, tables
from tests.clickhouse_local import clickhouse_local_command, render

REPO = Path(__file__).resolve().parents[3]
MIGRATION = REPO / "clickhouse" / "migrations" / "000460_corpscout_provider_recon.up.sql"
FIXTURE = Path(__file__).parent / "fixtures" / "provider_recon" / "aws_latest.json"
T1 = datetime(2026, 9, 28, 3, 12, 30)
T2 = datetime(2026, 9, 29, 3, 12, 30)


def schema_without_s3() -> str:
    """The migration's tables, with the S3 mapping swapped for a Memory table."""
    statements = [s.strip() for s in MIGRATION.read_text().split(";") if s.strip()]
    kept = []
    for s in statements:
        if tables.DOCUMENTS_S3_TABLE in s and "ENGINE = S3" in s:
            kept.append(f"CREATE TABLE corpscout.{tables.DOCUMENTS_S3_TABLE} (json String) ENGINE = Memory")
        else:
            kept.append(s)
    return ";\n".join(kept) + ";"


def run(sql_text: str) -> list[list]:
    result = subprocess.run(clickhouse_local_command(), input=sql_text, capture_output=True, text=True, timeout=120)
    assert result.returncode == 0, result.stderr
    return [json.loads(line) for line in result.stdout.splitlines() if line.strip()]


def insert_doc(doc: dict) -> str:
    raw = json.dumps(doc).replace("\\", "\\\\").replace("'", "\\'")
    return f"INSERT INTO corpscout.{tables.DOCUMENTS_S3_TABLE} VALUES ('{raw}');\n"


def load(at: datetime) -> str:
    return "".join(render(stmt, {"loaded_at": at}) + ";\n" for _, stmt in sql.load_statements("corpscout"))


def fixture() -> dict:
    return json.loads(FIXTURE.read_text())


def test_load_normalises_services_ranges_and_rules() -> None:
    rows = run(
        schema_without_s3()
        + insert_doc(fixture())
        + load(T1)
        + """
SELECT provider_slug, provider_name, service_key, service_types, provider_keys, toString(collected_at) FROM corpscout.provider_services FINAL FORMAT JSONCompactEachRow;
SELECT cidr, ip_family, toString(range_start), toString(range_end), status, toString(first_seen), toString(removed_at), removal_action FROM corpscout.provider_ip_ranges FINAL ORDER BY cidr, first_seen FORMAT JSONCompactEachRow;
SELECT kind, rule_key, status, removal_action FROM corpscout.provider_rules FINAL ORDER BY kind FORMAT JSONCompactEachRow;
SELECT cidr FROM corpscout.provider_ip_ranges_current ORDER BY cidr FORMAT JSONCompactEachRow;
"""
    )
    assert rows[0] == ["aws", "Amazon Web Services", "aws.cloudfront", ["cdn"], ["amazonaws.com", "cloudfront.net"], "2026-09-28 03:12:05.000"]
    ranges = rows[1:5]
    assert ranges == [
        ["13.32.0.0/15", 4, "::ffff:13.32.0.0", "::ffff:13.33.255.255", "removed", "2026-06-01", "2026-06-18", "grace_expired"],
        ["13.32.0.0/15", 4, "::ffff:13.32.0.0", "::ffff:13.33.255.255", "active", "2026-07-01", None, ""],
        ["2600:9000::/28", 6, "2600:9000::", "2600:900f:ffff:ffff:ffff:ffff:ffff:ffff", "missing", "2026-09-27", None, ""],
        ["52.84.0.0/15", 4, "::ffff:52.84.0.0", "::ffff:52.85.255.255", "active", "2026-09-27", None, ""],
    ]
    rules = rows[5:9]
    assert rules == [
        ["asn", "AS16509", "active", ""],
        ["dns", "CNAME target suffix cloudfront.net", "active", ""],
        ["http", "header x-amz-cf-id exists ", "removed", "definition_removed"],
        ["ptr", "suffix cloudfront.net", "active", ""],
    ]
    assert rows[9:] == [["13.32.0.0/15"], ["2600:9000::/28"], ["52.84.0.0/15"]]


def test_reload_is_idempotent_after_final() -> None:
    rows = run(
        schema_without_s3()
        + insert_doc(fixture())
        + load(T1)
        + load(T2)
        + """
SELECT count(), toString(max(loaded_at)) FROM corpscout.provider_ip_ranges FINAL FORMAT JSONCompactEachRow;
SELECT count() FROM corpscout.provider_rules FINAL FORMAT JSONCompactEachRow;
"""
    )
    assert rows == [[4, "2026-09-29 03:12:30.000"], [4]]


def test_load_keeps_rows_absent_from_later_documents() -> None:
    later = fixture()
    ranges = later["services"][0]["evidence"]["ip_ranges"]
    later["services"][0]["evidence"]["ip_ranges"] = [r for r in ranges if r["first_seen"] != "2026-06-01"]
    rows = run(
        schema_without_s3()
        + insert_doc(fixture())
        + load(T1)
        + f"TRUNCATE TABLE corpscout.{tables.DOCUMENTS_S3_TABLE};\n"
        + insert_doc(later)
        + load(T2)
        + """
SELECT status, toString(removed_at), toString(loaded_at) FROM corpscout.provider_ip_ranges FINAL WHERE first_seen = '2026-06-01' FORMAT JSONCompactEachRow;
"""
    )
    assert rows == [["removed", "2026-06-18", "2026-09-28 03:12:30.000"]]


def test_documents_count_sql() -> None:
    rows = run(schema_without_s3() + insert_doc(fixture()) + sql.documents_count_sql("corpscout") + " FORMAT JSONCompactEachRow;")
    assert rows == [[1]]


def test_insert_columns_match_the_migration() -> None:
    ddl = MIGRATION.read_text()
    for table, stmt in sql.load_statements("corpscout"):
        body = re.search(rf"CREATE TABLE IF NOT EXISTS corpscout\.{table}\s*\((.*?)\)\s*ENGINE", ddl, re.S).group(1)
        columns = [line.strip().split()[0] for line in body.strip().splitlines() if line.strip()]
        inserted = re.search(r"INSERT INTO `?\w+`?\.`?\w+`?\s*\((.*?)\)", stmt, re.S).group(1)
        assert [c.strip() for c in inserted.split(",")] == columns, table
```

- [ ] **Step 3: Run to verify failure**

Run: `uv run pytest tests/test_provider_recon_sql.py -q`
Expected: FAIL (`ModuleNotFoundError: dagster_v3.defs.provider_recon`). If Docker isn't running and there is no `clickhouse-local`, the helper skips. Start Docker Desktop; the tests must run, not skip.

- [ ] **Step 4: Implement**

`src/dagster_v3/defs/provider_recon/tables.py`:
```python
"""ClickHouse objects owned by migration 000460 (provider-recon)."""

DOCUMENTS_S3_TABLE = "provider_recon_documents_s3"
SERVICES_TABLE = "provider_services"
IP_RANGES_TABLE = "provider_ip_ranges"
RULES_TABLE = "provider_rules"
CURRENT_VIEW = "provider_ip_ranges_current"

ALL_TABLES = (DOCUMENTS_S3_TABLE, SERVICES_TABLE, IP_RANGES_TABLE, RULES_TABLE, CURRENT_VIEW)
```

`src/dagster_v3/defs/provider_recon/sql.py`:
```python
"""INSERT … SELECT statements that normalise provider-recon documents.

The source is the S3-engine table (one JSON document per provider). Every
statement upserts into a ReplacingMergeTree(loaded_at) table and deletes
nothing, so instances that later leave latest.json keep their last state.
"""

from dagster_v3.defs.provider_recon.tables import (
    DOCUMENTS_S3_TABLE,
    IP_RANGES_TABLE,
    RULES_TABLE,
    SERVICES_TABLE,
)

_LIFECYCLE = """
    if(JSONExtractString(item, 'status') = '', 'active', JSONExtractString(item, 'status')) AS status,
    toDateOrZero(JSONExtractString(item, 'first_seen')) AS first_seen,
    toDateOrZero(JSONExtractString(item, 'last_seen')) AS last_seen,
    toDateOrNull(JSONExtractString(item, 'missing_since')) AS missing_since,
    toDateOrNull(JSONExtractString(item, 'removed_at')) AS removed_at,
    JSONExtractString(item, 'removal_action') AS removal_action,
    toDateOrNull(JSONExtractString(item, 'restored_at')) AS restored_at"""


def documents_count_sql(database: str, source_table: str = DOCUMENTS_S3_TABLE) -> str:
    return f"SELECT count() FROM `{database}`.`{source_table}`"


def _services_sql(database: str, source: str) -> str:
    return f"""INSERT INTO `{database}`.`{SERVICES_TABLE}` (provider_slug, provider_name, provider_category, provider_country, provider_website, provider_keys, service_key, service_name, service_types, traits, removed_at, collected_at, content_hash, loaded_at)
SELECT
    JSONExtractString(json, 'slug'),
    JSONExtractString(json, 'display_name'),
    JSONExtractString(json, 'category'),
    JSONExtractString(json, 'country'),
    JSONExtractString(json, 'website'),
    JSONExtract(json, 'provider_keys', 'Array(String)'),
    JSONExtractString(svc, 'service_key'),
    JSONExtractString(svc, 'display_name'),
    JSONExtract(svc, 'service_types', 'Array(String)'),
    JSONExtract(svc, 'traits', 'Array(String)'),
    toDateOrNull(JSONExtractString(svc, 'removed_at')),
    parseDateTime64BestEffort(JSONExtractString(json, 'collection', 'collected_at'), 3, 'UTC'),
    JSONExtractString(json, 'collection', 'content_hash'),
    %(loaded_at)s
FROM `{database}`.`{source}`
ARRAY JOIN JSONExtractArrayRaw(json, 'services') AS svc"""


def _ip_ranges_sql(database: str, source: str) -> str:
    return f"""INSERT INTO `{database}`.`{IP_RANGES_TABLE}` (provider_slug, service_key, cidr, ip_family, range_start, range_end, collector, source, feed_tag, region, confidence, status, first_seen, last_seen, missing_since, removed_at, removal_action, restored_at, source_url, source_version, loaded_at)
SELECT
    provider_slug, service_key, cidr,
    if(is_v4, 4, 6) AS ip_family,
    if(is_v4, toIPv6(IPv4NumToString(tupleElement(IPv4CIDRToRange(toIPv4(addr), prefix_len), 1))),
              tupleElement(IPv6CIDRToRange(toIPv6(addr), prefix_len), 1)) AS range_start,
    if(is_v4, toIPv6(IPv4NumToString(tupleElement(IPv4CIDRToRange(toIPv4(addr), prefix_len), 2))),
              tupleElement(IPv6CIDRToRange(toIPv6(addr), prefix_len), 2)) AS range_end,
    collector, source, feed_tag, region, confidence,
    status, first_seen, last_seen, missing_since, removed_at, removal_action, restored_at,
    source_url, source_version,
    %(loaded_at)s
FROM (
    SELECT
        JSONExtractString(json, 'slug') AS provider_slug,
        JSONExtractString(svc, 'service_key') AS service_key,
        JSONExtractString(item, 'cidr') AS cidr,
        splitByChar('/', cidr)[1] AS addr,
        toUInt8(splitByChar('/', cidr)[2]) AS prefix_len,
        isIPv4String(addr) AS is_v4,
        JSONExtractString(item, 'collector') AS collector,
        JSONExtractString(item, 'source') AS source,
        JSONExtractString(item, 'feed_tag') AS feed_tag,
        JSONExtractString(item, 'region') AS region,
        toFloat32(JSONExtractFloat(item, 'confidence')) AS confidence,
        JSONExtractString(item, 'source_url') AS source_url,
        JSONExtractString(item, 'source_version') AS source_version,{_LIFECYCLE}
    FROM `{database}`.`{source}`
    ARRAY JOIN JSONExtractArrayRaw(json, 'services') AS svc
    ARRAY JOIN JSONExtractArrayRaw(svc, 'evidence', 'ip_ranges') AS item
)"""


# Per kind: the evidence array and the kind-specific column expressions. Any
# rule column not listed takes its type's empty value.
_RULE_KINDS = {
    "asn": ("asns", {
        "rule_key": "concat('AS', toString(JSONExtractUInt(item, 'asn')))",
        "asn": "toUInt32(JSONExtractUInt(item, 'asn'))",
    }),
    "dns": ("dns_rules", {
        "rule_key": "concat(JSONExtractString(item, 'record_type'), ' ', JSONExtractString(item, 'match_field'), ' ', JSONExtractString(item, 'matcher_type'), ' ', JSONExtractString(item, 'pattern'))",
        "record_type": "JSONExtractString(item, 'record_type')",
        "match_field": "JSONExtractString(item, 'match_field')",
        "matcher_type": "JSONExtractString(item, 'matcher_type')",
        "pattern": "JSONExtractString(item, 'pattern')",
        "case_sensitive": "toUInt8(JSONExtractBool(item, 'case_sensitive'))",
        "priority": "toInt32(JSONExtractInt(item, 'priority'))",
    }),
    "http": ("http_rules", {
        "rule_key": "concat(JSONExtractString(item, 'http_part'), ' ', JSONExtractString(item, 'header_name'), ' ', JSONExtractString(item, 'matcher_type'), ' ', JSONExtractString(item, 'pattern'))",
        "http_part": "JSONExtractString(item, 'http_part')",
        "header_name": "JSONExtractString(item, 'header_name')",
        "matcher_type": "JSONExtractString(item, 'matcher_type')",
        "pattern": "JSONExtractString(item, 'pattern')",
        "case_sensitive": "toUInt8(JSONExtractBool(item, 'case_sensitive'))",
        "path_scope": "JSONExtractString(item, 'path_scope')",
        "priority": "toInt32(JSONExtractInt(item, 'priority'))",
    }),
    "ptr": ("ptr_rules", {
        "rule_key": "concat(JSONExtractString(item, 'matcher_type'), ' ', JSONExtractString(item, 'pattern'))",
        "matcher_type": "JSONExtractString(item, 'matcher_type')",
        "pattern": "JSONExtractString(item, 'pattern')",
    }),
    "certificate": ("certificate_identities", {
        "rule_key": "concat(JSONExtractString(item, 'identity_type'), ' ', JSONExtractString(item, 'identity_value'))",
        "identity_type": "JSONExtractString(item, 'identity_type')",
        "pattern": "JSONExtractString(item, 'identity_value')",
    }),
}

# Kind-specific columns in migration order with their empty values.
_RULE_SPECIFIC = (
    ("rule_key", "''"), ("record_type", "''"), ("match_field", "''"), ("http_part", "''"),
    ("header_name", "''"), ("matcher_type", "''"), ("pattern", "''"), ("case_sensitive", "toUInt8(0)"),
    ("path_scope", "''"), ("identity_type", "''"), ("asn", "toUInt32(0)"),
)

RULE_COLUMNS = (
    "provider_slug", "service_key", "kind", *(name for name, _ in _RULE_SPECIFIC),
    "confidence", "priority", "note", "source", "source_url",
    "status", "first_seen", "last_seen", "missing_since", "removed_at", "removal_action", "restored_at",
    "loaded_at",
)


def _rules_select(database: str, source: str, kind: str) -> str:
    array, specific = _RULE_KINDS[kind]
    columns = ",\n    ".join(f"{specific.get(name, empty)} AS {name}" for name, empty in _RULE_SPECIFIC)
    priority = specific.get("priority", "toInt32(0)")
    return f"""SELECT
    JSONExtractString(json, 'slug') AS provider_slug,
    JSONExtractString(svc, 'service_key') AS service_key,
    '{kind}' AS kind,
    {columns},
    toFloat32(JSONExtractFloat(item, 'confidence')) AS confidence,
    {priority} AS priority,
    JSONExtractString(item, 'note') AS note,
    JSONExtractString(item, 'source') AS source,
    JSONExtractString(item, 'source_url') AS source_url,{_LIFECYCLE},
    %(loaded_at)s AS loaded_at
FROM `{database}`.`{source}`
ARRAY JOIN JSONExtractArrayRaw(json, 'services') AS svc
ARRAY JOIN JSONExtractArrayRaw(svc, 'evidence', '{array}') AS item"""


def _rules_sql(database: str, source: str) -> str:
    union = "\nUNION ALL\n".join(_rules_select(database, source, kind) for kind in _RULE_KINDS)
    return f"INSERT INTO `{database}`.`{RULES_TABLE}` ({', '.join(RULE_COLUMNS)})\n{union}"


def load_statements(database: str, source_table: str = DOCUMENTS_S3_TABLE) -> list[tuple[str, str]]:
    """(target table, statement) in load order; each binds %(loaded_at)s."""
    return [
        (SERVICES_TABLE, _services_sql(database, source_table)),
        (IP_RANGES_TABLE, _ip_ranges_sql(database, source_table)),
        (RULES_TABLE, _rules_sql(database, source_table)),
    ]
```

- [ ] **Step 5: Run to verify pass**

Run: `uv run pytest tests/test_provider_recon_sql.py -v`
Expected: 5 PASS (not skipped).
- If the `http` rule_key assertion differs only by the trailing space (the `exists` pattern is empty), the Go `Key()` produces the same trailing space. Keep it.
- If the `JSONCompactEachRow` rendering of `IPv6` or `Nullable(Date)` differs from the expected literals, adjust the test expectation to the engine's canonical rendering and ledger it. Don't change the stored types.

- [ ] **Step 6: Commit**

```bash
git add services/dagster_v3/src/dagster_v3/defs/provider_recon services/dagster_v3/tests/fixtures/provider_recon services/dagster_v3/tests/test_provider_recon_sql.py
git commit -m "feat(dagster): provider-recon loader SQL normalising S3 documents into ClickHouse timeline tables"
```

---

### Task 3: Service client resource

**Files:**
- Create: `src/dagster_v3/defs/provider_recon/resource.py`, `tests/test_provider_recon_resource.py`

**Interfaces:**
- Produces:
  - `class ProviderReconError(Exception)`
  - `class ProviderReconResource(dg.ConfigurableResource)`, with fields `api_url` (default `http://companycollect.taileb086.ts.net:8095`), `api_token`, `request_timeout_s`
  - `start_collect(providers=()) -> dict`
  - `get_run(run_id) -> dict`
  - `wait_for_run(run_id, *, timeout_s=3600, poll_s=10, sleep=time.sleep, clock=time.monotonic) -> dict`

- [ ] **Step 1: Write the failing tests** — `tests/test_provider_recon_resource.py`

```python
"""ProviderReconResource against a real local HTTP server."""

import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

from dagster_v3.defs.provider_recon.resource import ProviderReconError, ProviderReconResource

TOKEN = "t" * 40


class _API(BaseHTTPRequestHandler):
    state: dict = {}

    def _send(self, status: int, body: dict) -> None:
        payload = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def do_POST(self) -> None:
        self.state.setdefault("auth", []).append(self.headers.get("Authorization"))
        length = int(self.headers.get("Content-Length") or 0)
        self.state.setdefault("bodies", []).append(json.loads(self.rfile.read(length) or b"{}"))
        if self.state.get("busy"):
            self._send(409, {"error": "an operation is already running", "run_id": "r0"})
        else:
            self._send(202, {"run_id": "r1", "status": "running"})

    def do_GET(self) -> None:
        polls = self.state["polls"] = self.state.get("polls", 0) + 1
        status = "running" if polls < 3 else self.state.get("final", "succeeded")
        self._send(200, {"run_id": "r1", "status": status, "changed": ["aws"], "unchanged_count": 36, "issues": []})

    def log_message(self, *args) -> None:
        pass


@pytest.fixture
def api():
    _API.state = {}
    server = HTTPServer(("127.0.0.1", 0), _API)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield ProviderReconResource(api_url=f"http://127.0.0.1:{server.server_port}", api_token=TOKEN), _API.state
    server.shutdown()


def test_start_and_wait_until_succeeded(api) -> None:
    resource, state = api
    started = resource.start_collect(["aws"])
    assert started["run_id"] == "r1"
    assert state["auth"] == [f"Bearer {TOKEN}"]
    assert state["bodies"] == [{"providers": ["aws"]}]
    run = resource.wait_for_run("r1", poll_s=0, sleep=lambda _: None)
    assert run["status"] == "succeeded" and state["polls"] == 3


def test_busy_service_raises(api) -> None:
    resource, state = api
    state["busy"] = True
    with pytest.raises(ProviderReconError, match="busy with run r0"):
        resource.start_collect()


def test_wait_times_out(api) -> None:
    resource, _ = api
    ticks = iter([0.0, 0.0, 10.0, 20.0])
    with pytest.raises(ProviderReconError, match="still running"):
        resource.wait_for_run("r1", timeout_s=5, poll_s=0, sleep=lambda _: None, clock=lambda: next(ticks))


def test_token_is_not_in_repr() -> None:
    assert TOKEN not in repr(ProviderReconResource(api_token=TOKEN))
```

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest tests/test_provider_recon_resource.py -q`
Expected: FAIL (`ModuleNotFoundError … resource`).

- [ ] **Step 3: Implement** — `src/dagster_v3/defs/provider_recon/resource.py`

```python
"""Dagster resource for the provider-recon HTTP service on companycollect."""

import time
from collections.abc import Callable, Sequence

import dagster as dg
import requests
from pydantic import Field

# Full tailnet name: bare hostnames break on the dagster host (memory: short hostnames).
DEFAULT_API_URL = "http://companycollect.taileb086.ts.net:8095"
TERMINAL_STATUSES = frozenset({"succeeded", "failed"})


class ProviderReconError(Exception):
    """The service refused, failed or did not finish a run."""


class ProviderReconResource(dg.ConfigurableResource):
    """Starts provider-recon collects and polls them to completion."""

    api_url: str = DEFAULT_API_URL
    api_token: str = Field(repr=False)
    request_timeout_s: float = 30.0

    def _headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self.api_token}"}

    def start_collect(self, providers: Sequence[str] = ()) -> dict:
        body = {"providers": list(providers)} if providers else {}
        response = requests.post(
            f"{self.api_url}/v1/collect", json=body, headers=self._headers(), timeout=self.request_timeout_s
        )
        if response.status_code == 409:
            raise ProviderReconError(f"provider-recon is busy with run {response.json().get('run_id')}")
        if response.status_code != 202:
            raise ProviderReconError(f"collect refused: HTTP {response.status_code} {response.text}")
        return response.json()

    def get_run(self, run_id: str) -> dict:
        response = requests.get(
            f"{self.api_url}/v1/runs/{run_id}", headers=self._headers(), timeout=self.request_timeout_s
        )
        if response.status_code != 200:
            raise ProviderReconError(f"run {run_id}: HTTP {response.status_code} {response.text}")
        return response.json()

    def wait_for_run(
        self,
        run_id: str,
        *,
        timeout_s: float = 3600,
        poll_s: float = 10,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.monotonic,
    ) -> dict:
        deadline = clock() + timeout_s
        while True:
            run = self.get_run(run_id)
            if run["status"] in TERMINAL_STATUSES:
                return run
            if clock() >= deadline:
                raise ProviderReconError(f"run {run_id} still running after {timeout_s:.0f}s")
            sleep(poll_s)
```

- [ ] **Step 4: Run to verify pass**

Run: `uv run pytest tests/test_provider_recon_resource.py -q`
Expected: 4 PASS.

- [ ] **Step 5: Commit**

```bash
git add services/dagster_v3/src/dagster_v3/defs/provider_recon/resource.py services/dagster_v3/tests/test_provider_recon_resource.py
git commit -m "feat(dagster): provider-recon service client resource"
```

---

### Task 4: Assets, check, job and schedule

**Files:**
- Create: `src/dagster_v3/defs/provider_recon/assets.py`, `tests/test_provider_recon_assets.py`

**Interfaces:**
- Consumes: Tasks 2–3; `ClickhouseResource`; `RESOLVED_DATABASE`, `assert_clickhouse_tables_exist`.
- Produces:
  - assets `provider_recon_documents`, `provider_recon_clickhouse`
  - asset check `provider_recon_feeds_ok` (WARN)
  - job `provider_recon_job`
  - schedule `provider_recon_daily` (stopped)
  - module `defs`, which registers resource key `provider_recon`

- [ ] **Step 1: Write the failing tests** — `tests/test_provider_recon_assets.py`

```python
from contextlib import contextmanager

import dagster as dg
import pytest

from dagster_v3.defs.provider_recon import assets
from dagster_v3.defs.provider_recon.resource import ProviderReconResource


class FakeRecon(ProviderReconResource):
    final: dict

    def start_collect(self, providers=()):
        return {"run_id": "20260928T031200Z-collect", "status": "running"}

    def wait_for_run(self, run_id, **_):
        return self.final


class FakeClient:
    def __init__(self, documents: int) -> None:
        self.documents = documents
        self.statements: list[str] = []

    def execute(self, query, params=None, **_):
        self.statements.append(query)
        if "system.tables" in query:
            return [(name,) for name in assets.tables.ALL_TABLES]
        if query.startswith("SELECT count()"):
            return [(self.documents,)]
        return []


class FakeClickhouse:
    def __init__(self, documents: int) -> None:
        self.client = FakeClient(documents)

    @contextmanager
    def get_connection(self):
        yield self.client


def succeeded(issues=()):
    return {"run_id": "20260928T031200Z-collect", "status": "succeeded", "changed": ["aws"], "unchanged_count": 36, "issues": list(issues)}


def test_documents_asset_reports_run_and_passes_check() -> None:
    recon = FakeRecon(api_token="t" * 40, final=succeeded())
    with dg.build_asset_context() as context:
        result = assets.provider_recon_documents(context, provider_recon=recon, clickhouse=FakeClickhouse(37))
    assert result.metadata["run_id"] == "20260928T031200Z-collect"
    assert result.metadata["documents"] == 37
    assert result.check_results[0].passed


def test_documents_asset_warns_on_feed_issues() -> None:
    issue = {"slug": "aws", "collector": "aws_ip_ranges", "status": "stale", "error": "status 503"}
    recon = FakeRecon(api_token="t" * 40, final=succeeded([issue]))
    with dg.build_asset_context() as context:
        result = assets.provider_recon_documents(context, provider_recon=recon, clickhouse=FakeClickhouse(37))
    check = result.check_results[0]
    assert not check.passed and check.severity == dg.AssetCheckSeverity.WARN
    assert "aws aws_ip_ranges stale: status 503" in check.metadata["issues"].value


def test_documents_asset_fails_when_run_fails() -> None:
    recon = FakeRecon(api_token="t" * 40, final={"run_id": "x", "status": "failed", "error": "put failed", "changed": [], "unchanged_count": 0, "issues": []})
    with dg.build_asset_context() as context, pytest.raises(dg.Failure, match="put failed"):
        assets.provider_recon_documents(context, provider_recon=recon, clickhouse=FakeClickhouse(37))


def test_clickhouse_asset_refuses_empty_mapping() -> None:
    with dg.build_asset_context() as context, pytest.raises(ValueError, match="no documents"):
        assets.provider_recon_clickhouse(context, clickhouse=FakeClickhouse(0))


def test_clickhouse_asset_runs_every_load_statement() -> None:
    ch = FakeClickhouse(37)
    with dg.build_asset_context() as context:
        result = assets.provider_recon_clickhouse(context, clickhouse=ch)
    inserts = [s for s in ch.client.statements if s.startswith("INSERT INTO")]
    assert len(inserts) == 3
    assert result.metadata["documents"] == 37


def test_schedule_is_created_stopped() -> None:
    assert assets.provider_recon_daily.default_status == dg.DefaultScheduleStatus.STOPPED
    assert assets.provider_recon_daily.cron_schedule == "12 3 * * *"
```

`assert_clickhouse_tables_exist` may query differently than `system.tables … name`. Read it in `defs/clickhouse/resolved.py` and adapt `FakeClient.execute` to answer its actual query. Existing fakes, e.g. `tests/test_commoncrawl_rdap_assets.py`, show the shape.

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest tests/test_provider_recon_assets.py -q`
Expected: FAIL (`cannot import name 'assets'`).

- [ ] **Step 3: Implement** — `src/dagster_v3/defs/provider_recon/assets.py`

```python
"""provider-recon: trigger the service, then normalise its S3 documents in ClickHouse.

provider_recon_documents runs a collect on the provider-recon service
(companycollect) and reports it; the documents it writes are what the
S3-engine table provider_recon_documents_s3 maps. provider_recon_clickhouse
upserts those documents into the permanent range/rule timeline tables.
"""

from datetime import UTC, datetime

import dagster as dg
from dagster import AssetExecutionContext
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.clickhouse.resolved import RESOLVED_DATABASE, assert_clickhouse_tables_exist
from dagster_v3.defs.provider_recon import sql, tables
from dagster_v3.defs.provider_recon.resource import ProviderReconResource

GROUP_NAME = "provider_recon"
FEEDS_OK_CHECK = "provider_recon_feeds_ok"


@dg.asset(
    name="provider_recon_documents",
    group_name=GROUP_NAME,
    kinds={"s3", "clickhouse"},
    check_specs=[dg.AssetCheckSpec(FEEDS_OK_CHECK, asset="provider_recon_documents",
                                   description="WARN when any provider feed ended stale or failed in the run.")],
    description=(
        "Runs a provider-recon collect on the companycollect service and waits for it. The "
        "per-provider documents land in the provider-recon bucket, mapped in ClickHouse as "
        "corpscout.provider_recon_documents_s3."
    ),
)
def provider_recon_documents(
    context: AssetExecutionContext,
    provider_recon: ProviderReconResource,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    started = provider_recon.start_collect()
    run_id = started["run_id"]
    context.log.info("provider-recon run %s started", run_id)
    run = provider_recon.wait_for_run(run_id)
    if run["status"] != "succeeded":
        raise dg.Failure(description=f"provider-recon run {run_id} failed: {run.get('error', '')}",
                         metadata={"run_id": run_id})
    assert_clickhouse_tables_exist(clickhouse, database=RESOLVED_DATABASE, tables=(tables.DOCUMENTS_S3_TABLE,))
    with clickhouse.get_connection() as client:
        documents = int(client.execute(sql.documents_count_sql(RESOLVED_DATABASE))[0][0])
    issues = run.get("issues") or []
    issue_lines = "\n".join(f"{i['slug']} {i['collector']} {i['status']}: {i.get('error', '')}" for i in issues)
    return dg.MaterializeResult(
        metadata={
            "run_id": run_id,
            "changed": len(run.get("changed") or []),
            "changed_providers": ", ".join(run.get("changed") or []) or "none",
            "unchanged": run.get("unchanged_count", 0),
            "issues": len(issues),
            "documents": documents,
        },
        check_results=[
            dg.AssetCheckResult(
                check_name=FEEDS_OK_CHECK,
                passed=not issues,
                severity=dg.AssetCheckSeverity.WARN,
                metadata={"issues": dg.MetadataValue.text(issue_lines or "none")},
            )
        ],
    )


@dg.asset(
    name="provider_recon_clickhouse",
    deps=[provider_recon_documents],
    group_name=GROUP_NAME,
    kinds={"clickhouse", "sql"},
    description=(
        "Upserts provider-recon documents from provider_recon_documents_s3 into "
        "provider_services, provider_ip_ranges and provider_rules (ReplacingMergeTree by "
        "loaded_at; nothing is deleted, so ClickHouse keeps the full timeline)."
    ),
)
def provider_recon_clickhouse(context: AssetExecutionContext, clickhouse: ClickhouseResource) -> dg.MaterializeResult:
    assert_clickhouse_tables_exist(clickhouse, database=RESOLVED_DATABASE, tables=tables.ALL_TABLES)
    loaded_at = datetime.now(UTC).replace(tzinfo=None)
    rows: dict[str, int] = {}
    with clickhouse.get_connection() as client:
        documents = int(client.execute(sql.documents_count_sql(RESOLVED_DATABASE))[0][0])
        if documents == 0:
            raise ValueError(
                "provider_recon_documents_s3 returned no documents; refusing to load "
                "(check the provider_recon named collection and the bucket)"
            )
        for table, statement in sql.load_statements(RESOLVED_DATABASE):
            context.log.info("loading %s", table)
            client.execute(statement, {"loaded_at": loaded_at})
            result = client.execute(
                f"SELECT count() FROM `{RESOLVED_DATABASE}`.`{table}` WHERE loaded_at = %(loaded_at)s",
                {"loaded_at": loaded_at},
            )
            rows[table] = int(result[0][0]) if result else 0
    return dg.MaterializeResult(metadata={"documents": documents, **{f"{t}_rows": n for t, n in rows.items()}})


provider_recon_job = dg.define_asset_job(
    name="provider_recon_job",
    selection=dg.AssetSelection.assets(provider_recon_documents, provider_recon_clickhouse),
)

# Daily 03:12 UTC (a minute no other schedule uses). STOPPED by default per
# house pattern; start it on the instance after the first verified run.
provider_recon_daily = dg.ScheduleDefinition(
    name="provider_recon_daily",
    job=provider_recon_job,
    cron_schedule="12 3 * * *",
    execution_timezone="UTC",
    default_status=dg.DefaultScheduleStatus.STOPPED,
)

defs = dg.Definitions(
    assets=[provider_recon_documents, provider_recon_clickhouse],
    jobs=[provider_recon_job],
    schedules=[provider_recon_daily],
    resources={"provider_recon": ProviderReconResource(api_token=dg.EnvVar("PROVIDER_RECON_API_TOKEN"))},
)
```

- [ ] **Step 4: Run to verify pass**

Run: `uv run pytest tests/test_provider_recon_assets.py tests/test_provider_recon_resource.py tests/test_provider_recon_sql.py tests/test_clickhouse_migrations.py -q && uv run dg check defs`
Expected: PASS; `dg check defs` reports all definitions load. If `dg check defs` needs `PROVIDER_RECON_API_TOKEN` at load time, it does not, because `EnvVar` resolves at run time. If it does complain, add the variable to the local `.env` template with a placeholder and ledger it.

- [ ] **Step 5: Commit**

```bash
git add services/dagster_v3/src/dagster_v3/defs/provider_recon/assets.py services/dagster_v3/tests/test_provider_recon_assets.py
git commit -m "feat(dagster): provider-recon documents and ClickHouse assets, daily job (schedule stopped)"
```

---

### Task 5: Rollout

**Prerequisite:** plan A is deployed. The service answers on :8095, and `system.named_collections` lists `provider_recon`.

- [ ] **Step 1: Apply the migration on prod**

Follow `services/dagster_v3/docs/deployment-runbook.md` §5 (`make clickhouse-migrate-up` against `CLICKHOUSE_MIGRATE_URL`). Then verify:
```bash
ssh companycollect "docker exec clickhouse-clickhouse-1 clickhouse-client -q \"SELECT count() FROM corpscout.provider_recon_documents_s3; SELECT name FROM system.tables WHERE database='corpscout' AND name LIKE 'provider_%' ORDER BY name\""
```
Expected: `37`, plus the five objects.

- [ ] **Step 2: Add the token to the Dagster server environment**

Append `PROVIDER_RECON_API_TOKEN=<the value in services/provider_recon/.env>` to the server-owned `/opt/companycollect/corpscout/dagster_v3/.env` on the `dagster` host. Keep the ownership and mode the file already has. If the permission classifier blocks the write, hand the owner the one-line command. Confirm without printing the value:
```bash
ssh dagster "sudo grep -c '^PROVIDER_RECON_API_TOKEN=' /opt/companycollect/corpscout/dagster_v3/.env"
```

- [ ] **Step 3: Deploy dagster_v3**

Follow the worktree deploy recipe (memory: pristine worktree, dbt-state refresh, deploy lock at `/run/lock/corpscout-dagster-deploy`). Then confirm the code location loads the new assets:
```bash
ssh dagster "cd /opt/companycollect/corpscout/dagster_v3 && sudo -n env PATH=/opt/companycollect/corpscout/dagster_v3/.venv/bin:/usr/local/bin:/usr/bin:/bin ./.venv/bin/dg list defs 2>/dev/null | grep -c provider_recon"
```

- [ ] **Step 4: First run on the server**

Launch the job once with the server one-off recipe:
```bash
ssh dagster "cd /opt/companycollect/corpscout/dagster_v3 && sudo -n env PATH=/opt/companycollect/corpscout/dagster_v3/.venv/bin:/usr/local/bin:/usr/bin:/bin ./.venv/bin/dg launch --job provider_recon_job"
```
Then verify in ClickHouse:
```bash
ssh companycollect "docker exec clickhouse-clickhouse-1 clickhouse-client -q \"
SELECT count(DISTINCT provider_slug), count() FROM corpscout.provider_services FINAL;
SELECT status, count() FROM corpscout.provider_ip_ranges FINAL GROUP BY status;
SELECT kind, count() FROM corpscout.provider_rules FINAL GROUP BY kind ORDER BY kind;
SELECT provider_slug, service_key FROM corpscout.provider_ip_ranges_current WHERE toIPv6('::ffff:52.84.1.1') BETWEEN range_start AND range_end\""
```
Expected:
- 37 providers
- active ranges in the tens of thousands (AWS ~12k + Azure ~19k + …)
- rules for dns/http/ptr/asn
- the 52.84.1.1 lookup returns `aws | aws.cloudfront`

- [ ] **Step 5: Start the schedule**

Start `provider_recon_daily` on the instance (UI, or `dagster schedule start` per the runbook). Record in memory that it is running.
