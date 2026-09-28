# Domain services detection Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `corpscout.domain_service_evidence` and `corpscout.domain_services`: for every domain in the DNS record store, which **(service type, provider)** it uses. Examples: `(dns, cloudflare)`, `(email, google)`, `(cdn, fastly)`, `(email_sending, sendgrid.net)`. Each row keeps the record that proves it and the window it was seen in. A Dagster asset refreshes the tables per hash bucket, driven by a weekly schedule and a provider-definitions sensor.

**Architecture:**
- **Asset.** `dagster_v3/defs/domain_services` has one asset with 128 static partitions, `hash_000`…`hash_127` (`cityHash64(root_domain) % 128`).
- **Per bucket**, the asset:
  - scans the DNS store's partition once into a candidates temp table;
  - labels host candidates by provider-recon DNS rules, or by the registrable domain of the host (self-hosted / named provider / unmapped);
  - matches apex and www A/AAAA against `provider_ip_ranges` through a per-bucket /16 (IPv4) and /32 (IPv6) key table, using validity windows and longest prefix;
  - aggregates per `(service_type, provider_key, root_domain)` with the CommonCrawl harmonic rank;
  - swaps both tables' partition in with `REPLACE PARTITION`.
- **Verified before writing.** All SQL was prototyped against real record shapes and passes the tests below in clickhouse-local (26.5). The candidate pass over one production bucket (bucket 3) takes 72 s and yields 4.4M candidates.

**Tech Stack:** Python 3.14 (uv), Dagster, ClickHouse 26.5 (clickhouse-driver), clickhouse-local through Docker for the SQL tests, golang-migrate ledger.

**Spec:** `services/dagster_v3/docs/superpowers/specs/2026-09-27-domain-services-and-technology-domains-design.md` (revision 2, owner-approved 2026-09-28). This plan covers the "Assets and orchestration", "Tables" and "IP matching" sections. `technology_domains`, the backoffice pages and the removals are separate plans.

## Global Constraints

- Domain data only: nothing reads `company_domains`, and there are no company views (owner ruling 2026-09-28).
- Provider evidence comes only from provider-recon's `provider_services`, `provider_rules` (kind `dns`) and `provider_ip_ranges`. There is no mapping JSON.
- ClickHouse objects are created only by migration `000465_corpscout_domain_services`, and code never duplicates DDL. Tables are MergeTree, `PARTITION BY cityHash64(root_domain) % 128`.
- Every DNS-store query repeats `cityHash64(root_domain) % 16 = bucket % 16` so ClickHouse prunes to one store partition.
- The asset refuses to replace a partition when the bucket produced 0 candidates or 0 evidence rows.
- Pool `domain_signal_detection` (limit 1); backfill policy `multi_run(max_partitions_per_run=1)`.
- The schedule and the sensor are `STOPPED` by default and started only after the verified full run (Task 4).
- Orchestration lives on the server: the full refresh is a Dagster backfill or schedule on the server, never a local loop.
- Commit by explicit path from the `corpscout` root. Conventional Commits, each ending with `Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>`. Work in a worktree under `companycollect/.worktrees/`.
- Commands run from `corpscout/services/dagster_v3` with `uv run`. The worktree needs `.env` and the dbt `target/manifest.json` files copied from the main checkout for `dg check defs`.

## Review Focus

1. **A provider-recon rule with a regex ClickHouse's re2 rejects.** `match()` throws and every bucket fails. Expected: the run fails loudly with the ClickHouse error, and no partition is replaced (the swap is the last step). Provider-recon validates regexes with Go's RE2, which accepts the same syntax. *(Task 3 test `test_asset_refuses_to_replace_an_empty_bucket` covers the no-swap-on-failure path; the regex dialect itself is a review item.)*
2. **Hosts under multi-label public suffixes (`.co.uk`, `.com.au`) and private suffixes (`github.io`).** Expected: the provider key is the registrable domain as ClickHouse's `cutToFirstSignificantSubdomain` computes it, e.g. `awsdns-01.co.uk`, which then matches the `awsdns-*` key pattern. *(Task 2: `aws.se` row in `test_host_evidence_rules_fallback_and_self_hosted`.)*
3. **TXT values split into several strings, and null MX.** Expected: the strings are joined before SPF parsing, and `0 .` gives no candidate. *(Task 2: `test_candidates_cover_every_signal`.)*
4. **A-record values that aren't IPs, and IP ranges newer than the record's window.** Expected: garbage is dropped and a range first seen after the record stopped being seen doesn't match. Ranges from the first provider-recon run count as always valid. *(Task 2: `test_ip_evidence_longest_prefix_window_and_wide_ranges`.)*
5. **The sensor firing while a refresh is still queued or running.** Expected: it skips without advancing its cursor, so the change is picked up once the refresh ends, and no second set of 128 runs is queued. *(Task 3: `test_sensor_waits_while_a_refresh_is_running`.)*

---

### Task 1: Migration 000465 — tables and views

**Files:**
- Create: `corpscout/clickhouse/migrations/000465_corpscout_domain_services.up.sql`, `…down.sql`
- Modify: `corpscout/services/dagster_v3/tests/test_clickhouse_migrations.py`

**Interfaces:**
- Produces:
  - tables `corpscout.domain_service_evidence` and `corpscout.domain_services` (the column orders match `sql.EVIDENCE_COLUMNS` and `sql.SERVICES_COLUMNS` in Task 2);
  - views `domain_services_current` and `unmapped_provider_keys`.

**Numbering:** committed main ends at 000460. 000461–000464 are another session's uncommitted files in the main checkout, and 000461 is already applied on prod. This migration takes **000465** so it can't collide. Task 4 checks the ledger again before applying.

- [ ] **Step 1: Failing test.** Apply this change to `tests/test_clickhouse_migrations.py`:

```diff
@@ -473,6 +473,7 @@ EXPECTED_MIGRATIONS = (
     "000458_corpscout_ip_enrichment_queue_contract",
     "000459_corpscout_website_company_lookup_results",
     "000460_corpscout_provider_recon",
+    "000465_corpscout_domain_services",
 )
 
 NOOP_MIGRATIONS = {"000276_noop"}
@@ -4603,3 +4604,15 @@ def test_provider_recon_migration_maps_s3_without_credentials() -> None:
         assert f"CREATE TABLE IF NOT EXISTS corpscout.{table}" in up
         assert "ENGINE = ReplacingMergeTree(loaded_at)" in up
     assert "CREATE VIEW IF NOT EXISTS corpscout.provider_ip_ranges_current" in up
+
+
+def test_domain_services_migration_partitions_by_detection_bucket() -> None:
+    up = (MIGRATIONS_DIR / "000465_corpscout_domain_services.up.sql").read_text()
+    for table in ("domain_service_evidence", "domain_services"):
+        assert f"CREATE TABLE IF NOT EXISTS corpscout.{table}" in up
+    # Both tables are rebuilt one detection bucket at a time with REPLACE PARTITION.
+    assert up.count("PARTITION BY cityHash64(root_domain) % 128") == 2
+    assert "ORDER BY (service_type, provider_key, harmonic_rank, root_domain)" in up
+    assert "CREATE VIEW IF NOT EXISTS corpscout.domain_services_current" in up
+    assert "CREATE VIEW IF NOT EXISTS corpscout.unmapped_provider_keys" in up
+
```

Run: `uv run pytest -q tests/test_clickhouse_migrations.py`
Expected: FAIL. `test_clickhouse_migration_files_are_explicit` fails because the files are missing, and the new test fails with `FileNotFoundError`.

- [ ] **Step 2: Migration files.**

`corpscout/clickhouse/migrations/000465_corpscout_domain_services.up.sql`:
```sql
CREATE DATABASE IF NOT EXISTS corpscout;

-- One row per DNS record (or IP) that proves a domain uses a service.
-- Built per hash bucket by the Dagster asset domain_services_clickhouse
-- (stage table + REPLACE PARTITION), so a bucket is replaced whole.
CREATE TABLE IF NOT EXISTS corpscout.domain_service_evidence
(
    root_domain String,
    service_type LowCardinality(String),
    provider_slug LowCardinality(String),
    service_key LowCardinality(String),
    provider_key String,
    signal_type LowCardinality(String),
    record_name String,
    evidence String,
    rule_key String,
    ip_cidr String,
    feed_tag LowCardinality(String),
    first_seen DateTime64(3, 'UTC'),
    last_seen DateTime64(3, 'UTC'),
    confidence Float32,
    source LowCardinality(String),
    source_run_id String,
    detected_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
PARTITION BY cityHash64(root_domain) % 128
ORDER BY (root_domain, service_type, provider_key, signal_type, evidence);

-- One row per (service type, provider, domain). provider_key is the
-- provider-recon slug for a named provider, the registrable domain for an
-- unmapped one, and 'self-hosted' when the evidence points at the domain
-- itself. Sorted by rank inside each provider, so top domains are a range read.
CREATE TABLE IF NOT EXISTS corpscout.domain_services
(
    service_type LowCardinality(String),
    provider_key String,
    provider_slug LowCardinality(String),
    service_keys Array(String),
    root_domain String,
    signal_types Array(LowCardinality(String)),
    first_seen DateTime64(3, 'UTC'),
    last_seen DateTime64(3, 'UTC'),
    domain_last_seen DateTime64(3, 'UTC'),
    confidence Float32,
    harmonic_rank UInt64,
    source_run_id String,
    detected_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
PARTITION BY cityHash64(root_domain) % 128
ORDER BY (service_type, provider_key, harmonic_rank, root_domain);

-- Services seen in the domain's latest DNS observations (within 7 days of
-- the newest record the pass saw for that domain).
CREATE VIEW IF NOT EXISTS corpscout.domain_services_current AS
SELECT *
FROM corpscout.domain_services
WHERE last_seen >= domain_last_seen - INTERVAL 7 DAY;

-- Providers not yet named in provider-recon, ranked by the domains using them.
CREATE VIEW IF NOT EXISTS corpscout.unmapped_provider_keys AS
SELECT
    service_type,
    provider_key,
    count() AS domains,
    countIf(last_seen >= domain_last_seen - INTERVAL 7 DAY) AS current_domains,
    arrayMap(x -> tupleElement(x, 2), groupArraySorted(10)(tuple(harmonic_rank, root_domain))) AS top_domains
FROM corpscout.domain_services
WHERE provider_slug = '' AND provider_key != 'self-hosted'
GROUP BY service_type, provider_key;
```

`corpscout/clickhouse/migrations/000465_corpscout_domain_services.down.sql`:
```sql
DROP VIEW IF EXISTS corpscout.unmapped_provider_keys;
DROP VIEW IF EXISTS corpscout.domain_services_current;
DROP TABLE IF EXISTS corpscout.domain_services;
DROP TABLE IF EXISTS corpscout.domain_service_evidence;
```

- [ ] **Step 3: Run.**

Run: `uv run pytest -q tests/test_clickhouse_migrations.py`
Expected: PASS (135 passed, as measured on a throwaway tree at 05fcb87d8).

- [ ] **Step 4: Commit.**

```bash
git add clickhouse/migrations/000465_corpscout_domain_services.up.sql clickhouse/migrations/000465_corpscout_domain_services.down.sql services/dagster_v3/tests/test_clickhouse_migrations.py
git commit -m "feat(clickhouse): 000465 domain_service_evidence and domain_services tables

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

---

### Task 2: Detection SQL module

**Files:**
- Create:
  - `src/dagster_v3/defs/domain_services/__init__.py` (empty)
  - `src/dagster_v3/defs/domain_services/sql.py`
  - `tests/test_domain_services_sql.py`

**Interfaces:**
- Consumes: Task 1's tables and views; migration 000460's provider tables.
- Produces (in `dagster_v3.defs.domain_services.sql`):
  - Constants: `PARTITION_COUNT = 128`, `EVIDENCE_TABLE`, `SERVICES_TABLE`, `DNS_RECORDS_TABLE`, `PROVIDER_SERVICES_TABLE`, `PROVIDER_RULES_TABLE`, `PROVIDER_IP_RANGES_TABLE`, `UNRANKED`.
  - Partition helpers: `partition_keys() -> list[str]`, `partition_bucket(key) -> int`.
  - Candidate SQL: `candidates_ddl(candidates)`, `candidates_insert_sql(database, candidates, bucket)`.
  - Evidence SQL: `host_evidence_sql(database, candidates, stage)`, `range_keys_ddl(keys)`, `range_keys_insert_sql(database, keys)`, `wide_ranges_count_sql(database)`, `ip_evidence_sql(database, candidates, keys, stage)`.
  - Publishing SQL: `services_insert_sql(database, candidates, evidence_stage, services_stage)`, `replace_partition_sql(qualified, stage, bucket)`.
  - Query parameters: `%(source_run_id)s`, `%(detected_at)s` and `%(ranking_release)s`.

- [ ] **Step 1: Failing test.** `tests/test_domain_services_sql.py`:

```python
"""Domain services detection SQL, run against a real ClickHouse engine."""

import json
import subprocess
from pathlib import Path

from dagster_v3.defs.domain_services import sql
from tests.clickhouse_local import clickhouse_local_command, render

REPO = Path(__file__).resolve().parents[3]
MIGRATIONS = REPO / "clickhouse" / "migrations"
PARAMS = {"source_run_id": "run1", "detected_at": "2026-09-28 10:00:00.000", "ranking_release": "rel1"}

DNS_STORE = """
CREATE TABLE corpscout.commoncrawl_domain_dns_records (`record_id` FixedString(16), `root_domain` String, `name` String, `record_type` SimpleAggregateFunction(any, LowCardinality(String)), `record_type_code` UInt16, `record_class_code` UInt16, `value` SimpleAggregateFunction(any, String), `rdata_wire` SimpleAggregateFunction(any, String), `priority` SimpleAggregateFunction(any, UInt16), `sources` SimpleAggregateFunction(groupUniqArrayArray, Array(String)), `discoveries` SimpleAggregateFunction(groupUniqArrayArray, Array(String)), `seen_dates` SimpleAggregateFunction(groupUniqArrayArray, Array(Date)), `first_seen` SimpleAggregateFunction(min, DateTime64(3, 'UTC')), `last_seen` SimpleAggregateFunction(max, DateTime64(3, 'UTC')), `last_loaded_at` SimpleAggregateFunction(max, DateTime64(3, 'UTC'))) ENGINE = AggregatingMergeTree PARTITION BY cityHash64(root_domain) % 16 ORDER BY (root_domain, name, record_type_code, record_class_code, record_id);
CREATE TABLE corpscout.commoncrawl_domain_graph_ranks (graph_release LowCardinality(String), root_domain String, cc_harmonic_centrality Float64, cc_harmonic_rank UInt64, cc_pagerank Float64, cc_pagerank_rank UInt64, n_hosts Nullable(UInt32), source_run_id String, loaded_at DateTime64(3, 'UTC')) ENGINE = MergeTree PARTITION BY graph_release ORDER BY (root_domain, graph_release);
"""

SEEN = "'2026-07-15 00:00:00', '2026-09-25 00:00:00'"
DNS_ROWS = f"""
INSERT INTO corpscout.commoncrawl_domain_dns_records (record_id, root_domain, name, record_type, value, first_seen, last_seen) VALUES
 ('a', 'spotify.com', 'spotify.com', 'NS', 'ns-cloud-a2.googledomains.com.', {SEEN}),
 ('b', 'spotify.com', 'spotify.com', 'MX', '5 alt1.aspmx.l.google.com.', {SEEN}),
 ('c', 'spotify.com', 'spotify.com', 'TXT', '"v=spf1 include:_spf.google.com" " include:sendgrid.net ~all"', {SEEN}),
 ('d', 'spotify.com', 'k1._domainkey.spotify.com', 'CNAME', 'dkim.mcsv.net.', {SEEN}),
 ('e', 'spotify.com', 'www.spotify.com', 'CNAME', 'atc.spotify.map.fastly.net.', {SEEN}),
 ('f', 'spotify.com', 'spotify.com', 'A', '35.186.224.24', {SEEN}),
 ('g', 'spotify.com', 'spotify.com', 'AAAA', '2600:1901:1:7c5::', {SEEN}),
 ('h', 'spotify.com', 'spotify.com', 'SOA', 'ns-cloud-a1.googledomains.com. host. 1 2 3 4 5', {SEEN}),
 ('i', 'nons.se', 'nons.se', 'SOA', 'ns1.binero.se. hostmaster.binero.se. 1 2 3 4 5', {SEEN}),
 ('j', 'nons.se', 'nons.se', 'MX', '10 mail.nons.se.', {SEEN}),
 ('k', 'nons.se', 'nons.se', 'MX', '0 .', {SEEN}),
 ('l', 'nons.se', 'nons.se', 'TXT', '"apple-domain-verification=xyz"', {SEEN}),
 ('m', 'nons.se', 'nons.se', 'A', 'not-an-ip', {SEEN}),
 ('n', 'other.se', 'other.se', 'NS', 'ns1.other.se.', {SEEN}),
 ('o', 'other.se', 'deep.other.se', 'A', '1.2.3.4', {SEEN}),
 ('p', 'aws.se', 'aws.se', 'NS', 'ns-12.awsdns-01.co.uk.', {SEEN}),
 ('q', 'old.se', 'old.se', 'NS', 'ns1.binero.se.', '2026-01-01 00:00:00', '2026-02-01 00:00:00'),
 ('r', 'old.se', 'old.se', 'NS', 'ns1.loopia.se.', '2026-02-02 00:00:00', '2026-09-25 00:00:00');
"""

PROVIDERS = """
INSERT INTO corpscout.provider_services (provider_slug, provider_keys, service_key, service_types, loaded_at) VALUES
 ('google', ['google.com', 'googledomains.com'], 'google.cloud-dns', ['dns'], now()),
 ('google', ['google.com', 'googledomains.com'], 'google.workspace-mail', ['email'], now()),
 ('google', ['google.com', 'googledomains.com'], 'google.workspace-sending', ['email_sending'], now()),
 ('google', ['google.com', 'googledomains.com'], 'google.cloud', ['iaas'], now()),
 ('google', ['google.com', 'googledomains.com'], 'google.lb', ['cdn'], now()),
 ('binero', ['binero.se'], 'binero.dns', ['dns'], now()),
 ('aws', ['amazonaws.com', 'awsdns-*'], 'aws.route53', ['dns'], now()),
 ('fastly', ['fastly.net'], 'fastly.edge', ['cdn', 'ddos_protection'], now()),
 ('apple', [], 'apple.domain-verification', ['saas_verification'], now()),
 ('wide', [], 'wide.x', ['iaas'], now());
INSERT INTO corpscout.provider_rules (provider_slug, service_key, kind, rule_key, record_type, match_field, matcher_type, pattern, case_sensitive, confidence, priority, status, loaded_at) VALUES
 ('google', 'google.cloud-dns', 'dns', 'ns-regex', 'NS', 'target', 'regex', '^ns-cloud-[a-e][1-4]\\\\.googledomains\\\\.com$', 0, 1, 0, 'active', now()),
 ('google', 'google.workspace-mail', 'dns', 'mx-suffix', 'MX', 'target', 'suffix', 'google.com', 0, 1, 0, 'active', now()),
 ('google', 'google.workspace-sending', 'dns', 'spf', 'TXT', 'value', 'contains', 'include:_spf.google.com', 0, 1, 0, 'active', now()),
 ('apple', 'apple.domain-verification', 'dns', 'apple', 'TXT', 'value', 'prefix', 'apple-domain-verification=', 0, 0.5, 0, 'active', now()),
 ('fastly', 'fastly.edge', 'dns', 'cname', 'CNAME', 'target', 'suffix', 'fastly.net', 0, 1, 0, 'active', now()),
 ('fastly', 'fastly.other', 'dns', 'cname-removed', 'CNAME', 'target', 'suffix', 'map.fastly.net', 0, 1, 9, 'removed', now());
INSERT INTO corpscout.provider_ip_ranges (provider_slug, service_key, cidr, ip_family, range_start, range_end, feed_tag, confidence, status, first_seen, last_seen, removed_at, loaded_at) VALUES
 ('google', 'google.cloud', '35.184.0.0/13', 4, '::ffff:35.184.0.0', '::ffff:35.191.255.255', 'GOOG', 1, 'active', '2026-09-27', '2026-09-28', NULL, now()),
 ('google', 'google.lb', '35.186.224.0/24', 4, '::ffff:35.186.224.0', '::ffff:35.186.224.255', 'LB', 1, 'active', '2026-09-27', '2026-09-28', NULL, now()),
 ('google', 'google.cloud', '2600:1900::/28', 6, '2600:1900::', '2600:190f:ffff:ffff:ffff:ffff:ffff:ffff', 'GOOG6', 1, 'active', '2026-09-28', '2026-09-28', NULL, now()),
 ('wide', 'wide.x', '32.0.0.0/7', 4, '::ffff:32.0.0.0', '::ffff:33.255.255.255', '', 1, 'active', '2026-09-27', '2026-09-28', NULL, now());
INSERT INTO corpscout.commoncrawl_domain_graph_ranks (graph_release, root_domain, cc_harmonic_rank) VALUES
 ('rel1', 'spotify.com', 120), ('rel0', 'nons.se', 5);
"""


def migration(name: str) -> str:
    """A migration's statements, minus the S3 mapping clickhouse-local can't reach."""
    text = (MIGRATIONS / f"{name}.up.sql").read_text()
    kept = [s.strip() for s in text.split(";") if s.strip() and "ENGINE = S3" not in s]
    return ";\n".join(kept) + ";\n"


def run(sql_text: str) -> list[list]:
    result = subprocess.run(clickhouse_local_command(), input=sql_text, capture_output=True, text=True, timeout=300)
    assert result.returncode == 0, result.stderr
    return [json.loads(line) for line in result.stdout.splitlines() if line.strip()]


def setup() -> str:
    return (
        migration("000460_corpscout_provider_recon")
        + migration("000465_corpscout_domain_services")
        + DNS_STORE
        + DNS_ROWS
        + PROVIDERS
        + sql.candidates_ddl("corpscout.cand")
        + ";\n"
        + "".join(sql.candidates_insert_sql("corpscout", "corpscout.cand", b) + ";\n" for b in range(sql.PARTITION_COUNT))
    )


def detect() -> str:
    """Every step, writing straight into the real tables (tests use no stage)."""
    evidence = f"corpscout.{sql.EVIDENCE_TABLE}"
    return (
        render(sql.host_evidence_sql("corpscout", "corpscout.cand", evidence), PARAMS) + ";\n"
        + sql.range_keys_ddl("corpscout.keys") + ";\n"
        + sql.range_keys_insert_sql("corpscout", "corpscout.keys") + ";\n"
        + render(sql.ip_evidence_sql("corpscout", "corpscout.cand", "corpscout.keys", evidence), PARAMS) + ";\n"
        + render(sql.services_insert_sql("corpscout", "corpscout.cand", evidence, f"corpscout.{sql.SERVICES_TABLE}"), PARAMS) + ";\n"
    )


def test_candidates_cover_every_signal() -> None:
    rows = run(setup() + "SELECT root_domain, record_name, signal_type, candidate FROM corpscout.cand ORDER BY ALL FORMAT JSONCompactEachRow;")
    got = {tuple(r) for r in rows}
    assert ("spotify.com", "spotify.com", "dns_spf", "_spf.google.com") in got
    assert ("spotify.com", "spotify.com", "dns_spf", "sendgrid.net") in got
    assert ("spotify.com", "k1._domainkey.spotify.com", "dns_dkim", "dkim.mcsv.net") in got
    assert ("spotify.com", "www.spotify.com", "dns_cname", "atc.spotify.map.fastly.net") in got
    assert ("spotify.com", "spotify.com", "dns_mx", "alt1.aspmx.l.google.com") in got
    assert ("nons.se", "nons.se", "dns_soa", "ns1.binero.se") in got
    # Null MX ('0 .') yields nothing; a subdomain's A record is out of scope.
    assert not any(r[2] == "dns_mx" and r[3] == "" for r in rows)
    assert not any(r[1] == "deep.other.se" for r in rows)


def test_host_evidence_rules_fallback_and_self_hosted() -> None:
    rows = run(
        setup() + detect()
        + "SELECT root_domain, service_type, provider_slug, service_key, provider_key, signal_type, evidence, rule_key, confidence "
        + "FROM corpscout.domain_service_evidence WHERE source = 'dns' ORDER BY ALL FORMAT JSONCompactEachRow;"
    )
    assert rows == [
        ["aws.se", "dns", "aws", "aws.route53", "aws", "dns_ns", "ns-12.awsdns-01.co.uk", "", 0.8],
        ["nons.se", "dns", "binero", "binero.dns", "binero", "dns_soa", "ns1.binero.se", "", 0.8],
        ["nons.se", "email", "", "", "self-hosted", "dns_mx", "mail.nons.se", "", 0.8],
        ["nons.se", "saas_verification", "apple", "apple.domain-verification", "apple", "dns_txt", "apple-domain-verification=xyz", "apple", 0.5],
        ["old.se", "dns", "", "", "loopia.se", "dns_ns", "ns1.loopia.se", "", 0.5],
        ["old.se", "dns", "binero", "binero.dns", "binero", "dns_ns", "ns1.binero.se", "", 0.8],
        ["other.se", "dns", "", "", "self-hosted", "dns_ns", "ns1.other.se", "", 0.8],
        ["spotify.com", "cdn", "fastly", "fastly.edge", "fastly", "dns_cname", "atc.spotify.map.fastly.net", "cname", 1],
        ["spotify.com", "ddos_protection", "fastly", "fastly.edge", "fastly", "dns_cname", "atc.spotify.map.fastly.net", "cname", 1],
        ["spotify.com", "dns", "google", "google.cloud-dns", "google", "dns_ns", "ns-cloud-a2.googledomains.com", "ns-regex", 1],
        ["spotify.com", "email", "google", "google.workspace-mail", "google", "dns_mx", "alt1.aspmx.l.google.com", "mx-suffix", 1],
        ["spotify.com", "email_sending", "", "", "mcsv.net", "dns_dkim", "dkim.mcsv.net", "", 0.5],
        ["spotify.com", "email_sending", "", "", "sendgrid.net", "dns_spf", "sendgrid.net", "", 0.5],
        ["spotify.com", "email_sending", "google", "google.workspace-sending", "google", "dns_spf", "_spf.google.com", "", 0.8],
        ["spotify.com", "email_sending", "google", "google.workspace-sending", "google", "dns_txt", "v=spf1 include:_spf.google.com include:sendgrid.net ~all", "spf", 1],
    ]


def test_ip_evidence_longest_prefix_window_and_wide_ranges() -> None:
    rows = run(
        setup() + detect()
        + "SELECT root_domain, service_type, provider_slug, service_key, evidence, ip_cidr, feed_tag, toString(first_seen), toString(last_seen) "
        + "FROM corpscout.domain_service_evidence WHERE source = 'ip_range' ORDER BY ALL FORMAT JSONCompactEachRow;"
        + sql.wide_ranges_count_sql("corpscout") + " FORMAT JSONCompactEachRow;"
    )
    # 35.186.224.24 sits in both the /13 and the /24: the /24 wins. The IPv6
    # range was first seen after the timeline start and after the record's
    # window, so it doesn't match; 'not-an-ip' is dropped; the /7 is refused.
    assert rows == [
        ["spotify.com", "cdn", "google", "google.lb", "35.186.224.24", "35.186.224.0/24", "LB", "2026-07-15 00:00:00.000", "2026-09-25 00:00:00.000"],
        [1],
    ]


def test_services_aggregate_with_rank_and_history() -> None:
    rows = run(
        setup() + detect()
        + "SELECT service_type, provider_key, provider_slug, service_keys, root_domain, signal_types, confidence, harmonic_rank "
        + "FROM corpscout.domain_services WHERE root_domain IN ('spotify.com', 'nons.se') ORDER BY ALL FORMAT JSONCompactEachRow;"
        + "SELECT provider_key FROM corpscout.domain_services_current WHERE root_domain = 'old.se' ORDER BY ALL FORMAT JSONCompactEachRow;"
        + "SELECT provider_key, toString(last_seen) FROM corpscout.domain_services WHERE root_domain = 'old.se' ORDER BY ALL FORMAT JSONCompactEachRow;"
    )
    unranked = sql.UNRANKED
    assert rows[:11] == [
        ["cdn", "fastly", "fastly", ["fastly.edge"], "spotify.com", ["dns_cname"], 1, 120],
        ["cdn", "google", "google", ["google.lb"], "spotify.com", ["dns_ip"], 1, 120],
        ["ddos_protection", "fastly", "fastly", ["fastly.edge"], "spotify.com", ["dns_cname"], 1, 120],
        ["dns", "binero", "binero", ["binero.dns"], "nons.se", ["dns_soa"], 0.8, unranked],
        ["dns", "google", "google", ["google.cloud-dns"], "spotify.com", ["dns_ns"], 1, 120],
        ["email", "google", "google", ["google.workspace-mail"], "spotify.com", ["dns_mx"], 1, 120],
        ["email", "self-hosted", "", [], "nons.se", ["dns_mx"], 0.8, unranked],
        ["email_sending", "google", "google", ["google.workspace-sending"], "spotify.com", ["dns_spf", "dns_txt"], 1, 120],
        ["email_sending", "mcsv.net", "", [], "spotify.com", ["dns_dkim"], 0.5, 120],
        ["email_sending", "sendgrid.net", "", [], "spotify.com", ["dns_spf"], 0.5, 120],
        ["saas_verification", "apple", "apple", ["apple.domain-verification"], "nons.se", ["dns_txt"], 0.5, unranked],
    ]
    # old.se moved from Binero to Loopia: both are history, only Loopia is current.
    assert rows[11:] == [
        ["loopia.se"],
        ["binero", "2026-02-01 00:00:00.000"],
        ["loopia.se", "2026-09-25 00:00:00.000"],
    ]


def test_unmapped_provider_keys_lists_only_unnamed_providers() -> None:
    rows = run(
        setup() + detect()
        + "SELECT service_type, provider_key, domains, current_domains, top_domains FROM corpscout.unmapped_provider_keys ORDER BY ALL FORMAT JSONCompactEachRow;"
    )
    assert rows == [
        ["dns", "loopia.se", 1, 1, ["old.se"]],
        ["email_sending", "mcsv.net", 1, 1, ["spotify.com"]],
        ["email_sending", "sendgrid.net", 1, 1, ["spotify.com"]],
    ]


def test_replace_partition_swaps_only_the_bucket() -> None:
    evidence = f"corpscout.{sql.EVIDENCE_TABLE}"
    bucket = run("SELECT cityHash64('spotify.com') % 128 FORMAT JSONCompactEachRow;")[0][0]
    rows = run(
        setup() + detect()
        + f"CREATE TABLE corpscout.stage AS {evidence};\n"
        + f"INSERT INTO corpscout.stage SELECT * REPLACE ('replaced' AS source_run_id) FROM {evidence} "
        + "WHERE root_domain = 'spotify.com' AND signal_type = 'dns_ns';\n"
        + sql.replace_partition_sql(evidence, "corpscout.stage", bucket) + ";\n"
        + f"SELECT root_domain, source_run_id, count() FROM {evidence} GROUP BY ALL ORDER BY ALL FORMAT JSONCompactEachRow;"
    )
    by_domain = {r[0]: (r[1], r[2]) for r in rows}
    # spotify.com's bucket now holds only the stage's single row; other buckets are untouched.
    assert by_domain["spotify.com"] == ("replaced", 1)
    assert by_domain["nons.se"][0] == "run1"
    assert by_domain["other.se"][0] == "run1"
```

Run: `uv run pytest -q tests/test_domain_services_sql.py`
Expected: FAIL with `ModuleNotFoundError: No module named 'dagster_v3.defs.domain_services'`.

- [ ] **Step 2: Implement.** Create an empty `src/dagster_v3/defs/domain_services/__init__.py`, and `src/dagster_v3/defs/domain_services/sql.py`:

```python
"""Domain services detection SQL: DNS records + provider-recon evidence.

One bucket (cityHash64(root_domain) % 128) at a time:

1. candidates  — one scan of the DNS store's partition into a temp table;
2. host evidence — provider-recon DNS rules, then the provider-key fallback;
3. IP evidence  — apex/www A/AAAA inside provider ranges (per-bucket range keys);
4. services     — evidence aggregated per (service_type, provider_key, root_domain);
5. REPLACE PARTITION on both tables.

Every statement takes the database name so tests can run it in clickhouse-local.
"""

DNS_RECORDS_TABLE = "commoncrawl_domain_dns_records"
RANKS_TABLE = "commoncrawl_domain_graph_ranks"
PROVIDER_SERVICES_TABLE = "provider_services"
PROVIDER_RULES_TABLE = "provider_rules"
PROVIDER_IP_RANGES_TABLE = "provider_ip_ranges"
EVIDENCE_TABLE = "domain_service_evidence"
SERVICES_TABLE = "domain_services"

# The DNS store is PARTITION BY cityHash64(root_domain) % 16; detection bucket
# N lives in store partition N % 16, so repeating that expression prunes.
DNS_RECORDS_HASH_BUCKETS = 16
PARTITION_COUNT = 128

SELF_HOSTED = "self-hosted"
UNRANKED = 18446744073709551615  # UInt64 max: unranked domains sort last

# Confidence of a detection that no provider-recon rule produced.
KEY_MATCH_CONFIDENCE = 0.8
SELF_HOSTED_CONFIDENCE = 0.8
UNMAPPED_CONFIDENCE = 0.5

# Service type an unmatched host candidate proves, by signal.
FALLBACK_SERVICE_TYPE = {
    "dns_ns": "dns",
    "dns_soa": "dns",
    "dns_mx": "email",
    "dns_spf": "email_sending",
    "dns_dkim": "email_sending",
    "dns_cname": "hosting",
}
# Signals provider-recon DNS rules are evaluated against. DKIM targets and SPF
# include hosts only take the key fallback: a CNAME/TXT rule written for a web
# edge or a verification token must not label a mail-signing host.
RULE_SIGNALS = ("dns_ns", "dns_soa", "dns_mx", "dns_txt", "dns_cname")

# Widest range the IP join expands: one key per /16 (IPv4) or /32 (IPv6).
MIN_PREFIX = {4: 8, 6: 20}

EVIDENCE_COLUMNS = (
    "root_domain", "service_type", "provider_slug", "service_key", "provider_key",
    "signal_type", "record_name", "evidence", "rule_key", "ip_cidr", "feed_tag",
    "first_seen", "last_seen", "confidence", "source", "source_run_id", "detected_at",
)
SERVICES_COLUMNS = (
    "service_type", "provider_key", "provider_slug", "service_keys", "root_domain",
    "signal_types", "first_seen", "last_seen", "domain_last_seen", "confidence",
    "harmonic_rank", "source_run_id", "detected_at",
)


def partition_keys() -> list[str]:
    return [f"hash_{bucket:03d}" for bucket in range(PARTITION_COUNT)]


def partition_bucket(partition_key: str) -> int:
    bucket = int(partition_key.removeprefix("hash_"))
    if not 0 <= bucket < PARTITION_COUNT:
        raise ValueError(f"partition key {partition_key!r} is out of range")
    return bucket


def candidates_ddl(candidates: str) -> str:
    return f"""CREATE TABLE {candidates}
(
    root_domain String,
    record_name String,
    record_type LowCardinality(String),
    signal_type LowCardinality(String),
    candidate String,
    first_seen DateTime64(3, 'UTC'),
    last_seen DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY (signal_type, root_domain)"""


def candidates_insert_sql(database: str, candidates: str, bucket: int) -> str:
    """One pass over the bucket's DNS records: every signal's candidates.

    Hosts lose case and the trailing dot; MX keeps only the exchange host;
    SOA keeps MNAME; TXT strings are joined and unquoted, and a v=spf1 value
    also yields one dns_spf candidate per include:/redirect= host. Null-MX
    placeholders ('.', '~', 'localhost') produce no candidate.
    """
    return f"""INSERT INTO {candidates}
    (root_domain, record_name, record_type, signal_type, candidate, first_seen, last_seen)
SELECT
    root_domain,
    name AS record_name,
    record_type,
    tupleElement(pair, 1) AS signal_type,
    tupleElement(pair, 2) AS candidate,
    min(first_seen) AS first_seen,
    max(last_seen) AS last_seen
FROM (
    SELECT
        root_domain, name, record_type, first_seen, last_seen,
        lower(trim(TRAILING '.' FROM value)) AS host,
        trim(BOTH '"' FROM replaceAll(value, '" "', '')) AS txt,
        arrayJoin(multiIf(
            record_type = 'NS', [('dns_ns', host)],
            record_type = 'SOA', [('dns_soa', lower(trim(TRAILING '.' FROM splitByChar(' ', value)[1])))],
            record_type = 'MX', [('dns_mx', lower(trim(TRAILING '.' FROM substringIndex(value, ' ', -1))))],
            record_type = 'CNAME' AND endsWith(name, concat('._domainkey.', root_domain)), [('dns_dkim', host)],
            record_type = 'CNAME', [('dns_cname', host)],
            record_type IN ('A', 'AAAA'), [('dns_ip', trim(value))],
            record_type = 'TXT', arrayConcat(
                [('dns_txt', txt)],
                if(startsWith(lower(txt), 'v=spf1'),
                   arrayMap(h -> ('dns_spf', lower(trim(TRAILING '.' FROM h))),
                            extractAll(lower(txt), '(?:include:|redirect=)([^\\\\s]+)')),
                   [])),
            [])) AS pair
    FROM `{database}`.`{DNS_RECORDS_TABLE}`
    WHERE cityHash64(root_domain) % {DNS_RECORDS_HASH_BUCKETS} = {int(bucket) % DNS_RECORDS_HASH_BUCKETS}
      AND cityHash64(root_domain) % {PARTITION_COUNT} = {int(bucket)}
      AND record_type IN ('NS', 'SOA', 'MX', 'TXT', 'CNAME', 'A', 'AAAA')
      AND (
        name = root_domain
        OR (name = concat('www.', root_domain) AND record_type IN ('CNAME', 'A', 'AAAA'))
        OR (record_type = 'CNAME' AND endsWith(name, concat('._domainkey.', root_domain)))
      )
)
WHERE candidate NOT IN ('', '~', 'localhost')
GROUP BY root_domain, record_name, record_type, signal_type, candidate"""


def _fallback_type_expr(signal: str) -> str:
    branches = ", ".join(
        f"{signal} = '{name}', '{service_type}'" for name, service_type in FALLBACK_SERVICE_TYPE.items()
    )
    return f"multiIf({branches}, '')"


# What a rule is matched against: the candidate, or the record name for
# match_field 'name'; lower-cased unless the rule is case-sensitive.
_SUBJECT = (
    "if(r.case_sensitive = 1, if(r.match_field = 'name', h.record_name, h.candidate), "
    "lower(if(r.match_field = 'name', h.record_name, h.candidate)))"
)


def _rule_matches(subject: str) -> str:
    return f"""multiIf(
                r.matcher_type = 'suffix', {subject} = r.pattern OR endsWith({subject}, concat('.', r.pattern)),
                r.matcher_type = 'prefix', startsWith({subject}, r.pattern),
                r.matcher_type = 'contains', position({subject}, r.pattern) > 0,
                r.matcher_type = 'regex', match({subject}, r.pattern),
                r.matcher_type = 'exists', {subject} != '',
                false)"""


def host_evidence_sql(database: str, candidates: str, stage: str) -> str:
    """Host candidates → evidence: the best DNS rule, else the provider key.

    A rule matches on the candidate (or the record name for match_field
    'name'); several matching rules resolve by (priority, confidence,
    rule_key). An unmatched candidate is keyed by its registrable domain:
    the domain itself → self-hosted; a provider's provider_keys (exact, or a
    '*' pattern such as 'awsdns-*') → that provider, with its first service
    of the signal's service type; otherwise unmapped. SOA only speaks for a
    domain without NS. TXT only ever matches rules.
    """
    columns = ", ".join(EVIDENCE_COLUMNS)
    rule_signals = ", ".join(f"'{s}'" for s in RULE_SIGNALS)
    host_signals = ", ".join(f"'{s}'" for s in sorted({*RULE_SIGNALS, *FALLBACK_SERVICE_TYPE}))
    fallback_type = _fallback_type_expr("h.signal_type")
    return f"""INSERT INTO {stage} ({columns})
WITH
    rules AS (
        SELECT provider_slug, service_key, rule_key, record_type, match_field, matcher_type,
               if(case_sensitive = 1, pattern, lower(pattern)) AS pattern, case_sensitive, confidence, priority
        FROM `{database}`.`{PROVIDER_RULES_TABLE}` FINAL
        WHERE kind = 'dns' AND status != 'removed'
    ),
    services AS (
        SELECT provider_slug, service_key, service_types, provider_keys
        FROM `{database}`.`{PROVIDER_SERVICES_TABLE}` FINAL
        WHERE removed_at IS NULL
    ),
    host AS (
        SELECT root_domain, record_name, signal_type, candidate, first_seen, last_seen,
               if(signal_type = 'dns_soa', 'NS', record_type) AS rule_record_type
        FROM {candidates}
        WHERE signal_type IN ({host_signals})
          AND (signal_type != 'dns_soa'
               OR root_domain NOT IN (SELECT root_domain FROM {candidates} WHERE signal_type = 'dns_ns'))
    ),
    hits AS (
        SELECT h.root_domain AS root_domain, h.record_name AS record_name, h.signal_type AS signal_type,
               h.candidate AS candidate, any(h.first_seen) AS first_seen, any(h.last_seen) AS last_seen,
               argMax(tuple(r.provider_slug, r.service_key, r.rule_key, r.confidence),
                      tuple(r.priority, r.confidence, r.rule_key)) AS best
        FROM host AS h
        INNER JOIN rules AS r ON r.record_type = h.rule_record_type
        WHERE h.signal_type IN ({rule_signals})
          AND {_rule_matches(_SUBJECT)}
        GROUP BY h.root_domain, h.record_name, h.signal_type, h.candidate
    ),
    key_patterns AS (
        SELECT groupUniqArray(k) AS patterns FROM services ARRAY JOIN provider_keys AS k WHERE position(k, '*') > 0
    ),
    key_map AS (
        SELECT k AS lookup_key, any(provider_slug) AS provider_slug,
               groupArray(tuple(service_key, service_types)) AS provider_services
        FROM services ARRAY JOIN provider_keys AS k
        GROUP BY k
    )
SELECT
    hits.root_domain, service_type, tupleElement(best, 1), tupleElement(best, 2), tupleElement(best, 1),
    hits.signal_type, hits.record_name, hits.candidate, tupleElement(best, 3), '', '',
    hits.first_seen, hits.last_seen, tupleElement(best, 4), 'dns', %(source_run_id)s, %(detected_at)s
FROM hits
INNER JOIN services AS s ON s.provider_slug = tupleElement(hits.best, 1) AND s.service_key = tupleElement(hits.best, 2)
ARRAY JOIN s.service_types AS service_type
UNION ALL
SELECT
    root_domain, fallback_type,
    if(self_hosted, '', km.provider_slug),
    if(self_hosted, '', tupleElement(arrayFirst(x -> has(tupleElement(x, 2), fallback_type), km.provider_services), 1)),
    multiIf(self_hosted, '{SELF_HOSTED}', km.provider_slug != '', km.provider_slug, host_key),
    signal_type, record_name, candidate, '', '', '',
    first_seen, last_seen,
    multiIf(self_hosted, {SELF_HOSTED_CONFIDENCE}, km.provider_slug != '', {KEY_MATCH_CONFIDENCE}, {UNMAPPED_CONFIDENCE}),
    'dns', %(source_run_id)s, %(detected_at)s
FROM (
    SELECT h.root_domain AS root_domain, h.record_name AS record_name, h.signal_type AS signal_type,
           h.candidate AS candidate, h.first_seen AS first_seen, h.last_seen AS last_seen,
           {fallback_type} AS fallback_type,
           cutToFirstSignificantSubdomain(h.candidate) AS host_key,
           host_key = h.root_domain OR h.candidate = h.root_domain
               OR endsWith(h.candidate, concat('.', h.root_domain)) AS self_hosted,
           ifNull(arrayFirst(p -> like(host_key, replaceAll(p, '*', '%')), (SELECT patterns FROM key_patterns)), '') AS pattern_key,
           if(pattern_key != '', pattern_key, host_key) AS lookup_key
    FROM host AS h
    LEFT ANTI JOIN hits ON hits.root_domain = h.root_domain AND hits.record_name = h.record_name
                        AND hits.signal_type = h.signal_type AND hits.candidate = h.candidate
    WHERE h.signal_type != 'dns_txt'
) AS f
LEFT JOIN key_map AS km ON km.lookup_key = f.lookup_key
WHERE host_key != '' OR self_hosted"""


def range_keys_ddl(keys: str) -> str:
    return f"""CREATE TABLE {keys}
(
    ip_family UInt8,
    net_key UInt32,
    provider_slug LowCardinality(String),
    service_key String,
    cidr String,
    prefix_len UInt8,
    range_start IPv6,
    range_end IPv6,
    feed_tag LowCardinality(String),
    confidence Float32,
    valid_from Date,
    valid_to Date
)
ENGINE = MergeTree
ORDER BY (ip_family, net_key)"""


def _net_key(ip: str, family: str) -> str:
    """The join key of an IPv6 (IPv4-mapped for family 4) address: its /16
    (IPv4) or /32 (IPv6) network as a UInt32."""
    return (
        f"if({family} = 4, "
        f"bitShiftRight(reinterpretAsUInt32(reverse(substring(IPv6StringToNum(toString({ip})), 13, 4))), 16), "
        f"reinterpretAsUInt32(reverse(substring(IPv6StringToNum(toString({ip})), 1, 4))))"
    )


def range_keys_insert_sql(database: str, keys: str) -> str:
    """Provider ranges expanded to one row per covered /16 (IPv4) or /32 (IPv6).

    Ranges wider than MIN_PREFIX are left out (see wide_ranges_count_sql).
    Validity: [first_seen, removed_at or today]; ranges from the first
    provider-recon run (the timeline start) count as valid since forever,
    because nothing earlier is known.
    """
    return f"""INSERT INTO {keys}
SELECT
    ip_family,
    arrayJoin(range(toUInt64({_net_key('range_start', 'ip_family')}), toUInt64({_net_key('range_end', 'ip_family')}) + 1)) AS net_key,
    provider_slug, service_key, cidr, prefix_len, range_start, range_end, feed_tag, confidence,
    if(first_seen <= (SELECT min(first_seen) FROM `{database}`.`{PROVIDER_IP_RANGES_TABLE}`), toDate('1970-01-01'), first_seen) AS valid_from,
    ifNull(removed_at, today()) AS valid_to
FROM (
    SELECT *, toUInt8(splitByChar('/', cidr)[2]) AS prefix_len
    FROM `{database}`.`{PROVIDER_IP_RANGES_TABLE}` FINAL
)
WHERE prefix_len >= if(ip_family = 4, {MIN_PREFIX[4]}, {MIN_PREFIX[6]})"""


def wide_ranges_count_sql(database: str) -> str:
    return f"""SELECT count()
FROM `{database}`.`{PROVIDER_IP_RANGES_TABLE}` FINAL
WHERE toUInt8(splitByChar('/', cidr)[2]) < if(ip_family = 4, {MIN_PREFIX[4]}, {MIN_PREFIX[6]})"""


def ip_evidence_sql(database: str, candidates: str, keys: str, stage: str) -> str:
    """Apex/www A/AAAA inside a provider range whose validity overlaps the
    record's seen window. Overlapping ranges resolve to the longest prefix;
    the evidence window is the overlap. One row per matched service type."""
    columns = ", ".join(EVIDENCE_COLUMNS)
    return f"""INSERT INTO {stage} ({columns})
WITH
    services AS (
        SELECT provider_slug, service_key, service_types
        FROM `{database}`.`{PROVIDER_SERVICES_TABLE}` FINAL
        WHERE removed_at IS NULL
    ),
    ips AS (
        SELECT root_domain, record_name, candidate, first_seen, last_seen, ip, family,
               {_net_key('ip', 'family')} AS net_key
        FROM (
            SELECT root_domain, record_name, candidate, first_seen, last_seen,
                   if(position(candidate, ':') > 0, 6, 4) AS family,
                   if(family = 4,
                      if(isNull(toIPv4OrNull(candidate)), NULL, toIPv6(concat('::ffff:', candidate))),
                      toIPv6OrNull(candidate)) AS ip
            FROM {candidates}
            WHERE signal_type = 'dns_ip'
        )
        WHERE ip IS NOT NULL
    ),
    matched AS (
        SELECT i.root_domain AS root_domain, i.record_name AS record_name, i.candidate AS candidate,
               argMax(tuple(k.provider_slug, k.service_key, k.cidr, k.feed_tag, k.confidence,
                            greatest(i.first_seen, toDateTime64(k.valid_from, 3, 'UTC')),
                            least(i.last_seen, toDateTime64(k.valid_to, 3, 'UTC') + toIntervalDay(1) - toIntervalMillisecond(1))),
                      k.prefix_len) AS best
        FROM ips AS i
        INNER JOIN {keys} AS k ON k.ip_family = i.family AND k.net_key = i.net_key
        WHERE assumeNotNull(i.ip) >= k.range_start AND assumeNotNull(i.ip) <= k.range_end
          AND toDate(i.first_seen) <= k.valid_to AND toDate(i.last_seen) >= k.valid_from
        GROUP BY i.root_domain, i.record_name, i.candidate
    )
SELECT
    m.root_domain, service_type, tupleElement(m.best, 1), tupleElement(m.best, 2), tupleElement(m.best, 1),
    'dns_ip', m.record_name, m.candidate, '', tupleElement(m.best, 3), tupleElement(m.best, 4),
    tupleElement(m.best, 6), tupleElement(m.best, 7), tupleElement(m.best, 5),
    'ip_range', %(source_run_id)s, %(detected_at)s
FROM matched AS m
INNER JOIN services AS s ON s.provider_slug = tupleElement(m.best, 1) AND s.service_key = tupleElement(m.best, 2)
ARRAY JOIN s.service_types AS service_type"""


def services_insert_sql(database: str, candidates: str, evidence_stage: str, services_stage: str) -> str:
    """Evidence → one row per (service_type, provider_key, root_domain).

    domain_last_seen is the newest last_seen of any of the domain's
    candidates in this pass; harmonic_rank comes from %(ranking_release)s
    (UInt64 max when unranked or when no release is given).
    """
    columns = ", ".join(SERVICES_COLUMNS)
    return f"""INSERT INTO {services_stage} ({columns})
WITH
    domains AS (
        SELECT root_domain, max(last_seen) AS domain_last_seen
        FROM {candidates}
        GROUP BY root_domain
    ),
    ranks AS (
        SELECT root_domain, min(cc_harmonic_rank) AS harmonic_rank
        FROM `{database}`.`{RANKS_TABLE}`
        WHERE graph_release = %(ranking_release)s
          AND root_domain IN (SELECT root_domain FROM {evidence_stage})
        GROUP BY root_domain
    )
SELECT
    e.service_type, e.provider_key, any(e.provider_slug),
    arraySort(groupUniqArrayIf(e.service_key, e.service_key != '')),
    e.root_domain,
    arraySort(groupUniqArray(e.signal_type)),
    min(e.first_seen), max(e.last_seen),
    any(d.domain_last_seen),
    max(e.confidence),
    ifNull(any(r.harmonic_rank), {UNRANKED}),
    %(source_run_id)s, %(detected_at)s
FROM {evidence_stage} AS e
LEFT JOIN domains AS d ON d.root_domain = e.root_domain
LEFT JOIN ranks AS r ON r.root_domain = e.root_domain
GROUP BY e.service_type, e.provider_key, e.root_domain
SETTINGS join_use_nulls = 1"""


def replace_partition_sql(qualified: str, stage: str, bucket: int) -> str:
    return f"ALTER TABLE {qualified} REPLACE PARTITION {int(bucket)} FROM {stage}"
```

- [ ] **Step 3: Run.**

Run: `uv run pytest -q tests/test_domain_services_sql.py`
Expected: PASS, 6 passed (about 90 s: each test boots clickhouse-local in Docker).

- [ ] **Step 4: Commit.**

```bash
git add services/dagster_v3/src/dagster_v3/defs/domain_services/__init__.py services/dagster_v3/src/dagster_v3/defs/domain_services/sql.py services/dagster_v3/tests/test_domain_services_sql.py
git commit -m "feat(dagster): domain services detection SQL (DNS rules, provider keys, IP ranges)

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

---

### Task 3: Asset, job, weekly schedule, definitions sensor

**Files:**
- Create: `src/dagster_v3/defs/domain_services/assets.py`, `tests/test_domain_services_assets.py`

**Interfaces:**
- Consumes: everything from Task 2's `sql`; the `graph_catalog` resource (`GraphCatalogResource.get_store().latest_ranking_release(client) -> str | None`), registered globally by `commoncrawl_domain_graph/discovery.py`; and `clickhouse`.
- Produces:
  - asset `domain_services_clickhouse` (group `domain_services`, deps `provider_recon_clickhouse`, pool `domain_signal_detection`);
  - job `domain_services_job`;
  - schedule `domain_services_weekly` (`40 4 * * 0` UTC, STOPPED);
  - sensor `domain_services_provider_definitions_sensor` (STOPPED; its cursor is the definitions digest).

- [ ] **Step 1: Failing test.** `tests/test_domain_services_assets.py`:

```python
from contextlib import contextmanager
from datetime import datetime

import dagster as dg
import pytest

from dagster_v3.defs.commoncrawl_domain_graph.store import GraphCatalogResource
from dagster_v3.defs.domain_services import assets, sql


class FakeStore:
    def __init__(self, release):
        self.release = release

    def latest_ranking_release(self, client):
        return self.release


class FakeCatalog(GraphCatalogResource):
    release: str | None = "rel1"

    @contextmanager
    def get_store(self):
        yield FakeStore(self.release)


class FakeClient:
    def __init__(self, candidates: int, evidence: dict[str, int]) -> None:
        self.candidates = candidates
        self.evidence = evidence
        self.calls: list[tuple[str, object]] = []

    def execute(self, query, params=None, **_):
        self.calls.append((query, params))
        if "system.tables" in query:
            return [(name,) for name in assets.REQUIRED_TABLES]
        if query.startswith("SELECT count() FROM") and "_tmp_dns_service_candidates_" in query:
            return [(self.candidates,)]
        if "GROUP BY source" in query:
            return list(self.evidence.items())
        if query.startswith("SELECT count() FROM") and "_tmp_domain_services_" in query:
            return [(sum(self.evidence.values()) // 2,)]
        if "splitByChar('/', cidr)[2]) <" in query:
            return [(1,)]
        return []

    @property
    def statements(self) -> list[str]:
        return [q for q, _ in self.calls]


class FakeClickhouse:
    def __init__(self, client: FakeClient) -> None:
        self.client = client

    @contextmanager
    def get_connection(self):
        yield self.client


def materialize(client: FakeClient, release="rel1"):
    with dg.build_asset_context(partition_key="hash_005") as context:
        return assets.domain_services_clickhouse(
            context, clickhouse=FakeClickhouse(client), graph_catalog=FakeCatalog(postgres_url="postgresql://x", release=release)
        )


def test_asset_rebuilds_one_bucket_and_swaps_both_tables() -> None:
    client = FakeClient(candidates=4_000, evidence={"dns": 3_000, "ip_range": 900})
    result = materialize(client)
    statements = client.statements
    candidates_insert = next(s for s in statements if s.startswith("INSERT INTO") and "commoncrawl_domain_dns_records" in s)
    assert "cityHash64(root_domain) % 128 = 5" in candidates_insert
    assert "cityHash64(root_domain) % 16 = 5" in candidates_insert
    replaces = [s for s in statements if "REPLACE PARTITION" in s]
    assert [s.split(" REPLACE PARTITION ")[1].split(" FROM ")[0] for s in replaces] == ["5", "5"]
    assert "`domain_service_evidence`" in replaces[0] and "`domain_services`" in replaces[1]
    assert sum(1 for s in statements if s.startswith("DROP TABLE IF EXISTS")) == 4
    host_params = next(p for q, p in client.calls if q.startswith("INSERT INTO") and "WITH\n    rules AS" in q)
    assert host_params["ranking_release"] == "rel1"
    assert result.metadata["evidence_rows"] == 3_900
    assert result.metadata["ip_evidence"] == 900
    assert result.metadata["wide_ranges_skipped"] == 1


@pytest.mark.parametrize("candidates,evidence", [(0, {}), (4_000, {})])
def test_asset_refuses_to_replace_an_empty_bucket(candidates, evidence) -> None:
    client = FakeClient(candidates=candidates, evidence=evidence)
    with pytest.raises(ValueError, match="refusing to replace"):
        materialize(client)
    assert not any("REPLACE PARTITION" in s for s in client.statements)
    assert sum(1 for s in client.statements if s.startswith("DROP TABLE IF EXISTS")) == 4


def test_asset_stores_domains_unranked_without_a_release() -> None:
    client = FakeClient(candidates=10, evidence={"dns": 10})
    result = materialize(client, release=None)
    assert result.metadata["ranking_release"] == ""


def test_weekly_schedule_requests_every_bucket() -> None:
    with dg.instance_for_test() as instance:
        context = dg.build_schedule_context(instance=instance, scheduled_execution_time=datetime(2026, 10, 4, 4, 40))
        requests = assets.domain_services_weekly(context)
    assert len(requests) == sql.PARTITION_COUNT
    assert requests[0].partition_key == "hash_000"
    assert requests[0].run_key == "weekly-20261004-hash_000"


class DigestClient:
    def __init__(self, digest: str) -> None:
        self.digest = digest

    def execute(self, query, params=None, **_):
        return [(self.digest,)]


def evaluate_sensor(digest: str, cursor: str | None, running: bool = False):
    with dg.instance_for_test() as instance:
        if running:
            instance.get_run_records = lambda **_: [object()]
        context = dg.build_sensor_context(instance=instance, cursor=cursor)
        return assets.domain_services_provider_definitions_sensor(context, clickhouse=FakeClickhouse(DigestClient(digest)))


def test_sensor_records_a_baseline_first() -> None:
    result = evaluate_sensor("d1", None)
    assert result.cursor == "d1"
    assert result.skip_reason is not None
    assert not result.run_requests


def test_sensor_skips_unchanged_definitions() -> None:
    assert isinstance(evaluate_sensor("d1", "d1"), dg.SkipReason)


def test_sensor_refreshes_every_bucket_when_definitions_change() -> None:
    result = evaluate_sensor("d2", "d1")
    assert len(result.run_requests) == sql.PARTITION_COUNT
    assert result.run_requests[5].run_key == "definitions-d2-hash_005"
    assert result.cursor == "d2"


def test_sensor_waits_while_a_refresh_is_running() -> None:
    # A SkipReason carries no cursor, so the change is retried after the refresh ends.
    assert isinstance(evaluate_sensor("d2", "d1", running=True), dg.SkipReason)
```

Run: `uv run pytest -q tests/test_domain_services_assets.py`
Expected: FAIL with `ImportError: cannot import name 'assets'`.

- [ ] **Step 2: Implement.** `src/dagster_v3/defs/domain_services/assets.py`:

```python
"""domain_services_clickhouse: DNS records + provider-recon evidence → domain services.

Design: docs/superpowers/specs/2026-09-27-domain-services-and-technology-domains-design.md
(revision 2). 128 static hash partitions; each run rebuilds one bucket of
corpscout.domain_service_evidence and corpscout.domain_services (migration
000465) from one pass over the DNS record store, then swaps both slices in with
REPLACE PARTITION. Pool domain_signal_detection keeps it off the DNS store
while the old detection asset runs.

Refresh is server-side only: a weekly schedule re-runs every bucket (new DNS
records, provider range churn), and a sensor does the same when provider
definitions (services, keys, DNS rules) change. Both start STOPPED.
"""

import uuid
from datetime import UTC, datetime

import dagster as dg
from dagster import AssetExecutionContext
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.clickhouse.resolved import RESOLVED_DATABASE, assert_clickhouse_tables_exist
from dagster_v3.defs.commoncrawl_domain_graph.store import GraphCatalogResource
from dagster_v3.defs.domain_services import sql

GROUP_NAME = "domain_services"
JOB_NAME = "domain_services_job"
PARTITIONS = dg.StaticPartitionsDefinition(sql.partition_keys())
QUERY_SETTINGS = {
    "max_bytes_before_external_group_by": 8 * 1024**3,
    "max_memory_usage": 12 * 1024**3,
}
REQUIRED_TABLES = (
    sql.EVIDENCE_TABLE,
    sql.SERVICES_TABLE,
    sql.PROVIDER_SERVICES_TABLE,
    sql.PROVIDER_RULES_TABLE,
    sql.PROVIDER_IP_RANGES_TABLE,
    sql.DNS_RECORDS_TABLE,
)


@dg.asset(
    name="domain_services_clickhouse",
    deps=[dg.AssetKey("provider_recon_clickhouse")],
    group_name=GROUP_NAME,
    kinds={"clickhouse", "sql"},
    partitions_def=PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    pool="domain_signal_detection",
    code_version="detect1",
    description=(
        "Domain services (service type + provider) from DNS records and "
        "provider-recon evidence: corpscout.domain_service_evidence and "
        "corpscout.domain_services (migration 000465). 128 static hash "
        "partitions (cityHash64(root_domain) %% 128); each run extracts one "
        "bucket's NS/SOA/MX/TXT/SPF/DKIM/CNAME/A/AAAA candidates, labels hosts "
        "by provider-recon DNS rules or their registrable domain, matches IPs "
        "against provider ranges valid in the record's window, and swaps only "
        "its slice in with REPLACE PARTITION."
    ),
)
def domain_services_clickhouse(
    context: AssetExecutionContext,
    clickhouse: ClickhouseResource,
    graph_catalog: GraphCatalogResource,
) -> dg.MaterializeResult:
    assert_clickhouse_tables_exist(clickhouse, database=RESOLVED_DATABASE, tables=REQUIRED_TABLES)
    bucket = sql.partition_bucket(context.partition_key)
    db = RESOLVED_DATABASE
    suffix = uuid.uuid4().hex
    evidence = f"`{db}`.`{sql.EVIDENCE_TABLE}`"
    services = f"`{db}`.`{sql.SERVICES_TABLE}`"
    evidence_stage = f"`{db}`.`_tmp_{sql.EVIDENCE_TABLE}_{suffix}`"
    services_stage = f"`{db}`.`_tmp_{sql.SERVICES_TABLE}_{suffix}`"
    candidates = f"`{db}`.`_tmp_dns_service_candidates_{suffix}`"
    keys = f"`{db}`.`_tmp_provider_range_keys_{suffix}`"

    with clickhouse.get_connection() as client:
        with graph_catalog.get_store() as store:
            release = store.latest_ranking_release(client)
        if not release:
            context.log.warning("no ranking release loaded; every domain is stored unranked")
        params = {
            "source_run_id": context.run_id,
            "detected_at": datetime.now(UTC).replace(tzinfo=None),
            "ranking_release": release or "",
        }
        try:
            client.execute(f"CREATE TABLE {evidence_stage} AS {evidence}")
            client.execute(f"CREATE TABLE {services_stage} AS {services}")
            client.execute(sql.candidates_ddl(candidates))
            client.execute(sql.range_keys_ddl(keys))
            # Each INSERT…SELECT blocks for a while; log the phase before it.
            context.log.info("bucket %d: extracting candidates…", bucket)
            client.execute(sql.candidates_insert_sql(db, candidates, bucket), settings=QUERY_SETTINGS)
            candidate_count = int(client.execute(f"SELECT count() FROM {candidates}")[0][0])
            wide_ranges = int(client.execute(sql.wide_ranges_count_sql(db))[0][0])
            if wide_ranges:
                context.log.warning("%d provider ranges are too wide to match and were skipped", wide_ranges)
            client.execute(sql.range_keys_insert_sql(db, keys))
            context.log.info("bucket %d: %d candidates, labelling hosts…", bucket, candidate_count)
            client.execute(sql.host_evidence_sql(db, candidates, evidence_stage), params, settings=QUERY_SETTINGS)
            context.log.info("bucket %d: matching IPs…", bucket)
            client.execute(sql.ip_evidence_sql(db, candidates, keys, evidence_stage), params, settings=QUERY_SETTINGS)
            per_source = dict(client.execute(f"SELECT source, count() FROM {evidence_stage} GROUP BY source"))
            evidence_rows = sum(per_source.values())
            if candidate_count == 0 or evidence_rows == 0:
                raise ValueError(
                    f"bucket {bucket}: {candidate_count} candidates and {evidence_rows} evidence rows; "
                    "refusing to replace the partition"
                )
            client.execute(
                sql.services_insert_sql(db, candidates, evidence_stage, services_stage), params, settings=QUERY_SETTINGS
            )
            service_rows = int(client.execute(f"SELECT count() FROM {services_stage}")[0][0])
            context.log.info("bucket %d: publishing partition…", bucket)
            client.execute(sql.replace_partition_sql(evidence, evidence_stage, bucket))
            client.execute(sql.replace_partition_sql(services, services_stage, bucket))
        finally:
            for table in (candidates, keys, evidence_stage, services_stage):
                client.execute(f"DROP TABLE IF EXISTS {table}")
    return dg.MaterializeResult(
        metadata={
            "candidates": candidate_count,
            "evidence_rows": evidence_rows,
            "dns_evidence": int(per_source.get("dns", 0)),
            "ip_evidence": int(per_source.get("ip_range", 0)),
            "service_rows": service_rows,
            "wide_ranges_skipped": wide_ranges,
            "ranking_release": release or "",
        }
    )


domain_services_job = dg.define_asset_job(
    name=JOB_NAME,
    selection=dg.AssetSelection.assets(domain_services_clickhouse),
)

_ACTIVE = [
    dg.DagsterRunStatus.QUEUED,
    dg.DagsterRunStatus.NOT_STARTED,
    dg.DagsterRunStatus.STARTING,
    dg.DagsterRunStatus.STARTED,
]


def full_refresh(context, refresh_key: str) -> list[dg.RunRequest] | dg.SkipReason:
    """One run request per bucket, unless a previous refresh is still running."""
    active = context.instance.get_run_records(filters=dg.RunsFilter(job_name=JOB_NAME, statuses=_ACTIVE), limit=1)
    if active:
        return dg.SkipReason(f"{JOB_NAME} still has queued or running buckets; not starting another refresh")
    return [
        dg.RunRequest(run_key=f"{refresh_key}-{key}", partition_key=key, tags={"domain_services/refresh": refresh_key})
        for key in PARTITIONS.get_partition_keys()
    ]


# Sunday 04:40 UTC: after the daily provider-recon load (03:12) and outside
# the Tuesday SE address chain.
@dg.schedule(
    job=domain_services_job,
    cron_schedule="40 4 * * 0",
    execution_timezone="UTC",
    default_status=dg.DefaultScheduleStatus.STOPPED,
)
def domain_services_weekly(context: dg.ScheduleEvaluationContext):
    return full_refresh(context, f"weekly-{context.scheduled_execution_time:%Y%m%d}")


# Provider definitions only: services, their keys and types, and the active DNS
# rules. IP ranges churn daily and are picked up by the weekly refresh.
DEFINITIONS_DIGEST_SQL = f"""SELECT toString(cityHash64(
    (SELECT arrayStringConcat(arraySort(groupUniqArray(concat(provider_slug, '|', service_key, '|',
        arrayStringConcat(arraySort(service_types), ','), '|', arrayStringConcat(arraySort(provider_keys), ','))
    )), ';') FROM `{RESOLVED_DATABASE}`.`{sql.PROVIDER_SERVICES_TABLE}` FINAL WHERE removed_at IS NULL),
    (SELECT arrayStringConcat(arraySort(groupUniqArray(concat(provider_slug, '|', service_key, '|', rule_key, '|',
        record_type, '|', match_field, '|', matcher_type, '|', pattern, '|', toString(priority))
    )), ';') FROM `{RESOLVED_DATABASE}`.`{sql.PROVIDER_RULES_TABLE}` FINAL WHERE kind = 'dns' AND status != 'removed')
))"""


@dg.sensor(
    job=domain_services_job,
    minimum_interval_seconds=600,
    default_status=dg.DefaultSensorStatus.STOPPED,
)
def domain_services_provider_definitions_sensor(context: dg.SensorEvaluationContext, clickhouse: ClickhouseResource):
    with clickhouse.get_connection() as client:
        digest = str(client.execute(DEFINITIONS_DIGEST_SQL)[0][0])
    if not context.cursor:
        return dg.SensorResult(
            skip_reason=dg.SkipReason("recorded the current provider definitions as the baseline"), cursor=digest
        )
    if context.cursor == digest:
        return dg.SkipReason("provider definitions unchanged")
    result = full_refresh(context, f"definitions-{digest}")
    if isinstance(result, dg.SkipReason):
        return result  # cursor not advanced: retried once the running refresh ends
    return dg.SensorResult(run_requests=result, cursor=digest)


defs = dg.Definitions(
    assets=[domain_services_clickhouse],
    jobs=[domain_services_job],
    schedules=[domain_services_weekly],
    sensors=[domain_services_provider_definitions_sensor],
)
```

- [ ] **Step 3: Run.**

Run: `uv run pytest -q tests/test_domain_services_assets.py tests/test_domain_services_sql.py tests/test_clickhouse_migrations.py && uv run dg check defs`
Expected: 9 + 6 + 135 passed; `All definitions loaded successfully.`

- [ ] **Step 4: Commit.**

```bash
git add services/dagster_v3/src/dagster_v3/defs/domain_services/assets.py services/dagster_v3/tests/test_domain_services_assets.py
git commit -m "feat(dagster): domain_services_clickhouse asset with weekly refresh and provider-definitions sensor

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

---

### Task 4: Production rollout and verification

Migrations, deploys and launches are the executor's to run (owner rule 2026-09-13). The executor stops and asks only at the points marked **STOP**.

- [ ] **Step 1: Ledger precondition.**

```bash
ssh companycollect "docker exec clickhouse-clickhouse-1 clickhouse-client -q \"SELECT version, dirty FROM corpscout.schema_migrations ORDER BY version DESC LIMIT 3\""
git -C <main checkout> ls-tree --name-only main corpscout/clickhouse/migrations/ | tail -4
```

- **Continue** if main contains committed 000461–000464 (or whatever sits between the prod ledger and 000465) and those are applied: rebase the branch on main, re-run Task 3's Step 3, and continue.
- **STOP** and report to the owner if the migrations between the ledger and 000465 are still another session's uncommitted files, or are committed but not applied (000464 carries inline `throwIf` gates). golang-migrate can't step from the ledger version to 000465 without the intermediate files. The owner decides the order.

- [ ] **Step 2: Apply 000465** with the same migrate invocation used for 000460 (the dagster_v3 deployment runbook's migration section).

Verify:
```bash
ssh companycollect "docker exec clickhouse-clickhouse-1 clickhouse-client -q \"SELECT name FROM system.tables WHERE database='corpscout' AND name IN ('domain_service_evidence','domain_services','domain_services_current','unmapped_provider_keys') ORDER BY name\""
```
Expected: all 4 names.

- [ ] **Step 3: Deploy dagster_v3 from the branch worktree** with the pristine-worktree recipe:
  - refresh the dbt state;
  - honour the deploy lock at `/run/lock/corpscout-dagster-deploy`: wait if it's held, never delete it;
  - check that main hasn't moved.

Expected: the code location reloads, and `domain_services_clickhouse`, `domain_services_job`, `domain_services_weekly` (STOPPED) and `domain_services_provider_definitions_sensor` (STOPPED) are visible.

- [ ] **Step 4: One bucket.** Launch partition `hash_003` of `domain_services_job` from the Dagster UI (or with GraphQL `launchRun` and the tag `dagster/partition=hash_003`). Wait for SUCCESS and record the duration and the metadata.

Then spot-check:
```bash
ssh companycollect "docker exec clickhouse-clickhouse-1 clickhouse-client -q \"
SELECT service_type, provider_key, count() c FROM corpscout.domain_services_current GROUP BY ALL ORDER BY c DESC LIMIT 25;
SELECT source, count() FROM corpscout.domain_service_evidence GROUP BY source;
SELECT count() FROM corpscout.domain_services WHERE cityHash64(root_domain) % 128 != 3\""
```
Expected:
- `dns`/`email` rows led by large providers (GoDaddy's `domaincontrol.com`, `cloudflare`, `google`…);
- both `dns` and `ip_range` sources present;
- the last count 0, since only bucket 3 is built.

**STOP** if the run took over 20 minutes or used over 12 GiB. Report the numbers before running all 128 buckets.

- [ ] **Step 5: Full run.** Launch a backfill of all 128 partitions of `domain_services_clickhouse` from the Dagster UI. That is a server-side backfill with one run per partition, serialised by the pool. Don't babysit it in a local loop. Check it later, then verify:
```bash
ssh companycollect "docker exec clickhouse-clickhouse-1 clickhouse-client -q \"
SELECT uniqExact(_partition_id) FROM corpscout.domain_services;
SELECT uniqExact(root_domain) FROM corpscout.domain_services;
SELECT service_type, count() FROM corpscout.domain_services_current GROUP BY 1 ORDER BY 2 DESC;
SELECT * FROM corpscout.unmapped_provider_keys ORDER BY current_domains DESC LIMIT 20\""
```
Expected:
- 128 partitions;
- the domain count close to the DNS store's domain count (about 46M);
- every service type present;
- the unmapped list led by real hosting providers, which are candidates to add to provider-recon's YAML.

- [ ] **Step 6: Start refresh.** Start `domain_services_weekly` and `domain_services_provider_definitions_sensor`. The sensor's first tick only records the baseline digest (a SkipReason), so it doesn't trigger a second full run.

- [ ] **Step 7: Memory.** Update `domain-services-provider-model.md`: live date, bucket timing, full-run duration, row counts, and the top unmapped keys for the owner.
