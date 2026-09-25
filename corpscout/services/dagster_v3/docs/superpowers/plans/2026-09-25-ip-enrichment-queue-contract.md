# IP Enrichment Queue Contract Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move IP enrichment onto the shared processing queue contract (drafts with receipts, a task-partitioned entry table, live remaining work, acknowledged result batches, completion from results, `DROP PARTITION` cleanup), cut the per-IP ClickHouse round trips that made the 48.6M-IP run take 76 days, stop reusing registry-level RDAP blocks such as `APNIC-AP` for other addresses, and automate GeoLite2 updates.

**Architecture:** The draft import (`ip_enrichment_input`) appends selections to the open workspace draft with one `INSERT … SELECT` from `selected_ips_sql`, keyed by an `input_id` that starts with the IP's 256-way bucket. The execution (`ip_enrichment_results`) freezes the draft through `defs/common/queue_execution.py`, walks the task bucket by bucket with a live "remaining" query (entries without a result of this execution, each query one primary-key range of `ip_enrichment_results`), resolves each page's RDAP coverage with a fixed number of ClickHouse round trips (one negative-cache read, one trie `dictGet`, one read of the network rows the page needs, one insert of lookup markers) and HTTP only for misses, stores outcomes through `ResultBuffer`, and finishes with counts derived from results. A registry-level classifier (Python + an identical SQL predicate baked into the trie's source view) keeps RIR/IANA/unallocated blocks out of `rdap_network_trie`. A weekly asset downloads, verifies and atomically installs the GeoLite2 files, with a 14-day staleness check. The backoffice "Enrich" action becomes "Add to enrichment queue" and processing starts from `/admin/queues/ip-enrichment`.

**Tech Stack:** Python 3.14, Dagster 1.13.9, ClickHouse 26.5 (clickhouse-driver), PostgreSQL (psycopg2), whoisit 4 (RDAP), maxminddb, netaddr, dlt requests helpers, React Router 8 + vitest backoffice, pytest against disposable ClickHouse/PostgreSQL containers.

**Spec:** `services/dagster_v3/docs/superpowers/specs/2026-09-24-shared-processing-queue-contract-design.md`
**Binding decisions:** the owner's decisions file of 2026-09-25 (D1–D10 below, with its two review reports as evidence). Reference implementations on prod: webtech (`defs/webtech/{input,execution,task_assets}.py`, migration `000446`), crawl (`defs/website_crawl/{queue_input,queue_execution}.py`, migration `000448`, plan `2026-09-25-crawl-queue-contract.md`), shared `defs/common/{queue_execution,draft_queue,result_buffer}.py`.

## Global Constraints

- ClickHouse holds entry lists, PostgreSQL holds coordination. Never `UPDATE`/`DELETE` individual entry rows; the only exception is the submission retry `DELETE … WHERE task_id AND submission_id` while the draft is open, which is why the table keeps `SETTINGS number_of_free_entries_in_pool_to_execute_mutation = 1` (D1).
- Entry table `corpscout.ip_enrichment_input`: `ENGINE = MergeTree`, `PARTITION BY task_id`, `ORDER BY (task_id, input_id)`, `task_id String`, required `submission_id`. Plain `MergeTree` rejects `FINAL`; the backoffice reads it ordered by `(task_id, input_id)` without `FINAL` (D1, D8).
- `input_id` = `leftPad(bucket, 3, '0') + ':' + toJSONString(tuple(source_name, source_record_id, ip))`, computed in ClickHouse (`INPUT_ID_SQL`, Task 2) and enforced by a `CHECK`. This is the one deliberate change to today's `toJSONString(tuple(...))` identity: it makes `ORDER BY (task_id, input_id)` walk a task bucket by bucket, so every remaining/completion query anti-joins one primary-key range (`bucket = b`) of `ip_enrichment_results` instead of scanning the whole results table per page (48.6M-row tasks). Results keep carrying `input_id`; identity is per task, so the ~1.08M kept results of the terminated run (old format, legacy task) are unaffected.
- Results table `corpscout.ip_enrichment_results` and view `ip_enrichment_current` are never touched; legacy GeoIP rows (task `cd603d91-…`) stay.
- Freeze via `queue_execution.start_execution`: frozen profile `force_rdap, rdap_cache_days, parent_depth, rate_limit_retry_seconds, transient_retry_seconds, processor_version`; transport keys `batch_size, max_requests, request_delay_seconds`; `execution_id` = the original Dagster run id (`default_execution_id = root run id`); an explicit `execution_id` only resumes (D4).
- RDAP cache freshness is bounded by the frozen execution: a network row or negative marker counts as fresh when its time is `>= freshness_cutoff = started_at − rdap_cache_days`; a retryable-error marker is honoured when `retry_after > started_at`. Never `datetime.now()` per lookup. The window has no upper bound at `started_at` on purpose: a network this execution fetched (`fetched_at > started_at`) must stay reusable on resume, otherwise every resume re-requests every network the run already discovered (D4, decided here).
- Remaining = live ClickHouse anti-join per bucket; done means a result row exists; an RDAP error is a published outcome (`completed_with_errors`); failed IPs are retried by a new draft (`retry_failed_task_id`, Task 3) (D4).
- Per page: ONE negative-cache query, ONE trie lookup, ONE read of uncached network rows (never `raw_response`), ONE lookup-marker insert, results through `ResultBuffer`; RDAP HTTP only for misses; a test asserts the ClickHouse query count per page is bounded (D5). Network/segment writes for misses stay per miss (decided: coverage must be durable before the result that references it, and a miss already costs ≥ 1.4 s of HTTP + delay; ~1% of IPs). Lookup markers (`rdap_ip_lookup_results`) are buffered per page.
- Registry-level rule (D6): a registration is per-IP only (never in the trie, never reused in-run) when its widest segment is shorter than `/8` (IPv4) or `/12` (IPv6), or a token of `name`/`handle`/`registration_type`/`status` is one of `IANA, APNIC, ARIN, LACNIC, AFRINIC, RIPE, UNALLOCATED, UNSPECIFIED`, or a registrant handle is one of `ARIN, IANA, APNIC, LACNIC, AFRINIC, ORG-NCC1-RIPE`, or a registrant name contains a registry's full name, or `country_code = 'ZZ'`. Remarks are not consulted (decided: they are not a normalized column, and the Python rule and the view predicate must agree exactly; every example in the review is caught by normalized fields). Such segments are stored with `segment_role = 'registry_level'`; the view `rdap_network_segments_current` excludes them and every existing poisoned row by the same predicate.
- GeoLite2 (D7): asset `geolite2_databases` + weekly schedule (STOPPED by default, started at instance level) + check `geolite2_databases_fresh` (fails at > 14 days). Credentials `MAXMIND_ACCOUNT_ID` / `MAXMIND_LICENSE_KEY` (none exist today; `.env.example` gets them and its line 70 is corrected).
- `ip_enrichment_workflow` is removed; the backoffice "Enrich" adds to the draft (`ip_enrichment_input_job`), processing starts from the queue page (D8).
- Out of scope, listed as follow-ups at the end (D9).
- Destructive migrations carry an inline `throwIf` gate; migration comments must not contain `;`. Next free ClickHouse numbers on main and prod are **000449** (trie exclusion) and **000450** (entry table); re-check at merge (the Common Crawl graph-ranks work took 447 during the crawl plan) with `ls clickhouse/migrations | tail -2` and prod `SELECT max(version) FROM corpscout.schema_migrations WHERE dirty=0` (the ledger is TinyLog with a dirty=1 and a dirty=0 row per version).
- Commands from `services/dagster_v3`: `uv run --frozen --no-sync pytest … -q -p no:cacheprovider`, `uv run --frozen --no-sync dg check defs`, `uv run --frozen --no-sync ruff format <touched files>` and `uv run --frozen --no-sync ruff check <touched files>` on touched Python files only. Backoffice from `services/backoffice`: `npm run typecheck` and targeted `npx vitest run <file>` only (the full suite hits prod ClickHouse). Test fixtures that start containers wait for `docker port` and probe with `docker exec … clickhouse-client` before use (already the case in `tests/test_ip_enrichment_input.py:server`).
- Commit by explicit path, never `git add -A` (`searcher/` is unrelated untracked work). Conventional commits with trailer `Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>`.
- Do not restart `corpscout-dagster-dev`; deploy by `light_sync`. Task 9 requires the owner's go-ahead. Never call real RDAP servers or MaxMind from tests.

## Task ordering note

The owner's suggested order is kept. Task 1 (registry-level fix, migration 449) comes first because it is independent of the queue work and the trie view it changes is what Tasks 4–5 test against. Task 2 (migration 450) precedes the import rewrite because the new table requires `submission_id` and the bucket-prefixed `input_id`, which today's `ip_enrichment_input` does not write; the input suite is red between Task 2 and Task 3 and the results suite between Task 2 and Task 5 (both are rewritten in those tasks), so run only the files each task names.

## File Structure

| File | Change | Responsibility after this plan |
| --- | --- | --- |
| `services/dagster_v3/src/dagster_v3/defs/commoncrawl_rdap/rdap.py` | modify | `registry_level_reason`, `REGISTRY_LEVEL_SQL`, `registry_level` segment role |
| `clickhouse/migrations/000449_corpscout_rdap_registry_level_exclusion.{up,down}.sql` | create | trie source view excludes registry-level networks; reader grants |
| `clickhouse/migrations/000450_corpscout_ip_enrichment_queue_contract.{up,down}.sql` | create | partitioned entry table |
| `services/dagster_v3/src/dagster_v3/defs/ip_enrichment/input.py` | rewrite | config, `selected_ips_sql` (+ failed-of-task mode), `INPUT_ID_SQL`, `load_ip_draft`, input asset/job |
| `services/dagster_v3/src/dagster_v3/defs/ip_enrichment/enrichment.py` | rewrite | GeoIP per address, `RdapEnricher.resolve_page` (bounded round trips), registry-level handling |
| `services/dagster_v3/src/dagster_v3/defs/ip_enrichment/results.py` | rewrite | freeze, bucket walk of remaining entries, `ResultBuffer`, finish, purge; no workflow job |
| `services/dagster_v3/src/dagster_v3/defs/commoncrawl_geoip/update.py` | create | GeoLite2 download/verify/install asset, freshness check, job, schedule |
| `services/dagster_v3/.env.example` | modify | MaxMind directory comment + credentials |
| `services/dagster_v3/tests/test_ip_enrichment_input.py` | rewrite | draft import against disposable ClickHouse/PostgreSQL; owns the module `server` fixture other suites import |
| `services/dagster_v3/tests/test_ip_enrichment_results.py` | rewrite | registry-level parity, execution loop, bounded queries, completion/purge |
| `services/dagster_v3/tests/test_ip_enrichment_clickhouse_local.py` | modify | new entry-table layout |
| `services/dagster_v3/tests/test_clickhouse_migrations.py` | modify | `EXPECTED_MIGRATIONS` |
| `services/dagster_v3/tests/test_geolite2_update.py` | create | install/verify/check unit tests |
| `services/backoffice/app/lib/ip-enrichment.server.ts` | rewrite | `addIpsToEnrichmentQueue`, `ipEnrichmentQueueSubmission`, selection parsing |
| `services/backoffice/app/routes/admin-ip-enrichment-queue-submission.ts` | create | import status route |
| `services/backoffice/app/routes/admin-ip-addresses.tsx`, `app/routes.ts`, `app/components/admin/queue-import-status.tsx`, `app/lib/queues.ts`, `app/lib/queues.server.ts`, `app/routes/admin-queue.tsx`, `app/components/admin/queue-process-sheet.tsx` | modify | draft semantics for the IP enrichment queue |
| `services/backoffice/tests/{ip-enrichment.server.test.ts, admin-ip-addresses-action.test.ts, queues.server.test.ts, queue-route.test.ts}` | modify | |
| `services/dagster_v3/docs/operations/ip-enrichment-draft-queue.md` | create | operations guide |
| `services/dagster_v3/docs/ip-enrichment-schema.md`, `services/backoffice/docs/queues.md`, `defs/commoncrawl_geoip/docs/commoncrawl_geoip-design.md`, `defs/commoncrawl_rdap/docs/commoncrawl_rdap-design.md`, `services/dagster_v3/docs/deployment-runbook.md` | modify | |
| spec status line | modify (Task 9) | |

---

### Task 1: Registry-level classifier and the trie exclusion migration (000449)

Evidence from the review: 173,991 IPs were served from `/8` blocks (`apnic:103.0.0.0-103.255.255.255` "APNIC-AP" 135,677; apnic 101/8, 113/8, 111/8; afrinic 102/8), plus `ripe:2A00::/11` "EU-ZZ-2A00", `arin:NET6-2600-1` (2600::/12, registrant ARIN) and LACNIC "UNALLOCATED" ranges stored as found. The trie source is the view `corpscout.rdap_network_segments_current` (migration 000258: `segment_role = 'lookup_result' AND prefix_length > 0`), read by the dictionary as user `corpscout_rdap_dictionary` (000126), which today has `SELECT` only on the segment tables.

**Files:**
- Modify: `services/dagster_v3/src/dagster_v3/defs/commoncrawl_rdap/rdap.py:1-12, 99-100`
- Create: `clickhouse/migrations/000449_corpscout_rdap_registry_level_exclusion.up.sql`, `…down.sql`
- Modify: `services/dagster_v3/tests/test_clickhouse_migrations.py:463` (`EXPECTED_MIGRATIONS`)
- Modify: `services/dagster_v3/tests/test_ip_enrichment_results.py:60-101` (`response`, `environment`) and append the parity tests

**Interfaces:**
- Produces (module `dagster_v3.defs.commoncrawl_rdap.rdap`):
  - `VALID_SEGMENT_ROLES = frozenset({"lookup_result", "parent", "registry_level"})`
  - `REGISTRY_TOKENS`, `REGISTRY_REGISTRANT_HANDLES`, `REGISTRY_REGISTRANT_PHRASES`, `MIN_REUSABLE_PREFIX = {4: 8, 6: 12}`
  - `REGISTRY_LEVEL_SQL: str` — the predicate over one `rdap_networks` row (attributes only; the prefix rule is evaluated over segments in the view). Copied verbatim into the migration; a test asserts it.
  - `registry_level_reason(network: RdapNetwork, segments: Sequence[RdapNetworkSegment]) -> str | None`.
- The dictionary `corpscout.rdap_network_trie` keeps its name, columns and `USER 'corpscout_rdap_dictionary'` source (`_assert_rdap_storage_exists` in `commoncrawl_rdap/assets.py:814-824` checks that string).

- [ ] **Step 1: Write the failing parity tests**

In `services/dagster_v3/tests/test_ip_enrichment_results.py` change the `response` helper (lines 60-80) to accept raw overrides:

```python
def response(ip, *, start=None, end=None, **raw):
    ipv6 = ":" in ip
    return RdapLookupResponse(
        rir="arin",
        raw_response={
            "objectClassName": "ip network",
            "handle": "TEST-" + ip,
            "startAddress": start
            or ("2001:4860::" if ipv6 else ip.rsplit(".", 1)[0] + ".0"),
            "endAddress": end
            or (
                "2001:4860:ffff:ffff:ffff:ffff:ffff:ffff"
                if ipv6
                else ip.rsplit(".", 1)[0] + ".255"
            ),
            "ipVersion": "v6" if ipv6 else "v4",
            "name": "Test registration",
            "country": "CA",
            "status": ["active"],
            **raw,
        },
    )
```

Replace the start of the `environment` fixture (lines 84-101, up to and including the `CREATE DICTIONARY` statement) with:

```python
MIGRATIONS = Path(__file__).resolve().parents[3] / "clickhouse/migrations"


def statements(sql: str, *, before: str | None = None) -> list[str]:
    """Executable statements of a migration file, comments and GRANTs stripped."""
    if before is not None:
        sql = sql.split(before, 1)[0]
    chosen = []
    for statement in sql.split(";"):
        lines = [
            line
            for line in statement.splitlines()
            if line.strip() and not line.lstrip().startswith("--")
        ]
        # Grants need the dictionary reader user; the test dictionary reads as `test`.
        if lines and not lines[0].lstrip().startswith("GRANT"):
            chosen.append("\n".join(lines))
    return chosen


@pytest.fixture
def environment(server, store, tmp_path, monkeypatch):
    client, resource = server
    for statement in statements(
        (MIGRATIONS / "000124_corpscout_rdap_networks.up.sql").read_text(),
        before="CREATE DICTIONARY",
    ):
        client.execute(statement)
    for statement in statements(
        (MIGRATIONS / "000449_corpscout_rdap_registry_level_exclusion.up.sql").read_text(),
        before="CREATE DICTIONARY",
    ):
        client.execute(statement)
    client.execute("DROP DICTIONARY IF EXISTS corpscout.rdap_network_trie")
    client.execute("""CREATE DICTIONARY corpscout.rdap_network_trie
        (cidr String, matched_cidr String, network_key String) PRIMARY KEY cidr
        SOURCE(CLICKHOUSE(HOST 'localhost' PORT 9000 USER 'test' PASSWORD 'test'
            DB 'corpscout' TABLE 'rdap_network_segments_current'))
        LAYOUT(IP_TRIE()) LIFETIME(0)""")
```

(The rest of the fixture — table truncation, readers, monkeypatches, `DagsterInstance.ephemeral()` — stays.) Add to the imports (`datetime` is needed by `registration()` here and by the Task 4/5 tests):

```python
from datetime import UTC, datetime, timedelta

from dagster_v3.defs.commoncrawl_rdap.assets import (
    RDAP_NETWORK_INSERT_SQL,
    RDAP_SEGMENT_INSERT_SQL,
)
from dagster_v3.defs.commoncrawl_rdap.rdap import (
    REGISTRY_LEVEL_SQL,
    RdapLookupResponse,
    normalize_rdap_network,
    registry_level_reason,
)
```

Append the parity tests:

```python
def registration(index, **raw):
    """A normalized IPv4 /24 holder registration unless ``raw`` says otherwise."""
    return normalize_rdap_network(
        RdapLookupResponse(
            rir="arin",
            raw_response={
                "objectClassName": "ip network",
                "handle": f"CASE-{index}",
                "startAddress": "8.8.8.0",
                "endAddress": "8.8.8.255",
                "ipVersion": "v4",
                "name": "HOLDER-NET",
                "type": "ASSIGNMENT",
                "status": ["active"],
                **raw,
            },
        ),
        fetched_at=datetime.now(UTC),
        segment_role="lookup_result",
    )


def registrant(handle, name):
    return {
        "objectClassName": "entity",
        "handle": handle,
        "roles": ["registrant"],
        "vcardArray": ["vcard", [["fn", {}, "text", name]]],
    }


REGISTRY_CASES = [
    ("holder /24", {}, True),
    ("apnic /8 block", {"handle": "103.0.0.0 - 103.255.255.255", "startAddress": "103.0.0.0", "endAddress": "103.255.255.255", "name": "APNIC-AP", "type": "ALLOCATED PORTABLE"}, False),
    ("wider than /8 without a marker", {"startAddress": "100.0.0.0", "endAddress": "103.255.255.255", "name": "SOMEONE"}, False),
    ("iana block", {"name": "IANA-BLOCK"}, False),
    ("allocated unspecified", {"type": "ALLOCATED UNSPECIFIED"}, False),
    ("unallocated status", {"status": ["UNALLOCATED"]}, False),
    ("registrant handle arin", {"entities": [registrant("ARIN", "American Registry for Internet Numbers")]}, False),
    ("registrant name ripe ncc", {"entities": [registrant("ORG-XY1-RIPE", "RIPE Network Coordination Centre")]}, False),
    ("country zz", {"country": "ZZ"}, False),
    ("marina is not arin", {"name": "MARINA-NET"}, True),
    ("ripe org handle suffix is not a marker", {"entities": [registrant("ORG-HZ1-RIPE", "Hetzner Online GmbH")]}, True),
    ("ipv6 /11", {"startAddress": "2a00::", "endAddress": "2a1f:ffff:ffff:ffff:ffff:ffff:ffff:ffff", "ipVersion": "v6", "name": "EU-ZZ-2A00"}, False),
    ("ipv6 /12 holder", {"startAddress": "2600::", "endAddress": "260f:ffff:ffff:ffff:ffff:ffff:ffff:ffff", "ipVersion": "v6", "name": "HOLDER6"}, True),
]


@pytest.mark.parametrize(("label", "raw", "reusable"), REGISTRY_CASES)
def test_registry_level_rule_in_python(label, raw, reusable):
    normalized = registration(0, **raw)
    assert (registry_level_reason(normalized.network, normalized.segments) is None) == reusable, label


def test_trie_view_and_python_agree_on_registry_level_networks(environment):
    env = environment
    expected = {}
    for index, (label, raw, reusable) in enumerate(REGISTRY_CASES):
        normalized = registration(index, **raw)
        expected[normalized.network.network_key] = reusable
        env.client.execute(RDAP_NETWORK_INSERT_SQL, [normalized.network.clickhouse_values()])
        env.client.execute(
            RDAP_SEGMENT_INSERT_SQL,
            [segment.clickhouse_values() for segment in normalized.segments],
        )
    served = {
        key
        for (key,) in env.client.execute(
            "SELECT DISTINCT network_key FROM corpscout.rdap_network_segments_current"
        )
    }
    assert {key: key in served for key in expected} == expected
    # A registry-level segment written by the enricher is excluded by its role alone.
    marked = registration(99, name="OK-NET")
    env.client.execute(RDAP_NETWORK_INSERT_SQL, [marked.network.clickhouse_values()])
    env.client.execute(
        RDAP_SEGMENT_INSERT_SQL,
        [(*segment.clickhouse_values()[:4], "registry_level", *segment.clickhouse_values()[5:]) for segment in marked.segments],
    )
    assert env.client.execute(
        "SELECT count() FROM corpscout.rdap_network_segments_current WHERE network_key = %(key)s",
        {"key": marked.network.network_key},
    ) == [(0,)]


def test_migration_449_carries_the_python_predicate_and_grants_the_reader():
    migration = (MIGRATIONS / "000449_corpscout_rdap_registry_level_exclusion.up.sql").read_text()
    normalize = lambda text: " ".join(text.split())  # noqa: E731
    assert normalize(REGISTRY_LEVEL_SQL) in normalize(migration)
    assert "min(prefix_length) < if(any(ip_version) = 4, 8, 12)" in migration
    assert "GRANT SELECT ON corpscout.rdap_networks_current TO corpscout_rdap_dictionary" in migration
    assert "USER 'corpscout_rdap_dictionary'" in migration
    assert migration.index("DROP DICTIONARY") < migration.index("DROP VIEW") < migration.index("CREATE VIEW") < migration.index("CREATE DICTIONARY")
```

- [ ] **Step 2: Run to verify they fail**

Run: `uv run --frozen --no-sync pytest tests/test_ip_enrichment_results.py -k "registry or migration_449" -q -p no:cacheprovider`
Expected: FAIL — `ImportError: cannot import name 'REGISTRY_LEVEL_SQL' from 'dagster_v3.defs.commoncrawl_rdap.rdap'`.

- [ ] **Step 3: Add the classifier to `rdap.py`**

Replace lines 1-12 of `services/dagster_v3/src/dagster_v3/defs/commoncrawl_rdap/rdap.py` with:

```python
import hashlib
import json
import re
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from ipaddress import ip_address
from typing import Any

from netaddr import iprange_to_cidrs


# A registry_level segment answers only the IP that was queried: it is stored for audit
# but never feeds rdap_network_trie (the view filters on segment_role).
VALID_SEGMENT_ROLES = frozenset({"lookup_result", "parent", "registry_level"})

# Marks a registration as an RIR/IANA block or unallocated space rather than a holder's
# network. The tokens are matched against whole tokens of name, handle, type and status
# (so MARINA is not ARIN); the handles are exact registrant handles; the phrases are
# searched in registrant names. RIPE org handles end in -RIPE, which is why the token
# rule never looks at registrant_handles.
REGISTRY_TOKENS = (
    "IANA",
    "APNIC",
    "ARIN",
    "LACNIC",
    "AFRINIC",
    "RIPE",
    "UNALLOCATED",
    "UNSPECIFIED",
)
REGISTRY_REGISTRANT_HANDLES = (
    "ARIN",
    "IANA",
    "APNIC",
    "LACNIC",
    "AFRINIC",
    "ORG-NCC1-RIPE",
)
REGISTRY_REGISTRANT_PHRASES = (
    "INTERNET ASSIGNED NUMBERS AUTHORITY",
    "ASIA PACIFIC NETWORK INFORMATION CENTRE",
    "RIPE NETWORK COORDINATION CENTRE",
    "RIPE NCC",
    "AMERICAN REGISTRY FOR INTERNET NUMBERS",
    "LATIN AMERICAN AND CARIBBEAN IP ADDRESS REGIONAL REGISTRY",
    "AFRICAN NETWORK INFORMATION CENTER",
)
# Widest reusable prefix per IP version: IANA hands /8s (v4) and /12s (v6) to the RIRs.
MIN_REUSABLE_PREFIX = {4: 8, 6: 12}


def _sql_list(values: Sequence[str]) -> str:
    return "[" + ", ".join(f"'{value}'" for value in values) + "]"


# The SQL twin of registry_level_reason over one corpscout.rdap_networks row, without the
# prefix rule (the view evaluates that over rdap_network_segments). Migration 000449
# embeds this text verbatim; tests/test_ip_enrichment_results.py asserts it.
REGISTRY_LEVEL_SQL = (
    "hasAny(splitByRegexp('[^A-Z0-9]+', upperUTF8(concat(ifNull(name, ''), ' ', handle, ' ', "
    "ifNull(registration_type, ''), ' ', arrayStringConcat(status, ' ')))), "
    + _sql_list(REGISTRY_TOKENS)
    + ") OR hasAny(registrant_handles, "
    + _sql_list(REGISTRY_REGISTRANT_HANDLES)
    + ") OR arrayExists(registrant -> multiSearchAny(upperUTF8(registrant), "
    + _sql_list(REGISTRY_REGISTRANT_PHRASES)
    + "), registrant_names) OR ifNull(country_code, '') = 'ZZ'"
)
```

After `is_registry_catch_all` (line 100) add:

```python
def registry_level_reason(
    network: "RdapNetwork", segments: Sequence["RdapNetworkSegment"]
) -> str | None:
    """Why a registration answers only the queried IP, or None when it is reusable coverage."""
    widest = min((segment.prefix_length for segment in segments), default=0)
    if widest < MIN_REUSABLE_PREFIX[network.ip_version]:
        return f"range wider than a /{MIN_REUSABLE_PREFIX[network.ip_version]}"
    text = " ".join(
        [network.name or "", network.handle, network.registration_type or "", *network.status]
    ).upper()
    tokens = set(re.split(r"[^A-Z0-9]+", text)) - {""}
    if tokens & set(REGISTRY_TOKENS):
        return "registry or unallocated marker in name, handle, type or status"
    if set(network.registrant_handles) & set(REGISTRY_REGISTRANT_HANDLES):
        return "registrant is a registry"
    if any(
        phrase in name.upper()
        for name in network.registrant_names
        for phrase in REGISTRY_REGISTRANT_PHRASES
    ):
        return "registrant is a registry"
    if network.country_code == "ZZ":
        return "country ZZ is a registry placeholder"
    return None
```

- [ ] **Step 4: Write the migrations**

`clickhouse/migrations/000449_corpscout_rdap_registry_level_exclusion.up.sql` (the `WHERE` of the second subquery is `REGISTRY_LEVEL_SQL` copied verbatim on one line):

```sql
CREATE DATABASE IF NOT EXISTS corpscout;

-- Registry-level and unallocated registrations (an RIR or IANA block, a range wider than
-- a /8 or /12, or a placeholder such as ALLOCATED UNSPECIFIED) answer only the IP that
-- was queried. They must not serve other IPs from the longest-prefix trie, so the view
-- feeding rdap_network_trie excludes them by range size and by normalized attributes.
-- The predicate is the SQL twin of registry_level_reason in commoncrawl_rdap/rdap.py.
-- The dictionary reader now needs the network attributes as well as the segments.
GRANT SELECT ON corpscout.rdap_networks TO corpscout_rdap_dictionary;
GRANT SELECT ON corpscout.rdap_networks_current TO corpscout_rdap_dictionary;

DROP DICTIONARY IF EXISTS corpscout.rdap_network_trie;
DROP VIEW IF EXISTS corpscout.rdap_network_segments_current;

CREATE VIEW corpscout.rdap_network_segments_current AS
SELECT
    cidr,
    cidr AS matched_cidr,
    argMax(network_key, tuple(derived_at, network_key)) AS network_key
FROM corpscout.rdap_network_segments FINAL
WHERE segment_role = 'lookup_result'
  AND prefix_length > 0
  AND network_key NOT IN (
      SELECT network_key
      FROM corpscout.rdap_network_segments FINAL
      WHERE segment_role = 'lookup_result'
      GROUP BY network_key
      HAVING min(prefix_length) < if(any(ip_version) = 4, 8, 12))
  AND network_key NOT IN (
      SELECT network_key
      FROM corpscout.rdap_networks_current
      WHERE hasAny(splitByRegexp('[^A-Z0-9]+', upperUTF8(concat(ifNull(name, ''), ' ', handle, ' ', ifNull(registration_type, ''), ' ', arrayStringConcat(status, ' ')))), ['IANA', 'APNIC', 'ARIN', 'LACNIC', 'AFRINIC', 'RIPE', 'UNALLOCATED', 'UNSPECIFIED']) OR hasAny(registrant_handles, ['ARIN', 'IANA', 'APNIC', 'LACNIC', 'AFRINIC', 'ORG-NCC1-RIPE']) OR arrayExists(registrant -> multiSearchAny(upperUTF8(registrant), ['INTERNET ASSIGNED NUMBERS AUTHORITY', 'ASIA PACIFIC NETWORK INFORMATION CENTRE', 'RIPE NETWORK COORDINATION CENTRE', 'RIPE NCC', 'AMERICAN REGISTRY FOR INTERNET NUMBERS', 'LATIN AMERICAN AND CARIBBEAN IP ADDRESS REGIONAL REGISTRY', 'AFRICAN NETWORK INFORMATION CENTER']), registrant_names) OR ifNull(country_code, '') = 'ZZ')
GROUP BY cidr;

CREATE DICTIONARY corpscout.rdap_network_trie
(
    cidr          String,
    matched_cidr  String,
    network_key   String
)
PRIMARY KEY cidr
SOURCE(
    CLICKHOUSE(
        USER 'corpscout_rdap_dictionary'
        DB 'corpscout'
        TABLE 'rdap_network_segments_current'
    )
)
LAYOUT(IP_TRIE())
LIFETIME(MIN 300 MAX 600);
```

`clickhouse/migrations/000449_corpscout_rdap_registry_level_exclusion.down.sql` (the 000258 layout):

```sql
CREATE DATABASE IF NOT EXISTS corpscout;

DROP DICTIONARY IF EXISTS corpscout.rdap_network_trie;
DROP VIEW IF EXISTS corpscout.rdap_network_segments_current;

CREATE VIEW corpscout.rdap_network_segments_current AS
SELECT
    cidr,
    cidr AS matched_cidr,
    argMax(network_key, tuple(derived_at, network_key)) AS network_key
FROM corpscout.rdap_network_segments FINAL
WHERE segment_role = 'lookup_result'
  AND prefix_length > 0
GROUP BY cidr;

CREATE DICTIONARY corpscout.rdap_network_trie
(
    cidr          String,
    matched_cidr  String,
    network_key   String
)
PRIMARY KEY cidr
SOURCE(
    CLICKHOUSE(
        USER 'corpscout_rdap_dictionary'
        DB 'corpscout'
        TABLE 'rdap_network_segments_current'
    )
)
LAYOUT(IP_TRIE())
LIFETIME(MIN 300 MAX 600);

REVOKE SELECT ON corpscout.rdap_networks_current FROM corpscout_rdap_dictionary;
REVOKE SELECT ON corpscout.rdap_networks FROM corpscout_rdap_dictionary;
```

In `services/dagster_v3/tests/test_clickhouse_migrations.py` append `"000449_corpscout_rdap_registry_level_exclusion",` after line 463 (`"000448_corpscout_crawl_queue_contract",`).

- [ ] **Step 5: Run the tests**

Run: `uv run --frozen --no-sync pytest tests/test_ip_enrichment_results.py -k "registry or migration_449" tests/test_clickhouse_migrations.py tests/test_commoncrawl_rdap_assets.py -q -p no:cacheprovider`
Expected: all pass (the remaining `test_ip_enrichment_results.py` tests still pass here — nothing else changed yet).

Run ruff format/check on `src/dagster_v3/defs/commoncrawl_rdap/rdap.py tests/test_ip_enrichment_results.py`.

- [ ] **Step 6: Commit**

```bash
git add services/dagster_v3/src/dagster_v3/defs/commoncrawl_rdap/rdap.py clickhouse/migrations/000449_corpscout_rdap_registry_level_exclusion.up.sql clickhouse/migrations/000449_corpscout_rdap_registry_level_exclusion.down.sql services/dagster_v3/tests/test_clickhouse_migrations.py services/dagster_v3/tests/test_ip_enrichment_results.py
git commit -m "fix(clickhouse): keep registry-level and unallocated RDAP blocks out of rdap_network_trie

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

---

### Task 2: Partitioned entry table (migration 000450) and the entry-table contract test

`task_id` becomes `String` (as in 446/448: the partition key and the shared `purge_completed_inputs` pass `DROP PARTITION %(task)s` as a string). The results table keeps `task_id UUID`; comparisons with a string parameter already work today (`results.py:83`).

**Files:**
- Create: `clickhouse/migrations/000450_corpscout_ip_enrichment_queue_contract.up.sql`, `…down.sql`
- Modify: `services/dagster_v3/tests/test_clickhouse_migrations.py` (`EXPECTED_MIGRATIONS`)
- Modify: `services/dagster_v3/tests/test_ip_enrichment_input.py:88-96` (`server` fixture) and append the contract test
- Modify: `services/dagster_v3/tests/test_ip_enrichment_clickhouse_local.py:12-28, 82-98, 125-144`

**Interfaces:**
- Produces: `corpscout.ip_enrichment_input(task_id String, input_id String, ip String, ip_version UInt8 MATERIALIZED, bucket UInt16 MATERIALIZED, source_name LowCardinality(String), source_record_id String, source_run_id String, submission_id String, observed_at Nullable(DateTime64(6,'UTC')), submitted_at DateTime64(6,'UTC'))`, `ENGINE = MergeTree PARTITION BY task_id ORDER BY (task_id, input_id)`, constraints `valid_identity` (non-empty `task_id`/`submission_id`, `input_id` equals the bucket-prefixed JSON tuple), `valid_source`, `canonical_ip`.
- The `server` fixture of `tests/test_ip_enrichment_input.py` (imported by `test_ip_enrichment_results.py`, `test_webtech_input.py`, `test_webtech_draft_execution.py`, `test_domains_inventory.py`, `test_web_inventory.py`, `test_domains_search.py`) applies 000433 then 000450; its name and shape are unchanged.

- [ ] **Step 1: Confirm the migration numbers are free**

Run: `ls clickhouse/migrations | tail -2`
Expected: `000449_corpscout_rdap_registry_level_exclusion.up.sql` is the highest.

Run: `ssh companycollect 'sudo docker exec clickhouse-clickhouse-1 clickhouse-client -q "SELECT max(version) FROM corpscout.schema_migrations WHERE dirty=0"'`
Expected: `448`. If another workstream took 449 or 450 on main or prod, renumber both files of Task 1 and this task and every mention in Tasks 3–9.

- [ ] **Step 2: Write the migrations**

`clickhouse/migrations/000450_corpscout_ip_enrichment_queue_contract.up.sql`:

```sql
CREATE DATABASE IF NOT EXISTS corpscout;

-- Shared queue contract for IP enrichment drafts: one partition per task so cleanup is
-- DROP PARTITION, sorted by task and input_id, and a required submission_id so a retried
-- import replaces only its own rows. input_id starts with the address's 256-way bucket
-- (leftPad(bucket, 3, '0') then ':' then the JSON tuple of source, record and IP), so a
-- task is walked bucket by bucket and each remaining or completion query joins exactly
-- one primary-key range of ip_enrichment_results. The table is rebuilt only while empty.
-- The mutation-pool setting stays because the submission retry deletes its own rows with
-- a lightweight DELETE.
SELECT throwIf(count() > 0, 'ip_enrichment_input must be empty before its layout changes')
FROM corpscout.ip_enrichment_input;

DROP TABLE IF EXISTS corpscout.ip_enrichment_input;

CREATE TABLE corpscout.ip_enrichment_input
(
    task_id String,
    input_id String,
    ip String,
    ip_version UInt8 MATERIALIZED if(isIPv4String(ip), toUInt8(4), toUInt8(6)),
    bucket UInt16 MATERIALIZED toUInt16(cityHash64(ip) % 256),
    source_name LowCardinality(String),
    source_record_id String,
    source_run_id String,
    submission_id String,
    observed_at Nullable(DateTime64(6, 'UTC')),
    submitted_at DateTime64(6, 'UTC') DEFAULT now64(6),
    CONSTRAINT valid_identity CHECK notEmpty(task_id) AND notEmpty(submission_id)
        AND input_id = concat(leftPad(toString(toUInt16(cityHash64(ip) % 256)), 3, '0'), ':', toJSONString(tuple(toString(source_name), source_record_id, ip))),
    CONSTRAINT valid_source CHECK notEmpty(trimBoth(source_name))
        AND notEmpty(source_record_id) AND position(source_record_id, char(0)) = 0,
    CONSTRAINT canonical_ip CHECK
        ifNull(ip = toString(toIPv4OrNull(ip)), 0)
        OR ifNull(ip = toString(toIPv6OrNull(ip)), 0)
)
ENGINE = MergeTree
PARTITION BY task_id
ORDER BY (task_id, input_id)
SETTINGS number_of_free_entries_in_pool_to_execute_mutation = 1;
```

`clickhouse/migrations/000450_corpscout_ip_enrichment_queue_contract.down.sql` (the 000433 layout):

```sql
CREATE DATABASE IF NOT EXISTS corpscout;

SELECT throwIf(count() > 0, 'ip_enrichment_input must be empty before its layout changes')
FROM corpscout.ip_enrichment_input;

DROP TABLE IF EXISTS corpscout.ip_enrichment_input;

CREATE TABLE corpscout.ip_enrichment_input
(
    task_id UUID,
    input_id String,
    ip String,
    ip_version UInt8 MATERIALIZED if(isIPv4String(ip), toUInt8(4), toUInt8(6)),
    bucket UInt16 MATERIALIZED toUInt16(cityHash64(ip) % 256),
    source_name LowCardinality(String),
    source_record_id String,
    source_run_id String,
    observed_at Nullable(DateTime64(6, 'UTC')),
    submitted_at DateTime64(6, 'UTC') DEFAULT now64(6),
    CONSTRAINT valid_input CHECK notEmpty(trimBoth(input_id))
        AND position(input_id, char(0)) = 0 AND notEmpty(trimBoth(source_name)),
    CONSTRAINT canonical_ip CHECK
        ifNull(ip = toString(toIPv4OrNull(ip)), 0)
        OR ifNull(ip = toString(toIPv6OrNull(ip)), 0)
)
ENGINE = MergeTree
ORDER BY (input_id, task_id);
```

Append `"000450_corpscout_ip_enrichment_queue_contract",` after the 449 entry in `EXPECTED_MIGRATIONS`.

- [ ] **Step 3: Apply it in the shared fixture and write the contract test**

In `services/dagster_v3/tests/test_ip_enrichment_input.py` replace lines 88-96 (the `with resource.get_connection() as client:` block of `server`) with:

```python
        with resource.get_connection() as client:
            migrations = Path(__file__).resolve().parents[3] / "clickhouse/migrations"
            for name in (
                "000433_corpscout_ip_enrichment.up.sql",
                "000450_corpscout_ip_enrichment_queue_contract.up.sql",
            ):
                for statement in (migrations / name).read_text(encoding="utf-8").split(";"):
                    if statement.strip():
                        client.execute(statement)
            yield client, resource
```

Append to the same file (the rest of the file is rewritten in Task 3; this test survives unchanged):

```python
def test_entry_table_follows_the_queue_contract(database):
    from clickhouse_driver.errors import ServerException

    from dagster_v3.defs.ip_enrichment.input import INPUT_ID_SQL

    client, _ = database
    assert client.execute(
        "SELECT engine, partition_key, sorting_key FROM system.tables WHERE database='corpscout' AND name='ip_enrichment_input'"
    ) == [("MergeTree", "task_id", "task_id, input_id")]
    identity = INPUT_ID_SQL.format(ip="'8.8.8.8'", source="'manual'", record="'8.8.8.8'")
    # input_id is the bucket-prefixed identity computed in ClickHouse, nothing else.
    with pytest.raises(ServerException, match="valid_identity"):
        client.execute(
            f"INSERT INTO {INPUT_RELATION} (task_id,input_id,ip,source_name,source_record_id,source_run_id,submission_id) VALUES",
            [("task", "017:x", "8.8.8.8", "manual", "8.8.8.8", "run", "submission")],
        )
    # Every row names its submission; the retry delete relies on it.
    with pytest.raises(ServerException, match="valid_identity"):
        client.execute(
            f"INSERT INTO {INPUT_RELATION} (task_id,input_id,ip,source_name,source_record_id,source_run_id) "
            f"SELECT 'task', {identity}, '8.8.8.8', 'manual', '8.8.8.8', 'run'"
        )
    client.execute(
        f"INSERT INTO {INPUT_RELATION} (task_id,input_id,ip,source_name,source_record_id,source_run_id,submission_id) "
        f"SELECT 'task', {identity}, '8.8.8.8', 'manual', '8.8.8.8', 'run', 'submission'"
    )
    [(input_id, bucket)] = client.execute(f"SELECT input_id, bucket FROM {INPUT_RELATION}")
    assert input_id == f"{bucket:03d}:" + '["manual","8.8.8.8","8.8.8.8"]'
```

`INPUT_ID_SQL` does not exist until Task 3; add it now to `services/dagster_v3/src/dagster_v3/defs/ip_enrichment/input.py` after `PROCESSOR_VERSION` (line 21) so this task is green on its own:

```python
# The entry identity, computed where the entries live: the address's 256-way bucket first,
# so a task is walked bucket by bucket, then the JSON tuple of source, record and IP.
# Migration 000450 enforces the same expression in its valid_identity CHECK.
INPUT_ID_SQL = (
    "concat(leftPad(toString(toUInt16(cityHash64({ip}) % 256)), 3, '0'), ':', "
    "toJSONString(tuple({source}, {record}, {ip})))"
)
```

- [ ] **Step 4: Update the clickhouse-local test to the new layout**

In `services/dagster_v3/tests/test_ip_enrichment_clickhouse_local.py`:

Replace lines 12-13 with:

```python
MIGRATIONS = Path(__file__).resolve().parents[3] / "clickhouse/migrations"
MIGRATION = "000433_corpscout_ip_enrichment"
QUEUE_MIGRATION = "000450_corpscout_ip_enrichment_queue_contract"
IDENTITY = "concat(leftPad(toString(toUInt16(cityHash64(ip) % 256)), 3, '0'), ':', toJSONString(tuple(source_name, source_record_id, ip)))"
```

Replace the input insert and count (lines 21-27) with:

```sql
    INSERT INTO corpscout.ip_enrichment_input
        (task_id, input_id, ip, source_name, source_record_id, source_run_id, submission_id)
    SELECT task_id, {IDENTITY}, ip, source_name, source_record_id, 'run', 'submission'
    FROM (SELECT '00000000-0000-0000-0000-000000000001' AS task_id, '8.8.8.8' AS ip, 'dns' AS source_name, '1' AS source_record_id
          UNION ALL
          SELECT '00000000-0000-0000-0000-000000000002', '8.8.8.8', 'commoncrawl', '2');
    SELECT count(), uniqExact(ip), uniqExact(source_name), countIf(input_id LIKE '___:%')
    FROM corpscout.ip_enrichment_input FORMAT JSONCompactEachRow;
```

(the `sql` string becomes an f-string: `sql = f"""…"""`; escape nothing else — it contains no braces). At the end of `sql`, before the closing `"""`, add the test-only cleanup so the gated down migrations can run:

```sql
    TRUNCATE TABLE corpscout.ip_enrichment_input;
```

Replace lines 84-98 (the `subprocess.run` input) with:

```python
    queue_up = (MIGRATIONS / f"{QUEUE_MIGRATION}.up.sql").read_text()
    queue_down = (MIGRATIONS / f"{QUEUE_MIGRATION}.down.sql").read_text()
    result = subprocess.run(
        clickhouse_local_command(),
        input=up
        + queue_up
        + sql
        + queue_down
        + down
        + up
        + queue_up
        + queue_down
        + down
        + """
        SELECT count() FROM system.tables WHERE database='corpscout' FORMAT JSONCompactEachRow;
        """,
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
```

and the first expected row `[2, 1, 2],` becomes `[2, 1, 2, 2],`.

In `test_ip_enrichment_rejects_invalid_or_noncanonical_ips` replace lines 128-134 with:

```python
    up = (MIGRATIONS / f"{MIGRATION}.up.sql").read_text() + (
        MIGRATIONS / f"{QUEUE_MIGRATION}.up.sql"
    ).read_text()
    if table == "ip_enrichment_input":
        insert = f"""INSERT INTO corpscout.{table}
        (task_id, input_id, ip, source_name, source_record_id, source_run_id, submission_id)
        SELECT 'task', {IDENTITY}, ip, source_name, source_record_id, 'run', 'submission'
        FROM (SELECT {literal(ip)} AS ip, 'dns' AS source_name, 'record' AS source_record_id);"""
    else:
        insert = f"""INSERT INTO corpscout.{table} (ip, result_id)
        VALUES ({literal(ip)}, '00000000-0000-0000-0000-000000000001');"""
```

- [ ] **Step 5: Run the migration suites**

Run: `uv run --frozen --no-sync pytest tests/test_clickhouse_migrations.py tests/test_ip_enrichment_clickhouse_local.py tests/test_legacy_geoip_migration.py tests/test_ip_enrichment_input.py::test_entry_table_follows_the_queue_contract -q -p no:cacheprovider`
Expected: all pass. (`test_legacy_geoip_migration.py` only applies 000433 and touches the results table.) The other tests of `test_ip_enrichment_input.py` and of `test_ip_enrichment_results.py` are red until Tasks 3 and 5.

- [ ] **Step 6: Commit**

```bash
git add clickhouse/migrations/000450_corpscout_ip_enrichment_queue_contract.up.sql clickhouse/migrations/000450_corpscout_ip_enrichment_queue_contract.down.sql services/dagster_v3/tests/test_clickhouse_migrations.py services/dagster_v3/tests/test_ip_enrichment_input.py services/dagster_v3/tests/test_ip_enrichment_clickhouse_local.py services/dagster_v3/src/dagster_v3/defs/ip_enrichment/input.py
git commit -m "feat(clickhouse): partition ip_enrichment_input by task with bucket-prefixed input ids and a required submission_id

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

---

### Task 3: Draft import through the shared draft queue

Today `ip_enrichment_input` (`input.py:244-357`) uses `store.prepare_selection`/`finish_selection` (legacy, `queue_scope NULL`), a task-wide `ALTER TABLE … DELETE` retry and no receipts. It becomes the crawl-style import: `draft_queue.find_draft` → `prepare_submission` → `KILL QUERY` + `DELETE … WHERE task_id AND submission_id` + one `INSERT … SELECT` → `finish_submission`, `fail_submission` on any error. `input_count` is the number of rows this submission added to the draft (after dedup against the draft), not a separate evaluation of the selection: evaluating a 48.6M-row selection twice costs minutes and the receipt only needs a count. `source_info.unique_ips` disappears (a `uniqExact` over 48.6M strings is not worth a freeze-time query); the materialization reports `input_count` and `total`. No draft size cap: entries cost nothing in PostgreSQL. A third selection mode, `retry_failed_task_id`, queues the addresses whose result in that task has any component in an error status (D4's "failed IPs of task X").

**Files:**
- Rewrite: `services/dagster_v3/src/dagster_v3/defs/ip_enrichment/input.py`
- Rewrite: `services/dagster_v3/tests/test_ip_enrichment_input.py` (keep `server`, `database`, `materialize`, the contract test)

**Interfaces:**
- Produces (module `dagster_v3.defs.ip_enrichment.input`):
  - `INPUT_RELATION`, `RESULT_RELATION = "corpscout.ip_enrichment_results"`, `PROCESSOR_VERSION = "ip-enrichment-v1"`, `ERROR_STATUSES = ("retryable_error", "terminal_error")`, `INPUT_ID_SQL`, `bucket_prefix(bucket: int) -> str` (`"017:"`).
  - `IpEnrichmentInputConfig` — today's fields plus `submission_id: str | None`, `queue_scope: str = "workspace"`, `retry_failed_task_id: str | None`; exactly one of `ips`, `source_relation`, `retry_failed_task_id`.
  - `selected_ips_sql(config) -> tuple[str, dict]` — columns `ip, source_record_id, observed_at` (unchanged shape; new mode added).
  - `load_ip_draft(config, submission_id: str, store: ProcessingStore, clickhouse: ClickhouseResource, *, run_id: str) -> dict` with keys `task_id, submission_id, input_count, total`.
  - Asset `ip_enrichment_input`, job `ip_enrichment_input_job` (names unchanged).
- Removes: the `ClickHouseInputQueue` use, `store.prepare_selection`/`finish_selection` for IPs, `processing/task_id`-derived task ids (the run tag `processing/task_id` is still written after import for the backoffice).

- [ ] **Step 1: Write the failing tests**

Replace everything in `services/dagster_v3/tests/test_ip_enrichment_input.py` after the `database` fixture (keep lines 1-110 as modified in Task 2, and keep `test_entry_table_follows_the_queue_contract`) with:

```python
def materialize(resource, dsn, **config):
    return dg.materialize(
        [ip_enrichment_input],
        resources={
            "clickhouse": resource,
            "processing": ProcessingResource(postgres_url=dsn),
        },
        run_config={"ops": {"ip_enrichment_input": {"config": config}}},
    )


def metadata(result):
    return {
        key: value.value
        for key, value in result.asset_materializations_for_node("ip_enrichment_input")[0]
        .metadata.items()
    }


def scope():
    return "scope-" + uuid4().hex


@pytest.mark.parametrize(
    "config",
    [
        {},
        {"ips": []},
        {"ips": ["8.8.8.8/24"]},
        {"ips": ["fe80::1%eth0"]},
        {"ips": ["bad"]},
        {"ips": ["8.8.8.8"], "source_relation": "corpscout.ips"},
        {"ips": ["8.8.8.8"], "filters": {"country": ["US"]}},
        {"source_relation": "corpscout.ips"},
        {"source_relation": INPUT_RELATION, "select_all": True},
        {"source_relation": "corpscout.ips; DROP TABLE x", "select_all": True},
        {"source_relation": "corpscout.ips", "ip_column": "ip`", "select_all": True},
        {"source_relation": "corpscout.ips", "filters": {"ip": []}},
        {"ips": ["8.8.8.8"], "max_rows": 0},
        {"ips": ["8.8.8.8"], "submission_id": "not-a-uuid"},
        {"ips": ["8.8.8.8"], "queue_scope": "  "},
        {"retry_failed_task_id": "not-a-uuid"},
        {"retry_failed_task_id": str(uuid4()), "ips": ["8.8.8.8"]},
        {"retry_failed_task_id": str(uuid4()), "filters": {"country": ["US"]}},
        {"retry_failed_task_id": str(uuid4()), "source_relation": "corpscout.ips"},
    ],
)
def test_invalid_or_ambiguous_selection_is_rejected(config):
    with pytest.raises(ValidationError):
        IpEnrichmentInputConfig(**config)


def test_explicit_ips_are_canonical_deduplicated_and_appended_to_one_draft(database, store):
    client, resource = database
    queue, dsn = store
    space = scope()
    submission = str(uuid4())
    ips = [
        " 8.8.8.8 ",
        "8.8.8.8",
        "2001:4860:4860:0:0:0:0:8888",
        "127.0.0.1",
        "::ffff:0808:0808",
    ]
    first = metadata(
        materialize(resource, dsn, queue_scope=space, submission_id=submission, ips=ips, source_name="manual-test")
    )
    assert (first["input_count"], first["total"]) == (4, 4)
    assert client.execute(
        f"SELECT ip, ip_version, source_name, source_record_id, submission_id FROM {INPUT_RELATION} ORDER BY ip"
    ) == [
        ("127.0.0.1", 4, "manual-test", "127.0.0.1", submission),
        ("2001:4860:4860::8888", 6, "manual-test", "2001:4860:4860::8888", submission),
        ("8.8.8.8", 4, "manual-test", "8.8.8.8", submission),
        ("::ffff:8.8.8.8", 6, "manual-test", "::ffff:8.8.8.8", submission),
    ]
    assert client.execute(
        f"SELECT count() FROM {INPUT_RELATION} WHERE input_id LIKE '___:%' AND toString(task_id) = %(task)s",
        {"task": first["task_id"]},
    ) == [(4,)]
    rows_before = client.execute(f"SELECT * FROM {INPUT_RELATION} ORDER BY input_id")
    # The same submission with the same selection is a no-op, even with a changed source.
    replay = metadata(
        materialize(resource, dsn, queue_scope=space, submission_id=submission, ips=list(reversed(ips)), source_name="manual-test")
    )
    assert replay == first
    assert client.execute(f"SELECT * FROM {INPUT_RELATION} ORDER BY input_id") == rows_before
    # Another submission appends to the same draft; overlapping addresses are kept once.
    second = metadata(
        materialize(resource, dsn, queue_scope=space, ips=["8.8.8.8", "1.1.1.1"], source_name="manual-test")
    )
    assert second["task_id"] == first["task_id"]
    assert (second["input_count"], second["total"]) == (1, 5)
    task = queue.task(first["task_id"])
    assert task["status"] == "draft" and task["queue_scope"] == space and task["total"] == 5
    with queue.transaction() as cursor:
        cursor.execute("SELECT count(*) AS n FROM processing.items")
        assert cursor.fetchone()["n"] == 0
        cursor.execute(
            "SELECT status, input_count, selection_config->'ips' AS ips FROM processing.input_submissions WHERE submission_id=%s",
            (submission,),
        )
        receipt = cursor.fetchone()
    assert receipt["status"] == "completed" and receipt["input_count"] == 4
    assert set(receipt["ips"]) == {"count", "sha256"}  # bulk values stay out of PostgreSQL
    with pytest.raises(ValueError, match="different selection"):
        materialize(resource, dsn, queue_scope=space, submission_id=submission, ips=["9.9.9.9"], source_name="manual-test")


def test_table_filters_final_and_source_lineage(database, store):
    client, resource = database
    queue, dsn = store
    client.execute("""INSERT INTO corpscout.ip_source_test VALUES
        ('a', '8.8.8.8', 1, 'US', '2026-09-01', 1),
        ('a', '8.8.8.8', 1, 'US', '2026-09-02', 2),
        ('b', '8.8.8.8', 1, 'US', '2026-09-01', 1),
        ('a', '2001:4860:4860:0:0:0:0:8888', 1, 'US', '2026-09-01', 1),
        ('c', '1.1.1.1', 1, 'US', '2026-09-01', 1),
        ('c', '1.1.1.1', 0, 'US', '2026-09-02', 2),
        ('d', '9.9.9.9', 1, 'DE', '2026-09-01', 1),
        ('e', 'bad', 1, 'US', '2026-09-01', 1),
        ('f', NULL, 1, 'US', '2026-09-01', 1)""")
    space = scope()
    config = dict(
        queue_scope=space,
        source_relation="corpscout.ip_source_test",
        ip_column="address",
        source_record_id_column="record_id",
        observed_at_column="observed_at",
        source_final=True,
        filters={"active": ["1"], "country": ["US"]},
    )
    submission = str(uuid4())
    first = metadata(materialize(resource, dsn, submission_id=submission, **config))
    assert client.execute(
        f"SELECT ip, source_record_id, toString(observed_at), source_name FROM {INPUT_RELATION} ORDER BY ip, source_record_id"
    ) == [
        ("2001:4860:4860::8888", "a", "2026-09-01 00:00:00.000000", "corpscout.ip_source_test"),
        ("8.8.8.8", "a", "2026-09-02 00:00:00.000000", "corpscout.ip_source_test"),
        ("8.8.8.8", "b", "2026-09-01 00:00:00.000000", "corpscout.ip_source_test"),
    ]
    client.execute(
        "INSERT INTO corpscout.ip_source_test VALUES ('new', '4.4.4.4', 1, 'US', '2026-09-01', 1)"
    )
    # A completed receipt is not re-evaluated; a new submission sees the new row.
    assert metadata(materialize(resource, dsn, submission_id=submission, **config)) == first
    assert queue.task(first["task_id"])["total"] == 3
    later = metadata(materialize(resource, dsn, **config))
    assert (later["input_count"], later["total"]) == (1, 4)
    # A bounded selection in another scope freezes only the first two distinct submissions.
    bounded = metadata(materialize(resource, dsn, **{**config, "queue_scope": scope(), "max_rows": 2}))
    assert (bounded["input_count"], bounded["total"]) == (2, 2)
    empty = metadata(
        materialize(resource, dsn, **{**config, "queue_scope": scope(), "filters": {"country": ["US') OR 1=1 --"]}})
    )
    assert (empty["input_count"], empty["total"]) == (0, 0)
    assert queue.task(empty["task_id"])["status"] == "draft"


def test_retry_replaces_only_its_own_submission_rows(database, store, monkeypatch):
    from clickhouse_driver import Client

    from dagster_v3.defs.common import draft_queue

    client, resource = database
    queue, dsn = store
    space = scope()
    kept = metadata(materialize(resource, dsn, queue_scope=space, ips=["1.1.1.1"]))
    execute = Client.execute
    interrupted = False

    def lost_ack(self, query, *args, **kwargs):
        nonlocal interrupted
        value = execute(self, query, *args, **kwargs)
        if query.lstrip().startswith(f"INSERT INTO {INPUT_RELATION}") and not interrupted:
            interrupted = True
            raise ConnectionError("lost insert acknowledgement")
        return value

    monkeypatch.setattr(Client, "execute", lost_ack)
    submission = str(uuid4())
    with pytest.raises(ConnectionError, match="acknowledgement"):
        materialize(resource, dsn, queue_scope=space, submission_id=submission, ips=["8.8.8.8", "1.1.1.1"])
    assert draft_queue.submission(queue, submission)["status"] == "failed"
    # The lost insert did land; the retry deletes only this submission's rows and re-inserts them.
    result = metadata(materialize(resource, dsn, queue_scope=space, submission_id=submission, ips=["8.8.8.8", "1.1.1.1"]))
    assert result["task_id"] == kept["task_id"] and (result["input_count"], result["total"]) == (1, 2)
    assert client.execute(
        f"SELECT ip, submission_id FROM {INPUT_RELATION} ORDER BY ip"
    ) == [("1.1.1.1", kept["submission_id"]), ("8.8.8.8", submission)]
    assert draft_queue.submission(queue, submission)["status"] == "completed"


def test_missing_source_column_fails_the_submission_without_rows(database, store):
    from dagster_v3.defs.common import draft_queue

    client, resource = database
    queue, dsn = store
    submission = str(uuid4())
    with pytest.raises(ValueError, match="source is missing columns: missing"):
        materialize(
            resource,
            dsn,
            queue_scope=scope(),
            submission_id=submission,
            source_relation="corpscout.ip_source_test",
            ip_column="missing",
            select_all=True,
        )
    assert draft_queue.submission(queue, submission)["status"] == "failed"
    assert client.execute(f"SELECT count() FROM {INPUT_RELATION}") == [(0,)]


@pytest.mark.parametrize(
    ("search", "excluded", "expected"),
    [
        ("8.8.", ["8.8.4.4"], ["8.8.8.8"]),
        ("8.8.8.8", [], ["8.8.8.8"]),
        ("2001:4860:0:0:0:0:0:8888", [], ["2001:4860::8888"]),
        ("", ["2001:4860:0:0:0:0:0:8888"], ["8.8.4.4", "8.8.8.8", "9.9.9.9"]),
    ],
)
def test_inventory_search_and_exclusions_match_all_pages(
    database, store, search, excluded, expected
):
    client, resource = database
    client.execute("""INSERT INTO corpscout.ip_source_test VALUES
        ('a', '8.8.8.8', 1, 'US', '2026-09-01', 1),
        ('b', '8.8.8.8', 1, 'US', '2026-09-02', 1),
        ('c', '8.8.4.4', 1, 'US', '2026-09-01', 1),
        ('d', '9.9.9.9', 1, 'US', '2026-09-01', 1),
        ('e', '2001:4860::8888', 1, 'US', '2026-09-01', 1),
        ('f', '8.8.0.1', 0, 'US', '2026-09-01', 1)""")
    assert materialize(
        resource,
        store[1],
        queue_scope=scope(),
        source_relation="corpscout.ip_source_test",
        ip_column="address",
        observed_at_column="observed_at",
        filters={"active": ["1"]},
        select_all=True,
        ip_search=search,
        excluded_ips=excluded,
    ).success
    assert [
        row[0] for row in client.execute(f"SELECT ip FROM {INPUT_RELATION} ORDER BY ip")
    ] == expected


@pytest.mark.parametrize(
    "config",
    [
        {"ips": ["8.8.8.8"], "ip_search": "8.8."},
        {"ips": ["8.8.8.8"], "excluded_ips": ["8.8.8.8"]},
        {"source_relation": "corpscout.ips", "ip_search": "%' OR 1=1"},
        {
            "source_relation": "corpscout.ips",
            "select_all": True,
            "excluded_ips": ["bad"],
        },
    ],
)
def test_invalid_inventory_filters(config):
    with pytest.raises(ValidationError):
        IpEnrichmentInputConfig(**config)


def test_failed_results_of_a_task_can_be_queued_again(database, store):
    client, resource = database
    failed_task = str(uuid4())
    completed = datetime(2026, 9, 1, tzinfo=UTC)
    client.execute(
        "INSERT INTO corpscout.ip_enrichment_results (ip, result_id, task_id, execution_id, input_id, completed_at, city_lookup_status, asn_lookup_status, rdap_lookup_status) VALUES",
        [
            ("8.8.8.8", str(uuid4()), failed_task, failed_task, "a", completed, "found", "found", "retryable_error"),
            ("1.1.1.1", str(uuid4()), failed_task, failed_task, "b", completed, "found", "found", "found"),
            ("9.9.9.9", str(uuid4()), str(uuid4()), failed_task, "c", completed, "terminal_error", "found", "found"),
        ],
    )
    result = metadata(materialize(resource, store[1], queue_scope=scope(), retry_failed_task_id=failed_task))
    assert (result["input_count"], result["total"]) == (1, 1)
    assert client.execute(f"SELECT ip, source_name, source_record_id FROM {INPUT_RELATION}") == [
        ("8.8.8.8", "retry:" + failed_task, "8.8.8.8")
    ]


def test_new_submissions_after_start_form_the_next_draft(database, store):
    from dagster_v3.defs.common import queue_execution
    from dagster_v3.defs.ip_enrichment.input import PROCESSOR_VERSION

    _, resource = database
    queue, dsn = store
    space = scope()
    first = metadata(materialize(resource, dsn, queue_scope=space, ips=["8.8.8.8"]))
    with queue.selection_lock(first["task_id"]):
        queue_execution.start_execution(
            queue,
            task_id=first["task_id"],
            processor=PROCESSOR_VERSION,
            profile={},
            execution_id=None,
            freshness_days=30,
            run_id=str(uuid4()),
            snapshot=lambda: ({"relation": INPUT_RELATION, "total": 1}, 1),
        )
    second = metadata(materialize(resource, dsn, queue_scope=space, ips=["1.1.1.1"]))
    assert second["task_id"] != first["task_id"]
    with pytest.raises(ValueError, match="open draft"):
        materialize(resource, dsn, queue_scope=space, task_id=first["task_id"], ips=["1.1.1.1"])
```

(The import block at the top keeps `dg`, `pytest`, `ClickhouseResource`, `ValidationError`, `ProcessingResource`, `INPUT_RELATION`, `IpEnrichmentInputConfig`, `ip_enrichment_input`, the clickhouse_local helpers and the store fixtures; add `from datetime import UTC, datetime`; delete `from dagster_v3.defs.common.clickhouse_queue import ClickHouseInputQueue`.)

- [ ] **Step 2: Run to verify they fail**

Run: `uv run --frozen --no-sync pytest tests/test_ip_enrichment_input.py -q -p no:cacheprovider`
Expected: FAIL — `ValidationError` for `submission_id`/`queue_scope`/`retry_failed_task_id` ("extra fields not permitted") and `KeyError: 'input_count'`.

- [ ] **Step 3: Rewrite `input.py`**

Replace the whole of `services/dagster_v3/src/dagster_v3/defs/ip_enrichment/input.py` with:

```python
"""Append IP selections to the open IP enrichment draft (shared queue contract).

A submission selects addresses from an explicit list, a filtered ClickHouse relation
or the failed results of an earlier task, normalizes them in ClickHouse and appends
the ones the draft does not hold yet. Nothing is frozen here; the results asset
freezes the draft when it starts.
"""

import hashlib
import json
import re
from ipaddress import IPv6Address, ip_address
from typing import Self
from uuid import UUID

import dagster as dg
from dagster_clickhouse import ClickhouseResource
from pydantic import Field, field_validator, model_validator

from dagster_v3.defs.common import draft_queue
from dagster_v3.defs.common.clickhouse_queue import validate_relation
from dagster_v3.defs.common.processing import ProcessingResource, ProcessingStore

INPUT_RELATION = "corpscout.ip_enrichment_input"
RESULT_RELATION = "corpscout.ip_enrichment_results"
PROCESSOR_VERSION = "ip-enrichment-v1"
ERROR_STATUSES = ("retryable_error", "terminal_error")
# The entry identity, computed where the entries live: the address's 256-way bucket first,
# so a task is walked bucket by bucket, then the JSON tuple of source, record and IP.
# Migration 000450 enforces the same expression in its valid_identity CHECK.
INPUT_ID_SQL = (
    "concat(leftPad(toString(toUInt16(cityHash64({ip}) % 256)), 3, '0'), ':', "
    "toJSONString(tuple({source}, {record}, {ip})))"
)


def bucket_prefix(bucket: int) -> str:
    """Every input_id of ``bucket`` starts with this; ``bucket_prefix(bucket + 1)`` ends the range."""
    return f"{bucket:03d}:"


class IpEnrichmentInputConfig(dg.Config):
    task_id: str | None = Field(
        default=None,
        description="Open draft to append to. Omit to use the scope's open draft.",
    )
    submission_id: str | None = Field(
        default=None,
        description="Stable receipt UUID; retry a failed import with the same value.",
    )
    queue_scope: str = Field(default="workspace", min_length=1)
    ips: list[str] = Field(
        default_factory=list,
        description="Explicit IPv4/IPv6 addresses. Use one of ips, source_relation, retry_failed_task_id.",
    )
    source_relation: str | None = Field(
        default=None,
        description="Source ClickHouse table/view, as corpscout.table.",
    )
    retry_failed_task_id: str | None = Field(
        default=None,
        description="Queue the addresses whose result in this task has a City, ASN or RDAP error.",
    )
    ip_column: str = "ip"
    source_record_id_column: str | None = Field(
        default=None,
        description="Optional source record identity. Defaults to the canonical IP.",
    )
    observed_at_column: str | None = Field(
        default=None,
        description="Optional source observation timestamp column.",
    )
    filters: dict[str, list[str]] = Field(
        default_factory=dict,
        description="Exact matches: OR within each value list, AND between columns.",
    )
    ip_search: str = Field(
        default="", description="Exact IP or literal IP prefix to select."
    )
    excluded_ips: list[str] = Field(default_factory=list)
    source_final: bool = False
    select_all: bool = False
    max_rows: int | None = Field(
        default=None,
        ge=1,
        description="Limit distinct source-record/IP submissions, sorted by IP and record ID.",
    )
    source_name: str | None = Field(
        default=None,
        description="Provenance label. Defaults to source_relation, retry:<task> or manual.",
    )
    source_run_id: str | None = Field(
        default=None,
        description="Original source run ID. Defaults to this Dagster run ID.",
    )

    @field_validator("task_id", "submission_id", "retry_failed_task_id")
    @classmethod
    def valid_uuid(cls, value: str | None) -> str | None:
        return str(UUID(value)) if value is not None else None

    @field_validator("queue_scope")
    @classmethod
    def valid_scope(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("queue_scope must not be blank")
        return value

    @field_validator("ips", "excluded_ips")
    @classmethod
    def canonical_ips(cls, values: list[str]) -> list[str]:
        normalized = set()
        for value in values:
            if "%" in value:
                raise ValueError("scoped IPv6 addresses are not supported")
            address = ip_address(value.strip())
            # ClickHouse renders IPv4-mapped IPv6 with dotted IPv4 notation.
            if isinstance(address, IPv6Address) and address.ipv4_mapped is not None:
                normalized.add(f"::ffff:{address.ipv4_mapped}")
            else:
                normalized.add(str(address))
        return sorted(normalized)

    @field_validator("ip_search")
    @classmethod
    def valid_search(cls, value: str) -> str:
        value = value.strip().lower()
        if value and re.fullmatch(r"[0-9a-f:.]{1,45}", value) is None:
            raise ValueError("ip_search must be an IP address or literal IP prefix")
        return value

    @field_validator("source_relation")
    @classmethod
    def valid_source(cls, value: str | None) -> str | None:
        if value is None:
            return None
        value = validate_relation(value)
        if value.split(".")[0] != "corpscout":
            raise ValueError("source_relation must be in the corpscout database")
        if value == INPUT_RELATION:
            raise ValueError("the source must differ from the input queue")
        return value

    @field_validator("source_name")
    @classmethod
    def valid_source_name(cls, value: str | None) -> str | None:
        if value is None:
            return None
        value = value.strip()
        if not value or "\0" in value:
            raise ValueError("source_name must be nonempty and contain no NUL")
        return value

    @model_validator(mode="after")
    def valid_selection(self) -> Self:
        modes = [
            bool(self.ips),
            self.source_relation is not None,
            self.retry_failed_task_id is not None,
        ]
        if sum(modes) != 1:
            raise ValueError(
                "provide exactly one of nonempty ips, source_relation or retry_failed_task_id"
            )
        for column in (
            self.ip_column,
            self.source_record_id_column,
            self.observed_at_column,
            *self.filters,
        ):
            if (
                column is not None
                and re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", column) is None
            ):
                raise ValueError("column names must be simple SQL identifiers")
        if any(not values for values in self.filters.values()):
            raise ValueError("each filter needs at least one value")
        if self.source_relation is None:
            if (
                self.filters
                or self.ip_search
                or self.excluded_ips
                or self.source_final
                or self.select_all
                or self.ip_column != "ip"
                or self.source_record_id_column is not None
                or self.observed_at_column is not None
            ):
                raise ValueError("table selection options require source_relation")
        elif not (
            self.filters
            or self.ip_search
            or self.max_rows is not None
            or self.select_all
        ):
            raise ValueError("provide filters, max_rows, or explicit select_all=true")
        return self


def selected_ips_sql(config: IpEnrichmentInputConfig) -> tuple[str, dict]:
    """Normalize in ClickHouse and group submissions without fetching the source into Python."""
    params = {}
    where = ""
    if config.retry_failed_task_id is not None:
        failed = " OR ".join(
            f"{column} IN %(errors)s"
            for column in ("city_lookup_status", "asn_lookup_status", "rdap_lookup_status")
        )
        relation = (
            f"(SELECT ip FROM {RESULT_RELATION} FINAL "
            f"WHERE task_id = %(failed_task)s AND ({failed})) AS source"
        )
        ip_value = "source.ip"
        params.update(failed_task=config.retry_failed_task_id, errors=ERROR_STATUSES)
    elif config.source_relation is None:
        relation = "(SELECT arrayJoin(%(ips)s) AS explicit_ip) AS source"
        ip_value = "source.explicit_ip"
        params["ips"] = config.ips
    else:
        relation = (
            config.source_relation
            + " AS source"
            + (" FINAL" if config.source_final else "")
        )
        ip_value = f"trimBoth(ifNull(toString(source.`{config.ip_column}`), ''))"
        predicates = []
        for index, (column, values) in enumerate(sorted(config.filters.items())):
            predicates.append(f"source.`{column}` IN %(filter_{index})s")
            params[f"filter_{index}"] = tuple(sorted(set(values)))
        if config.ip_search:
            try:
                address = ip_address(config.ip_search)
            except ValueError:
                predicates.extend(
                    [
                        f"source.`{config.ip_column}` >= %(ip_prefix)s",
                        f"source.`{config.ip_column}` < %(ip_prefix_end)s",
                    ]
                )
                params["ip_prefix"] = config.ip_search
                params["ip_prefix_end"] = config.ip_search[:-1] + chr(
                    ord(config.ip_search[-1]) + 1
                )
            else:
                canonical = (
                    f"::ffff:{address.ipv4_mapped}"
                    if isinstance(address, IPv6Address)
                    and address.ipv4_mapped is not None
                    else str(address)
                )
                predicates.append(f"{ip_value} = %(exact_ip)s")
                params["exact_ip"] = canonical
        where = " WHERE " + " AND ".join(predicates) if predicates else ""
    record_id = (
        f"trimBoth(ifNull(toString(source.`{config.source_record_id_column}`), ''))"
        if config.source_record_id_column is not None
        else "ifNull(normalized_ip, '')"
    )
    observed_at = (
        f"toDateTime64(source.`{config.observed_at_column}`, 6, 'UTC')"
        if config.observed_at_column is not None
        else "CAST(NULL AS Nullable(DateTime64(6, 'UTC')))"
    )
    excluded = ""
    if config.excluded_ips:
        excluded = " AND normalized_ip NOT IN %(excluded_ips)s"
        params["excluded_ips"] = tuple(config.excluded_ips)
    limit = ""
    if config.max_rows is not None:
        limit = " LIMIT %(limit)s"
        params["limit"] = config.max_rows
    return (
        f"""SELECT assumeNotNull(normalized_ip) AS ip,
        record_ref AS source_record_id, max(source_observed_at) AS observed_at
    FROM (
        SELECT coalesce(toString(toIPv4OrNull({ip_value})),
                        toString(toIPv6OrNull({ip_value}))) AS normalized_ip,
            {record_id} AS record_ref, {observed_at} AS source_observed_at
        FROM {relation}{where}
    )
    WHERE normalized_ip IS NOT NULL{excluded}
    GROUP BY normalized_ip, record_ref
    ORDER BY ip, source_record_id{limit}""",
        params,
    )


def source_label(config: IpEnrichmentInputConfig) -> str:
    if config.source_name is not None:
        return config.source_name
    if config.source_relation is not None:
        return config.source_relation
    if config.retry_failed_task_id is not None:
        return "retry:" + config.retry_failed_task_id
    return "manual"


def load_ip_draft(
    config: IpEnrichmentInputConfig,
    submission_id: str,
    store: ProcessingStore,
    clickhouse: ClickhouseResource,
    *,
    run_id: str,
) -> dict:
    selection = config.model_dump(exclude={"task_id", "submission_id", "queue_scope"})
    selection["filters"] = {
        key: sorted(set(values)) for key, values in config.filters.items()
    }
    fingerprint = hashlib.sha256(
        json.dumps(selection, sort_keys=True).encode()
    ).hexdigest()
    # Keep bulk manual values out of PostgreSQL receipts.
    for key in ("ips", "excluded_ips"):
        selection[key] = {
            "count": len(selection[key]),
            "sha256": hashlib.sha256(json.dumps(selection[key]).encode()).hexdigest(),
        }
    source = source_label(config)
    receipt = draft_queue.submission(store, submission_id)
    task_id = str(receipt["task_id"]) if receipt else None
    if receipt:
        task = store.task(task_id)
        if (
            task["processor"] != PROCESSOR_VERSION
            or task["queue_scope"] != config.queue_scope
            or (config.task_id is not None and config.task_id != task_id)
            or receipt["selection_fingerprint"] != fingerprint
        ):
            raise ValueError(
                "submission_id belongs to a different selection, task or scope"
            )
        if receipt["status"] == "completed":
            return {
                "task_id": task_id,
                "submission_id": submission_id,
                "input_count": receipt["input_count"],
                "total": task["total"],
            }
    while True:
        if receipt is None:
            task_id = draft_queue.find_draft(
                store,
                scope=config.queue_scope,
                processor=PROCESSOR_VERSION,
                task_id=config.task_id,
            )
        with store.selection_lock(task_id):
            latest = draft_queue.submission(store, submission_id)
            if latest is not None:
                if (
                    str(latest["task_id"]) != task_id
                    or latest["selection_fingerprint"] != fingerprint
                ):
                    raise ValueError(
                        "submission_id belongs to another task or selection"
                    )
                if latest["status"] == "completed":
                    return {
                        "task_id": task_id,
                        "submission_id": submission_id,
                        "input_count": latest["input_count"],
                        "total": store.task(task_id)["total"],
                    }
            if (
                store.task(task_id)["status"] != "draft"
                and receipt is None
                and config.task_id is None
            ):
                continue  # Start won the race: re-resolve to the next draft.
            receipt = draft_queue.prepare_submission(
                store,
                task_id=task_id,
                submission_id=submission_id,
                source=source,
                selection=selection,
                fingerprint=fingerprint,
            )
            if receipt["status"] == "completed":
                return {
                    "task_id": task_id,
                    "submission_id": submission_id,
                    "input_count": receipt["input_count"],
                    "total": store.task(task_id)["total"],
                }
            try:
                with clickhouse.get_connection() as client:
                    query_id = "ip-queue-import:" + submission_id
                    client.execute(
                        "KILL QUERY WHERE query_id=%(id)s SYNC", {"id": query_id}
                    )
                    # A retry replaces only this submission's rows, from the current source.
                    client.execute(
                        f"DELETE FROM {INPUT_RELATION} WHERE task_id=%(task)s AND submission_id=%(submission)s",
                        {"task": task_id, "submission": submission_id},
                        settings={"lightweight_deletes_sync": 2},
                    )
                    if config.source_relation is not None:
                        columns = {
                            row[0]
                            for row in client.execute(
                                f"DESCRIBE TABLE {config.source_relation}"
                            )
                        }
                        required = {config.ip_column, *config.filters}
                        if config.source_record_id_column is not None:
                            required.add(config.source_record_id_column)
                        if config.observed_at_column is not None:
                            required.add(config.observed_at_column)
                        if required - columns:
                            raise ValueError(
                                "source is missing columns: "
                                + ", ".join(sorted(required - columns))
                            )
                    selected_sql, params = selected_ips_sql(config)
                    params.update(
                        task=task_id,
                        submission=submission_id,
                        source_name=source,
                        source_run_id=config.source_run_id or run_id,
                    )
                    identity = INPUT_ID_SQL.format(
                        ip="chosen.ip", source="%(source_name)s", record="chosen.source_record_id"
                    )
                    # Entries land straight in the task's partition; the draft keeps one row
                    # per identity, so overlapping submissions append only what is new.
                    client.execute(
                        f"""INSERT INTO {INPUT_RELATION}
                        (task_id, input_id, ip, source_name, source_record_id, source_run_id, observed_at, submission_id)
                        SELECT %(task)s, s.input_id, s.ip, %(source_name)s, s.source_record_id,
                            %(source_run_id)s, s.observed_at, %(submission)s
                        FROM (
                            SELECT {identity} AS input_id, chosen.ip AS ip,
                                chosen.source_record_id AS source_record_id, chosen.observed_at AS observed_at
                            FROM ({selected_sql}) AS chosen
                        ) AS s
                        LEFT ANTI JOIN (
                            SELECT input_id FROM {INPUT_RELATION} WHERE task_id=%(task)s
                        ) AS e USING (input_id)""",
                        params,
                        query_id=query_id,
                        settings={
                            "async_insert": 0,
                            "use_query_cache": 0,
                            "max_threads": 4,
                            "max_bytes_before_external_group_by": 536870912,
                            "max_bytes_before_external_sort": 536870912,
                            # The draft side of the anti-join can hold tens of millions of ids.
                            "join_algorithm": "grace_hash",
                        },
                    )
                    [(count,)] = client.execute(
                        f"SELECT count() FROM {INPUT_RELATION} WHERE task_id=%(task)s AND submission_id=%(submission)s",
                        {"task": task_id, "submission": submission_id},
                    )
                    [(total,)] = client.execute(
                        f"SELECT count() FROM {INPUT_RELATION} WHERE task_id=%(task)s",
                        {"task": task_id},
                    )
                draft_queue.finish_submission(
                    store,
                    submission_id=submission_id,
                    task_id=task_id,
                    count=count,
                    total=total,
                )
                return {
                    "task_id": task_id,
                    "submission_id": submission_id,
                    "input_count": count,
                    "total": total,
                }
            except BaseException:
                draft_queue.fail_submission(store, submission_id)
                raise


@dg.asset(
    group_name="ip_enrichment",
    kinds={"clickhouse", "postgres"},
    pool="ip_enrichment_input",
    metadata={"dagster/table_name": INPUT_RELATION},
    description="Append IPs from a list, a filtered ClickHouse relation or a task's failed "
    "results to the open IP enrichment draft. Does not start enrichment.",
)
def ip_enrichment_input(
    context: dg.AssetExecutionContext,
    config: IpEnrichmentInputConfig,
    clickhouse: ClickhouseResource,
    processing: ProcessingResource,
) -> dg.MaterializeResult:
    submission_id = str(
        UUID(
            config.submission_id
            or context.run.tags.get("processing/submission_id")
            or context.run.root_run_id
            or context.run.run_id
        )
    )
    context.instance.add_run_tags(
        context.run.run_id, {"processing/submission_id": submission_id}
    )
    with processing.get_store() as store:
        metadata = load_ip_draft(
            config, submission_id, store, clickhouse, run_id=context.run.run_id
        )
    context.instance.add_run_tags(
        context.run.run_id, {"processing/task_id": metadata["task_id"]}
    )
    context.log.info(
        "IP enrichment draft %s: submission %s added %s rows, total %s",
        metadata["task_id"],
        submission_id,
        metadata["input_count"],
        metadata["total"],
    )
    return dg.MaterializeResult(
        metadata={**metadata, "input_relation": INPUT_RELATION}
    )


ip_enrichment_input_job = dg.define_asset_job(
    "ip_enrichment_input_job",
    selection=dg.AssetSelection.assets(ip_enrichment_input),
)
defs = dg.Definitions(assets=[ip_enrichment_input], jobs=[ip_enrichment_input_job])
```

- [ ] **Step 4: Run the tests**

Run: `uv run --frozen --no-sync pytest tests/test_ip_enrichment_input.py -q -p no:cacheprovider`
Expected: all pass.

Run: `uv run --frozen --no-sync dg check defs`
Expected: `All definitions loaded successfully.` (`results.py` still imports `INPUT_RELATION`/`PROCESSOR_VERSION`, which exist.)

Run ruff format/check on `src/dagster_v3/defs/ip_enrichment/input.py tests/test_ip_enrichment_input.py`.

- [ ] **Step 5: Commit**

```bash
git add services/dagster_v3/src/dagster_v3/defs/ip_enrichment/input.py services/dagster_v3/tests/test_ip_enrichment_input.py
git commit -m "feat(dagster): IP enrichment drafts with receipts; imports insert straight into the task partition

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

---

### Task 4: Page-batched RDAP resolution bounded by the frozen execution window

Today `RdapEnricher.lookup` (`enrichment.py:302-402`) issues, per IP, one `rdap_ip_lookup_results_current` query, one `rdap_networks_current FINAL` query that returns `raw_response` and re-parses it, and one `INSERT` per lookup marker; the review measured 90% of wall time in these round trips. The new `RdapEnricher.resolve_page(rows)` answers a whole page with four round trips at most, keeps two in-process caches (`cached`: fresh network rows read from ClickHouse, `recent`: reusable networks fetched over HTTP in this run) and requests RDAP only for misses. Registry-level responses (Task 1) are stored as `registry_level` segments, answer only their IP, and are never added to `recent`; a later task asking for the same IP is served by its own `found` marker in `rdap_ip_lookup_results` (per-IP hit) rather than by the trie.

**Files:**
- Rewrite: `services/dagster_v3/src/dagster_v3/defs/ip_enrichment/enrichment.py`
- Modify: `services/dagster_v3/tests/test_ip_enrichment_results.py` (append the resolver tests; they drive `RdapEnricher` directly with the `environment` fixture)

**Interfaces:**
- Produces (module `dagster_v3.defs.ip_enrichment.enrichment`):
  - `IpEnrichmentResultsConfig` — unchanged fields and defaults (`batch_size=250`, `max_requests=250`, `request_delay_seconds=1.0`, `parent_depth=1`, `rdap_cache_days=30`, `force_rdap=False`, `rate_limit_retry_seconds=3600`, `transient_retry_seconds=900`).
  - `geoip_result(ip, city_reader, asn_reader, *, checked_at, retry_seconds) -> dict` (unchanged), `rdap_result(...)` (unchanged), `matching_cidr(normalized, address)` (unchanged), `cidr_containing(start: str, end: str, address) -> str | None`.
  - `NETWORK_COLUMNS: tuple[str, ...]` (every `rdap_networks` column except `raw_response`), `cached_network_row(row) -> RdapNetwork` (with `raw_response=""`).
  - `RdapEnricher(client, rdap: RdapClient, config, log, *, started_at: datetime, cache_cutoff: datetime)` with counters `requests, cache_hits, networks_written, parent_failures, registry_level_responses`, flag `budget_reached`, and `resolve_page(rows: list[dict]) -> dict[str, dict]` mapping each row's `ip` to the `rdap_*` result fields; an IP missing from the result was not resolved because the budget ran out (`budget_reached` is then `True`).
- Removes: `RdapEnricher.lookup`. `RequestBudgetReached` stays defined (unused) so `results.py` keeps importing until Task 5 deletes both; the `maxminddb` re-export stays (`results.py` and tests patch `enrichment.maxminddb.open_database`).

- [ ] **Step 1: Write the failing resolver tests**

Append to `services/dagster_v3/tests/test_ip_enrichment_results.py` (add `from clickhouse_driver import Client` and `from dagster_v3.defs.ip_enrichment.enrichment import IpEnrichmentResultsConfig, RdapEnricher` to the imports; `SimpleNamespace` and `datetime` are already imported):

```python
def resolver(env, *, started_at=None, cache_days=30, **config):
    started = started_at or datetime.now(UTC)
    settings = IpEnrichmentResultsConfig(
        task_id=str(uuid4()), request_delay_seconds=0, rdap_cache_days=cache_days, **config
    )
    return RdapEnricher(
        env.client,
        enrichment.RdapClient(user_agent="test"),
        settings,
        SimpleNamespace(info=lambda *a: None, warning=lambda *a: None),
        started_at=started,
        cache_cutoff=started - timedelta(days=cache_days),
    )


def page(env, *ips):
    """Rows as the results loop would read them, with ClickHouse's own buckets."""
    buckets = dict(
        env.client.execute(
            "SELECT ip, toUInt16(cityHash64(ip) % 256) FROM (SELECT arrayJoin(%(ips)s) AS ip)",
            {"ips": list(ips)},
        )
    )
    return [
        {"input_id": f"{buckets[ip]:03d}:{ip}", "ip": ip, "ip_version": 4 if "." in ip else 6, "bucket": buckets[ip]}
        for ip in ips
    ]


def seed_network(env, ip, *, fetched_at, **raw):
    """A reusable registration in the RDAP cache, visible to the trie after a reload."""
    normalized = normalize_rdap_network(response(ip, **raw), fetched_at=fetched_at, segment_role="lookup_result")
    env.client.execute(RDAP_NETWORK_INSERT_SQL, [normalized.network.clickhouse_values()])
    env.client.execute(RDAP_SEGMENT_INSERT_SQL, [s.clickhouse_values() for s in normalized.segments])
    env.client.execute("SYSTEM RELOAD DICTIONARY corpscout.rdap_network_trie")
    return normalized


def test_page_resolution_uses_a_fixed_number_of_round_trips(environment, monkeypatch):
    env = environment
    seed_network(env, "8.8.8.1", fetched_at=datetime.now(UTC))
    queries = []
    execute = Client.execute
    monkeypatch.setattr(Client, "execute", lambda self, query, *a, **k: (queries.append(query), execute(self, query, *a, **k))[1])
    rows = page(env, *[f"8.8.8.{n}" for n in range(1, 41)], "127.0.0.1", "10.0.0.1")
    enricher = resolver(env)
    resolved = enricher.resolve_page(rows)
    assert env.calls == []  # every global address was a trie hit
    assert {ip: r["rdap_lookup_status"] for ip, r in resolved.items()} == {
        **{row["ip"]: "found" for row in rows[:40]}, "127.0.0.1": "not_global", "10.0.0.1": "not_global"}
    assert resolved["8.8.8.40"]["rdap_matched_cidr"] == "8.8.8.0/24"
    kinds = {
        "negative": sum(q.lstrip().startswith("SELECT ip, lookup_status") for q in queries),
        "trie": sum("dictGetOrDefault" in q for q in queries),
        "networks": sum(q.lstrip().startswith("SELECT network_key, rir") for q in queries),
        "markers": sum(q.lstrip().startswith("INSERT INTO corpscout.rdap_ip_lookup_results") for q in queries),
    }
    assert kinds == {"negative": 1, "trie": 1, "networks": 1, "markers": 1}
    assert not any("raw_response" in q for q in queries)
    # A second page of the same run hits the in-process network cache: no network read.
    queries.clear()
    assert enricher.resolve_page(page(env, "8.8.8.7"))["8.8.8.7"]["rdap_lookup_status"] == "found"
    assert sum(q.lstrip().startswith("SELECT network_key, rir") for q in queries) == 0
    # A fresh resolver (a resume) reads the row once.
    resolver(env).resolve_page(page(env, "8.8.8.7"))
    assert sum(q.lstrip().startswith("SELECT network_key, rir") for q in queries) == 1


def test_cache_window_is_the_frozen_execution_not_now(environment):
    env = environment
    started = datetime.now(UTC)
    seed_network(env, "8.8.8.1", fetched_at=started - timedelta(days=40))
    stale = resolver(env, started_at=started)  # 30-day window: the network is stale
    resolved = stale.resolve_page(page(env, "8.8.8.8"))
    assert env.calls == ["8.8.8.8"] and resolved["8.8.8.8"]["rdap_lookup_status"] == "found"
    assert stale.cache_hits == 0 and stale.requests == 1
    wide = resolver(env, started_at=started, cache_days=60)
    env.calls.clear()
    # A network fetched after started_at (by this or another execution) is still reusable.
    env.client.execute("SYSTEM RELOAD DICTIONARY corpscout.rdap_network_trie")
    assert wide.resolve_page(page(env, "8.8.8.9"))["8.8.8.9"]["rdap_lookup_status"] == "found"
    assert env.calls == [] and wide.cache_hits == 1


def test_negative_markers_are_honoured_by_the_frozen_start(environment, monkeypatch):
    env = environment
    started = datetime.now(UTC)

    def limited(self, ip):
        env.calls.append(ip)
        raise RdapClientError("rate limit", code="rate_limited", retryable=True, status_code=429)

    monkeypatch.setattr(enrichment.RdapClient, "lookup_ip", limited)
    first = resolver(env, started_at=started, rate_limit_retry_seconds=3600).resolve_page(page(env, "8.8.8.8"))
    assert first["8.8.8.8"]["rdap_lookup_status"] == "retryable_error"
    assert env.calls == ["8.8.8.8"]
    # Still inside the backoff relative to a later execution's start: served from the marker.
    again = resolver(env, started_at=started + timedelta(minutes=5)).resolve_page(page(env, "8.8.8.8"))
    assert again["8.8.8.8"]["rdap_error_code"] == "rate_limited" and env.calls == ["8.8.8.8"]
    # An execution that starts after retry_after asks again; force_rdap always asks.
    later = resolver(env, started_at=started + timedelta(hours=2)).resolve_page(page(env, "8.8.8.8"))
    assert later["8.8.8.8"]["rdap_lookup_status"] == "retryable_error" and env.calls == ["8.8.8.8", "8.8.8.8"]
    resolver(env, started_at=started, force_rdap=True).resolve_page(page(env, "8.8.8.8"))
    assert env.calls == ["8.8.8.8"] * 3


def test_registry_level_response_answers_only_its_ip(environment, monkeypatch):
    env = environment
    monkeypatch.setattr(
        enrichment.RdapClient,
        "lookup_ip",
        lambda self, ip: (env.calls.append(ip), response(ip, start="103.0.0.0", end="103.255.255.255", handle="103.0.0.0 - 103.255.255.255", name="APNIC-AP"))[1],
    )
    first = resolver(env).resolve_page(page(env, "103.35.64.49"))
    assert first["103.35.64.49"]["rdap_lookup_status"] == "found"
    assert first["103.35.64.49"]["rdap_name"] == "APNIC-AP"
    assert first["103.35.64.49"]["rdap_matched_cidr"] == "103.0.0.0/8"
    assert env.client.execute("SELECT DISTINCT segment_role FROM corpscout.rdap_network_segments") == [("registry_level",)]
    env.client.execute("SYSTEM RELOAD DICTIONARY corpscout.rdap_network_trie")
    assert env.client.execute("SELECT count() FROM corpscout.rdap_network_segments_current") == [(0,)]
    # Another address in the block is not served by the trie nor by the in-run cache.
    second = resolver(env).resolve_page(page(env, "103.15.66.50"))
    assert second["103.15.66.50"]["rdap_lookup_status"] == "found"
    assert env.calls == ["103.35.64.49", "103.15.66.50"]
    # The queried address itself is served by its own lookup marker next time.
    third = resolver(env)
    assert third.resolve_page(page(env, "103.35.64.49"))["103.35.64.49"]["rdap_name"] == "APNIC-AP"
    assert env.calls == ["103.35.64.49", "103.15.66.50"] and third.cache_hits == 1


def test_misses_reuse_networks_fetched_earlier_in_the_run_and_stop_at_the_budget(environment):
    env = environment
    enricher = resolver(env, max_requests=1)
    resolved = enricher.resolve_page(page(env, "8.8.8.8", "8.8.8.9", "1.1.1.1"))
    assert env.calls == ["8.8.8.8"]
    assert resolved["8.8.8.9"]["rdap_matched_cidr"] == "8.8.8.0/24"  # in-run reuse, no HTTP
    assert "1.1.1.1" not in resolved and enricher.budget_reached
    assert enricher.cache_hits == 1 and enricher.requests == 1
    # The page's markers were written for what was resolved.
    assert env.client.execute(
        "SELECT ip, lookup_status FROM corpscout.rdap_ip_lookup_results_current ORDER BY ip"
    ) == [("8.8.8.8", "found")]
```

(The bucket comes from ClickHouse's `cityHash64(ip) % 256` in `page()`; Python has no twin of that hash, which is also why `input_id` is computed in SQL.)

- [ ] **Step 2: Run to verify they fail**

Run: `uv run --frozen --no-sync pytest tests/test_ip_enrichment_results.py -k "page_resolution or cache_window or negative_markers or registry_level_response or misses_reuse" -q -p no:cacheprovider`
Expected: FAIL — `TypeError: RdapEnricher.__init__() got an unexpected keyword argument 'started_at'`.

- [ ] **Step 3: Rewrite `enrichment.py`**

Replace the whole of `services/dagster_v3/src/dagster_v3/defs/ip_enrichment/enrichment.py` with:

```python
"""Shared IP lookups: GeoIP per address, RDAP resolved one page at a time.

A page's ClickHouse work is a fixed number of round trips whatever its size: one
negative-cache read, one trie lookup, one read of the network rows the page needs
(never raw_response) and one insert of the page's lookup markers. RDAP HTTP requests
happen only for misses. Freshness is judged against the frozen execution start, so a
resume gives the same answers.
"""

import time
from collections import OrderedDict
from dataclasses import asdict, replace
from datetime import UTC, datetime, timedelta
from ipaddress import ip_address, ip_network
from uuid import UUID

import dagster as dg
import maxminddb
from netaddr import iprange_to_cidrs
from pydantic import Field, field_validator

from dagster_v3.defs.commoncrawl_geoip.maxmind import (
    build_geoip_enrichment,
    classify_ip_scope,
    lookup_maxmind_record,
)
from dagster_v3.defs.commoncrawl_rdap.assets import (
    RDAP_LOOKUP_INSERT_SQL,
    RDAP_NETWORK_INSERT_SQL,
    RDAP_SEGMENT_INSERT_SQL,
)
from dagster_v3.defs.commoncrawl_rdap.client import RdapClient, RdapClientError
from dagster_v3.defs.commoncrawl_rdap.rdap import (
    NormalizedRdapNetwork,
    RdapLookupResponse,
    RdapNetwork,
    is_registry_catch_all,
    normalize_rdap_network,
    registry_level_reason,
)

NEGATIVE_STATUSES = {"not_found", "unsupported", "terminal_error"}
# Every rdap_networks column except raw_response; the cache never loads the JSON.
NETWORK_COLUMNS = (
    "network_key",
    "rir",
    "handle",
    "ip_version",
    "start_address",
    "end_address",
    "name",
    "registration_type",
    "country_code",
    "status",
    "registrant_handles",
    "registrant_names",
    "parent_network_key",
    "parent_handle",
    "self_url",
    "up_url",
    "registration_date",
    "last_changed_at",
    "response_sha256",
    "fetched_at",
)
WRITE_SETTINGS = {"async_insert": 1, "wait_for_async_insert": 1}


class RequestBudgetReached(Exception):
    """Kept for results.py until Task 5 replaces it with RdapEnricher.budget_reached."""


class IpEnrichmentResultsConfig(dg.Config):
    task_id: str = Field(description="IP enrichment draft (task) UUID.")
    execution_id: str | None = Field(
        default=None,
        description="Saved execution to resume. Omit to start, or to resume the task's saved execution.",
    )
    batch_size: int = Field(default=250, ge=1, le=10_000)
    max_requests: int | None = Field(
        default=250,
        ge=1,
        description="RDAP HTTP request budget, including parents. Null processes the whole task.",
    )
    request_delay_seconds: float = Field(default=1.0, ge=0, le=60)
    parent_depth: int = Field(default=1, ge=0, le=5)
    rdap_cache_days: int = Field(default=30, ge=1)
    force_rdap: bool = False
    rate_limit_retry_seconds: int = Field(default=3600, ge=1)
    transient_retry_seconds: int = Field(default=900, ge=1)

    @field_validator("task_id", "execution_id")
    @classmethod
    def uuid_identity(cls, value: str | None) -> str | None:
        return str(UUID(value)) if value is not None else None


def geoip_result(ip, city_reader, asn_reader, *, checked_at, retry_seconds):
    address = ip_address(ip)
    city_meta, asn_meta = city_reader.metadata(), asn_reader.metadata()
    errors = {}
    lookups = {}
    for component, reader in (("city", city_reader), ("asn", asn_reader)):
        lookups[component] = None
        if classify_ip_scope(address) == "global":
            try:
                lookups[component] = lookup_maxmind_record(reader, address)
            except ValueError, maxminddb.InvalidDatabaseError:
                errors[component] = "maxmind_lookup_error"
    result = asdict(
        build_geoip_enrichment(
            bucket=0,
            address=address,
            city_lookup=lookups["city"],
            asn_lookup=lookups["asn"],
            city_build_epoch=datetime.fromtimestamp(city_meta.build_epoch, UTC),
            asn_build_epoch=datetime.fromtimestamp(asn_meta.build_epoch, UTC),
            enriched_at=checked_at,
        )
    )
    for key in ("ip", "ip_version", "bucket", "enriched_at"):
        del result[key]
    for component in ("city", "asn"):
        result[f"{component}_checked_at"] = checked_at
        result[f"{component}_error_code"] = errors.get(component)
        result[f"{component}_retry_after"] = None
        if component in errors:
            result[f"{component}_lookup_status"] = "retryable_error"
            result[f"{component}_retry_after"] = checked_at + timedelta(
                seconds=retry_seconds
            )
    return result


def rdap_result(
    *, status, checked_at, error_code=None, retry_after=None, network=None, cidr=None
):
    result = dict.fromkeys(
        (
            "rdap_network_key",
            "rdap_matched_cidr",
            "rdap_rir",
            "rdap_handle",
            "rdap_start_address",
            "rdap_end_address",
            "rdap_name",
            "rdap_registration_type",
            "rdap_country_code",
            "rdap_parent_network_key",
            "rdap_parent_handle",
            "rdap_self_url",
            "rdap_registration_date",
            "rdap_last_changed_at",
        )
    )
    result.update(
        rdap_lookup_status=status,
        rdap_checked_at=checked_at,
        rdap_error_code=error_code,
        rdap_retry_after=retry_after,
        rdap_statuses=[],
        rdap_registrant_handles=[],
        rdap_registrant_names=[],
    )
    if network is not None:
        for field in (
            "network_key",
            "rir",
            "handle",
            "start_address",
            "end_address",
            "name",
            "registration_type",
            "country_code",
            "parent_network_key",
            "parent_handle",
            "self_url",
            "registration_date",
            "last_changed_at",
        ):
            result[f"rdap_{field}"] = getattr(network, field)
        result.update(
            rdap_matched_cidr=cidr,
            rdap_statuses=list(network.status),
            rdap_registrant_handles=list(network.registrant_handles),
            rdap_registrant_names=list(network.registrant_names),
        )
    return result


def matching_cidr(normalized: NormalizedRdapNetwork, address) -> str | None:
    return next(
        (s.cidr for s in normalized.segments if address in ip_network(s.cidr)), None
    )


def cidr_containing(start: str, end: str, address) -> str | None:
    """The exact CIDR fragment of an inclusive range that holds ``address`` (None outside it)."""
    return next(
        (
            str(cidr)
            for cidr in iprange_to_cidrs(start, end)
            if cidr.version == address.version and cidr.first <= int(address) <= cidr.last
        ),
        None,
    )


def cached_network_row(row) -> RdapNetwork:
    """A network as read from the cache; raw_response is never loaded."""
    values = dict(zip(NETWORK_COLUMNS, row, strict=True))
    for key in ("status", "registrant_handles", "registrant_names"):
        values[key] = tuple(values[key])
    return RdapNetwork(raw_response="", **values)


def _clickhouse_time(value: datetime) -> str:
    return value.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S.%f")


class RdapEnricher:
    """Resolve RDAP coverage for a page of addresses with a bounded number of round trips."""

    def __init__(
        self,
        client,
        rdap: RdapClient,
        config: IpEnrichmentResultsConfig,
        log,
        *,
        started_at: datetime,
        cache_cutoff: datetime,
    ):
        self.client = client
        self.rdap = rdap
        self.config = config
        self.log = log
        self.started_at = started_at
        self.cache_cutoff = cache_cutoff
        self.requests = 0
        self.cache_hits = 0
        self.networks_written = 0
        self.parent_failures = 0
        self.registry_level_responses = 0
        self.budget_reached = False
        # Reusable networks fetched over HTTP in this run, checked before any request.
        self.recent: OrderedDict[str, NormalizedRdapNetwork] = OrderedDict()
        # Fresh network rows read from ClickHouse, keyed by network_key.
        self.cached: OrderedDict[str, RdapNetwork] = OrderedDict()

    # --- page resolution -------------------------------------------------------

    def resolve_page(self, rows: list[dict]) -> dict[str, dict]:
        """RDAP fields per address of the page; an address is absent only when the budget ran out."""
        addresses = {row["ip"]: ip_address(row["ip"]) for row in rows}
        buckets = {row["ip"]: row["bucket"] for row in rows}
        results: dict[str, dict] = {}
        markers: list[tuple] = []
        pending: list[str] = []
        for ip, address in addresses.items():
            if classify_ip_scope(address) != "global":
                results[ip] = rdap_result(status="not_global", checked_at=datetime.now(UTC))
                markers.append(self._marker(ip, address, buckets[ip], results[ip]))
            else:
                pending.append(ip)
        if pending and not self.config.force_rdap:
            hits: dict[str, str] = {}
            for ip, status, network_key, code, retry_after, checked_at, fresh, backoff in (
                self._markers_of(pending, addresses, buckets)
            ):
                if status == "retryable_error" and backoff:
                    results[ip] = rdap_result(
                        status=status, checked_at=checked_at, error_code=code, retry_after=retry_after
                    )
                elif status in NEGATIVE_STATUSES and fresh:
                    results[ip] = rdap_result(
                        status="terminal_error" if status == "unsupported" else status,
                        checked_at=checked_at,
                        error_code=code,
                        retry_after=retry_after,
                    )
                elif status == "found" and network_key and fresh:
                    hits[ip] = network_key  # a per-address answer, e.g. a registry-level block
                else:
                    continue
                if ip in results:
                    self.cache_hits += 1
            pending = [ip for ip in pending if ip not in results and ip not in hits]
            for ip, network_key in self._trie(pending, addresses):
                if network_key:
                    hits[ip] = network_key
            pending = [ip for ip in pending if ip not in hits]
            self._load_networks({key for key in hits.values() if key not in self.cached})
            for ip, network_key in hits.items():
                network = self.cached.get(network_key)
                cidr = (
                    cidr_containing(network.start_address, network.end_address, addresses[ip])
                    if network is not None
                    else None
                )
                if cidr is None:  # stale, gone or not containing the address: ask again
                    pending.append(ip)
                    continue
                results[ip] = rdap_result(
                    status="found", checked_at=network.fetched_at, network=network, cidr=cidr
                )
                self.cache_hits += 1
        for ip in pending:
            recent = self._recent_match(addresses[ip])
            if recent is not None:  # fetched earlier in this run: no HTTP, no marker
                normalized, cidr = recent
                self.cache_hits += 1
                results[ip] = rdap_result(
                    status="found", checked_at=normalized.network.fetched_at, network=normalized.network, cidr=cidr
                )
                continue
            if self._budget_exhausted():
                self.budget_reached = True
                break
            results[ip] = self._request_ip(ip, addresses[ip], buckets[ip], markers)
        if markers:
            self.client.execute(RDAP_LOOKUP_INSERT_SQL, markers, settings=WRITE_SETTINGS)
        return results

    def _markers_of(self, ips, addresses, buckets):
        return self.client.execute(
            """SELECT ip, lookup_status, ifNull(network_key, ''), error_code, retry_after, queried_at,
                queried_at >= toDateTime64(%(cutoff)s, 6, 'UTC') AS fresh,
                ifNull(retry_after > toDateTime64(%(started)s, 6, 'UTC'), 0) AS backoff
            FROM corpscout.rdap_ip_lookup_results_current
            WHERE (bucket, ip_version, ip) IN %(keys)s""",
            {
                "keys": tuple((buckets[ip], addresses[ip].version, ip) for ip in ips),
                "cutoff": _clickhouse_time(self.cache_cutoff),
                "started": _clickhouse_time(self.started_at),
            },
        )

    def _trie(self, ips, addresses):
        if not ips:
            return []
        return self.client.execute(
            """SELECT ip, dictGetOrDefault('corpscout.rdap_network_trie', 'network_key', tuple(toIPv4(ip)), '')
            FROM (SELECT arrayJoin(%(v4)s) AS ip)
            UNION ALL
            SELECT ip, dictGetOrDefault('corpscout.rdap_network_trie', 'network_key', tuple(toIPv6(ip)), '')
            FROM (SELECT arrayJoin(%(v6)s) AS ip)""",
            {
                "v4": [ip for ip in ips if addresses[ip].version == 4],
                "v6": [ip for ip in ips if addresses[ip].version == 6],
            },
        )

    def _load_networks(self, keys: set[str]) -> None:
        if not keys:
            return
        rows = self.client.execute(
            f"""SELECT {", ".join(NETWORK_COLUMNS)} FROM corpscout.rdap_networks_current
            WHERE network_key IN %(keys)s AND fetched_at >= toDateTime64(%(cutoff)s, 6, 'UTC')""",
            {"keys": tuple(keys), "cutoff": _clickhouse_time(self.cache_cutoff)},
        )
        for row in rows:
            network = cached_network_row(row)
            self.cached[network.network_key] = network
            self.cached.move_to_end(network.network_key)
        while len(self.cached) > 4096:
            self.cached.popitem(last=False)

    # --- misses ---------------------------------------------------------------

    def _budget_exhausted(self) -> bool:
        return (
            self.config.max_requests is not None
            and self.requests >= self.config.max_requests
        )

    def _request(self, address_or_url, *, rir=None):
        if self.requests and self.config.request_delay_seconds:
            time.sleep(self.config.request_delay_seconds)
        self.requests += 1
        if rir is not None:
            return self.rdap.lookup_up_url(address_or_url, rir=rir)
        return self.rdap.lookup_ip(address_or_url)

    def _persist(self, normalized: NormalizedRdapNetwork) -> None:
        self.client.execute(
            RDAP_NETWORK_INSERT_SQL,
            [normalized.network.clickhouse_values()],
            settings=WRITE_SETTINGS,
        )
        self.client.execute(
            RDAP_SEGMENT_INSERT_SQL,
            [segment.clickhouse_values() for segment in normalized.segments],
            settings=WRITE_SETTINGS,
        )
        self.networks_written += 1

    def _remember(self, normalized: NormalizedRdapNetwork) -> None:
        self.recent[normalized.network.network_key] = normalized
        self.recent.move_to_end(normalized.network.network_key)
        if len(self.recent) > 1024:
            self.recent.popitem(last=False)

    def _recent_match(self, address):
        matches = []
        for normalized in self.recent.values():
            cidr = matching_cidr(normalized, address)
            if cidr is not None:
                matches.append((ip_network(cidr).prefixlen, normalized.network.fetched_at, normalized.network.network_key, normalized, cidr))
        if not matches:
            return None
        _, _, _, normalized, cidr = max(matches, key=lambda match: match[:3])
        return normalized, cidr

    def _marker(self, ip, address, bucket, result) -> tuple:
        return (
            bucket,
            ip,
            address.version,
            result["rdap_lookup_status"],
            result["rdap_network_key"],
            result["rdap_error_code"],
            result["rdap_retry_after"],
            result["rdap_checked_at"],
        )

    def _request_ip(self, ip, address, bucket, markers: list[tuple]) -> dict:
        try:
            response = self._request(ip)
            checked_at = datetime.now(UTC)
            direct = normalize_rdap_network(
                response, fetched_at=checked_at, segment_role="lookup_result"
            )
            cidr = matching_cidr(direct, address)
            if is_registry_catch_all(direct):
                raise RdapClientError(
                    "Universal registry coverage", code="registry_catch_all", retryable=False
                )
            if cidr is None:
                raise RdapClientError(
                    "Response range does not contain requested IP", code="range_mismatch", retryable=False
                )
        except (RdapClientError, ValueError) as error:
            checked_at = datetime.now(UTC)
            if isinstance(error, RdapClientError):
                code = error.code
                status = "retryable_error" if error.retryable else "terminal_error"
                if code == "not_found":
                    status = "not_found"
            else:
                code, status = "invalid_response", "terminal_error"
            retry_after = None
            if status == "retryable_error":
                seconds = (
                    self.config.rate_limit_retry_seconds
                    if code == "rate_limited"
                    else self.config.transient_retry_seconds
                )
                retry_after = checked_at + timedelta(seconds=seconds)
            result = rdap_result(
                status=status, checked_at=checked_at, error_code=code, retry_after=retry_after
            )
            markers.append(self._marker(ip, address, bucket, result))
            return result
        reason = registry_level_reason(direct.network, direct.segments)
        if reason is not None:
            # Answers only this address: stored for audit, kept out of the trie and of `recent`.
            self.registry_level_responses += 1
            self.log.info(
                "RDAP registration %s answers only %s (%s)", direct.network.network_key, ip, reason
            )
            direct = NormalizedRdapNetwork(
                network=direct.network,
                segments=tuple(replace(segment, segment_role="registry_level") for segment in direct.segments),
            )
        # Coverage is durable before any exact-IP outcome refers to it.
        self._persist(direct)
        if reason is None:
            self._remember(direct)
        current = direct
        visited = {direct.network.network_key}
        for _ in range(self.config.parent_depth):
            if current.network.up_url is None or self._budget_exhausted():
                break
            try:
                parent = normalize_rdap_network(
                    self._request(current.network.up_url, rir=current.network.rir),
                    fetched_at=datetime.now(UTC),
                    segment_role="parent",
                )
                if parent.network.network_key in visited:
                    break
                visited.add(parent.network.network_key)
                self._persist(parent)
                current = parent
            except (RdapClientError, ValueError) as error:
                self.parent_failures += 1
                self.log.warning(
                    "Optional RDAP parent lookup failed for %s: %s", ip, type(error).__name__
                )
                break
        result = rdap_result(
            status="found", checked_at=direct.network.fetched_at, network=direct.network, cidr=cidr
        )
        markers.append(self._marker(ip, address, bucket, result))
        return result
```

Notes for the implementer: `RdapLookupResponse` stays imported because tests build responses through it; `_markers_of` and `_load_networks` compare times in SQL so the driver's timezone handling never enters the decision; `rdap_result(checked_at=network.fetched_at)` keeps today's semantics (the cached registration's fetch time is the RDAP check time).

- [ ] **Step 4: Run the resolver tests**

Run: `uv run --frozen --no-sync pytest tests/test_ip_enrichment_results.py -k "page_resolution or cache_window or negative_markers or registry_level_response or misses_reuse or registry_level_rule or agree_on_registry" -q -p no:cacheprovider`
Expected: all pass. The asset-level tests of this file stay red until Task 5 rewrites `results.py` and them; `-k` selects only the resolver tests. The module still imports because `RequestBudgetReached` is kept as an unused class until Task 5 deletes it.

Run ruff format/check on `src/dagster_v3/defs/ip_enrichment/enrichment.py tests/test_ip_enrichment_results.py`.

- [ ] **Step 5: Commit**

```bash
git add services/dagster_v3/src/dagster_v3/defs/ip_enrichment/enrichment.py services/dagster_v3/tests/test_ip_enrichment_results.py
git commit -m "perf(dagster): resolve RDAP coverage per page with a fixed number of ClickHouse round trips, bounded by the frozen window

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

---

### Task 5: Execution loop over live remaining work, completion from results, partition purge

`results.py` is rewritten around the shared lifecycle. Design decided here: the task is walked bucket by bucket (`SELECT DISTINCT bucket` once per pass); within a bucket, pages come from a cursor on `input_id` and the anti-join subquery reads `ip_enrichment_results WHERE bucket = b AND task_id AND execution_id` — one primary-key range of the results table (`ORDER BY (bucket, ip, result_id)`), so the cost per page is independent of the task size. Results go through a `ResultBuffer` (500 rows / 5 s) and the cursor never re-reads a page inside a pass; after each pass the buffer is flushed and the walk restarts, and a pass that finds nothing ends the loop. Completion counts use the same bucket-scoped queries. `attempt` is always 1: a task has one execution and failed addresses are retried in a new draft. A reached RDAP budget flushes what was resolved and raises `dg.Failure` (task stays `selected`, re-running the task resumes). `ip_enrichment_workflow` goes.

**Files:**
- Rewrite: `services/dagster_v3/src/dagster_v3/defs/ip_enrichment/results.py`
- Modify: `services/dagster_v3/src/dagster_v3/defs/ip_enrichment/enrichment.py` (delete the `RequestBudgetReached` shim)
- Modify: `services/dagster_v3/tests/test_ip_enrichment_results.py` (replace the asset-level tests from `select` down to `test_workflow_freezes_and_processes_the_same_task`; keep the Task 1 and Task 4 tests)

**Interfaces:**
- Produces (module `dagster_v3.defs.ip_enrichment.results`):
  - `TRANSPORT_SETTINGS = ("batch_size", "max_requests", "request_delay_seconds")`, `NOT_FROZEN`, `LOOKUP_STATUSES`, `FAILED_SQL`.
  - `start_ip_execution(store, client, config, run_id, *, default_execution_id=None) -> dict` (via `queue_execution.start_execution`, profile = config minus `task_id, execution_id, batch_size, max_requests, request_delay_seconds` plus `processor_version`, `freshness_days = rdap_cache_days`, label `"IP enrichment"`).
  - `task_buckets(client, task) -> list[int]`, `remaining_entries(client, task, *, bucket: int, after: str | None = None, limit: int) -> list[dict]` (keys `input_id, ip, ip_version, bucket`, ordered by `input_id`), `count_outcomes(client, task, buckets) -> tuple[int, int, int]` (`remaining, succeeded, failed`).
  - `store_results(client, records: list[dict]) -> None`, `run_ip_enrichment(context, client, task, config, *, enricher, city_reader, asn_reader) -> dict` with keys `written, pages, request_limit_reached`.
  - `finish_ip_execution(store, client, task) -> dict`.
  - Asset `ip_enrichment_results` with metadata keys `task_id, execution_id, results_table, coverage_semantics, rdap_requests, rdap_cache_hits, parent_lookup_failures, registry_level_responses, written, pages, request_limit_reached, completion_status, succeeded_pages, failed_pages, skipped_recent, inputs_purged` (or `already_completed`); run tags `processing/task_id, ip_enrichment/execution_id, ip_enrichment/execution, ip_enrichment/outcome, ip_enrichment/succeeded_pages, ip_enrichment/failed_pages, ip_enrichment/skipped_pages`; job `ip_enrichment_results_job`.
- Removes: `ip_enrichment_workflow`, `prepare_execution`, `page_outcomes`, `insert_result`, `EXECUTION_TAG`, `RequestBudgetReached`.

- [ ] **Step 1: Write the failing tests**

In `services/dagster_v3/tests/test_ip_enrichment_results.py` replace everything from `def select(env, ips):` through the end of `test_workflow_freezes_and_processes_the_same_task` with:

```python
def select(env, ips, *, scope=None, **config):
    """Append ``ips`` to the open draft of ``scope`` (a fresh scope by default); the task id."""
    result = prepare_input(
        env.resource, env.dsn, queue_scope=scope or "scope-" + uuid4().hex, ips=ips, **config
    )
    assert result.success
    return result.asset_materializations_for_node("ip_enrichment_input")[0].metadata["task_id"].value


def run(env, task, **config):
    return dg.materialize(
        [results.ip_enrichment_results, dg.AssetSpec("ip_enrichment_input")],
        instance=env.instance,
        resources={
            "clickhouse": env.resource,
            "processing": ProcessingResource(postgres_url=env.dsn),
            "maxmind_geoip": env.maxmind,
        },
        run_config={
            "ops": {
                "ip_enrichment_results": {
                    "config": {"task_id": task, "request_delay_seconds": 0, **config}
                }
            }
        },
        raise_on_error=False,
    )


def outcome(result):
    return {
        key: value.value
        for key, value in result.asset_materializations_for_node("ip_enrichment_results")[0]
        .metadata.items()
    }


def task_row(env, task):
    with ProcessingResource(postgres_url=env.dsn).get_store() as store:
        return store.task(task)


def test_draft_geoip_rdap_segments_completion_and_purge(environment):
    env = environment
    select(env, ["9.9.9.9"])
    task = select(env, ["8.8.8.8", "8.8.8.9", "2001:4860::8888", "127.0.0.1"])
    first = run(env, task, batch_size=1)
    assert first.success
    assert sorted(env.calls) == ["2001:4860::8888", "8.8.8.8"]
    assert "127.0.0.1" not in env.city.calls
    assert env.client.execute("""SELECT ip, country_iso_code, rdap_country_code,
        city_network, asn_network, rdap_matched_cidr FROM corpscout.ip_enrichment_current
        WHERE ip='8.8.8.8'""") == [
        ("8.8.8.8", "US", "CA", "8.8.8.0/24", "8.8.8.0/24", "8.8.8.0/24")
    ]
    assert env.client.execute(
        "SELECT count(), uniqExact(task_id), uniqExact(execution_id), min(attempt) FROM corpscout.ip_enrichment_results FINAL"
    ) == [(4, 1, 1, 1)]
    assert env.client.execute(
        "SELECT rdap_lookup_status, ip_scope FROM corpscout.ip_enrichment_current WHERE ip='127.0.0.1'"
    ) == [("not_global", "loopback")]
    assert env.client.execute(
        "SELECT count() FROM corpscout.rdap_network_segments FINAL"
    ) == [(2,)]
    metadata = outcome(first)
    assert metadata["completion_status"] == "completed"
    assert (metadata["written"], metadata["succeeded_pages"], metadata["failed_pages"], metadata["skipped_recent"]) == (4, 4, 0, 0)
    assert metadata["execution_id"] == first.run_id
    record = task_row(env, task)
    assert record["status"] == "completed" and record["inputs_purged_at"] is not None
    assert (record["succeeded_count"], record["terminal_failed_count"], record["skipped_count"]) == (4, 0, 0)
    assert env.client.execute(
        "SELECT count() FROM corpscout.ip_enrichment_input WHERE task_id = %(task)s", {"task": task}
    ) == [(0,)]
    tags = env.instance.get_run_by_id(first.run_id).tags
    assert tags["ip_enrichment/outcome"] == "completed" and tags["ip_enrichment/succeeded_pages"] == "4"
    calls_before = list(env.calls)
    # A completed task re-run only retries cleanup; another draft reuses the cached coverage.
    assert outcome(run(env, task))["already_completed"] is True
    assert run(env, select(env, ["8.8.8.10"])).success
    assert env.calls == calls_before


def test_errors_are_published_outcomes_and_backoff_holds_until_forced(environment, monkeypatch):
    env = environment
    attempts = []

    def unavailable(self, ip):
        attempts.append(ip)
        raise RdapClientError(
            "rate limit", code="rate_limited", retryable=True, status_code=429
        )

    monkeypatch.setattr(enrichment.RdapClient, "lookup_ip", unavailable)
    task = select(env, ["8.8.8.8"])
    first = run(env, task)
    assert first.success and outcome(first)["completion_status"] == "completed_with_errors"
    assert env.client.execute("""SELECT city_lookup_status, asn_lookup_status, rdap_lookup_status,
        country_iso_code, rdap_error_code, rdap_retry_after > rdap_checked_at
        FROM corpscout.ip_enrichment_current""") == [
        ("found", "found", "retryable_error", "US", "rate_limited", 1)
    ]
    record = task_row(env, task)
    assert (record["status"], record["terminal_failed_count"], record["inputs_purged_at"] is not None) == ("completed", 1, True)
    # The failed address goes to a new draft (retry mode); the saved backoff still applies there.
    retry = prepare_input(
        env.resource, env.dsn, queue_scope="scope-" + uuid4().hex, retry_failed_task_id=task
    )
    assert retry.success
    again = retry.asset_materializations_for_node("ip_enrichment_input")[0].metadata["task_id"].value
    assert task_row(env, again)["total"] == 1
    assert run(env, again).success and attempts == ["8.8.8.8"]
    monkeypatch.setattr(enrichment.RdapClient, "lookup_ip", lambda self, ip: response(ip))
    assert run(env, select(env, ["8.8.8.8"]), force_rdap=True).success
    assert env.client.execute(
        "SELECT rdap_lookup_status FROM corpscout.ip_enrichment_current"
    ) == [("found",)]


def test_request_budget_fails_the_run_and_the_same_task_resumes(environment):
    env = environment
    task = select(env, ["1.1.1.1", "8.8.8.8"])
    first = run(env, task, max_requests=1)
    assert not first.success
    assert env.client.execute(
        "SELECT count() FROM corpscout.ip_enrichment_results FINAL"
    ) == [(1,)]
    assert task_row(env, task)["status"] == "selected"
    # Transport settings may change; the saved execution is resumed without execution_id.
    resumed = run(env, task, max_requests=1, batch_size=7)
    assert resumed.success and outcome(resumed)["execution_id"] == first.run_id
    assert env.calls == ["1.1.1.1", "8.8.8.8"]
    assert env.client.execute(
        "SELECT count(), uniqExact(execution_id) FROM corpscout.ip_enrichment_results FINAL"
    ) == [(2, 1)]


@pytest.mark.parametrize("kind", ["catch_all", "wrong_range"])
def test_invalid_registration_coverage_is_a_terminal_error(environment, monkeypatch, kind):
    env = environment
    monkeypatch.setattr(
        enrichment.RdapClient,
        "lookup_ip",
        lambda self, ip: response(
            ip,
            start="0.0.0.0" if kind == "catch_all" else "9.9.9.0",
            end="255.255.255.255" if kind == "catch_all" else "9.9.9.255",
        ),
    )
    result = run(env, select(env, ["8.8.8.8"]))
    assert result.success and outcome(result)["failed_pages"] == 1
    assert env.client.execute(
        "SELECT rdap_lookup_status, city_lookup_status FROM corpscout.ip_enrichment_current"
    ) == [("terminal_error", "found")]
    assert env.client.execute("SELECT count() FROM corpscout.rdap_networks") == [(0,)]


def test_city_lookup_failure_does_not_discard_asn_or_rdap(environment):
    env = environment
    env.city.fail = True
    result = run(env, select(env, ["8.8.8.8"]))
    assert result.success and outcome(result)["completion_status"] == "completed_with_errors"
    assert env.client.execute(
        "SELECT city_lookup_status, asn_lookup_status, rdap_lookup_status, asn FROM corpscout.ip_enrichment_current"
    ) == [("retryable_error", "found", "found", 15169)]


def test_task_id_must_name_a_draft(environment):
    env = environment
    assert not run(env, str(uuid4())).success
    legacy = str(uuid4())
    with ProcessingResource(postgres_url=env.dsn).get_store() as store:
        store.prepare_selection(legacy, processor="ip-enrichment-v1", fingerprint="legacy")
        assert store.task(legacy)["queue_scope"] is None
    assert not run(env, legacy).success
    assert env.calls == env.city.calls == []


def test_resume_after_lost_write_acknowledgement_does_not_repeat_lookups(environment, monkeypatch):
    env = environment
    task = select(env, ["8.8.8.8"])
    execute = Client.execute
    interrupted = False

    def lost_ack(self, query, *args, **kwargs):
        nonlocal interrupted
        value = execute(self, query, *args, **kwargs)
        if query.lstrip().startswith("INSERT INTO corpscout.ip_enrichment_results") and not interrupted:
            interrupted = True
            raise ConnectionError("lost acknowledgement after durable insert")
        return value

    monkeypatch.setattr(Client, "execute", lost_ack)
    first = run(env, task)
    assert not first.success
    assert env.client.execute(
        "SELECT count() FROM corpscout.ip_enrichment_results FINAL"
    ) == [(1,)]
    assert run(env, task).success
    assert env.calls == env.city.calls == ["8.8.8.8"]
    assert env.client.execute(
        "SELECT count() FROM corpscout.ip_enrichment_results FINAL"
    ) == [(1,)]


def test_non_aligned_rdap_range_saves_exact_matching_segment(environment, monkeypatch):
    env = environment
    monkeypatch.setattr(
        enrichment.RdapClient,
        "lookup_ip",
        lambda self, ip: response(ip, start="8.8.8.1", end="8.8.8.10"),
    )
    assert run(env, select(env, ["8.8.8.8"])).success
    assert env.client.execute("""SELECT rdap_start_address, rdap_end_address, rdap_matched_cidr
        FROM corpscout.ip_enrichment_current""") == [
        ("8.8.8.1", "8.8.8.10", "8.8.8.8/31")
    ]
    assert env.client.execute(
        "SELECT count() FROM corpscout.rdap_network_segments FINAL"
    ) == [(5,)]


def test_optional_parent_failure_preserves_direct_registration(environment, monkeypatch):
    env = environment

    def direct(self, ip):
        found = response(ip)
        found.raw_response["links"] = [
            {"rel": "up", "href": "https://rdap.arin.net/registry/ip/8.0.0.0/8"}
        ]
        return found

    def parent(self, url, *, rir):
        raise RdapClientError("parent unavailable", code="remote_server", retryable=True)

    monkeypatch.setattr(enrichment.RdapClient, "lookup_ip", direct)
    monkeypatch.setattr(enrichment.RdapClient, "lookup_up_url", parent)
    result = run(env, select(env, ["8.8.8.8"]))
    assert result.success
    metadata = outcome(result)
    assert metadata["parent_lookup_failures"] == 1 and metadata["rdap_requests"] == 2
    assert env.client.execute(
        "SELECT rdap_lookup_status, rdap_matched_cidr FROM corpscout.ip_enrichment_current"
    ) == [("found", "8.8.8.0/24")]


def test_resume_rejects_a_changed_lookup_policy_or_a_foreign_execution(environment):
    env = environment
    task = select(env, ["1.1.1.1", "8.8.8.8"])
    first = run(env, task, max_requests=1)
    assert not first.success
    assert not run(env, task, force_rdap=True).success  # frozen profile
    assert not run(env, task, rdap_cache_days=5).success
    assert not run(env, task, execution_id=str(uuid4())).success  # only the saved execution resumes
    assert env.calls == ["1.1.1.1"]
    assert run(env, task, execution_id=first.run_id, max_requests=5).success


def test_page_work_is_bounded_per_page_not_per_address(environment, monkeypatch):
    env = environment
    seed_network(env, "8.8.8.1", fetched_at=datetime.now(UTC))
    ips = [f"8.8.8.{n}" for n in range(1, 41)]
    queries = []
    execute = Client.execute

    def counting(self, query, *args, **kwargs):
        queries.append(query)
        return execute(self, query, *args, **kwargs)

    monkeypatch.setattr(Client, "execute", counting)

    def per_kind():
        return {
            "negative": sum(q.lstrip().startswith("SELECT ip, lookup_status") for q in queries),
            "trie": sum("dictGetOrDefault" in q for q in queries),
            "networks": sum(q.lstrip().startswith("SELECT network_key, rir") for q in queries),
            "markers": sum(q.lstrip().startswith("INSERT INTO corpscout.rdap_ip_lookup_results") for q in queries),
            "results": sum(q.lstrip().startswith("INSERT INTO corpscout.ip_enrichment_results") for q in queries),
        }

    assert run(env, select(env, ips), batch_size=10).success
    assert env.calls == []
    four_pages = per_kind()
    assert four_pages["negative"] <= 4 and four_pages["trie"] <= 4 and four_pages["networks"] <= 1
    assert four_pages["markers"] == 0 and four_pages["results"] == 1  # 40 rows < 500: one flush
    assert not any("raw_response" in q for q in queries)
    queries.clear()
    assert run(env, select(env, ips), batch_size=40).success
    one_page = per_kind()
    assert one_page["negative"] <= 1 and one_page["trie"] <= 1 and one_page["results"] == 1
    assert env.client.execute(
        "SELECT count() FROM corpscout.ip_enrichment_results FINAL"
    ) == [(80,)]


def test_lost_cleanup_ack_does_not_repeat_lookups(environment, monkeypatch):
    env = environment
    task = select(env, ["8.8.8.8"])
    execute = Client.execute
    interrupted = False

    def lost_ack(self, query, *args, **kwargs):
        nonlocal interrupted
        value = execute(self, query, *args, **kwargs)
        if query.startswith("ALTER TABLE corpscout.ip_enrichment_input DROP PARTITION") and not interrupted:
            interrupted = True
            raise ConnectionError("lost cleanup acknowledgement")
        return value

    monkeypatch.setattr(Client, "execute", lost_ack)
    assert not run(env, task).success
    record = task_row(env, task)
    assert record["status"] == "completed" and record["inputs_purged_at"] is None
    before = list(env.calls)
    assert run(env, task).success
    assert env.calls == before and task_row(env, task)["inputs_purged_at"] is not None


def test_new_submissions_after_start_form_the_next_draft(environment):
    env = environment
    scope = "scope-" + uuid4().hex
    first = select(env, ["8.8.8.8"], scope=scope)
    assert run(env, first).success
    second = select(env, ["1.1.1.1"], scope=scope)
    assert second != first and task_row(env, second)["status"] == "draft"
```

Also delete `from dagster_v3.defs.ip_enrichment.input import ip_enrichment_input` from the imports (the workflow test was its only user) and add `from clickhouse_driver import Client` if Task 4 did not.

- [ ] **Step 2: Run to verify they fail**

Run: `uv run --frozen --no-sync pytest tests/test_ip_enrichment_results.py -q -p no:cacheprovider`
Expected: the new asset tests FAIL (`ValueError: materialize ip_enrichment_input first with this task_id` from the old asset, `KeyError: 'completion_status'`); the Task 1/4 tests pass.

- [ ] **Step 3: Rewrite `results.py`**

Replace the whole of `services/dagster_v3/src/dagster_v3/defs/ip_enrichment/results.py` with:

```python
"""Process a frozen IP enrichment draft: pages of remaining entries until nothing remains.

Remaining work is a live ClickHouse query per 256-way bucket (entries without a result
of this execution; each query anti-joins one primary-key range of the results table),
every page costs a fixed number of ClickHouse round trips whatever its size, outcomes
are stored in acknowledged micro-batches, and completion counts come from the results
table. Nothing about a page is persisted, so a resume is the same loop again.
"""

import json
from contextlib import closing
from datetime import UTC, datetime
from uuid import UUID, uuid5

import dagster as dg
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.clickhouse.resolved import assert_clickhouse_tables_exist
from dagster_v3.defs.common import queue_execution
from dagster_v3.defs.common.processing import ProcessingResource
from dagster_v3.defs.common.result_buffer import ResultBuffer
from dagster_v3.defs.commoncrawl_geoip.resources import MaxMindDatabaseResource
from dagster_v3.defs.commoncrawl_rdap.assets import (
    BEST_KNOWN_SEMANTICS_WARNING,
    RDAP_USER_AGENT,
)
from dagster_v3.defs.ip_enrichment.enrichment import (
    IpEnrichmentResultsConfig,
    RdapClient,
    RdapEnricher,
    geoip_result,
    maxminddb,
)
from dagster_v3.defs.ip_enrichment.input import (
    ERROR_STATUSES,
    INPUT_RELATION,
    PROCESSOR_VERSION,
    RESULT_RELATION,
    bucket_prefix,
)

LOOKUP_STATUSES = ("city_lookup_status", "asn_lookup_status", "rdap_lookup_status")
# Page size, request budget and pacing are transport: they may change between resumes.
TRANSPORT_SETTINGS = ("batch_size", "max_requests", "request_delay_seconds")
NOT_FROZEN = {"task_id", "execution_id", *TRANSPORT_SETTINGS}
FAILED_SQL = " OR ".join(f"{column} IN %(errors)s" for column in LOOKUP_STATUSES)
RESULT_BATCH = 500


def execution_parameters(task: dict) -> dict:
    execution = task["config"]["execution"]
    return {
        "task": str(task["task_id"]),
        "exec": execution["execution_id"],
        "errors": ERROR_STATUSES,
    }


def start_ip_execution(
    store, client, config: IpEnrichmentResultsConfig, run_id: str, *, default_execution_id=None
) -> dict:
    """Freeze the draft into an execution, or return the saved one to resume."""

    def snapshot() -> tuple[dict, int]:
        task = store.task(config.task_id)
        [(total,)] = client.execute(
            f"SELECT count() FROM {INPUT_RELATION} WHERE task_id=%(task)s",
            {"task": config.task_id},
        )
        if total == 0 or total != task["total"]:
            raise ValueError("Queue is empty or its membership changed")
        return {"relation": INPUT_RELATION, "total": total}, total

    return queue_execution.start_execution(
        store,
        task_id=config.task_id,
        processor=PROCESSOR_VERSION,
        profile={
            **config.model_dump(exclude=NOT_FROZEN),
            "processor_version": PROCESSOR_VERSION,
        },
        execution_id=config.execution_id,
        freshness_days=config.rdap_cache_days,
        run_id=run_id,
        snapshot=snapshot,
        transport_keys=TRANSPORT_SETTINGS,
        default_execution_id=default_execution_id,
        label="IP enrichment",
    )


def task_buckets(client, task: dict) -> list[int]:
    return [
        bucket
        for (bucket,) in client.execute(
            f"SELECT DISTINCT bucket FROM {INPUT_RELATION} WHERE task_id=%(task)s ORDER BY bucket",
            {"task": str(task["task_id"])},
        )
    ]


def _bucket_parameters(task: dict, bucket: int) -> dict:
    return {
        **execution_parameters(task),
        "bucket": bucket,
        "after": bucket_prefix(bucket),
        "stop": bucket_prefix(bucket + 1),
    }


def remaining_entries(
    client, task: dict, *, bucket: int, after: str | None = None, limit: int
) -> list[dict]:
    """Frozen entries of one bucket after ``after`` without a result of this execution."""
    parameters = {**_bucket_parameters(task, bucket), "limit": limit}
    if after is not None:
        parameters["after"] = after
    rows = client.execute(
        f"""SELECT input_id, ip, ip_version, bucket FROM {INPUT_RELATION}
        WHERE task_id = %(task)s AND input_id > %(after)s AND input_id < %(stop)s
          AND input_id NOT IN (
              SELECT input_id FROM {RESULT_RELATION}
              WHERE bucket = %(bucket)s AND task_id = %(task)s AND execution_id = %(exec)s)
        ORDER BY input_id LIMIT %(limit)s""",
        parameters,
    )
    return [
        dict(zip(("input_id", "ip", "ip_version", "bucket"), row, strict=True))
        for row in rows
    ]


def count_outcomes(client, task: dict, buckets: list[int]) -> tuple[int, int, int]:
    """(remaining, succeeded, failed) over the task; every query is one primary-key range."""
    remaining = succeeded = failed = 0
    for bucket in buckets:
        parameters = _bucket_parameters(task, bucket)
        [(left,)] = client.execute(
            f"""SELECT count() FROM {INPUT_RELATION}
            WHERE task_id = %(task)s AND input_id > %(after)s AND input_id < %(stop)s
              AND input_id NOT IN (
                  SELECT input_id FROM {RESULT_RELATION}
                  WHERE bucket = %(bucket)s AND task_id = %(task)s AND execution_id = %(exec)s)""",
            parameters,
        )
        [(ok, bad)] = client.execute(
            f"""SELECT countIf(NOT failed), countIf(failed) FROM (
                SELECT input_id, argMax({FAILED_SQL}, tuple(completed_at, result_id)) AS failed
                FROM {RESULT_RELATION}
                WHERE bucket = %(bucket)s AND task_id = %(task)s AND execution_id = %(exec)s
                  AND input_id IN (
                      SELECT input_id FROM {INPUT_RELATION}
                      WHERE task_id = %(task)s AND input_id > %(after)s AND input_id < %(stop)s)
                GROUP BY input_id)""",
            parameters,
        )
        remaining += left
        succeeded += ok
        failed += bad
    return remaining, succeeded, failed


def store_results(client, records: list[dict]) -> None:
    columns = list(records[0])
    client.execute(
        f"INSERT INTO {RESULT_RELATION} ({','.join(columns)}) VALUES",
        [tuple(record[column] for column in columns) for record in records],
        settings={"async_insert": 1, "wait_for_async_insert": 1},
    )


def run_ip_enrichment(
    context, client, task: dict, config: IpEnrichmentResultsConfig, *, enricher, city_reader, asn_reader
) -> dict:
    """Walk the task bucket by bucket until a whole pass finds nothing remaining.

    A page's outcomes enter the buffer together and the cursor moves past the page, so
    a page is never read twice inside a pass; the buffer is flushed at the end of every
    pass and a further pass confirms that nothing remains. A reached RDAP budget flushes
    what was resolved and stops; the rest stays remaining for the resume.
    """
    execution = task["config"]["execution"]
    execution_uuid = UUID(execution["execution_id"])
    counts = {"written": 0, "pages": 0, "request_limit_reached": False}

    def flush(records: list[dict]) -> None:
        store_results(client, records)
        counts["written"] += len(records)

    buffer: ResultBuffer[dict] = ResultBuffer(flush, max_items=RESULT_BATCH, max_seconds=5.0)
    buckets = task_buckets(client, task)
    while True:
        processed = 0
        for bucket in buckets:
            after = None
            while rows := remaining_entries(client, task, bucket=bucket, after=after, limit=config.batch_size):
                after = rows[-1]["input_id"]
                rdap = enricher.resolve_page(rows)
                records = []
                for row in rows:
                    if row["ip"] not in rdap:
                        continue  # the request budget ran out before this address
                    checked_at = datetime.now(UTC)
                    records.append(
                        {
                            "ip": row["ip"],
                            "result_id": str(uuid5(execution_uuid, row["input_id"])),
                            "task_id": str(task["task_id"]),
                            "execution_id": execution["execution_id"],
                            "input_id": row["input_id"],
                            "source_run_id": context.run.run_id,
                            "processor_version": PROCESSOR_VERSION,
                            "attempt": 1,
                            "completed_at": checked_at,
                            **geoip_result(
                                row["ip"],
                                city_reader,
                                asn_reader,
                                checked_at=checked_at,
                                retry_seconds=config.transient_retry_seconds,
                            ),
                            **rdap[row["ip"]],
                        }
                    )
                buffer.add(records)
                processed += len(records)
                counts["pages"] += 1
                context.log.info(
                    "IP enrichment execution=%s bucket=%s page=%s resolved=%s rdap_requests=%s cache_hits=%s",
                    execution["execution_id"],
                    bucket,
                    counts["pages"],
                    counts["written"] + len(buffer),
                    enricher.requests,
                    enricher.cache_hits,
                )
                if enricher.budget_reached:
                    buffer.flush()
                    counts["request_limit_reached"] = True
                    return counts
        buffer.flush()  # an unacknowledged batch fails the run; the resume re-reads its rows
        if processed == 0:
            return counts


def finish_ip_execution(store, client, task: dict) -> dict:
    """Counts come from this execution's results; skipped is the rest of the total (expected 0)."""
    remaining, succeeded, failed = count_outcomes(client, task, task_buckets(client, task))
    return queue_execution.record_completion(
        store, task_id=str(task["task_id"]), remaining=remaining, succeeded=succeeded, failed=failed
    )


@dg.asset(
    deps=["ip_enrichment_input"],
    group_name="ip_enrichment",
    kinds={"python", "clickhouse", "maxmind", "rdap"},
    # Share the RDAP pool with the legacy crawler so both respect its concurrency limit.
    pool="commoncrawl_rdap",
    metadata={"dagster/table_name": RESULT_RELATION},
    description="Freeze an IP enrichment draft, then enrich the remaining addresses page by page "
    "with GeoIP, ASN and RDAP; outcomes are stored in acknowledged batches. Re-run the task to "
    "resume its saved execution. Completed tasks drop their input partition; retry failed "
    "addresses in a new draft (retry_failed_task_id).",
)
def ip_enrichment_results(
    context: dg.AssetExecutionContext,
    config: IpEnrichmentResultsConfig,
    clickhouse: ClickhouseResource,
    processing: ProcessingResource,
    maxmind_geoip: MaxMindDatabaseResource,
) -> dg.MaterializeResult:
    def complete(store, task) -> dict:
        return queue_execution.complete_task(
            context,
            store,
            clickhouse,
            task,
            processor=PROCESSOR_VERSION,
            relation=INPUT_RELATION,
            tag_prefix="ip_enrichment",
            label="IP enrichment",
        )

    with (
        processing.get_store() as store,
        store.selection_lock(config.task_id),
        clickhouse.get_connection() as client,
    ):
        task = store.task(config.task_id)
        if (
            task is None
            or task["processor"] != PROCESSOR_VERSION
            or task["queue_scope"] is None
        ):
            raise ValueError(
                "task_id must name an IP enrichment draft; add addresses with ip_enrichment_input first"
            )
        task = start_ip_execution(
            store,
            client,
            config,
            context.run.run_id,
            default_execution_id=context.run.root_run_id or context.run.run_id,
        )
        execution = task["config"]["execution"]
        context.instance.add_run_tags(
            context.run.run_id,
            {
                "processing/task_id": config.task_id,
                "ip_enrichment/execution_id": execution["execution_id"],
                "ip_enrichment/execution": json.dumps(execution, sort_keys=True),
            },
        )
        if task["status"] == "completed":
            return dg.MaterializeResult(
                metadata={
                    "task_id": config.task_id,
                    "execution_id": execution["execution_id"],
                    "already_completed": True,
                    **complete(store, task),
                }
            )
        assert_clickhouse_tables_exist(
            clickhouse,
            database="corpscout",
            tables=(
                "ip_enrichment_results",
                "ip_enrichment_current",
                "rdap_networks",
                "rdap_networks_current",
                "rdap_network_segments",
                "rdap_network_segments_current",
                "rdap_ip_lookup_results_current",
            ),
        )
        city_path, asn_path = maxmind_geoip.database_paths()
        with (
            maxminddb.open_database(city_path) as city_reader,
            maxminddb.open_database(asn_path) as asn_reader,
            closing(RdapClient(user_agent=RDAP_USER_AGENT)) as rdap_client,
        ):
            for kind, reader in (("City", city_reader), ("ASN", asn_reader)):
                if kind not in reader.metadata().database_type:
                    raise ValueError(f"expected a MaxMind {kind} database")
            client.execute("SYSTEM RELOAD DICTIONARY corpscout.rdap_network_trie")
            enricher = RdapEnricher(
                client,
                rdap_client,
                config,
                context.log,
                started_at=datetime.fromisoformat(execution["started_at"]),
                cache_cutoff=datetime.fromisoformat(execution["freshness_cutoff"]),
            )
            counts = run_ip_enrichment(
                context, client, task, config,
                enricher=enricher, city_reader=city_reader, asn_reader=asn_reader,
            )
            if enricher.networks_written:
                client.execute("SYSTEM RELOAD DICTIONARY corpscout.rdap_network_trie")
        metadata = {
            "task_id": config.task_id,
            "execution_id": execution["execution_id"],
            "results_table": RESULT_RELATION,
            "coverage_semantics": BEST_KNOWN_SEMANTICS_WARNING,
            "rdap_requests": enricher.requests,
            "rdap_cache_hits": enricher.cache_hits,
            "parent_lookup_failures": enricher.parent_failures,
            "registry_level_responses": enricher.registry_level_responses,
            **counts,
        }
        if counts["request_limit_reached"]:
            raise dg.Failure(
                "RDAP request budget reached; results saved. Re-run this task to resume the saved execution.",
                metadata=metadata,
                allow_retries=False,
            )
        task = finish_ip_execution(store, client, task)
        return dg.MaterializeResult(metadata={**metadata, **complete(store, task)})


ip_enrichment_results_job = dg.define_asset_job(
    "ip_enrichment_results_job",
    selection=dg.AssetSelection.assets(ip_enrichment_results),
)
defs = dg.Definitions(assets=[ip_enrichment_results], jobs=[ip_enrichment_results_job])
```

Delete the `RequestBudgetReached` class from `enrichment.py`.

- [ ] **Step 4: Run the suites and the definitions check**

Run: `uv run --frozen --no-sync pytest tests/test_ip_enrichment_results.py tests/test_ip_enrichment_input.py tests/test_queue_execution_common.py -q -p no:cacheprovider`
Expected: all pass.

Run: `uv run --frozen --no-sync dg check defs`
Expected: `All definitions loaded successfully.` (no `ip_enrichment_workflow`).

Run: `rg -n "ip_enrichment_workflow|RequestBudgetReached|prepare_execution|page_outcomes|insert_result|EXECUTION_TAG|ClickHouseInputQueue" services/dagster_v3/src/dagster_v3/defs/ip_enrichment services/dagster_v3/tests/test_ip_enrichment_results.py services/dagster_v3/tests/test_ip_enrichment_input.py`
Expected: no matches.

Run ruff format/check on `src/dagster_v3/defs/ip_enrichment/results.py src/dagster_v3/defs/ip_enrichment/enrichment.py tests/test_ip_enrichment_results.py`.

- [ ] **Step 5: Commit**

```bash
git add services/dagster_v3/src/dagster_v3/defs/ip_enrichment/results.py services/dagster_v3/src/dagster_v3/defs/ip_enrichment/enrichment.py services/dagster_v3/tests/test_ip_enrichment_results.py
git commit -m "feat(dagster): IP enrichment runs the shared queue lifecycle over live remaining work and finishes from results

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

---

### Task 6: GeoLite2 update asset, weekly schedule, staleness check, `.env.example`

Evidence: every result row on prod carries City build `2026-07-10 06:34` and ASN build `2026-07-10 08:15`; there is no update mechanism anywhere (`rg MAXMIND` finds only `MaxMindDatabaseResource` and `.env.example:70`, whose comment describes versioned `GeoLite2-City_*` folders while `resources.py:14-15` wants `GeoLite2-City.mmdb` directly in the directory). No MaxMind credential variable exists in `.env.example`, `ansible/group_vars/dagster_hosts/vars.yml` or the deploy roles, so `MAXMIND_ACCOUNT_ID` / `MAXMIND_LICENSE_KEY` are new. MaxMind's permalinks: `https://download.maxmind.com/geoip/databases/<edition>/download?suffix=tar.gz` (HTTP Basic auth with account id + license key) and `?suffix=tar.gz.sha256` (text `"<sha256>  <file>"`); the archive holds `<edition>_YYYYMMDD/<edition>.mmdb`. Credentials are read from the process environment in the asset (like `CRAWLER_API_TOKEN` in `website_crawl/queue_execution.py:445`), never from `dg.EnvVar` on the shared resource, so a missing key cannot break `ip_enrichment_results`.

**Files:**
- Create: `services/dagster_v3/src/dagster_v3/defs/commoncrawl_geoip/update.py`
- Create: `services/dagster_v3/tests/test_geolite2_update.py`
- Modify: `services/dagster_v3/.env.example:70-71`

**Interfaces:**
- Produces (module `dagster_v3.defs.commoncrawl_geoip.update`): `EDITIONS = ("GeoLite2-City", "GeoLite2-ASN")`, `DOWNLOAD_URL`, `MAX_AGE = timedelta(days=14)`, `maxmind_credentials() -> tuple[str, str]`, `download_edition(http, edition, auth) -> tuple[bytes, str]`, `install_edition(directory: Path, edition: str, archive: bytes, expected_sha256: str, *, opener=maxminddb.open_database) -> datetime`, `database_build_times(resource, *, opener=maxminddb.open_database) -> dict[str, datetime]`, `freshness(times: dict[str, datetime], now: datetime) -> dg.AssetCheckResult`, asset `geolite2_databases`, check `geolite2_databases_fresh`, job `geolite2_update_job`, schedule `geolite2_update_weekly` (`15 3 * * 3` UTC, STOPPED by default).

- [ ] **Step 1: Write the failing tests**

Create `services/dagster_v3/tests/test_geolite2_update.py`:

```python
"""GeoLite2 install/verify/check logic with fake archives and readers; no MaxMind traffic."""

import hashlib
import io
import tarfile
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

from dagster_v3.defs.commoncrawl_geoip import update
from dagster_v3.defs.commoncrawl_geoip.resources import MaxMindDatabaseResource


def archive_for(edition: str, payload: bytes, *, member: str | None = None) -> bytes:
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w:gz") as tar:
        info = tarfile.TarInfo(member or f"{edition}_20260922/{edition}.mmdb")
        info.size = len(payload)
        tar.addfile(info, io.BytesIO(payload))
    return buffer.getvalue()


class Reader:
    def __init__(self, path, database_type, build_epoch):
        self.path, self.database_type, self.build_epoch = path, database_type, build_epoch

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def metadata(self):
        return SimpleNamespace(database_type=self.database_type, build_epoch=self.build_epoch)


def opener(database_type="GeoLite2-City", build_epoch=1_790_000_000):
    return lambda path: Reader(path, database_type, build_epoch)


def test_install_verifies_then_replaces_atomically(tmp_path: Path):
    target = tmp_path / "GeoLite2-City.mmdb"
    target.write_bytes(b"old")
    archive = archive_for("GeoLite2-City", b"new")
    built = update.install_edition(
        tmp_path, "GeoLite2-City", archive, hashlib.sha256(archive).hexdigest(), opener=opener()
    )
    assert built == datetime.fromtimestamp(1_790_000_000, UTC)
    assert target.read_bytes() == b"new"
    assert sorted(p.name for p in tmp_path.iterdir()) == ["GeoLite2-City.mmdb"]


@pytest.mark.parametrize(
    ("archive", "digest_ok", "reader_type", "message"),
    [
        (archive_for("GeoLite2-City", b"new"), False, "GeoLite2-City", "SHA-256"),
        (archive_for("GeoLite2-City", b"new"), True, "GeoLite2-ASN", "holds a GeoLite2-ASN"),
        (archive_for("GeoLite2-City", b"new", member="GeoLite2-City_20260922/README.txt"), True, "GeoLite2-City", "no GeoLite2-City.mmdb"),
    ],
)
def test_install_refuses_bad_archives_and_leaves_the_old_file(tmp_path, archive, digest_ok, reader_type, message):
    target = tmp_path / "GeoLite2-City.mmdb"
    target.write_bytes(b"old")
    digest = hashlib.sha256(archive).hexdigest() if digest_ok else "0" * 64
    with pytest.raises(ValueError, match=message):
        update.install_edition(tmp_path, "GeoLite2-City", archive, digest, opener=opener(reader_type))
    assert target.read_bytes() == b"old"
    assert sorted(p.name for p in tmp_path.iterdir()) == ["GeoLite2-City.mmdb"]


def test_download_uses_basic_auth_and_the_published_digest():
    calls = []

    class Http:
        def get(self, url, *, auth, timeout):
            calls.append((url, auth))
            body = b"archive" if url.endswith("suffix=tar.gz") else b"abc123  GeoLite2-ASN_20260922.tar.gz\n"
            return SimpleNamespace(content=body, text=body.decode(), raise_for_status=lambda: None)

    archive, digest = update.download_edition(Http(), "GeoLite2-ASN", ("123", "key"))
    assert (archive, digest) == (b"archive", "abc123")
    assert calls == [
        ("https://download.maxmind.com/geoip/databases/GeoLite2-ASN/download?suffix=tar.gz", ("123", "key")),
        ("https://download.maxmind.com/geoip/databases/GeoLite2-ASN/download?suffix=tar.gz.sha256", ("123", "key")),
    ]


def test_missing_credentials_fail_before_any_download(monkeypatch):
    monkeypatch.delenv("MAXMIND_ACCOUNT_ID", raising=False)
    monkeypatch.setenv("MAXMIND_LICENSE_KEY", "key")
    with pytest.raises(ValueError, match="MAXMIND_ACCOUNT_ID"):
        update.maxmind_credentials()
    monkeypatch.setenv("MAXMIND_ACCOUNT_ID", " 123 ")
    assert update.maxmind_credentials() == ("123", "key")


def test_freshness_check_fails_after_fourteen_days(tmp_path):
    for edition in update.EDITIONS:
        (tmp_path / f"{edition}.mmdb").touch()
    resource = MaxMindDatabaseResource(database_directory=str(tmp_path))
    now = datetime(2026, 9, 25, tzinfo=UTC)
    fresh = int((now - timedelta(days=3)).timestamp())
    stale = int((now - timedelta(days=20)).timestamp())
    times = update.database_build_times(resource, opener=opener(build_epoch=fresh))
    assert set(times) == set(update.EDITIONS)
    assert update.freshness(times, now).passed
    result = update.freshness({"GeoLite2-City": times["GeoLite2-City"], "GeoLite2-ASN": datetime.fromtimestamp(stale, UTC)}, now)
    assert not result.passed and "GeoLite2-ASN built 2026-09-05" in result.description
    assert result.metadata["max_age_days"].value == 14
```

- [ ] **Step 2: Run to verify they fail**

Run: `uv run --frozen --no-sync pytest tests/test_geolite2_update.py -q -p no:cacheprovider`
Expected: FAIL — `ImportError: cannot import name 'update' from 'dagster_v3.defs.commoncrawl_geoip'`.

- [ ] **Step 3: Create the update module**

Create `services/dagster_v3/src/dagster_v3/defs/commoncrawl_geoip/update.py`:

```python
"""Weekly GeoLite2 refresh: download, verify and atomically replace the .mmdb files.

MaxMind publishes GeoLite2 twice a week. The asset fetches each edition's tar.gz with
the account credentials, checks the published SHA-256 and the database type, and
replaces <MAXMIND_DATABASE_DIRECTORY>/<edition>.mmdb with os.replace, so a running
enrichment keeps reading the file it opened. The check fails when a database is
older than 14 days.
"""

import hashlib
import io
import os
import tarfile
from datetime import UTC, datetime, timedelta
from pathlib import Path

import dagster as dg
import maxminddb
from dlt.sources.helpers.requests import Session

from dagster_v3.defs.commoncrawl_geoip.resources import MaxMindDatabaseResource

EDITIONS = ("GeoLite2-City", "GeoLite2-ASN")
DOWNLOAD_URL = "https://download.maxmind.com/geoip/databases/{edition}/download?suffix={suffix}"
MAX_AGE = timedelta(days=14)


def maxmind_credentials() -> tuple[str, str]:
    account_id = os.environ.get("MAXMIND_ACCOUNT_ID", "").strip()
    license_key = os.environ.get("MAXMIND_LICENSE_KEY", "").strip()
    if not account_id or not license_key:
        raise ValueError(
            "Configure MAXMIND_ACCOUNT_ID and MAXMIND_LICENSE_KEY on the Dagster host"
        )
    return account_id, license_key


def download_edition(http, edition: str, auth: tuple[str, str]) -> tuple[bytes, str]:
    """The edition's tar.gz and the SHA-256 MaxMind publishes next to it."""
    archive = http.get(
        DOWNLOAD_URL.format(edition=edition, suffix="tar.gz"), auth=auth, timeout=(10, 300)
    )
    archive.raise_for_status()
    digest = http.get(
        DOWNLOAD_URL.format(edition=edition, suffix="tar.gz.sha256"), auth=auth, timeout=(10, 60)
    )
    digest.raise_for_status()
    return archive.content, digest.text.split()[0].lower()


def install_edition(
    directory: Path,
    edition: str,
    archive: bytes,
    expected_sha256: str,
    *,
    opener=maxminddb.open_database,
) -> datetime:
    """Verify, extract and atomically replace <directory>/<edition>.mmdb; return its build time."""
    if hashlib.sha256(archive).hexdigest() != expected_sha256.lower():
        raise ValueError(f"{edition} download does not match its published SHA-256")
    with tarfile.open(fileobj=io.BytesIO(archive), mode="r:gz") as tar:
        member = next(
            (
                item
                for item in tar.getmembers()
                if item.isfile() and item.name.rsplit("/", 1)[-1] == f"{edition}.mmdb"
            ),
            None,
        )
        if member is None:
            raise ValueError(f"{edition} archive holds no {edition}.mmdb")
        payload = tar.extractfile(member).read()
    target = directory / f"{edition}.mmdb"
    staged = directory / f".{edition}.{os.getpid()}.mmdb.tmp"
    staged.write_bytes(payload)
    try:
        with opener(staged) as reader:
            metadata = reader.metadata()
            if edition.split("-", 1)[1] not in metadata.database_type:
                raise ValueError(f"{edition} archive holds a {metadata.database_type} database")
            built = datetime.fromtimestamp(metadata.build_epoch, UTC)
        os.replace(staged, target)  # readers that already opened the old file keep their inode
    except BaseException:
        staged.unlink(missing_ok=True)
        raise
    return built


def database_build_times(
    resource: MaxMindDatabaseResource, *, opener=maxminddb.open_database
) -> dict[str, datetime]:
    times = {}
    for edition, path in zip(EDITIONS, resource.database_paths(), strict=True):
        with opener(path) as reader:
            times[edition] = datetime.fromtimestamp(reader.metadata().build_epoch, UTC)
    return times


def freshness(times: dict[str, datetime], now: datetime) -> dg.AssetCheckResult:
    stale = {edition: built for edition, built in times.items() if now - built > MAX_AGE}
    return dg.AssetCheckResult(
        passed=not stale,
        severity=dg.AssetCheckSeverity.ERROR,
        description=(
            "GeoLite2 databases are current."
            if not stale
            else "Stale GeoLite2 databases: "
            + ", ".join(f"{edition} built {built.date().isoformat()}" for edition, built in stale.items())
        ),
        metadata={
            **{f"{edition}_build": built.isoformat() for edition, built in times.items()},
            "max_age_days": MAX_AGE.days,
        },
    )


@dg.asset(
    group_name="ip_enrichment",
    kinds={"python", "maxmind"},
    pool="geolite2_databases",
    description="Download GeoLite2-City and GeoLite2-ASN with the MaxMind account credentials, "
    "verify the published SHA-256 and the database type, and atomically replace the .mmdb "
    "files in MAXMIND_DATABASE_DIRECTORY.",
)
def geolite2_databases(
    context: dg.AssetExecutionContext, maxmind_geoip: MaxMindDatabaseResource
) -> dg.MaterializeResult:
    auth = maxmind_credentials()
    directory = Path(maxmind_geoip.database_directory).expanduser()
    if not directory.is_dir():
        raise ValueError(f"MAXMIND_DATABASE_DIRECTORY is not a directory: {directory}")
    built = {}
    with Session(raise_for_status=False) as http:
        for edition in EDITIONS:
            archive, digest = download_edition(http, edition, auth)
            built[edition] = install_edition(directory, edition, archive, digest)
            context.log.info("%s installed, built %s", edition, built[edition].isoformat())
    return dg.MaterializeResult(
        metadata={
            "directory": str(directory),
            **{f"{edition}_build": built[edition].isoformat() for edition in EDITIONS},
        }
    )


@dg.asset_check(
    asset=geolite2_databases,
    name="geolite2_databases_fresh",
    description="Fails when either loaded GeoLite2 database was built more than 14 days ago "
    "(MaxMind publishes twice a week).",
)
def geolite2_databases_fresh(maxmind_geoip: MaxMindDatabaseResource) -> dg.AssetCheckResult:
    return freshness(database_build_times(maxmind_geoip), datetime.now(UTC))


geolite2_update_job = dg.define_asset_job(
    "geolite2_update_job", selection=dg.AssetSelection.assets(geolite2_databases)
)
# Wednesday 03:15 UTC, after MaxMind's Tuesday release; no other schedule uses 03:15.
# STOPPED by default per house pattern for new schedules; start it at instance level.
geolite2_update_weekly = dg.ScheduleDefinition(
    name="geolite2_update_weekly",
    job=geolite2_update_job,
    cron_schedule="15 3 * * 3",
    execution_timezone="UTC",
    default_status=dg.DefaultScheduleStatus.STOPPED,
)

defs = dg.Definitions(
    assets=[geolite2_databases],
    asset_checks=[geolite2_databases_fresh],
    jobs=[geolite2_update_job],
    schedules=[geolite2_update_weekly],
)
```

- [ ] **Step 4: Fix `.env.example`**

Replace lines 70-71 of `services/dagster_v3/.env.example` with:

```
# Directory holding GeoLite2-City.mmdb and GeoLite2-ASN.mmdb directly (no versioned
# subfolders). geolite2_databases (weekly, geolite2_update_weekly) replaces them in place.
MAXMIND_DATABASE_DIRECTORY=/path/to/geoip
# MaxMind account for the GeoLite2 downloads (Account > Manage License Keys).
MAXMIND_ACCOUNT_ID=
MAXMIND_LICENSE_KEY=
```

- [ ] **Step 5: Run the tests and the definitions check**

Run: `uv run --frozen --no-sync pytest tests/test_geolite2_update.py tests/test_commoncrawl_geoip_assets.py -q -p no:cacheprovider`
Expected: all pass.

Run: `uv run --frozen --no-sync dg check defs`
Expected: `All definitions loaded successfully.` (`geolite2_update_job`, `geolite2_update_weekly` and the check load; the `maxmind_geoip` resource comes from `commoncrawl_geoip/definitions.py`).

Run ruff format/check on `src/dagster_v3/defs/commoncrawl_geoip/update.py tests/test_geolite2_update.py`.

- [ ] **Step 6: Commit**

```bash
git add services/dagster_v3/src/dagster_v3/defs/commoncrawl_geoip/update.py services/dagster_v3/tests/test_geolite2_update.py services/dagster_v3/.env.example
git commit -m "feat(dagster): weekly GeoLite2 download with verification, atomic install and a 14-day freshness check

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

---

### Task 7: Backoffice — "Add to enrichment queue", draft semantics on the queue page

Determined from the code: `launchIpEnrichment` (`app/lib/ip-enrichment.server.ts:86-140`) is the only launcher of `ip_enrichment_workflow`; it is called from `routes/admin-ip-addresses.tsx:131` and mocked in `tests/admin-ip-addresses-action.test.ts`. The queue page reads `ip_enrichment_input` with `ORDER BY input_id, task_id` (`queues.server.ts:51`, the non-crawler branch), never with `FINAL`, and treats only `webtech`/`crawler` as draft queues (`admin-queue.tsx:23, 60, 103, 119, 122, 126, 129, 131`; `queue-process-sheet.tsx:81, 103, 110`; `queues.server.ts:99` maps history outcome tags for `crawler`/`webtech` only). `QueueImportStatus`/`useQueueSubmission` (`components/admin/queue-import-status.tsx`) poll `/admin/<queue>/queue-submissions/<runId>` and know two queues. `ip-enrichment` keeps `batch_size`, `max_requests` and `request_delay_seconds` as processing parameters (they are transport keys the results asset accepts on resume).

Run all commands from `services/backoffice`. New route file: the long-running dev server on 5183 needs a restart to see it (memory: stale Vite fs cache); verify on a second port if needed.

**Files:**
- Rewrite: `app/lib/ip-enrichment.server.ts`
- Create: `app/routes/admin-ip-enrichment-queue-submission.ts`
- Modify: `app/routes.ts:138`
- Modify: `app/components/admin/queue-import-status.tsx`
- Modify: `app/routes/admin-ip-addresses.tsx:1-14, 113-152, 158-317`
- Modify: `app/lib/queues.ts:15-17`
- Modify: `app/lib/queues.server.ts:51, 93-106`
- Modify: `app/routes/admin-queue.tsx:12, 23, 60, 103, 119, 122, 126, 129, 131, 140, 145`
- Modify: `app/components/admin/queue-process-sheet.tsx:4, 81-85, 103, 110`
- Rewrite: `tests/ip-enrichment.server.test.ts`
- Modify: `tests/admin-ip-addresses-action.test.ts`, `tests/queues.server.test.ts`, `tests/queue-route.test.ts`

**Interfaces:**
- Produces: `addIpsToEnrichmentQueue(value: unknown, submissionId: string, requestedBy: string, options?: DagsterOptions) -> {ok: true, runId, status, runUrl}` (launches `ip_enrichment_input_job`, asset `ip_enrichment_input`, config `{...inputConfig(selection), queue_scope: "workspace", submission_id}`, tags `processing/submission_id`, `ip_enrichment/selection_sha256`, `corpscout/requested_by`, `backoffice/action: "add-ip-enrichment-input"`); `ipEnrichmentQueueSubmission(runId, options?) -> {ok: true, runId, status, finished, taskId}`; `inputConfig(selection: WorkspaceIpSelection)`; `parseIpEnrichmentSelection`, `IpEnrichmentSelectionError` unchanged. `queues.ts`: `DRAFT_QUEUES`, `isDraftQueue(type: QueueType): boolean`. `queue-import-status.tsx`: `ImportQueue = "webtech" | "crawler" | "ip-enrichment"`, `useQueueSubmission(receipt, queue: ImportQueue)`, `QueueImportStatus` accepts `queue?: ImportQueue`.
- Removes: `launchIpEnrichment`.

- [ ] **Step 1: Write the failing tests**

Replace `tests/ip-enrichment.server.test.ts` with:

```ts
import { beforeEach, describe, expect, it, vi } from "vitest";
const dagster = vi.hoisted(() => ({launchRun: vi.fn(), listRuns: vi.fn(), runStatus: vi.fn(), dagsterRunUrl: vi.fn((id: string) => `http://dagster/runs/${id}`)}));
vi.mock("~/lib/dagster.server", () => dagster);
import { addIpsToEnrichmentQueue, ipEnrichmentQueueSubmission } from "~/lib/ip-enrichment.server";

const submission = "11111111-1111-4111-8111-111111111111";
beforeEach(() => { vi.clearAllMocks(); dagster.listRuns.mockResolvedValue([]); dagster.launchRun.mockResolvedValue({runId: "new", status: "QUEUED"}); });

describe("IP enrichment queue submission", () => {
  it("appends selected addresses from multiple pages to the workspace draft via the input asset only", async () => {
    const result = await addIpsToEnrichmentQueue({ mode: "ips", ips: ["8.8.8.8", "2001:4860::8888", "8.8.8.8"] }, submission, "operator");
    expect(dagster.launchRun).toHaveBeenCalledWith(expect.objectContaining({job: "ip_enrichment_input_job", assetSelection: ["ip_enrichment_input"],
      runConfig: {ops: {ip_enrichment_input: {config: {
        source_name: "backoffice:ip-addresses", source_relation: "corpscout.commoncrawl_ip_addresses", observed_at_column: "last_seen",
        filters: { ip: ["8.8.8.8", "2001:4860::8888"] }, queue_scope: "workspace", submission_id: submission,
      }}}},
      tags: expect.objectContaining({"processing/submission_id": submission, "backoffice/action": "add-ip-enrichment-input", "corpscout/requested_by": "operator"}),
    }));
    const config = dagster.launchRun.mock.calls[0][0].runConfig.ops.ip_enrichment_input.config;
    expect(config).not.toHaveProperty("task_id");
    expect(result).toEqual({ok: true, runId: "new", status: "QUEUED", runUrl: "http://dagster/runs/new"});
  });

  it.each(["any", "4", "6"])("submits all matching addresses compactly for version %s", async (version) => {
    await addIpsToEnrichmentQueue({ mode: "all", filters: { search: "2001:db8:", version }, excludedIps: ["2001:db8::1", "2001:db8::1"] }, submission, "operator");
    const config = dagster.launchRun.mock.calls[0][0].runConfig.ops.ip_enrichment_input.config;
    expect(config).toMatchObject({ source_relation: "corpscout.commoncrawl_ip_addresses", observed_at_column: "last_seen", select_all: true, ip_search: "2001:db8:",
      filters: version === "any" ? {} : { ip_version: [version] }, excluded_ips: ["2001:db8::1"] });
    expect(config).not.toHaveProperty("max_rows");
    expect(config).not.toHaveProperty("ips");
  });

  it("supports the entire inventory without a pagination limit", async () => {
    await addIpsToEnrichmentQueue({ mode: "all", filters: { search: "", version: "any" }, excludedIps: [] }, submission, "operator");
    expect(dagster.launchRun.mock.calls[0][0].runConfig.ops.ip_enrichment_input.config).toMatchObject({ select_all: true, ip_search: "", filters: {} });
  });

  it("recovers an acknowledged submission and rejects a changed selection", async () => {
    const selection = { mode: "ips", ips: ["8.8.8.8"] };
    await addIpsToEnrichmentQueue(selection, submission, "operator");
    const tags = dagster.launchRun.mock.calls[0][0].tags;
    dagster.listRuns.mockResolvedValue([{runId: "new", status: "SUCCESS", tags}]);
    expect(await addIpsToEnrichmentQueue(selection, submission, "operator")).toMatchObject({runId: "new"});
    expect(dagster.launchRun).toHaveBeenCalledTimes(1);
    await expect(addIpsToEnrichmentQueue({ mode: "ips", ips: ["1.1.1.1"] }, submission, "operator")).rejects.toThrow("another selection");
    dagster.listRuns.mockResolvedValue([{runId: "failed", status: "FAILURE", tags}]);
    await addIpsToEnrichmentQueue(selection, submission, "operator");
    expect(dagster.launchRun).toHaveBeenCalledTimes(2);  // a failed import is retried with the same receipt
  });

  it.each([
    null,
    { mode: "ips", ips: [] },
    { mode: "ips", ips: ["bad"] },
    { mode: "ips", ips: ["fe80::1%eth0"] },
    { mode: "ips", ips: ["8.8.8.8"], source_relation: "arbitrary" },
    { mode: "all", filters: { search: "' OR 1=1", version: "any" }, excludedIps: [] },
    { mode: "all", filters: { search: "", version: 4 }, excludedIps: [] },
    { mode: "all", filters: { search: "", version: "any", sql: "1=1" }, excludedIps: [] },
    { mode: "all", filters: { search: "", version: "any" }, excludedIps: ["invalid"] },
  ])("rejects invalid selections before contacting Dagster: %j", async (selection) => {
    await expect(addIpsToEnrichmentQueue(selection, submission, "operator")).rejects.toThrow();
    expect(dagster.launchRun).not.toHaveBeenCalled();
  });

  it("rejects a malformed submission id before contacting Dagster", async () => {
    await expect(addIpsToEnrichmentQueue({ mode: "ips", ips: ["8.8.8.8"] }, "not-a-uuid", "operator")).rejects.toThrow("submission ID");
    expect(dagster.listRuns).not.toHaveBeenCalled();
  });

  it("returns the resolved draft only for IP enrichment import runs", async () => {
    dagster.runStatus.mockResolvedValue({jobName: "ip_enrichment_input_job", status: "SUCCESS", tags: {"backoffice/action": "add-ip-enrichment-input", "processing/task_id": submission}});
    expect(await ipEnrichmentQueueSubmission(submission)).toMatchObject({finished: true, taskId: submission});
    dagster.runStatus.mockResolvedValue({jobName: "ip_enrichment_workflow", status: "SUCCESS", tags: {}});
    await expect(ipEnrichmentQueueSubmission(submission)).rejects.toThrow("not found");
  });
});
```

In `tests/admin-ip-addresses-action.test.ts` rename the hoisted mock: line 5 `addIpsToEnrichmentQueue: launch,` and the first test becomes:

```ts
it("uses the submitted selection and submission id independently of page filters", async () => {
  const selection = { mode: "ips", ips: ["8.8.8.8"] };
  const submissionId = "11111111-1111-4111-8111-111111111111";
  launch.mockResolvedValue({ ok: true, runId: "run", status: "QUEUED", runUrl: null });
  expect(await submit({ action: "enrich", selection, submissionId })).toMatchObject({ data: { ok: true, runId: "run" } });
  expect(launch).toHaveBeenCalledWith(selection, submissionId, expect.any(String));
});
```

(the "launch failure" test keeps working: its `submit` body gains no `submissionId`, which the action forwards as `""`).

Append to `tests/queues.server.test.ts`:

```ts
it("reads the IP enrichment entry table in sort-key order without FINAL and maps its outcome tags", async () => {
  vi.mocked(chQuery).mockResolvedValue([]);
  await loadQueueInputs(filters("ip-enrichment"));
  const sql = vi.mocked(chQuery).mock.calls.map(([query]) => query);
  expect(sql.some(query => query.includes("FROM corpscout.ip_enrichment_input") && query.includes("ORDER BY task_id, input_id"))).toBe(true);
  for (const query of sql) expect(query).not.toContain("ip_enrichment_input FINAL");
  const {loadQueueHistory} = await import("~/lib/queues.server");
  vi.mocked(listRuns).mockResolvedValue([{runId: "saved", status: "SUCCESS", startTime: null, tags: {"processing/task_id": task, "ip_enrichment/outcome": "completed_with_errors", "ip_enrichment/failed_pages": "3"}}] as never);
  expect(await loadQueueHistory(filters("ip-enrichment"))).toEqual([expect.objectContaining({outcome: "completed_with_errors", failedPages: 3})]);
});
```

Append to `tests/queue-route.test.ts`:

```ts
it("selects the current IP enrichment draft like Webtech", async () => {
  server.loadQueueInputs.mockResolvedValue({tasks: [{task_id: task}], selectedTotal: 2});
  try { await loader({params: {type: "ip-enrichment"}, request: new Request("http://x/admin/queues/ip-enrichment")} as never); throw new Error("expected redirect"); }
  catch (response) { expect((response as Response).headers.get("Location")).toBe(`/admin/queues/ip-enrichment?task=${task}`); }
});
```

- [ ] **Step 2: Run to verify they fail**

Run: `npx vitest run tests/ip-enrichment.server.test.ts tests/admin-ip-addresses-action.test.ts tests/queues.server.test.ts tests/queue-route.test.ts`
Expected: FAIL — `addIpsToEnrichmentQueue is not a function`, the ORDER BY assertion, the history outcome and the redirect.

- [ ] **Step 3: Rewrite `ip-enrichment.server.ts`**

Keep `IpEnrichmentSelectionError`, `record`, `ipList` and `parseIpEnrichmentSelection` (lines 10-84) and replace the imports and `launchIpEnrichment` with:

```ts
import { createHash } from "node:crypto";
import { isIP } from "node:net";
import { dagsterRunUrl, launchRun, listRuns, runStatus, type DagsterOptions } from "~/lib/dagster.server";
import { QUEUE_UUID } from "~/lib/queues";
import type { WorkspaceIpSelection } from "~/lib/workspace-ip-selection";

const JOB = "ip_enrichment_input_job";
const ASSET = "ip_enrichment_input";
const submissions = new Map<string, Promise<unknown>>();
```

and, after `parseIpEnrichmentSelection`:

```ts
export function inputConfig(selection: WorkspaceIpSelection): Record<string, unknown> {
  const input: Record<string, unknown> = {
    source_name: "backoffice:ip-addresses",
    source_relation: "corpscout.commoncrawl_ip_addresses",
    observed_at_column: "last_seen",
  };
  if (selection.mode === "ips") {
    input.filters = { ip: selection.ips };
  } else {
    // Dagster selects the whole matching inventory in ClickHouse, independently of pagination.
    Object.assign(input, {
      select_all: true,
      ip_search: selection.filters.search,
      filters: selection.filters.version === "any" ? {} : { ip_version: [selection.filters.version] },
      excluded_ips: selection.excludedIps,
    });
  }
  return input;
}

/** Append the selection to the open workspace draft. Dagster chooses/creates the task atomically. */
export async function addIpsToEnrichmentQueue(value: unknown, submissionId: string, requestedBy: string, options: DagsterOptions = {}) {
  const selection = parseIpEnrichmentSelection(value);
  if (!QUEUE_UUID.test(submissionId)) throw new IpEnrichmentSelectionError("Invalid submission ID. Reload the page.");
  const config = { ...inputConfig(selection), queue_scope: "workspace", submission_id: submissionId };
  const fingerprint = createHash("sha256").update(JSON.stringify(config)).digest("hex");
  const previous = submissions.get(submissionId);
  const pending = previous ? previous.then(submit, submit) : submit();
  submissions.set(submissionId, pending);
  try { return await pending; }
  finally { if (submissions.get(submissionId) === pending) submissions.delete(submissionId); }

  async function submit() {
    const [existing] = await listRuns({ job: JOB, limit: 1, tags: { "processing/submission_id": submissionId } }, options);
    if (existing && existing.tags["ip_enrichment/selection_sha256"] !== fingerprint) throw new IpEnrichmentSelectionError("This submission ID belongs to another selection.");
    // Retry failed imports using the same durable receipt, not a new selection.
    if (existing && !["FAILURE", "CANCELED"].includes(existing.status)) {
      return { ok: true as const, runId: existing.runId, status: existing.status, runUrl: dagsterRunUrl(existing.runId, options.url) };
    }
    const run = await launchRun({
      job: JOB, assetSelection: [ASSET], runConfig: { ops: { [ASSET]: { config } } },
      tags: {
        "processing/submission_id": submissionId, "ip_enrichment/selection_sha256": fingerprint,
        "corpscout/requested_by": requestedBy, "backoffice/action": "add-ip-enrichment-input",
      },
    }, options);
    return { ok: true as const, ...run, runUrl: dagsterRunUrl(run.runId, options.url) };
  }
}

export async function ipEnrichmentQueueSubmission(runId: string, options: DagsterOptions = {}) {
  if (!QUEUE_UUID.test(runId)) throw new IpEnrichmentSelectionError("Invalid import run ID.");
  const run = await runStatus(runId, options);
  if (run.jobName !== JOB || run.tags["backoffice/action"] !== "add-ip-enrichment-input") throw new IpEnrichmentSelectionError("IP enrichment queue submission not found.");
  const task = run.tags["processing/task_id"];
  return { ok: true as const, runId, status: run.status, finished: ["SUCCESS", "FAILURE", "CANCELED"].includes(run.status), taskId: task && QUEUE_UUID.test(task) ? task : null };
}
```

Delete `import { randomUUID } from "node:crypto";` (line 1) and the old `launchIpEnrichment`.

- [ ] **Step 4: Add the status route and generalize the import status component**

Create `app/routes/admin-ip-enrichment-queue-submission.ts`:

```ts
import { data } from "react-router";
import type { Route } from "./+types/admin-ip-enrichment-queue-submission";
import { IpEnrichmentSelectionError, ipEnrichmentQueueSubmission } from "~/lib/ip-enrichment.server";
export async function loader({params}: Route.LoaderArgs) {
  try { return data(await ipEnrichmentQueueSubmission(params.runId), {headers: {"Cache-Control": "no-store"}}); }
  catch (error) {
    return data({ok: false as const, error: error instanceof IpEnrichmentSelectionError ? error.message : "Import status is unavailable. Check the Dagster run or retry the status check."}, {status: error instanceof IpEnrichmentSelectionError ? 400 : 503, headers: {"Cache-Control": "no-store"}});
  }
}
```

In `app/routes.ts` after line 138 (`route("webtech/queue-submissions/:runId", …)`) add:

```ts
    route("ip-enrichment/queue-submissions/:runId", "routes/admin-ip-enrichment-queue-submission.ts"),
```

In `app/components/admin/queue-import-status.tsx` replace lines 8 and 19-26 so the component knows three queues:

```ts
export type ImportQueue = "webtech" | "crawler" | "ip-enrichment";
const LABELS: Record<ImportQueue, string> = {webtech: "Webtech", crawler: "Crawler", "ip-enrichment": "IP enrichment"};

export function useQueueSubmission(receipt: QueueImportReceipt | null, queue: ImportQueue = "webtech") {
```

```ts
export function QueueImportStatus({receipt, state, fallbackSearch = "", crawlType, queue}: {
  receipt: QueueImportReceipt;
  state: ReturnType<typeof useQueueSubmission>["state"];
  fallbackSearch?: string;
  crawlType?: string;
  queue?: ImportQueue;
}) {
  const target: ImportQueue = queue ?? (crawlType ? "crawler" : "webtech");
  const label = LABELS[target];
```

and the link becomes ``to={`/admin/queues/${target}${params.size ? `?${params}` : ""}`}``.

- [ ] **Step 5: The IP addresses page adds to the draft**

In `app/routes/admin-ip-addresses.tsx`:

Imports (lines 6-9) become:

```ts
import { addIpsToEnrichmentQueue, IpEnrichmentSelectionError } from "~/lib/ip-enrichment.server";
import { QueueImportStatus, useQueueSubmission } from "~/components/admin/queue-import-status";
```

The `action` (lines 113-152) becomes:

```ts
export async function action({ request }: Route.ActionArgs) {
  let body;
  try {
    body = await request.json();
  } catch {
    return data({ ok: false as const, error: "Invalid enrichment request." }, { status: 400 });
  }
  if (body?.action !== "enrich") {
    return data({ ok: false as const, error: "Choose a supported IP address action." }, { status: 400 });
  }
  try {
    return data(
      await addIpsToEnrichmentQueue(
        body.selection,
        String(body.submissionId ?? ""),
        process.env.BACKOFFICE_OPERATOR?.trim() || "backoffice",
      ),
    );
  } catch (error) {
    if (error instanceof IpEnrichmentSelectionError) {
      return data({ ok: false as const, error: error.message }, { status: 400 });
    }
    return data(
      { ok: false as const, error: "Could not submit the queue import. Check Dagster for a submitted run before retrying." },
      { status: 502 },
    );
  }
}
```

In the component, replace lines 162-190 (from `const navigationBusy` through the `useEffect` that clears the selection) with:

```ts
  type SelectionState = { filterKey: string; selection: WorkspaceIpSelection };
  const navigationBusy = useNavigation().state !== "idle";
  const fetcher = useFetcher<typeof action>();
  const receipt = fetcher.data?.ok ? fetcher.data : null;
  const { state: importState, error: importError } = useQueueSubmission(receipt, "ip-enrichment");
  const submitting = fetcher.state !== "idle" || Boolean(receipt && !importState?.finished);
  const busy = navigationBusy || submitting;
  const filterKey = JSON.stringify(filters);
  const [selectionState, setSelectionState] = useState<SelectionState>({ filterKey, selection: { mode: "ips", ips: [] } });
  const currentState =
    selectionState.filterKey === filterKey
      ? selectionState
      : { filterKey, selection: { mode: "ips" as const, ips: [] } };
  if (currentState !== selectionState) setSelectionState(currentState);
  const selection = currentState.selection;
  const setSelection = (selection: WorkspaceIpSelection) => setSelectionState({ filterKey, selection });
  const request = useRef<{ id: string; state: SelectionState } | null>(null);
  const [retry, setRetry] = useState(false);
  useEffect(() => {
    if (importState?.status === "SUCCESS") {
      // Only a saved import clears the selection; an accepted launch alone does not.
      setSelectionState((current) =>
        current === request.current?.state ? { ...current, selection: { mode: "ips", ips: [] } } : current,
      );
      setRetry(false);
    } else if (importState?.status === "FAILURE" || importState?.status === "CANCELED" || fetcher.data?.ok === false) {
      setRetry(true);
    }
  }, [importState?.status, fetcher.data]);
  const pageSelected = rows.filter((row) => isIpSelected(selection, row.ip)).length;
  const hasSelection = selection.mode === "all" || selection.ips.length > 0;
  const submitEnrichment = (again = false) => {
    if (busy || (!again && !hasSelection)) return;
    const id = again && request.current ? request.current.id : crypto.randomUUID();
    const state = again && request.current ? request.current.state : currentState;
    request.current = { id, state };
    setRetry(false);
    void fetcher.submit(
      JSON.stringify({ action: "enrich", selection: state.selection, submissionId: id }),
      { method: "post", encType: "application/json", action: "/admin/ip-addresses" },
    );
  };
```

(delete the old `pageSelected`/`hasSelection` lines 191-194 that this block now defines). Replace the result `Alert` block (lines 242-264) with:

```tsx
      {receipt && <QueueImportStatus receipt={receipt} state={importState} queue="ip-enrichment" />}
      {(fetcher.data?.ok === false || importError) && (
        <Alert variant="destructive">
          <AlertDescription>{fetcher.data?.ok === false ? fetcher.data.error : importError}</AlertDescription>
        </Alert>
      )}
      {retry && request.current && (
        <Button variant="outline" size="sm" disabled={busy} onClick={() => submitEnrichment(true)}>
          Retry queue import
        </Button>
      )}
```

Replace the "Enrich IP addresses" button (lines 294-311) with:

```tsx
          <Button size="sm" disabled={busy || !hasSelection} onClick={() => submitEnrichment()}>
            {submitting ? "Submitting…" : "Add to enrichment queue"}
          </Button>
```

and the paragraph below it (lines 313-316) with:

```tsx
        <p className="text-muted-foreground text-xs">
          Adds the selection to the open IP enrichment draft. Start processing from{" "}
          <Link to="/admin/queues/ip-enrichment">Queues → IP enrichment</Link>; GeoIP, ASN and
          RDAP are saved per address and fresh RDAP coverage is reused.
        </p>
```

- [ ] **Step 6: Queue page: IP enrichment is a draft queue**

`app/lib/queues.ts` — after line 17 add:

```ts
/** Queues on the shared processing queue contract: one open draft per scope, freeze at Start, partition purge. */
export const DRAFT_QUEUES: readonly QueueType[] = ["webtech", "crawler", "ip-enrichment"];
export function isDraftQueue(type: QueueType) { return DRAFT_QUEUES.includes(type); }
```

`app/lib/queues.server.ts`:
- line 51: `const inputOrder = filters.type === "brave" ? "input_id, task_id" : "task_id, input_id";` (Brave's table is still sorted the legacy way).
- lines 93-106 (`loadQueueHistory`): before the loop add `const OUTCOME_TAGS: Record<QueueFilters["type"], string | null> = {webtech: "webtech", crawler: "crawler", "ip-enrichment": "ip_enrichment", brave: null};` and replace lines 99-100 with:

```ts
    const prefix = OUTCOME_TAGS[filters.type];
    const outcome = prefix && run.status === "SUCCESS" && ["completed", "completed_with_errors"].includes(run.tags[`${prefix}/outcome`]) ? run.tags[`${prefix}/outcome`] : null;
```

`app/routes/admin-queue.tsx`:
- line 12: add `isDraftQueue` to the `~/lib/queues` import.
- line 23: `if (isDraftQueue(filters.type) && (!filters.task || inputs.value.selectedTotal === 0)) {`
- line 60: `const processingBlocked = !filters.task ? (isDraftQueue(filters.type) ? "The queue is empty. Add inputs to prepare the next execution." : "Choose a task below to configure processing.")`
- line 103: `{((!isDraftQueue(filters.type) && !filters.task) || (isDraftQueue(filters.type) && inputs.totalTasks > 1)) && <>`
- line 119: `<p className="text-sm text-muted-foreground">{isDraftQueue(filters.type) ? "Inputs can be appended while the task is a draft. Dagster checks submissions and freezes the task when execution begins." : "This queue uses a fixed input selection."}</p>`
- line 122: `{!isDraftQueue(filters.type) && runState && runState.runs.length > 0 && …`
- line 126: `{!isDraftQueue(filters.type) && <TableHead>Task</TableHead>}`
- line 129: `{!isDraftQueue(filters.type) && <TableCell className="max-w-64 whitespace-normal">…`
- line 131: `colSpan={isDraftQueue(filters.type) ? 3 : 4}`
- line 140: `Completed inputs are removed from Webtech, Crawler and IP enrichment queues; results and history remain available.`
- line 145: `{filters.type === "crawler" ? "crawl errors" : filters.type === "ip-enrichment" ? "address errors" : "page errors"}`

`app/components/admin/queue-process-sheet.tsx`:
- line 4: import `isDraftQueue` too.
- lines 81-85: the paragraph becomes

```tsx
            <p className="text-sm text-muted-foreground">{filters.type === "ip-enrichment"
              ? "A draft freezes when the results asset begins. GeoIP, ASN and RDAP are saved per address in acknowledged batches; cached RDAP coverage is judged against the frozen start time and the cache window. Leave the RDAP request budget empty to process the whole task; a reached budget keeps the task resumable."
              : isDraftQueue(filters.type)
              ? "Freshness is checked when execution is prepared. Recent inputs remain in the queue and are counted as skipped. A draft freezes when the results asset begins."
              : "Searches use the saved company inputs. Freshness and force options are evaluated during processing."}</p>
```

- line 103: `{isDraftQueue(filters.type) ? "For draft queues, leave empty …" : "Leave empty for a new execution. …"}` (same two texts as today).
- line 110: `{isDraftQueue(filters.type) ? "Inputs are removed when every entry has a saved outcome or is skipped as recent. Lookup errors remain in results and history. Pipeline failures keep inputs for recovery. Use the processing profile to control this execution." : "Existing input rows are retained for retries."}`

- [ ] **Step 7: Typecheck and targeted tests**

Run: `npm run typecheck`
Expected: clean.

Run: `npx vitest run tests/ip-enrichment.server.test.ts tests/admin-ip-addresses-action.test.ts tests/queues.server.test.ts tests/queue-route.test.ts tests/crawl-queue.server.test.ts tests/webtech-queue.server.test.ts`
Expected: all pass (these files mock ClickHouse and Dagster; do not run the full suite).

Run: `rg -n "launchIpEnrichment|ip_enrichment_workflow|ip_enrichment_input FINAL" app tests docs` → no matches.

- [ ] **Step 8: Commit**

```bash
git add app/lib/ip-enrichment.server.ts app/routes/admin-ip-enrichment-queue-submission.ts app/routes.ts app/components/admin/queue-import-status.tsx app/routes/admin-ip-addresses.tsx app/lib/queues.ts app/lib/queues.server.ts app/routes/admin-queue.tsx app/components/admin/queue-process-sheet.tsx tests/ip-enrichment.server.test.ts tests/admin-ip-addresses-action.test.ts tests/queues.server.test.ts tests/queue-route.test.ts
git commit -m "feat(backoffice): IP addresses add to the enrichment draft; the IP enrichment queue behaves like Webtech and Crawler

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

---

### Task 8: Documentation

**Files:**
- Create: `services/dagster_v3/docs/operations/ip-enrichment-draft-queue.md`
- Modify: `services/dagster_v3/docs/ip-enrichment-schema.md:1-8, 10-29, 87-146, 148-216, 243-264`
- Modify: `services/backoffice/docs/queues.md:11, 20, 22` and append a section
- Modify: `services/dagster_v3/src/dagster_v3/defs/commoncrawl_geoip/docs/commoncrawl_geoip-design.md:8-13`
- Modify: `services/dagster_v3/src/dagster_v3/defs/commoncrawl_rdap/docs/commoncrawl_rdap-design.md` (append a section)
- Modify: `services/dagster_v3/docs/deployment-runbook.md:40`

- [ ] **Step 1: Write the operations guide**

Create `services/dagster_v3/docs/operations/ip-enrichment-draft-queue.md`:

````markdown
# IP enrichment draft queues

Backoffice **Admin → IP addresses → Add to enrichment queue** launches only
`ip_enrichment_input_job`. There is one open draft per `queue_scope` (default `workspace`);
table selections, explicit IP lists and "failed addresses of task X" append to it. Adding
inputs never looks anything up. Since ClickHouse migration 450 the draft follows the shared
processing queue contract
([spec](../superpowers/specs/2026-09-24-shared-processing-queue-contract-design.md)), like
Webtech and the crawler.

## Storage

- ClickHouse `corpscout.ip_enrichment_input`: the only stored copy of each entry
  (`task_id, input_id, ip, source_name, source_record_id, source_run_id, submission_id,
  observed_at, submitted_at`; `ip_version` and `bucket` materialized), `MergeTree`,
  `PARTITION BY task_id`, `ORDER BY (task_id, input_id)`. Read it without `FINAL`.
  `input_id` is `bbb:` (the address's 256-way bucket, zero-padded) followed by the JSON
  tuple `[source_name, source_record_id, ip]`, computed in ClickHouse and enforced by a
  `CHECK`, so a task is walked bucket by bucket.
- PostgreSQL `processing.tasks`: lifecycle, the frozen execution (`config.execution`) and the
  counters written at completion. `processing.input_submissions`: idempotent import receipts
  (selection fingerprint and count; bulk IP lists are stored as count + sha256).
- `corpscout.ip_enrichment_results` (`ORDER BY (bucket, ip, result_id)`): one row per address
  and execution (`attempt` is always 1; a retry is a new draft). `ip_enrichment_current`
  serves the latest conclusive data per address. RDAP coverage stays in `rdap_networks`,
  `rdap_network_segments` (roles `lookup_result`, `parent`, `registry_level`) and
  `rdap_ip_lookup_results`.

## Import

Materialize `ip_enrichment_input` with a stable `submission_id` and exactly one of:

```yaml
ips: [116.203.39.236, 2001:4860:4860::8888]          # explicit list
source_relation: corpscout.commoncrawl_ip_addresses  # + filters / ip_search / max_rows / select_all
retry_failed_task_id: <task uuid>                    # addresses whose result in that task has an error
```

Selection, canonicalization and grouping run inside ClickHouse (`selected_ips_sql`); the
import is one `INSERT … SELECT` minus the identities the draft already holds, then two counts
for the receipt (`input_count` = rows this submission added, `total` = the draft). No size
cap. A failed import blocks Start; retry it with the same `submission_id` and selection: the
retry kills its ClickHouse query (`ip-queue-import:<submission_id>`), deletes only that
submission's rows (synchronous lightweight delete) and reselects from the source as it is now.
Imports and Start share the task's PostgreSQL advisory lock.

## Start and resume

**Queues → IP enrichment** selects the current draft; **Configure processing** launches
`ip_enrichment_results_job` with `task_id`. Under the task lock, the results asset:

1. freezes the draft (`status=selected`, `frozen_at`) and saves the execution in
   `processing.tasks.config.execution`: `execution_id` (the original Dagster run id),
   `profile` (`force_rdap`, `rdap_cache_days`, `parent_depth`, `rate_limit_retry_seconds`,
   `transient_retry_seconds`, `processor_version`), `started_at` and
   `freshness_cutoff = started_at − rdap_cache_days`. `batch_size`, `max_requests` and
   `request_delay_seconds` are transport settings and may change between resumes. The
   default-draft slot is released at once, so new additions form the next draft;
2. loops until nothing remains. *Remaining* is a live query per bucket: the task's entries
   in that bucket whose `input_id` has no row in `ip_enrichment_results` for this execution
   (`bucket = b AND task_id AND execution_id`, one primary-key range). Pages of `batch_size`
   follow an `input_id` cursor inside the bucket; each page costs one negative-cache read
   (`rdap_ip_lookup_results_current`), one trie `dictGet`, one read of the network rows the
   page needs (never `raw_response`), one insert of lookup markers and a share of one result
   insert. Freshness is judged against the frozen execution: a network or marker counts when
   its time is `>= freshness_cutoff`, a retryable error when `retry_after > started_at`;
   `force_rdap` skips both caches. RDAP HTTP requests happen only for misses, paced by
   `request_delay_seconds`, and networks fetched earlier in the run are reused before any
   request. GeoIP City/ASN are read locally per address;
3. stores outcomes through a `ResultBuffer` (500 rows or 5 seconds, acknowledged
   `async_insert`); the cursor never re-reads a page inside a pass, the buffer is flushed
   after every pass and a further pass confirms nothing remains. A reached `max_requests`
   flushes what was resolved and fails the run with "budget reached"; the task stays
   `selected` and re-running it resumes;
4. finishes when a pass finds nothing: counts succeeded/failed per bucket from the results
   (failed = any of City/ASN/RDAP in `retryable_error`/`terminal_error`), `skipped = total −
   succeeded − failed` (expected 0), marks the task `completed` (`completed_with_errors` in
   the run tags when any address failed), drops the task's partition and records
   `inputs_purged_at`.

Re-running the same task with the same profile resumes the saved execution (`execution_id`
may be omitted; an explicit one must be the saved one). A completed task re-run only retries
cleanup. A changed profile is rejected; to re-look addresses up, add them to a new draft
(`force_rdap: true` there bypasses caches; `retry_failed_task_id` selects the failures).
History stays readable from run tags (`ip_enrichment/execution_id`, `ip_enrichment/outcome`,
`ip_enrichment/succeeded_pages`, `ip_enrichment/failed_pages`, `ip_enrichment/skipped_pages`).

## Registry-level registrations

A registration answers only the queried address when it is an RIR/IANA block or unallocated
space: a range wider than /8 (IPv4) or /12 (IPv6); a token `IANA`, `APNIC`, `ARIN`, `LACNIC`,
`AFRINIC`, `RIPE`, `UNALLOCATED` or `UNSPECIFIED` in its name, handle, type or status; a
registrant handle `ARIN`, `IANA`, `APNIC`, `LACNIC`, `AFRINIC` or `ORG-NCC1-RIPE`; a registrant
name containing a registry's full name; or country `ZZ`. Such responses are stored with
`segment_role = 'registry_level'` and the trie's source view (migration 449) excludes them and
every older row matching the same predicate (`REGISTRY_LEVEL_SQL` in
`commoncrawl_rdap/rdap.py`). The queried address is still `found` with that registration,
and its own lookup marker serves it next time; other addresses in the block get their own
lookup.

## GeoLite2

`geolite2_databases` (job `geolite2_update_job`, schedule `geolite2_update_weekly`, Wednesdays
03:15 UTC, started at instance level) downloads GeoLite2-City and GeoLite2-ASN with
`MAXMIND_ACCOUNT_ID`/`MAXMIND_LICENSE_KEY`, verifies the published SHA-256 and the database
type, and atomically replaces the `.mmdb` files in `MAXMIND_DATABASE_DIRECTORY`. The check
`geolite2_databases_fresh` fails when a database is older than 14 days.

## Validation

`tests/test_ip_enrichment_input.py` (disposable ClickHouse + PostgreSQL): entry-table contract,
canonical dedup and append, receipt replay, a retry replacing only its own rows, source
filters, inventory search, the failed-results mode, freeze → next draft.
`tests/test_ip_enrichment_results.py`: registry-level rule parity with the view, bounded
round trips per page, frozen cache window, negative markers, in-run reuse and the budget,
completion with partition purge, errors as published outcomes, budget resume, lost write and
cleanup acknowledgements, changed-profile refusal. `tests/test_geolite2_update.py`: install,
verification, credentials, freshness.
````

- [ ] **Step 2: Update the schema doc**

In `services/dagster_v3/docs/ip-enrichment-schema.md`:
- lines 3-8: replace "The `ip_enrichment_input` Dagster asset prepares input batches from a list or a source relation. `ip_enrichment_results` processes a prepared task using the existing MaxMind mapping and RDAP network cache. The Workspace IP addresses page submits both steps through `ip_enrichment_workflow`." with "Since migration `000450` the input table follows the shared processing queue contract: `ip_enrichment_input` appends to an open draft and `ip_enrichment_results` freezes and processes it (see [ip-enrichment-draft-queue.md](operations/ip-enrichment-draft-queue.md)). The Workspace IP addresses page adds to the draft; processing starts from the queue page."
- lines 12-17: replace with "`ip_enrichment_input` is `MergeTree`, `PARTITION BY task_id`, `ORDER BY (task_id, input_id)`, read without `FINAL`. `input_id` is the bucket-prefixed JSON tuple of `source_name`, `source_record_id` and `ip`, computed and enforced in ClickHouse; the draft keeps one row per identity and a completed task drops its partition."
- lines 87-146 ("Materializing the input asset"): replace the first paragraph's asset description with the draft semantics (stable `submission_id`, `queue_scope`, one of `ips`/`source_relation`/`retry_failed_task_id`), keep the two YAML examples, add `retry_failed_task_id: "<task uuid>"` as a third, and replace the paragraphs from "The materialization reports `task_id`, `selected_inputs`, and `selected_ips`." to the end of the section with: "The materialization reports `task_id`, `submission_id`, `input_count` (rows this submission added) and `total`. Repeating a `submission_id` with the same selection is a no-op; a different selection under it is rejected; a failed import is retried by reselecting the source. New submissions after Start go to the next draft."
- lines 148-216 ("Materializing enrichment results"): replace from "The input batch must be fully prepared." through the end of the section with the summary: freeze via the shared lifecycle, per-bucket live remaining query, bounded round trips per page, `ResultBuffer` writes, frozen cache window, `max_requests` budget → failed run that resumes on re-run, errors are published outcomes (`completed_with_errors`), completion drops the partition, `attempt` always 1, retries via a new draft. Keep the YAML example (`batch_size`, `max_requests`, `request_delay_seconds`).
- lines 243-264 ("Workspace IP selection"): replace "`ip_enrichment_workflow` runs input preparation before results processing with one shared task UUID." with "**Add to enrichment queue** launches `ip_enrichment_input_job` with a stable `submission_id` and `queue_scope: workspace`; processing is started from Queues → IP enrichment." and delete the last paragraph's "The UI submits `max_requests: null` so the entire selected batch can be processed; the standalone worker default remains 250 requests." (the queue sheet's template still sends `max_requests: null`; say so).

- [ ] **Step 3: Update the backoffice queue doc and the two design docs**

`services/backoffice/docs/queues.md`: line 11 `| IP enrichment | `ip_enrichment_input` (draft) | `ip_enrichment_results_job` |`; line 20 "Other processors retain their existing fixed input selection lifecycle." → "Brave retains its fixed input selection lifecycle."; line 22 "Other processors use the original results run ID for resumption." → "Brave uses the original results run ID for resumption."; append:

```markdown
## IP enrichment drafts

Admin → IP addresses → **Add to enrichment queue** imports the selection into
`ip_enrichment_input` through `ip_enrichment_input_job` with a stable submission ID; the
status component polls `/admin/ip-enrichment/queue-submissions/<runId>` and links the saved
task. A successful import clears the selection; an accepted launch alone does not. The
entry table is partitioned by task and read without `FINAL`.

Queues → IP enrichment automatically selects the current draft; Start launches
`ip_enrichment_results` with `task_id`. `batch_size`, `max_requests` and
`request_delay_seconds` may change between resumes; the RDAP policy fields are frozen at
Start. Lookup errors complete the task “with errors”; add the failed addresses to a new
draft (`retry_failed_task_id`) to retry them. The old one-shot `ip_enrichment_workflow` is
removed.
```

`commoncrawl_geoip-design.md` lines 8-13: append "The `geolite2_databases` asset (`update.py`) refreshes the two `.mmdb` files weekly and `geolite2_databases_fresh` fails when they are older than 14 days; see [ip-enrichment-draft-queue.md](../../../../../docs/operations/ip-enrichment-draft-queue.md)."

`commoncrawl_rdap-design.md`: append a section "## Registry-level registrations" with the rule and the pointer to `REGISTRY_LEVEL_SQL` / migration 449 (the legacy bucket worker keeps writing `lookup_result` segments; the view excludes registry-level ones for every writer).

`deployment-runbook.md` line 40: `| MaxMind dir | `MAXMIND_DATABASE_DIRECTORY`, `MAXMIND_ACCOUNT_ID`, `MAXMIND_LICENSE_KEY` | ip_enrichment, geolite2_databases |`.

- [ ] **Step 4: Check for stale wording**

Run from `corpscout/`: `rg -n "ip_enrichment_workflow|prepare_selection|selected_ips\b|unique_ips|input_id, task_id|Enrich IP addresses" services/dagster_v3/docs services/dagster_v3/src/dagster_v3/defs/commoncrawl_geoip/docs services/dagster_v3/src/dagster_v3/defs/commoncrawl_rdap/docs services/backoffice/docs`
Expected: only the sentence that says the workflow is removed.

- [ ] **Step 5: Commit**

```bash
git add services/dagster_v3/docs/operations/ip-enrichment-draft-queue.md services/dagster_v3/docs/ip-enrichment-schema.md services/backoffice/docs/queues.md services/dagster_v3/src/dagster_v3/defs/commoncrawl_geoip/docs/commoncrawl_geoip-design.md services/dagster_v3/src/dagster_v3/defs/commoncrawl_rdap/docs/commoncrawl_rdap-design.md services/dagster_v3/docs/deployment-runbook.md
git commit -m "docs: IP enrichment on the shared queue contract, registry-level RDAP rule, GeoLite2 updates

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

---

### Task 9: Deploy, remediation and verification on prod — REQUIRES THE OWNER'S GO-AHEAD BEFORE STEP 1

**Files:** none until Step 11 (spec status line).

- [ ] **Step 1: Preconditions**

- Everything is merged on `main`, the tree is clean (`git status --short` shows only `?? searcher/`), and `ls clickhouse/migrations | tail -2` still shows 449/450 as the newest (renumber before merging if another workstream took a number).
- No active IP enrichment run: in the Dagster UI the run lists of `ip_enrichment_results_job`, `ip_enrichment_input_job` and `ip_enrichment_workflow` filtered to `STARTED`/`QUEUED`/`STARTING` are empty (the 48.6M run `83283501-…` was terminated on 2026-09-25).
- Prod ledger: `ssh companycollect 'sudo docker exec clickhouse-clickhouse-1 clickhouse-client -q "SELECT version, dirty FROM corpscout.schema_migrations ORDER BY version DESC LIMIT 4"'` → the highest version with a `dirty=0` row is `448` and no higher version has only a `dirty=1` row.
- Not inside the Tuesday 01:05 Stockholm address-chain window.
- GeoLite2 credentials: `ssh companycollect 'sudo grep -c "^MAXMIND_ACCOUNT_ID=.\+" /opt/companycollect/corpscout/dagster_v3/.env; sudo grep -c "^MAXMIND_LICENSE_KEY=.\+" /opt/companycollect/corpscout/dagster_v3/.env'` → `1` and `1`. If either is `0`, ask the owner for the MaxMind account id and license key and for permission to append them to the server-owned `.env` (Ansible never copies secrets). Do not invent values. Note: the running daemon loaded `.env` at service start, so `geolite2_databases` (Step 8) can only see new variables after a service restart — the owner decides when (`dagster_force_restart` in a full `site.yml` deploy, or a manual restart outside a run); until then Step 8 is deferred and the check stays red.

- [ ] **Step 2: Cancel the legacy tasks (owner-approved)**

```bash
ssh companycollect "sudo docker exec ppoint-postgres psql -U corpscout -d corpscout -Atc \"SELECT task_id, status, total, queue_scope FROM processing.tasks WHERE processor='ip-enrichment-v1' ORDER BY created_at\""
```

Expected: exactly the four tasks `4802549d-c320-482f-9bbf-6f21b8ffcd18`, `a0a328d6-…`, `422ce6d8-…`, `6f3377b2-…`, all `selected`, `queue_scope` empty. Then (only `cancelled` is allowed for `queue_scope IS NULL` by `tasks_queue_lifecycle_check`):

```bash
ssh companycollect "sudo docker exec ppoint-postgres psql -U corpscout -d corpscout -Atc \"UPDATE processing.tasks SET status='cancelled' WHERE processor='ip-enrichment-v1' AND queue_scope IS NULL AND status='selected' RETURNING task_id\""
```

Expected: the four ids. Results of those tasks in `ip_enrichment_results` are untouched.

- [ ] **Step 3: Empty the entry table (owner-approved destructive step)**

```bash
ssh companycollect 'sudo docker exec clickhouse-clickhouse-1 clickhouse-client -q "SELECT count(), uniqExact(task_id) FROM corpscout.ip_enrichment_input"'
```

Expected: `48596636	4` (only the four cancelled tasks). Then:

```bash
ssh companycollect 'sudo docker exec clickhouse-clickhouse-1 clickhouse-client -q "TRUNCATE TABLE corpscout.ip_enrichment_input"'
ssh companycollect 'sudo docker exec clickhouse-clickhouse-1 clickhouse-client -q "SELECT count() FROM corpscout.ip_enrichment_input"'
```

Expected: `0`. The rows are re-selectable from `corpscout.commoncrawl_ip_addresses`.

- [ ] **Step 4: Apply migration 449 and verify the trie**

Run from `corpscout/`: `make clickhouse-migrate-up-one </dev/null`
Expected: `449/u corpscout_rdap_registry_level_exclusion`. Then:

```bash
ssh companycollect 'sudo docker exec clickhouse-clickhouse-1 clickhouse-client -q "SYSTEM RELOAD DICTIONARY corpscout.rdap_network_trie"'
ssh companycollect 'sudo docker exec clickhouse-clickhouse-1 clickhouse-client -q "SELECT network_key, name, start_address, end_address FROM corpscout.rdap_networks_current WHERE network_key NOT IN (SELECT DISTINCT network_key FROM corpscout.rdap_network_segments_current) AND network_key IN (SELECT DISTINCT network_key FROM corpscout.rdap_network_segments WHERE segment_role = '"'"'lookup_result'"'"') ORDER BY rir, start_address FORMAT PrettyCompact"'
```

Expected: the excluded set contains `apnic:103.0.0.0 - 103.255.255.255` (APNIC-AP), the apnic 101/8, 111/8, 113/8 and afrinic 102/8 blocks, `ripe` EU-ZZ-2A00 (2a00::/11), `arin:NET6-2600-1` and the LACNIC UNALLOCATED ranges from the review; and no ordinary holder network (spot-check: FPT-VN, DIGITALPACIFIC, Hetzner and Google blocks are still served: `SELECT dictGet('corpscout.rdap_network_trie', 'network_key', tuple(toIPv4('8.8.8.8')))`). Save the excluded `network_key` list to the scratchpad; Step 10 needs it. `SELECT count() FROM system.dictionaries WHERE name='rdap_network_trie' AND status='LOADED'` → `1`.

- [ ] **Step 5: Apply migration 450**

Run: `make clickhouse-migrate-up-one </dev/null`
Expected: `450/u corpscout_ip_enrichment_queue_contract`, then

```bash
ssh companycollect 'sudo docker exec clickhouse-clickhouse-1 clickhouse-client -q "SELECT engine, partition_key, sorting_key FROM system.tables WHERE database='"'"'corpscout'"'"' AND name='"'"'ip_enrichment_input'"'"'"'
```

→ `MergeTree	task_id	task_id, input_id`.

- [ ] **Step 6: Deploy Dagster by light_sync**

Run: `cd services/dagster_v3/ansible && ANSIBLE_BECOME_TIMEOUT=60 LC_ALL=en_US.UTF-8 ansible-playbook -i inventory.ini light_sync.yml </dev/null > /private/tmp/claude-501/-Users-graovic-pulsarpoint-ppoint-companycollect-corpscout/9f2d193f-045d-4f26-91d7-d2b93320d3f5/scratchpad/ip/light_sync.log 2>&1; echo rc=$?`
Expected: `rc=0`; the code location reloads. In the Dagster UI: `ip_enrichment_input_job`, `ip_enrichment_results_job`, `geolite2_update_job` and the schedule `geolite2_update_weekly` (stopped) exist; no `ip_enrichment_workflow`. Start `geolite2_update_weekly` from the Schedules page.

- [ ] **Step 7: Backoffice (runs locally from main: merge = deploy)**

The owner restarts the local backoffice dev server on `main` (a new route file was added). Open `/admin/queues/ip-enrichment` — the empty queue renders with "The queue is empty…" and no legacy task rows; `/admin/ip-addresses` shows **Add to enrichment queue**.

- [ ] **Step 8: First GeoLite2 update (after the credentials are loaded, see Step 1)**

Launch `geolite2_update_job` from the UI. Expected: success, metadata `GeoLite2-City_build`/`GeoLite2-ASN_build` within the last week, the check `geolite2_databases_fresh` passes, and on the host `ls -l "$(sudo grep '^MAXMIND_DATABASE_DIRECTORY=' /opt/companycollect/corpscout/dagster_v3/.env | cut -d= -f2)"` shows only `GeoLite2-City.mmdb` and `GeoLite2-ASN.mmdb` with today's date (no `.tmp` files).

- [ ] **Step 9: End-to-end with a handful of addresses**

Launch `ip_enrichment_input_job`:

```yaml
ops:
  ip_enrichment_input:
    config:
      submission_id: "<new uuid>"
      source_name: e2e-2026-09
      ips: [8.8.8.8, 103.35.64.49, 2001:4860:4860::8888, 127.0.0.1]
```

Note `task_id` in the run metadata. On the backoffice, `/admin/queues/ip-enrichment` selects that draft; use **Configure processing** with the defaults (`max_requests` empty) or launch `ip_enrichment_results_job` with `{task_id: "<task_id>", max_requests: null}`. Verify:

```bash
ssh companycollect "sudo docker exec ppoint-postgres psql -U corpscout -d corpscout -Atc \"SELECT status, total, succeeded_count, terminal_failed_count, skipped_count, inputs_purged_at IS NOT NULL, config->'execution'->>'execution_id' FROM processing.tasks WHERE task_id='<task_id>'\""
ssh companycollect 'sudo docker exec clickhouse-clickhouse-1 clickhouse-client -q "SELECT ip, rdap_lookup_status, rdap_name, rdap_matched_cidr, country_iso_code, toDate(city_db_build_epoch) FROM corpscout.ip_enrichment_current WHERE ip IN ('"'"'8.8.8.8'"'"','"'"'103.35.64.49'"'"','"'"'2001:4860:4860::8888'"'"','"'"'127.0.0.1'"'"') FORMAT PrettyCompact"'
ssh companycollect 'sudo docker exec clickhouse-clickhouse-1 clickhouse-client -q "SELECT count() FROM corpscout.ip_enrichment_input WHERE task_id = '"'"'<task_id>'"'"'"'
```

Expected: `completed | 4 | 4 | 0 | 0 | t | <execution_id>` (the results run id); `103.35.64.49` is `found` with a specific holder (the review's spot check gave `FPT-VN`, a /22), not `APNIC-AP`; `2001:4860:4860::8888` is `found`; `127.0.0.1` is `not_global`; the GeoIP build date is the fresh one from Step 8 (or 2026-07-10 if Step 8 is deferred); `0` input rows. The run's tags carry `ip_enrichment/outcome=completed`. Then queue `103.35.64.49` once more in a new draft and process it: the results run's metadata shows `rdap_requests: 0` (served from coverage), and `SELECT segment_role, count() FROM corpscout.rdap_network_segments GROUP BY segment_role` shows no new `registry_level` rows for it.

- [ ] **Step 10: Remediation draft for the addresses served by registry-level blocks (owner go-ahead)**

Count them with the network keys saved in Step 4:

```bash
ssh companycollect 'sudo docker exec clickhouse-clickhouse-1 clickhouse-client -q "SELECT count() FROM corpscout.ip_enrichment_current WHERE rdap_network_key IN ('"'"'<key1>'"'"', '"'"'<key2>'"'"', ...)"'
```

Expected: about 170,000 (the review counted 173,991 on /8 blocks plus the IPv6 and LACNIC rows). Launch `ip_enrichment_input_job`:

```yaml
ops:
  ip_enrichment_input:
    config:
      submission_id: "<new uuid>"
      source_name: remediation:registry-level-2026-09
      source_relation: corpscout.ip_enrichment_current
      ip_column: ip
      select_all: true
      filters:
        rdap_network_key: ["<key1>", "<key2>", "..."]
```

(`ip_enrichment_current` is a view over the whole results history; this import evaluates it once, a minute or two.) Then process the draft from the queue page with `force_rdap: true`, `max_requests` empty, `request_delay_seconds: 1`, `parent_depth: 1`. Expected: ~10–30k RDAP requests (4–12 hours at one request per second), a task that completes (with errors for rate-limited registries), and afterwards `SELECT count() FROM corpscout.ip_enrichment_current WHERE rdap_network_key IN (<keys>)` drops to the addresses whose registries returned no more specific registration. Failed addresses can be queued again later with `retry_failed_task_id`.

- [ ] **Step 11: Mark the spec**

In `services/dagster_v3/docs/superpowers/specs/2026-09-24-shared-processing-queue-contract-design.md` change line 3 to:

`Status: agreed 2026-09-24. Webtech implemented 2026-09-25 (plan 2026-09-24-webtech-queue-contract); crawl implemented 2026-09-25 (plan 2026-09-25-crawl-queue-contract); IP enrichment implemented <today's date> (plan 2026-09-25-ip-enrichment-queue-contract); Brave pending.`

```bash
git add services/dagster_v3/docs/superpowers/specs/2026-09-24-shared-processing-queue-contract-design.md
git commit -m "docs(spec): IP enrichment is on the shared processing queue contract

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

---

## Decision coverage

| Decision | Task(s) |
| --- | --- |
| D1 entry table layout (String task_id, required submission_id, gate, mutation-pool setting), input_id choice stated | 2 |
| D2 legacy tasks cancelled, entry table truncated in the deploy task, results untouched | 9 |
| D3 draft queue import with receipts, queue_scope, submission-scoped retry, no manifest | 3 |
| D4 shared lifecycle, frozen profile vs transport keys, execution_id = original run, frozen cache window, live remaining, errors as outcomes, retry-failed mode, completion + purge | 4, 5 (retry mode in 3) |
| D5 per-page ClickHouse work, ResultBuffer, bounded-queries test, per-miss network writes stated | 4, 5 |
| D6 registry-level rule, segment role, view exclusion via migration, remediation draft | 1, 4, 9 |
| D7 GeoLite2 asset/schedule/check, credentials, `.env.example` fix | 6, 9 |
| D8 workflow removed, backoffice adds to the draft, queue page draft semantics, ordering, no FINAL | 5, 7 |
| D9 follow-ups listed below | — |
| D10 deploy with owner go-ahead, e2e incl. IPv6 and 103/8, remediation launch | 9 |

## Follow-ups (out of scope, D9)

- Bulk RIR delegation dumps as a range dictionary instead of per-network RDAP discovery.
- Per-registry parallel rate lanes and honouring `Retry-After`.
- Parent lookups served from the cache, and `parent_depth` default 0.
- Owner-name fallback from `remarks` for APNIC-family and AFRINIC registrations (also the reason remarks are not part of the registry-level rule).
- ARIN broad-allocation children (children of a `DIRECT ALLOCATION` block).
- whoisit's `User-Agent` override (`client.py:49` is never sent).
- Storing the HTTP status code and registry on `query_error` rows.
- GeoIP-only refresh of old result rows after a GeoLite2 update.
- Backoffice control to queue "failed addresses of task X" (the Dagster config `retry_failed_task_id` exists; the UI does not expose it).
- The legacy `commoncrawl_ip_rdap_networks` bucket worker still stores registry-level responses as `lookup_result` segments (the view excludes them); switching it to the shared classifier is a small change once that worker is next touched.
