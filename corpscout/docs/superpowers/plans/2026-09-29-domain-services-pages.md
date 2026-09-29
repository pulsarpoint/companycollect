# Domain services pages Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:**
- A per-bucket `domain_service_intervals` table plus `provider_service_counts`, rebuilt by Dagster after each resolver partition.
- Three backoffice views over them: a domain Services tab, a Providers list, and a provider Domains tab.

**Architecture:**
- **Dagster:** `defs/dns_detect` gains:
  - `intervals.py`, which rebuilds one bucket through stage tables and `REPLACE PARTITION`;
  - the bucket SQL in `sql.py`;
  - a second asset `domain_service_intervals_clickhouse` in the existing resolver job.
- **Backoffice:**
  - shared types and pure helpers in `app/lib/domain-services.ts`;
  - parameterized ClickHouse queries in `app/lib/domain-services.server.ts`;
  - three new route files, plus a small tab strip on the provider page.

**Tech Stack:**
- Python 3.14 with Dagster (uv), clickhouse-driver, pytest with clickhouse-local.
- React Router v7, TypeScript, shadcn/ui, Vitest.
- ClickHouse 26.5.

**Spec:** `docs/superpowers/specs/2026-09-29-domain-services-pages-design.md` (owner-approved 2026-09-29).

## Global Constraints

- **Table names and sort keys (verbatim from the spec):**
  - `domain_service_intervals`: `ENGINE = MergeTree PARTITION BY bucket ORDER BY (provider_slug, service_type, root_domain, first_seen)`.
  - `provider_service_counts`: `ENGINE = MergeTree PARTITION BY bucket ORDER BY (provider_slug, provider_key, service_type)`.
- **Bucket:** `bucket` = `cityHash64(root_domain) % 128`, the resolver asset's partition.
- **History rules are identical to `domain_services_history`:**
  - the latest resolution per record only;
  - fallback rows are dropped where a non-fallback row of the same service type overlaps them;
  - windows less than 45 days apart are merged.
- **`is_current` is the `domain_services_now` rule:** the period's `last_seen` reaches the domain's latest scan (`max(record_to)`) of one of its record types.
- **Swap only after both stage inserts succeed.** Refuse the swap when the stage is empty while `dns_record_services` has rows for the bucket.
- **SQL run through clickhouse-driver:**
  - a literal modulo is written `%%`;
  - every statement containing `%%` is executed with a params dict (`{}` at least), because the driver substitutes only when params is not None.
- **Migration number:** 000470. Re-check against main and the prod `schema_migrations` before merge; prod is at 469 on 2026-09-29.
- **Git:**
  - work in worktree `.worktrees/domain-services-pages` on branch `domain-services-pages`;
  - commit by explicit path;
  - every commit ends with `Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>`.
- **Checks:**
  - Dagster: `uv run pytest tests/test_dns_detect_*.py tests/test_clickhouse_migrations.py` and `uv run dg check defs`.
  - Backoffice: `npx vitest run <files>` and `npm run typecheck`.
- **Ruling (spec says Zod): the backoffice has no `zod` dependency.** Query parameters are parsed with the repo's existing clamp and whitelist helpers, not a new dependency. The cost if the owner wanted Zod: swapping one parser function.
- **Ruling (spec says `app/types/api/`): that directory does not exist.** The backoffice keeps types next to their module in `app/lib/*.ts`, which is followed here.
- **Ruling: `provider_service_counts` also carries one row per (bucket, provider_slug, provider_key) with `service_type = ''`, meaning any service type.** A provider's total distinct domains cannot be summed across its service types, because one domain is counted in several.
- **Deploying:**
  - dagster_v3 is deployed only after the running knowledge refresh (sensor tag `dns_detect/refresh`) has no queued or running runs, because a deploy cancels in-flight runs;
  - never delete another deployment's lock.

## Review Focus

1. **A domain whose only results are fallback (SOA) rows:** it must still get intervals (nothing overlaps them); covered in Task 2's parity test with a SOA-only domain.
2. **A bucket with results but where every record was re-resolved to nothing:** the stage is legitimately empty while `dns_record_services` still holds old rows. The guard must count *current* rows, not all rows. Task 3 counts rows of the latest resolutions.
3. **Provider slugs and domains with odd characters in URLs:** the routes must decode them and pass them only as query params. Task 5 and Task 7 tests use an encoded slug and an uppercase domain.
4. **Paging past the end of a provider's domain list:** it must clamp to the last page rather than show an empty table with no way back. Task 7 test.
5. **The unmapped key list must never show mapped providers:** it filters `provider_slug = ''` and `service_type = ''`. Task 4 test.

---

### Task 0: Branch setup, watermark branch merged, `%%` bug fixed

**Files:**
- Modify: `services/dagster_v3/src/dagster_v3/defs/dns_detect/assets.py` (the watermark call)
- Test: `services/dagster_v3/tests/test_dns_detect_assets.py`

- [ ] **Step 1: Create the worktree.**
  - `git worktree add -b domain-services-pages .worktrees/domain-services-pages main` from `companycollect/`.
  - `git merge --no-edit dns-detect-watermark`. Expected: a clean merge (the branch touches only `defs/dns_detect` and its tests).
- [ ] **Step 2: Failing test.** The asset's watermark query must reach ClickHouse with `%%` rendered as `%`: assert that the fake reader receives a params dict for the watermark statement.

```python
def test_watermark_query_is_executed_with_params_so_the_driver_renders_modulo() -> None:
    calls = []

    class Reader:
        def execute(self, query, params=None, **_):
            calls.append((query, params))
            return [[None]]

    assets.read_watermark(Reader(), 3)
    query, params = calls[0]
    assert "%%" in query and params == {}
```

- [ ] **Step 3: Run** `uv run pytest tests/test_dns_detect_assets.py -k watermark_query -v`. Expected: FAIL (`read_watermark` not defined).
- [ ] **Step 4: Implement** in `assets.py`, and use it in the asset in place of `reader.execute(sql.watermark_sql(...))[0][0]`:

```python
def read_watermark(reader, bucket: int):
    """The bucket's newest load time. Params are passed (even empty) so the
    driver renders the SQL's %% as %."""
    return reader.execute(sql.watermark_sql(RESOLVED_DATABASE, bucket), {})[0][0]
```

- [ ] **Step 5: Run** `uv run pytest tests/test_dns_detect_*.py -q`. Expected: all pass.
- [ ] **Step 6: Commit:** `fix(dagster): dns-detect watermark query renders its modulo (params passed)`.

### Task 1: Migration 000470

**Files:**
- Create: `clickhouse/migrations/000470_corpscout_domain_service_intervals.up.sql`, `…down.sql`
- Modify: `services/dagster_v3/tests/test_clickhouse_migrations.py` (the `EXPECTED_MIGRATIONS` entry and a contract test)

- [ ] **Step 1: Failing test** (append to `test_clickhouse_migrations.py` and add `"000470_corpscout_domain_service_intervals"` to `EXPECTED_MIGRATIONS` after 000469):

```python
def test_domain_service_intervals_migration_defines_both_tables() -> None:
    up = (MIGRATIONS_DIR / "000470_corpscout_domain_service_intervals.up.sql").read_text()
    down = (MIGRATIONS_DIR / "000470_corpscout_domain_service_intervals.down.sql").read_text()
    assert "CREATE TABLE IF NOT EXISTS corpscout.domain_service_intervals" in up
    assert "ORDER BY (provider_slug, service_type, root_domain, first_seen)" in up
    assert "CREATE TABLE IF NOT EXISTS corpscout.provider_service_counts" in up
    assert "ORDER BY (provider_slug, provider_key, service_type)" in up
    assert up.count("PARTITION BY bucket") == 2
    assert "DROP TABLE IF EXISTS corpscout.domain_service_intervals" in down
    assert "DROP TABLE IF EXISTS corpscout.provider_service_counts" in down
```

- [ ] **Step 2: Run** `uv run pytest tests/test_clickhouse_migrations.py -q`. Expected: FAIL (file missing).
- [ ] **Step 3: Write the migration.** Follow the repo's rule: no semicolons inside comments.

```sql
-- Service usage periods per domain, rebuilt per hash bucket by the Dagster
-- asset domain_service_intervals_clickhouse from dns_record_services (same
-- rules as the domain_services_history view). Sorted by provider so provider
-- pages read a range.
CREATE TABLE IF NOT EXISTS corpscout.domain_service_intervals
(
    root_domain String,
    service_type LowCardinality(String),
    provider_key String,
    provider_slug LowCardinality(String),
    service_keys Array(String),
    first_seen Date,
    last_seen Date,
    is_current UInt8,
    evidence UInt32,
    analyzers Array(LowCardinality(String)),
    record_types Array(LowCardinality(String)),
    confidence Float32,
    bucket UInt8,
    computed_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
PARTITION BY bucket
ORDER BY (provider_slug, service_type, root_domain, first_seen);

-- Distinct domains per provider key and service type in one bucket. A row
-- with service_type = '' counts the provider key over all service types.
-- Pages sum the 128 buckets.
CREATE TABLE IF NOT EXISTS corpscout.provider_service_counts
(
    bucket UInt8,
    provider_slug LowCardinality(String),
    provider_key String,
    service_type LowCardinality(String),
    domains_now UInt32,
    domains_ever UInt32,
    computed_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
PARTITION BY bucket
ORDER BY (provider_slug, provider_key, service_type);
```

  Down:

```sql
DROP TABLE IF EXISTS corpscout.provider_service_counts;
DROP TABLE IF EXISTS corpscout.domain_service_intervals;
```

- [ ] **Step 4: Run** the same test file. Expected: PASS.
- [ ] **Step 5: Commit:** `feat(clickhouse): 000470 domain service intervals and provider counts`.

### Task 2: Bucket SQL with parity tests

**Files:**
- Modify: `services/dagster_v3/src/dagster_v3/defs/dns_detect/sql.py`
- Create: `services/dagster_v3/tests/test_dns_detect_intervals_sql.py`

**Interfaces:**
- Produces:
  - `sql.INTERVALS_TABLE = "domain_service_intervals"` and `sql.COUNTS_TABLE = "provider_service_counts"`;
  - `sql.intervals_insert_sql(database: str, target: str, bucket: int) -> str`;
  - `sql.counts_insert_sql(database: str, target: str, intervals: str, bucket: int) -> str`;
  - `sql.current_results_count_sql(database: str, bucket: int) -> str`.
  - All contain `%%` and must be executed with params `{}`.

- [ ] **Step 1: Failing tests.** clickhouse-local; SQL rendered through the real driver.

```python
"""Bucket SQL for domain_service_intervals: same answers as the per-domain views."""

import json
import subprocess
from pathlib import Path

from clickhouse_driver import Client

from dagster_v3.defs.dns_detect import sql
from tests.clickhouse_local import clickhouse_local_command
from tests.test_dns_detect_views import T1, T2, insert, resolution, result, rid

ROOT = Path(__file__).resolve().parents[3] / "clickhouse" / "migrations"
SCHEMA = (ROOT / "000468_corpscout_dns_detect.up.sql").read_text() + (ROOT / "000470_corpscout_domain_service_intervals.up.sql").read_text()
_CLIENT = Client("localhost")


def render(query: str) -> str:
    return _CLIENT.substitute_params(query, {}, _CLIENT.connection.context)


def run(body: str) -> list[list]:
    out = subprocess.run(clickhouse_local_command(), input=SCHEMA + body, capture_output=True, text=True, timeout=300)
    assert out.returncode == 0, out.stderr
    return [json.loads(line) for line in out.stdout.splitlines() if line.strip()]


def bucket_of(domain: str) -> int:
    return run(f"SELECT cityHash64('{domain}') % 128 FORMAT JSONCompactEachRow;")[0][0]


def build(fixture: str, bucket: int) -> str:
    return (fixture
            + render(sql.intervals_insert_sql("corpscout", "domain_service_intervals", bucket)) + ";\n"
            + render(sql.counts_insert_sql("corpscout", "provider_service_counts", "domain_service_intervals", bucket)) + ";\n")


COLS = "service_type, provider_key, provider_slug, service_keys, first_seen, last_seen, evidence, analyzers, record_types"


def parity(domain: str, fixture: str) -> tuple[list, list]:
    body = build(fixture, bucket_of(domain))
    view = run(body + f"SELECT {COLS} FROM corpscout.domain_services_history(domain = '{domain}') ORDER BY ALL FORMAT JSONCompactEachRow;")
    table = run(body + f"SELECT {COLS} FROM corpscout.domain_service_intervals WHERE root_domain = '{domain}' ORDER BY ALL FORMAT JSONCompactEachRow;")
    return view, table


def test_intervals_equal_the_history_view_with_fallback_merge_and_gap() -> None:
    fixture = insert(
        [resolution(rid(1), "a.se", "NS", "ns", T1, "2025-01-01", "2026-09-01"),
         resolution(rid(2), "a.se", "SOA", "soa", T1, "2024-01-01", "2026-09-01"),
         resolution(rid(3), "a.se", "MX", "mx", T1, "2025-01-01", "2026-09-01")],
        [result(rid(1), "a.se", "NS", "ns", "dns", "loopia", "2025-01-01", "2025-06-01", T1),
         result(rid(1), "a.se", "NS", "ns", "dns", "loopia", "2025-06-20", "2025-09-01", T1),   # 19-day gap: merged
         result(rid(1), "a.se", "NS", "ns", "dns", "loopia", "2026-01-01", "2026-09-01", T1),   # 122-day gap: new period
         result(rid(2), "a.se", "SOA", "soa", "dns", "nsone.net", "2025-01-01", "2026-09-01", T1, fallback=1),
         result(rid(2), "a.se", "SOA", "soa", "dns", "binero", "2024-01-01", "2024-06-01", T1, fallback=1),
         result(rid(3), "a.se", "MX", "mx", "email", "google", "2025-01-01", "2026-09-01", T1)],
    )
    view, table = parity("a.se", fixture)
    assert table == view and len(table) == 4  # loopia x2, binero, google


def test_older_resolutions_are_ignored_like_the_view() -> None:
    fixture = insert(
        [resolution(rid(1), "b.se", "NS", "ns", T1, "2026-01-01", "2026-09-01"),
         resolution(rid(1), "b.se", "NS", "ns", T2, "2026-01-01", "2026-09-01")],
        [result(rid(1), "b.se", "NS", "ns", "dns", "old", "2026-01-01", "2026-09-01", T1),
         result(rid(1), "b.se", "NS", "ns", "dns", "new", "2026-01-01", "2026-09-01", T2)],
    )
    view, table = parity("b.se", fixture)
    assert table == view and [r[1] for r in table] == ["new"]


def test_soa_only_domain_keeps_its_fallback_interval() -> None:
    fixture = insert(
        [resolution(rid(1), "c.se", "SOA", "soa", T1, "2026-01-01", "2026-09-01")],
        [result(rid(1), "c.se", "SOA", "soa", "dns", "binero", "2026-01-01", "2026-09-01", T1, fallback=1)],
    )
    view, table = parity("c.se", fixture)
    assert table == view and len(table) == 1


def test_is_current_matches_domain_services_now() -> None:
    fixture = insert(
        [resolution(rid(1), "d.se", "NS", "ns", T1, "2025-01-01", "2026-09-01"),
         resolution(rid(2), "d.se", "MX", "mx", T1, "2025-01-01", "2026-09-01")],
        [result(rid(1), "d.se", "NS", "ns", "dns", "loopia", "2025-01-01", "2025-05-01", T1),
         result(rid(1), "d.se", "NS", "ns", "dns", "cloudflare", "2025-06-01", "2026-09-01", T1),
         result(rid(2), "d.se", "MX", "mx", "email", "google", "2025-01-01", "2026-09-01", T1)],
    )
    body = build(fixture, bucket_of("d.se"))
    now = run(body + "SELECT provider_key FROM corpscout.domain_services_now(domain = 'd.se') ORDER BY ALL FORMAT JSONCompactEachRow;")
    table = run(body + "SELECT provider_key FROM corpscout.domain_service_intervals WHERE root_domain = 'd.se' AND is_current ORDER BY ALL FORMAT JSONCompactEachRow;")
    assert table == now == [["cloudflare"], ["google"]]


def test_counts_per_service_type_and_totals_and_unmapped_keys() -> None:
    fixture = insert(
        [resolution(rid(1), "e.se", "NS", "ns", T1, "2026-01-01", "2026-09-01"),
         resolution(rid(2), "e.se", "MX", "mx", T1, "2026-01-01", "2026-09-01")],
        [result(rid(1), "e.se", "NS", "ns", "dns", "google", "2026-01-01", "2026-09-01", T1),
         result(rid(2), "e.se", "MX", "mx", "email", "google", "2026-01-01", "2026-09-01", T1)],
    )
    # an unmapped key: provider_slug '' (the fixture helper sets slug = key, so insert one by hand)
    fixture += ("INSERT INTO corpscout.dns_record_resolutions VALUES (unhex('00000000000000000000000000000009'), 'e.se', 'e.se', 'CNAME', 'cname', "
                "'r1', 'i1', 1, [], toDate('2026-01-01'), toDate('2026-09-01'), toDateTime64('2026-09-01 00:00:00', 3, 'UTC'));\n"
                "INSERT INTO corpscout.dns_record_services VALUES (unhex('00000000000000000000000000000009'), 'e.se', 'e.se', 'CNAME', 'cname', 's', "
                "'hosting', 'unmapped.net', '', '', 'r', 0.5, 0, toDate('2026-01-01'), toDate('2026-09-01'), toDateTime64('2026-09-01 00:00:00', 3, 'UTC'));\n")
    body = build(fixture, bucket_of("e.se"))
    rows = run(body + "SELECT provider_slug, provider_key, service_type, domains_now, domains_ever FROM corpscout.provider_service_counts ORDER BY ALL FORMAT JSONCompactEachRow;")
    assert rows == [["", "unmapped.net", "", 1, 1], ["", "unmapped.net", "hosting", 1, 1],
                    ["google", "google", "", 1, 1], ["google", "google", "dns", 1, 1], ["google", "google", "email", 1, 1]]


def test_current_results_count_ignores_records_re_resolved_to_nothing() -> None:
    fixture = insert(
        [resolution(rid(1), "f.se", "NS", "ns", T1, "2026-01-01", "2026-09-01"),
         resolution(rid(1), "f.se", "NS", "ns", T2, "2026-01-01", "2026-09-01")],
        [result(rid(1), "f.se", "NS", "ns", "dns", "old", "2026-01-01", "2026-09-01", T1)],
    )
    q = render(sql.current_results_count_sql("corpscout", bucket_of("f.se")))
    assert run(fixture + q + " FORMAT JSONCompactEachRow;") == [[0]]
```

- [ ] **Step 2: Run** `uv run pytest tests/test_dns_detect_intervals_sql.py -v`. Expected: FAIL (`intervals_insert_sql` not defined).
- [ ] **Step 3: Implement** in `sql.py`. The history body is copied from migration 000468's `domain_services_history`, with the domain filter replaced by the bucket filter.

```python
INTERVALS_TABLE = "domain_service_intervals"
COUNTS_TABLE = "provider_service_counts"
INTERVAL_COLUMNS = (
    "root_domain", "service_type", "provider_key", "provider_slug", "service_keys", "first_seen", "last_seen",
    "is_current", "evidence", "analyzers", "record_types", "confidence", "bucket", "computed_at",
)


def _in_bucket(column: str, bucket: int) -> str:
    return f"cityHash64({column}) %% {PARTITION_COUNT} = {int(bucket)}"


def _latest_results(database: str, bucket: int) -> str:
    """Result rows of each record's latest resolution in the bucket."""
    return f"""SELECT s.*
    FROM `{database}`.`{SERVICES_TABLE}` AS s
    INNER JOIN
    (
        SELECT root_domain, record_id, max(resolved_at) AS resolved_at
        FROM `{database}`.`{RESOLUTIONS_TABLE}`
        WHERE {_in_bucket('root_domain', bucket)}
        GROUP BY root_domain, record_id
    ) AS latest USING (root_domain, record_id, resolved_at)
    WHERE {_in_bucket('s.root_domain', bucket)}"""


def current_results_count_sql(database: str, bucket: int) -> str:
    """How many current result rows the bucket has: the empty-stage guard."""
    return f"SELECT count() FROM ({_latest_results(database, bucket)})"


def intervals_insert_sql(database: str, target: str, bucket: int) -> str:
    """The bucket's service periods into target, with the rules of the
    domain_services_history / domain_services_now views (migration 000468)."""
    columns = ", ".join(INTERVAL_COLUMNS)
    return f"""INSERT INTO `{database}`.`{target}` ({columns})
WITH cur AS
(
    {_latest_results(database, bucket)}
),
history AS
(
    SELECT
        root_domain,
        service_type,
        provider_key,
        anyIf(provider_slug, provider_slug != '') AS provider_slug,
        arraySort(groupUniqArrayIf(service_key, service_key != '')) AS service_keys,
        min(valid_from) AS first_seen,
        max(valid_to) AS last_seen,
        count() AS evidence,
        arraySort(groupUniqArray(analyzer)) AS analyzers,
        arraySort(groupUniqArray(record_type)) AS record_types,
        max(confidence) AS confidence
    FROM
    (
        SELECT
            *,
            sum(new_island) OVER (PARTITION BY root_domain, service_type, provider_key ORDER BY valid_from, valid_to ROWS UNBOUNDED PRECEDING) AS island
        FROM
        (
            SELECT
                *,
                valid_from >= addDays(max(valid_to) OVER (PARTITION BY root_domain, service_type, provider_key ORDER BY valid_from, valid_to ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 45) AS new_island
            FROM
            (
                SELECT c.*
                FROM cur AS c
                LEFT JOIN
                (
                    SELECT root_domain, service_type, groupArray((valid_from, valid_to)) AS covered
                    FROM cur
                    WHERE fallback = 0
                    GROUP BY root_domain, service_type
                ) AS primary USING (root_domain, service_type)
                WHERE c.fallback = 0
                   OR NOT arrayExists(w -> tupleElement(w, 1) <= c.valid_to AND tupleElement(w, 2) >= c.valid_from, primary.covered)
            )
        )
    )
    GROUP BY root_domain, service_type, provider_key, island
),
scans AS
(
    SELECT root_domain, CAST((groupArray(record_type), groupArray(last_scan)), 'Map(String, Date)') AS latest_scan
    FROM
    (
        SELECT root_domain, record_type, max(record_to) AS last_scan
        FROM `{database}`.`{RESOLUTIONS_TABLE}`
        WHERE {_in_bucket('root_domain', bucket)}
        GROUP BY root_domain, record_type
    )
    GROUP BY root_domain
)
SELECT
    h.root_domain, h.service_type, h.provider_key, h.provider_slug, h.service_keys, h.first_seen, h.last_seen,
    arrayExists(t -> h.last_seen >= s.latest_scan[t], h.record_types) AS is_current,
    h.evidence, h.analyzers, h.record_types, h.confidence, {int(bucket)} AS bucket, now64(3, 'UTC') AS computed_at
FROM history AS h
INNER JOIN scans AS s USING (root_domain)"""


def counts_insert_sql(database: str, target: str, intervals: str, bucket: int) -> str:
    """Distinct domains per provider key and service type, plus a '' service
    type row per provider key for the total, from the staged intervals."""
    return f"""INSERT INTO `{database}`.`{target}` (bucket, provider_slug, provider_key, service_type, domains_now, domains_ever, computed_at)
SELECT {int(bucket)}, provider_slug, provider_key, if(grouping(service_type) = 1, '', service_type) AS service_type,
       uniqExactIf(root_domain, is_current = 1), uniqExact(root_domain), now64(3, 'UTC')
FROM `{database}`.`{intervals}`
WHERE bucket = {int(bucket)} AND {_in_bucket('root_domain', bucket)}
GROUP BY GROUPING SETS ((provider_slug, provider_key, service_type), (provider_slug, provider_key))"""
```

  Both counts filters are redundant on a stage but make the SQL correct against the live table too. If `grouping()` is unsupported in this ClickHouse version, use `WITH TOTALS`-free `UNION ALL` of two GROUP BYs, and ledger the ruling.

- [ ] **Step 4: Run** the new test file. Expected: 6 passed. A test that fails on parity is a SQL bug, not a test to loosen.
- [ ] **Step 5: Run** `uv run pytest tests/test_dns_detect_*.py -q`. Expected: all pass.
- [ ] **Step 6: Commit:** `feat(dagster): bucket SQL for domain service intervals and provider counts`.

### Task 3: Rebuild step and the asset

**Files:**
- Create: `services/dagster_v3/src/dagster_v3/defs/dns_detect/intervals.py`
- Modify: `services/dagster_v3/src/dagster_v3/defs/dns_detect/assets.py` (new asset, job selection, defs)
- Create: `services/dagster_v3/tests/test_dns_detect_intervals.py`

**Interfaces:**
- Consumes (Task 2): `sql.intervals_insert_sql`, `sql.counts_insert_sql`, `sql.current_results_count_sql`, `sql.INTERVALS_TABLE`, `sql.COUNTS_TABLE`.
- Produces:
  - `intervals.rebuild_bucket(client, database: str, bucket: int, log, *, stage_suffix: str | None = None) -> dict` with keys `intervals`, `current`, `providers`, `unmapped_keys`;
  - the asset `domain_service_intervals_clickhouse`.

- [ ] **Step 1: Failing tests** (fake client that records statements and answers counts):

```python
import pytest

from dagster_v3.defs.dns_detect import assets, intervals, sql


class FakeClient:
    def __init__(self, staged: int, current: int) -> None:
        self.statements: list[tuple[str, dict | None]] = []
        self.staged, self.current = staged, current

    def execute(self, query: str, params=None, **_):
        self.statements.append((query, params))
        if query.startswith("SELECT count() FROM ("):
            return [[self.current]]
        if query.startswith("SELECT count(), countIf(is_current = 1)"):
            return [[self.staged, self.staged, 2, 1]]
        return []


class Log:
    def info(self, *a) -> None: ...


def kinds(client: FakeClient) -> list[str]:
    return [q.split("(")[0].split(" `")[0].strip() for q, _ in client.statements]


def test_rebuild_swaps_both_tables_after_both_inserts_and_drops_stages() -> None:
    c = FakeClient(staged=10, current=10)
    out = intervals.rebuild_bucket(c, "corpscout", 3, Log(), stage_suffix="x")
    qs = [q for q, _ in c.statements]
    swap_i = next(i for i, q in enumerate(qs) if "REPLACE PARTITION 3" in q and sql.INTERVALS_TABLE in q.split("FROM")[0])
    counts_insert_i = next(i for i, q in enumerate(qs) if q.startswith("INSERT INTO") and "provider_service_counts_stage_x" in q)
    assert counts_insert_i < swap_i
    assert sum("REPLACE PARTITION 3" in q for q in qs) == 2
    assert qs[-2:] == ["DROP TABLE IF EXISTS `corpscout`.`domain_service_intervals_stage_x`",
                       "DROP TABLE IF EXISTS `corpscout`.`provider_service_counts_stage_x`"]
    assert all(p == {} for q, p in c.statements if "%%" in q)
    assert out == {"intervals": 10, "current": 10, "providers": 2, "unmapped_keys": 1}


def test_rebuild_refuses_an_empty_stage_when_the_bucket_has_results() -> None:
    c = FakeClient(staged=0, current=5)
    with pytest.raises(ValueError, match="bucket 3"):
        intervals.rebuild_bucket(c, "corpscout", 3, Log(), stage_suffix="x")
    assert not any("REPLACE PARTITION" in q for q, _ in c.statements)
    assert c.statements[-1][0].startswith("DROP TABLE IF EXISTS")


def test_rebuild_accepts_an_empty_bucket_without_results() -> None:
    c = FakeClient(staged=0, current=0)
    intervals.rebuild_bucket(c, "corpscout", 3, Log(), stage_suffix="x")
    assert sum("REPLACE PARTITION 3" in q for q, _ in c.statements) == 2


def test_intervals_asset_is_in_the_resolver_job_after_the_resolver() -> None:
    job = assets.defs.resolve_job_def(assets.JOB_NAME)
    keys = {n.name for n in job.graph.node_defs}
    assert {"dns_record_services_clickhouse", "domain_service_intervals_clickhouse"} <= keys
    spec = assets.domain_service_intervals_clickhouse
    assert spec.partitions_def == assets.PARTITIONS
    assert dg_key("dns_record_services_clickhouse") in spec.asset_deps[dg_key("domain_service_intervals_clickhouse")]


def dg_key(name: str):
    import dagster as dg
    return dg.AssetKey(name)
```

- [ ] **Step 2: Run** `uv run pytest tests/test_dns_detect_intervals.py -v`. Expected: FAIL (module `intervals` missing).
- [ ] **Step 3: Implement** `intervals.py`:

```python
"""Rebuild one hash bucket of domain_service_intervals and provider_service_counts.

Both are derived from dns_record_services and replaced whole per bucket: stage
tables are filled, checked, then swapped in with REPLACE PARTITION, so a
failure leaves the live tables untouched. Every statement carrying %% is run
with params so clickhouse-driver renders it.
"""

import uuid

from dagster_v3.defs.dns_detect import sql

SETTINGS = {"max_memory_usage": 16_000_000_000, "max_threads": 8}


def rebuild_bucket(client, database: str, bucket: int, log, *, stage_suffix: str | None = None) -> dict:
    suffix = stage_suffix or uuid.uuid4().hex[:12]
    stage_iv = f"{sql.INTERVALS_TABLE}_stage_{suffix}"
    stage_ct = f"{sql.COUNTS_TABLE}_stage_{suffix}"
    try:
        client.execute(f"CREATE TABLE `{database}`.`{stage_iv}` AS `{database}`.`{sql.INTERVALS_TABLE}`")
        client.execute(f"CREATE TABLE `{database}`.`{stage_ct}` AS `{database}`.`{sql.COUNTS_TABLE}`")
        client.execute(sql.intervals_insert_sql(database, stage_iv, bucket), {}, settings=SETTINGS)
        staged, current, providers, unmapped = client.execute(
            f"SELECT count(), countIf(is_current = 1), uniqExactIf(provider_slug, provider_slug != ''), "
            f"uniqExactIf(provider_key, provider_slug = '') FROM `{database}`.`{stage_iv}`")[0]
        if staged == 0:
            results = client.execute(sql.current_results_count_sql(database, bucket), {})[0][0]
            if results:
                raise ValueError(f"bucket {bucket}: no intervals staged but {results} current result rows exist; not swapping")
        client.execute(sql.counts_insert_sql(database, stage_ct, stage_iv, bucket), {}, settings=SETTINGS)
        client.execute(f"ALTER TABLE `{database}`.`{sql.INTERVALS_TABLE}` REPLACE PARTITION {int(bucket)} FROM `{database}`.`{stage_iv}`")
        client.execute(f"ALTER TABLE `{database}`.`{sql.COUNTS_TABLE}` REPLACE PARTITION {int(bucket)} FROM `{database}`.`{stage_ct}`")
        log.info("bucket %d: %d intervals (%d current), %d providers, %d unmapped keys", bucket, staged, current, providers, unmapped)
        return {"intervals": staged, "current": current, "providers": providers, "unmapped_keys": unmapped}
    finally:
        client.execute(f"DROP TABLE IF EXISTS `{database}`.`{stage_iv}`")
        client.execute(f"DROP TABLE IF EXISTS `{database}`.`{stage_ct}`")
```

  Adjust the fake's `startswith` matches in the tests if the exact statement text differs. The tests assert order and guards, not formatting.

  In `assets.py`, add the asset and extend the job:

```python
@dg.asset(
    name="domain_service_intervals_clickhouse",
    group_name=GROUP_NAME,
    kinds={"clickhouse"},
    partitions_def=PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    pool="dns_detect_intervals",
    deps=[dns_record_services_clickhouse],
    description=(
        "Service usage periods per domain (corpscout.domain_service_intervals) and distinct domains per "
        "provider and service type (corpscout.provider_service_counts), migration 000470. Rebuilt whole per "
        "bucket from dns_record_services with the domain_services_history rules and swapped in by REPLACE PARTITION."
    ),
)
def domain_service_intervals_clickhouse(context: AssetExecutionContext, clickhouse: ClickhouseResource) -> dg.MaterializeResult:
    assert_clickhouse_tables_exist(clickhouse, database=RESOLVED_DATABASE,
                                   tables=(sql.RESOLUTIONS_TABLE, sql.SERVICES_TABLE, sql.INTERVALS_TABLE, sql.COUNTS_TABLE))
    bucket = sql.partition_bucket(context.partition_key)
    started = datetime.now(UTC)
    with clickhouse.get_connection() as client:
        counts = intervals.rebuild_bucket(client, RESOLVED_DATABASE, bucket, context.log)
    return dg.MaterializeResult(metadata={**counts, "seconds": round((datetime.now(UTC) - started).total_seconds(), 1)})
```

  - The job becomes `dg.define_asset_job(name=JOB_NAME, selection=dg.AssetSelection.assets(dns_record_services_clickhouse, domain_service_intervals_clickhouse))`.
  - `defs.assets` lists both.
  - Import `intervals` at the top.

- [ ] **Step 4: Run** `uv run pytest tests/test_dns_detect_*.py -q && uv run dg check defs`. Expected: all pass; defs valid.
- [ ] **Step 5: Commit:** `feat(dagster): domain_service_intervals asset rebuilds each bucket after the resolver`.

### Task 4: Backoffice types, helpers and queries

**Files:**
- Create: `services/backoffice/app/lib/domain-services.ts` (types, labels, pure helpers, parameter parsing)
- Create: `services/backoffice/app/lib/domain-services.server.ts` (ClickHouse queries)
- Create: `services/backoffice/tests/domain-services.test.ts`

**Interfaces:**
- Produces (`domain-services.ts`):
  - `type ServiceInterval = { serviceType: string; providerKey: string; providerSlug: string; serviceKeys: string[]; firstSeen: string; lastSeen: string; isCurrent: boolean; evidence: number; recordTypes: string[]; confidence: number }`
  - `type ServiceEvidence = { serviceType: string; providerKey: string; recordName: string; recordType: string; subject: string; ruleId: string; validFrom: string; validTo: string }`
  - `type DomainServices = { domain: string; resolved: boolean; intervals: ServiceInterval[]; evidence: ServiceEvidence[] }`
  - `type ProviderSummary = { slug: string; name: string; category: string; domainsNow: number; domainsEver: number; byType: Record<string, { now: number; ever: number }> }`
  - `type UnmappedKey = { providerKey: string; domainsNow: number; domainsEver: number }`
  - `type ProviderDomainRow = { domain: string; serviceTypes: string[]; serviceKeys: string[]; firstSeen: string; lastSeen: string; isCurrent: boolean }`
  - `type ProviderDomainsFilter = { serviceType: string; service: string; now: boolean; page: number }`
  - `SERVICE_TYPE_LABELS: Record<string, string>` covering: dns "DNS", email "Mail", email_security "Mail filtering", email_sending "Outbound mail", hosting "Hosting", cdn "CDN", paas "PaaS", iaas "IaaS", waf "WAF", ddos_protection "DDoS protection", dmarc_reporting "DMARC reporting", saas_verification "Verification".
  - `groupCurrent(intervals: ServiceInterval[]): { serviceType: string; label: string; intervals: ServiceInterval[] }[]`, in `SERVICE_TYPE_LABELS` key order, current only.
  - `parseProviderDomainsFilter(search: URLSearchParams): ProviderDomainsFilter`: an unknown service type becomes `""`, a service must match `^[a-z0-9.-]{1,120}$` or becomes `""`, `now` is true unless `now=0`, and the page is clamped to ≥ 1.
- Produces (`domain-services.server.ts`):
  - `getDomainServices(domain: string): Promise<DomainServices>`
  - `getProviderSummaries(): Promise<ProviderSummary[]>`
  - `getUnmappedKeys(limit?: number): Promise<UnmappedKey[]>`
  - `getProviderDomains(slug: string, filter: ProviderDomainsFilter): Promise<{ rows: ProviderDomainRow[]; total: number; page: number; pageSize: 50; byType: Record<string, { now: number; ever: number }>; byService: { service: string; now: number; ever: number }[] }>`

- [ ] **Step 1: Failing tests** (`chQuery` mocked as in `tests/company-domains.server.test.ts`):

```ts
import { beforeEach, describe, expect, it, vi } from "vitest";

const ch = vi.hoisted(() => ({ query: vi.fn() }));
vi.mock("~/lib/clickhouse.server", () => ({ chQuery: ch.query }));

import { groupCurrent, parseProviderDomainsFilter, type ServiceInterval } from "~/lib/domain-services";
import { getDomainServices, getProviderDomains, getProviderSummaries, getUnmappedKeys } from "~/lib/domain-services.server";

beforeEach(() => ch.query.mockReset());

const iv = (o: Partial<ServiceInterval>): ServiceInterval => ({
  serviceType: "dns", providerKey: "loopia", providerSlug: "loopia", serviceKeys: [], firstSeen: "2025-01-01",
  lastSeen: "2026-09-01", isCurrent: true, evidence: 1, recordTypes: ["NS"], confidence: 1, ...o,
});

describe("domain services helpers", () => {
  it("groups current intervals by service type in label order and drops ended ones", () => {
    const groups = groupCurrent([iv({ serviceType: "email" }), iv({}), iv({ isCurrent: false, providerKey: "old" })]);
    expect(groups.map((g) => g.serviceType)).toEqual(["dns", "email"]);
    expect(groups[0].intervals.map((i) => i.providerKey)).toEqual(["loopia"]);
  });

  it("parses provider domain filters defensively", () => {
    expect(parseProviderDomainsFilter(new URLSearchParams("type=bogus&service=a%20b&now=0&page=-4")))
      .toEqual({ serviceType: "", service: "", now: false, page: 1 });
    expect(parseProviderDomainsFilter(new URLSearchParams("type=dns&service=ionos.dns&page=3")))
      .toEqual({ serviceType: "dns", service: "ionos.dns", now: true, page: 3 });
  });
});

describe("domain services queries", () => {
  it("reads a domain's history, current flags and evidence with the domain as a parameter", async () => {
    ch.query
      .mockResolvedValueOnce([{ resolutions: 3 }])
      .mockResolvedValueOnce([{ service_type: "dns", provider_key: "loopia", provider_slug: "loopia", service_keys: ["loopia.dns"],
        first_seen: "2025-01-01", last_seen: "2026-09-01", evidence: "2", record_types: ["NS"], confidence: 1 }])
      .mockResolvedValueOnce([{ service_type: "dns", provider_key: "loopia", first_seen: "2025-01-01" }])
      .mockResolvedValueOnce([{ service_type: "dns", provider_key: "loopia", record_name: "a.se", record_type: "NS",
        subject: "ns1.loopia.se", rule_id: "r", valid_from: "2025-01-01", valid_to: "2026-09-01" }]);
    const out = await getDomainServices("a.se");
    expect(out.resolved).toBe(true);
    expect(out.intervals[0]).toMatchObject({ providerSlug: "loopia", isCurrent: true, evidence: 2 });
    expect(out.evidence[0].subject).toBe("ns1.loopia.se");
    for (const [sql, params] of ch.query.mock.calls) {
      expect(sql).not.toContain("a.se");
      expect(params).toMatchObject({ domain: "a.se" });
    }
  });

  it("reports an unresolved domain without querying its history", async () => {
    ch.query.mockResolvedValueOnce([{ resolutions: 0 }]);
    const out = await getDomainServices("never.se");
    expect(out).toEqual({ domain: "never.se", resolved: false, intervals: [], evidence: [] });
    expect(ch.query).toHaveBeenCalledTimes(1);
  });

  it("builds provider summaries with totals from the '' service type row", async () => {
    ch.query.mockResolvedValueOnce([
      { provider_slug: "ionos", name: "IONOS", category: "hosting", service_type: "", domains_now: "10", domains_ever: "12" },
      { provider_slug: "ionos", name: "IONOS", category: "hosting", service_type: "dns", domains_now: "9", domains_ever: "11" },
    ]);
    const [p] = await getProviderSummaries();
    expect(p).toEqual({ slug: "ionos", name: "IONOS", category: "hosting", domainsNow: 10, domainsEver: 12, byType: { dns: { now: 9, ever: 11 } } });
  });

  it("lists only unmapped keys", async () => {
    ch.query.mockResolvedValueOnce([{ provider_key: "ui-dns.xyz", domains_now: "5", domains_ever: "6" }]);
    await getUnmappedKeys();
    const [sql] = ch.query.mock.calls[0];
    expect(sql).toContain("provider_slug = ''");
    expect(sql).toContain("service_type = ''");
  });

  it("clamps a provider domains page past the end to the last page", async () => {
    ch.query
      .mockResolvedValueOnce([{ total: "120" }])                       // count
      .mockResolvedValueOnce([{ root_domain: "x.se", service_types: ["dns"], service_keys: [], first_seen: "2025-01-01", last_seen: "2026-09-01", is_current: 1 }])
      .mockResolvedValueOnce([])                                       // by type
      .mockResolvedValueOnce([]);                                      // by service
    const out = await getProviderDomains("ionos", { serviceType: "", service: "", now: true, page: 99 });
    expect(out.page).toBe(3);
    expect(ch.query.mock.calls[1][1]).toMatchObject({ slug: "ionos", offset: 100, limit: 50 });
  });
});
```

- [ ] **Step 2: Run** `npx vitest run tests/domain-services.test.ts`. Expected: FAIL (modules missing).
- [ ] **Step 3: Implement** `domain-services.ts`: the types above, `SERVICE_TYPE_LABELS`, `groupCurrent` and `parseProviderDomainsFilter`.
- [ ] **Step 4: Implement** `domain-services.server.ts`. All values go through `{name:Type}` params.

  **Queries for `getDomainServices`:**

  1. Whether the domain was resolved at all:

```sql
SELECT count() AS resolutions FROM corpscout.dns_record_resolutions WHERE root_domain = {domain:String}
```

     If it returns 0, return `{ domain, resolved: false, intervals: [], evidence: [] }`.
  2. The history:

```sql
SELECT service_type, provider_key, provider_slug, service_keys, toString(first_seen) AS first_seen,
       toString(last_seen) AS last_seen, evidence, record_types, confidence
FROM corpscout.domain_services_history(domain = {domain:String})
ORDER BY service_type, first_seen
```

  3. The current periods:

```sql
SELECT service_type, provider_key, toString(first_seen) AS first_seen
FROM corpscout.domain_services_now(domain = {domain:String})
```

     A history row is current when (service_type, provider_key, first_seen) matches one of these rows.
  4. The evidence. The domain filter sits inside the latest-resolution subquery, because `dns_record_services_current` has no filter of its own:

```sql
SELECT s.service_type, s.provider_key, s.record_name, s.record_type, s.subject, s.rule_id,
       toString(s.valid_from) AS valid_from, toString(s.valid_to) AS valid_to
FROM corpscout.dns_record_services AS s
INNER JOIN (SELECT root_domain, record_id, max(resolved_at) AS resolved_at FROM corpscout.dns_record_resolutions
            WHERE root_domain = {domain:String} GROUP BY root_domain, record_id) AS l USING (root_domain, record_id, resolved_at)
WHERE s.root_domain = {domain:String}
ORDER BY s.service_type, s.provider_key, s.valid_from
LIMIT 2000
```

  **`getProviderSummaries`:**

```sql
SELECT c.provider_slug AS provider_slug, any(p.provider_name) AS name, any(p.provider_category) AS category, c.service_type AS service_type,
       sum(c.domains_now) AS domains_now, sum(c.domains_ever) AS domains_ever
FROM corpscout.provider_service_counts AS c
LEFT JOIN (SELECT provider_slug, any(provider_name) AS provider_name, any(provider_category) AS provider_category
           FROM corpscout.provider_services FINAL GROUP BY provider_slug) AS p USING (provider_slug)
WHERE c.provider_slug != ''
GROUP BY c.provider_slug, c.service_type
```

  The rows are folded per slug: `service_type = ''` gives the totals, the others fill `byType`. Numbers arrive as strings (UInt64 JSON) and are converted with `Number`.

  **`getUnmappedKeys(limit = 100)`:**

```sql
SELECT provider_key, sum(domains_now) AS domains_now, sum(domains_ever) AS domains_ever
FROM corpscout.provider_service_counts
WHERE provider_slug = '' AND service_type = ''
GROUP BY provider_key ORDER BY domains_now DESC, provider_key LIMIT {limit:UInt32}
```

  **`getProviderDomains`:**
  - Shared filter: `provider_slug = {slug:String} AND ({type:String} = '' OR service_type = {type:String}) AND ({service:String} = '' OR has(service_keys, {service:String})) AND ({now:UInt8} = 0 OR is_current = 1)`.
  - Count: `SELECT uniqExact(root_domain) AS total … WHERE <filter>`.
  - Page: clamp `page` to `max(1, ceil(total / 50))`, then:

```sql
SELECT root_domain, arraySort(groupUniqArray(service_type)) AS service_types,
       arraySort(arrayDistinct(arrayFlatten(groupArray(service_keys)))) AS service_keys,
       toString(min(first_seen)) AS first_seen, toString(max(last_seen)) AS last_seen, max(is_current) AS is_current
FROM corpscout.domain_service_intervals
WHERE <filter>
GROUP BY root_domain ORDER BY root_domain LIMIT {limit:UInt32} OFFSET {offset:UInt32}
```

  - `byType`: `SELECT service_type, uniqExactIf(root_domain, is_current = 1) AS now, uniqExact(root_domain) AS ever FROM corpscout.domain_service_intervals WHERE provider_slug = {slug:String} GROUP BY service_type`.
  - `byService`: `SELECT service, uniqExactIf(root_domain, is_current = 1) AS now, uniqExact(root_domain) AS ever FROM corpscout.domain_service_intervals ARRAY JOIN service_keys AS service WHERE provider_slug = {slug:String} GROUP BY service ORDER BY ever DESC`.

- [ ] **Step 5: Run** the test file. Expected: 7 passed.
- [ ] **Step 6: Commit:** `feat(backoffice): domain services types and ClickHouse queries`.

### Task 5: Domain page Services tab

**Files:**
- Create: `services/backoffice/app/routes/admin-domain-services.tsx`
- Modify: `services/backoffice/app/routes.ts` (a `route("services", "routes/admin-domain-services.tsx")` child of `domains/:domain`, after `dns`)
- Modify: `services/backoffice/app/routes/admin-domain.tsx` (`suffix === "services"` → section `"services"`)
- Modify: `services/backoffice/app/components/detail/technology-section-tabs.tsx` (the `section` type accepts `"services"`; when `dnsRecords` is set, a `Services` trigger after "DNS records" linking to `${basePath}/services${search}`)
- Create: `services/backoffice/tests/domain-services-routes.test.tsx`

**Interfaces:**
- Consumes (Task 4): `getDomainServices`, `groupCurrent`, `SERVICE_TYPE_LABELS`, `DomainServices`.

- [ ] **Step 1: Failing tests** (pattern of `tests/domain-technology-routes.test.tsx`: mock the server module, call the loader, render with `renderToStaticMarkup` inside a `MemoryRouter`):

```tsx
import { renderToStaticMarkup } from "react-dom/server";
import { MemoryRouter } from "react-router";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { TechnologySectionTabs } from "~/components/detail/technology-section-tabs";

const server = vi.hoisted(() => ({ getDomainServices: vi.fn() }));
vi.mock("~/lib/domain-services.server", () => server);
const route = await import("~/routes/admin-domain-services");

beforeEach(() => vi.clearAllMocks());

const interval = { serviceType: "dns", providerKey: "loopia", providerSlug: "loopia", serviceKeys: ["loopia.dns"], firstSeen: "2025-01-01",
  lastSeen: "2026-09-01", isCurrent: true, evidence: 2, recordTypes: ["NS"], confidence: 1 };

function view(loaderData: unknown) {
  const Page = route.default as (p: { loaderData: unknown }) => React.ReactElement;
  return renderToStaticMarkup(<MemoryRouter><Page loaderData={loaderData} /></MemoryRouter>);
}

describe("domain services tab", () => {
  it("lowercases the domain for the query", async () => {
    server.getDomainServices.mockResolvedValue({ domain: "example.se", resolved: false, intervals: [], evidence: [] });
    await route.loader({ params: { domain: "EXAMPLE.SE" } } as never);
    expect(server.getDomainServices).toHaveBeenCalledWith("example.se");
  });

  it("shows current providers linked to their provider pages, history and evidence", () => {
    const html = view({ domain: "a.se", resolved: true,
      intervals: [interval, { ...interval, providerKey: "ui-dns.xyz", providerSlug: "", isCurrent: false, lastSeen: "2024-01-01" }],
      evidence: [{ serviceType: "dns", providerKey: "loopia", recordName: "a.se", recordType: "NS", subject: "ns1.loopia.se", ruleId: "r",
        validFrom: "2025-01-01", validTo: "2026-09-01" }] });
    expect(html).toContain('href="/admin/provider-feeds/providers/loopia/domains"');
    expect(html).toContain("ui-dns.xyz");
    expect(html).toContain("unmapped");
    expect(html).toContain("ns1.loopia.se");
  });

  it("distinguishes not resolved from resolved without providers", () => {
    expect(view({ domain: "a.se", resolved: false, intervals: [], evidence: [] })).toContain("not been resolved");
    expect(view({ domain: "a.se", resolved: true, intervals: [], evidence: [] })).toContain("No providers found");
  });

  it("adds a Services tab to the domain tabs", () => {
    const html = renderToStaticMarkup(<MemoryRouter><TechnologySectionTabs basePath="/admin/domains/a.se" section="services" dnsRecords /></MemoryRouter>);
    expect(html).toContain('href="/admin/domains/a.se/services"');
  });
});
```

- [ ] **Step 2: Run** `npx vitest run tests/domain-services-routes.test.tsx`. Expected: FAIL (route missing).
- [ ] **Step 3: Implement the route.**
  - **Loader:** `getDomainServices(params.domain.trim().toLowerCase())`.
  - **Page:**
    - Empty states: "This domain has not been resolved yet." or "No providers found in this domain's DNS records."
    - **Now** card: one section per `groupCurrent` group with its `label`. Each interval shows the provider as a `Link` to `/admin/provider-feeds/providers/${encodeURIComponent(slug)}/domains` when `providerSlug` is set, otherwise the key and an "unmapped" `Badge`. It also shows the service keys, "since {firstSeen}" and the confidence.
    - **History** card: a `Table` with columns service type label, provider, first seen, last seen, a "current"/"ended" badge, evidence and record types.
    - **Evidence:** in each history row, a native `<details>` listing the matching `evidence` rows (same service type and provider key) with record name, type, subject, rule and window.
  - Update the tabs component, `admin-domain.tsx` and `routes.ts` as listed under Files.
- [ ] **Step 4: Run** the test file, then `npm run typecheck`. Expected: pass; no type errors.
- [ ] **Step 5: Commit:** `feat(backoffice): domain Services tab with current providers, history and evidence`.

### Task 6: Providers list

**Files:**
- Create: `services/backoffice/app/routes/admin-providers.tsx`
- Modify: `services/backoffice/app/routes.ts` (`route("providers", "routes/admin-providers.tsx")`)
- Modify: `services/backoffice/app/components/admin/admin-sidebar.tsx` (a "Providers" item before "Provider feeds", `ServerIcon` or another lucide icon already imported, active on `/admin/providers`)
- Create: `services/backoffice/tests/providers-route.test.tsx`

**Interfaces:**
- Consumes (Task 4): `getProviderSummaries`, `getUnmappedKeys`, `SERVICE_TYPE_LABELS`.

- [ ] **Step 1: Failing tests:**
  - **The loader returns both lists.** When ClickHouse throws, it returns `{ error: message, providers: [], unmapped: [] }` rather than crashing.
  - **The page:**
    - renders a provider row linking to `/admin/provider-feeds/providers/ionos/domains`;
    - shows the totals;
    - filters by the `q` search param on name or slug (client-side, over the loaded list);
    - lists unmapped keys under the "Unmapped provider domains" heading.

```tsx
import { renderToStaticMarkup } from "react-dom/server";
import { MemoryRouter } from "react-router";
import { describe, expect, it, vi } from "vitest";

const server = vi.hoisted(() => ({ getProviderSummaries: vi.fn(), getUnmappedKeys: vi.fn() }));
vi.mock("~/lib/domain-services.server", () => server);
const route = await import("~/routes/admin-providers");

const providers = [
  { slug: "ionos", name: "IONOS", category: "hosting", domainsNow: 10, domainsEver: 12, byType: { dns: { now: 9, ever: 11 } } },
  { slug: "zoho", name: "Zoho", category: "email", domainsNow: 3, domainsEver: 3, byType: {} },
];

function view(loaderData: unknown, url = "/admin/providers") {
  const Page = route.default as (p: { loaderData: unknown }) => React.ReactElement;
  return renderToStaticMarkup(<MemoryRouter initialEntries={[url]}><Page loaderData={loaderData} /></MemoryRouter>);
}

describe("providers list", () => {
  it("reports ClickHouse errors instead of crashing", async () => {
    server.getProviderSummaries.mockRejectedValue(new Error("boom"));
    server.getUnmappedKeys.mockResolvedValue([]);
    expect(await route.loader()).toEqual({ error: "boom", providers: [], unmapped: [] });
  });

  it("links providers to their domains and lists unmapped keys", () => {
    const html = view({ providers, unmapped: [{ providerKey: "ui-dns.xyz", domainsNow: 5, domainsEver: 6 }] });
    expect(html).toContain('href="/admin/provider-feeds/providers/ionos/domains"');
    expect(html).toContain("Unmapped provider domains");
    expect(html).toContain("ui-dns.xyz");
  });

  it("filters by the search parameter", () => {
    const html = view({ providers, unmapped: [] }, "/admin/providers?q=zoh");
    expect(html).toContain("Zoho");
    expect(html).not.toContain("IONOS");
  });
});
```

- [ ] **Step 2: Run** `npx vitest run tests/providers-route.test.tsx`. Expected: FAIL (route missing).
- [ ] **Step 3: Implement the route.**
  - **Loader:** `Promise.all` of both queries in try/catch.
  - **Page:**
    - a search input (`Form` method get, name `q`), read with `useSearchParams`;
    - a providers `Table` with columns name (linked), category, now, ever, and per-type "now" counts for dns, email, email_sending, email_security, and hosting+cdn summed as "Web";
    - rows sorted by `domainsNow` descending;
    - an "Unmapped provider domains" card with a table of provider domain, now, ever;
    - an error `Alert` when `error` is set.
  - Add the sidebar item and the route.
- [ ] **Step 4: Run** the test file and `npm run typecheck`. Expected: pass.
- [ ] **Step 5: Commit:** `feat(backoffice): providers list with domain counts and unmapped provider domains`.

### Task 7: Provider Domains tab

**Files:**
- Create: `services/backoffice/app/routes/admin-provider-feeds-provider-domains.tsx`
- Create: `services/backoffice/app/components/admin/provider-tabs.tsx` (`ProviderTabs({ slug, active }: { slug: string; active: "definition" | "domains" })`: two `NavLink` tabs, "Definition & feeds" → `/admin/provider-feeds/providers/${slug}` and "Domains" → `…/domains`)
- Modify: `services/backoffice/app/routes/admin-provider-feeds-provider.tsx` (render `<ProviderTabs slug={doc.slug} active="definition" />` under the header)
- Modify: `services/backoffice/app/routes.ts` (`route("provider-feeds/providers/:slug/domains", "routes/admin-provider-feeds-provider-domains.tsx")`)
- Create: `services/backoffice/tests/provider-domains-route.test.tsx`

**Interfaces:**
- Consumes (Task 4): `getProviderDomains`, `parseProviderDomainsFilter`, `SERVICE_TYPE_LABELS`.
- Consumes (existing): `loadProviderDocument(slug)` from `~/lib/provider-recon.server`, which returns null for an unknown slug.

- [ ] **Step 1: Failing tests:**

```tsx
import { renderToStaticMarkup } from "react-dom/server";
import { MemoryRouter } from "react-router";
import { beforeEach, describe, expect, it, vi } from "vitest";

const server = vi.hoisted(() => ({ getProviderDomains: vi.fn() }));
const recon = vi.hoisted(() => ({ loadProviderDocument: vi.fn() }));
vi.mock("~/lib/domain-services.server", () => server);
vi.mock("~/lib/provider-recon.server", () => recon);
const route = await import("~/routes/admin-provider-feeds-provider-domains");

beforeEach(() => vi.clearAllMocks());

const page = { rows: [{ domain: "x.se", serviceTypes: ["dns"], serviceKeys: ["ionos.dns"], firstSeen: "2025-01-01", lastSeen: "2026-09-01", isCurrent: true }],
  total: 120, page: 3, pageSize: 50, byType: { dns: { now: 100, ever: 120 } }, byService: [{ service: "ionos.dns", now: 100, ever: 120 }] };

describe("provider domains tab", () => {
  it("is a 404 for an unknown provider without querying ClickHouse", async () => {
    recon.loadProviderDocument.mockResolvedValue(null);
    await expect(route.loader({ params: { slug: "nope" }, request: new Request("http://x/admin/provider-feeds/providers/nope/domains") } as never))
      .rejects.toMatchObject({ init: { status: 404 } });
    expect(server.getProviderDomains).not.toHaveBeenCalled();
  });

  it("passes the decoded slug and parsed filters", async () => {
    recon.loadProviderDocument.mockResolvedValue({ slug: "one-com", display_name: "One.com" });
    server.getProviderDomains.mockResolvedValue(page);
    await route.loader({ params: { slug: "one-com" },
      request: new Request("http://x/admin/provider-feeds/providers/one-com/domains?type=dns&page=9&now=0") } as never);
    expect(server.getProviderDomains).toHaveBeenCalledWith("one-com", { serviceType: "dns", service: "", now: false, page: 9 });
  });

  it("renders domains linked to their Services tab with paging", () => {
    const Page = route.default as (p: { loaderData: unknown }) => React.ReactElement;
    const html = renderToStaticMarkup(<MemoryRouter initialEntries={["/admin/provider-feeds/providers/ionos/domains?page=3"]}>
      <Page loaderData={{ provider: { slug: "ionos", name: "IONOS" }, filter: { serviceType: "", service: "", now: true, page: 3 }, ...page }} />
    </MemoryRouter>);
    expect(html).toContain('href="/admin/domains/x.se/services"');
    expect(html).toContain("Page 3 of 3");
    expect(html).toContain("ionos.dns");
  });
});
```

- [ ] **Step 2: Run** `npx vitest run tests/provider-domains-route.test.tsx`. Expected: FAIL.
- [ ] **Step 3: Implement.**
  - **Loader:**
    - `loadProviderDocument(params.slug)`; null → `throw data("Provider … not found.", { status: 404 })`.
    - `filter = parseProviderDomainsFilter(url.searchParams)`, then `getProviderDomains(slug, filter)`.
    - ClickHouse errors return `{ provider, filter, error, rows: [], total: 0, page: 1, pageSize: 50, byType: {}, byService: [] }`.
  - **Page:**
    - the header with the provider name, and `ProviderTabs`;
    - a summary card with per-type and per-service counts;
    - a filter `Form` (GET) with a service type select (the types in `byType`), a service select (`byService`) and a now/ever select;
    - the domain `Table` (domain linked to `/admin/domains/${domain}/services`, types, services, first seen, last seen, current badge);
    - Previous/Next links preserving the other params, and "Page {page} of {max(1, ceil(total / 50))}".
- [ ] **Step 4: Run** the test file, the existing `tests/provider-feeds*.test.ts*` files and `npm run typecheck`. Expected: pass.
- [ ] **Step 5: Commit:** `feat(backoffice): provider Domains tab`.

### Task 8: Rollout

- [ ] **Step 1: Migration number.** `ls clickhouse/migrations | tail` on main and `SELECT max(version) FROM corpscout.schema_migrations` on prod. If 470 is taken, renumber the files, the `EXPECTED_MIGRATIONS` entry and the test, then commit `fix(clickhouse): renumber …`.
- [ ] **Step 2: Apply the migration on prod** with the temp-dir recipe (main's migration files plus this one; docker migrate with `--add-host=companycollect:100.85.212.113`). Verify both tables exist with `SHOW CREATE TABLE`.
- [ ] **Step 3: Merge `domain-services-pages` into main** (fast-forward or merge; commit by path only; leave other sessions' WIP alone).
- [ ] **Step 4: Wait for a quiet dns-detect job.** `runsOrError(filter:{pipelineName:"dns_record_services_job", statuses:[QUEUED,STARTED]})` must return none.
- [ ] **Step 5: Deploy dagster_v3 from a pristine worktree of main** (the worktree deploy recipe: refresh the dbt state, wait for the deploy lock). Set the pool limit `dns_detect_intervals` to 2 with GraphQL `setConcurrencyLimit`.
- [ ] **Step 6: Time one partition.** Launch `domain_service_intervals_clickhouse` for `hash_003` alone and record its seconds and counts.
  - STOP if it takes longer than 10 minutes or hits the memory limit, and report (the sub-slice split in the spec's guard).
  - Otherwise backfill the other 127 partitions server-side.
- [ ] **Step 7: Spot checks.**
  - `domain_service_intervals WHERE root_domain IN ('hubspot.com','loopia.se')` against `domain_services_history(domain=…)`: identical.
  - IONOS: `sum(domains_now)` from `provider_service_counts` (service_type '') against `uniqExact(root_domain) … WHERE provider_slug='ionos' AND is_current`.
- [ ] **Step 8: Enable `dns_record_services_daily`** (GraphQL `startSchedule`).
- [ ] **Step 9: Backoffice.** Tell the owner that new route files need the 5183 dev server restarted. Verify on a second port (`npx react-router dev --port 5184`): the Services tab for loopia.se, `/admin/providers`, and the IONOS Domains tab.
- [ ] **Step 10: Update memory**: `domain-services-provider-model.md` and `MEMORY.md`.
