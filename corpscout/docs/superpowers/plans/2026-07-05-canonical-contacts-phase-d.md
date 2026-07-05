# Canonical Contact/Domain Tables — Phase D (Norway, Finland, wikidata) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give the three pure website-field sources their canonical `_company_contacts`/`_company_domains` pairs, derived ClickHouse-side from their existing websites tables — completing all per-source conversions so only Phase E (graph collapse) remains.

**Architecture:** All three sources already land their website data in ClickHouse (`no_websites`, `fi_websites`, `wikidata_company_websites` — all live graph feeds, untouched until Phase E). The canonical pairs are pure functions of those tables, so each source gets ONE lightweight derivation asset running two INSERT SELECTs through a new shared `replace_table_from_select` helper (stage + `EXCHANGE TABLES`, the pattern already reimplemented 4× privately in this repo). Migrations create the six tables AND backfill them with the same SELECTs, so data exists immediately rather than waiting for each source's next scheduled run. Wikidata is the only non-trivial arm: country joins from `wikidata_companies`, per-company primary election (its `is_primary_candidate` is always 1), and constant confidence 1.0 — the spec's decision-5 mapping is corrected by addendum (the source's `confidence` column is a hardcoded literal, not high/medium/low).

**Tech Stack:** Python 3.14 (`uv run`), ClickHouse SQL (window functions in INSERT SELECT), golang-migrate.

**Spec:** `corpscout/docs/superpowers/specs/2026-07-04-company-contacts-domains-standard-design.md` + the decision-5 addendum added in Task 1. Canonical DDL reference: `000088`. Conformance: `tests/canonical_contact_tables.py`.

## Global Constraints

- Work dir `corpscout/dagster_v3`; migrations in `corpscout/clickhouse/migrations/`. Numbers: three migrations at highest+1/+2/+3 AT EXECUTION TIME (000096 highest at planning; **000097 may be claimed any moment by the parallel session's view fix — check `ls` AND live `schema_migrations` before writing, and re-check at merge**).
- Per-source mapping (all rows `domain_source='website'`, `validation_method=''`, confidence **1.0**, `contact_type='website'`, `contact_type_raw=''`, `valid_to` = ended_on where the source has it):

| | registry_id | country | source_slug | source_field | is_current | notes |
|---|---|---|---|---|---|---|
| norway_brreg | org_number | 'NO' | 'norway_brreg' | 'hjemmeside' | passthrough (always 1) | registered_on/ended_on always NULL at source |
| finland_ytj | business_id | 'FI' | 'finland_ytj' | 'website' | passthrough (real) | `root_domain` is Nullable — filter `nullIf(trim(root_domain),'') IS NOT NULL`; valid_to = ended_on |
| wikidata | wikidata_id | LEFT JOIN `wikidata_companies.headquarters_country_iso2`, '' if unknown | 'wikidata' | 'official_website' | literal 1 (matches graph arm) | election required: is_primary_candidate is constant 1; dedupe per (registry_id, domain) |

- Facts AND conclusions (spec decision 2): each websites row yields a `contact_type='website'` fact (contact_value = website_url as given) and a domains row carrying url/normalized/host. Fact rows are NOT filtered by root_domain validity (a URL the register stated is a fact even if `root_domain` failed to parse — Finland's nullable column); domains rows require a non-empty domain.
- **Untouchables**: `no_websites`/`fi_websites`/`wikidata_company_websites` tables, their builders/exports, and `defs/domains/assets.py` (ANOTHER SESSION IS ACTIVELY EDITING that file — zero excuse to touch it; the graph reads legacy tables until Phase E).
- The shared helper `replace_table_from_select(clickhouse_client, *, qualified_table, columns, select_sql, log=None) -> int` lands in `src/dagster_v3/contact_extraction.py` (stage `CREATE TABLE AS target` → `INSERT INTO stage (cols) SELECT …` → `EXCHANGE` → drop stage in `finally`; returns rows written via `count()` on the exchanged table).
- Migration backfills duplicate the asset SELECTs by design (documented in both places): instant data + the asset overwrites identically on its next run. Any future edit must change both — each source's test pins that the asset SQL and migration SQL produce identical output shape.
- Verification per task: relevant pytest files green; `dg check defs` green after asset tasks; full-suite standard excludes (`--ignore=tests/test_exchange_rates_v2_dbt.py --ignore=tests/test_finland_xbrl_parsed_assets.py --ignore=tests/test_sweden_company_normalized_duckdb.py`) no new failures; ruff clean. Worktree quirk: copy gitignored `exchange_rates_v2/dbt/target/manifest.json` from the main checkout for full-suite/defs runs.
- Conventional Commits.

---

### Task 1: Spec addendum + shared `replace_table_from_select`

**Files:**
- Modify: `corpscout/docs/superpowers/specs/2026-07-04-company-contacts-domains-standard-design.md` (addendum)
- Modify: `src/dagster_v3/contact_extraction.py`
- Test: `tests/test_contact_extraction.py`

**Interfaces:**
- Produces: `replace_table_from_select(clickhouse_client, *, qualified_table: str, columns: Sequence[str], select_sql: str, log=None) -> int` — consumed by Task 3/4 assets and mirrored by Task 2's migration backfills.

- [ ] **Step 1: Spec addendum** — append under "Key decisions":

```markdown
## Addendum (2026-07-05, Phase D): decision 5 correction

`wikidata_company_websites.confidence` is a hardcoded literal `'wikidata'`
on every row (verified against the builder SQL and live data) — the
high/medium/low mapping described in decision 5 has nothing to map from.
Wikidata website rows take the standard explicit-website confidence **1.0**,
like every other register website field. `website_kind`/`validation_status`
are likewise constants and carry no signal; they do not survive into the
canonical shape. `is_primary_candidate` is constantly 1, so the canonical
`is_primary` comes from the standard election (one winner per wikidata_id).
```

- [ ] **Step 2: Failing tests for the helper**

```python
class _FakeStageExchangeClient:
    def __init__(self):
        self.statements = []

    def execute(self, sql, params=None):
        self.statements.append(" ".join(str(sql).split()))
        if "count()" in str(sql):
            return [(42,)]
        return []


def test_replace_table_from_select_stage_exchange_order():
    client = _FakeStageExchangeClient()
    written = contact_extraction.replace_table_from_select(
        client,
        qualified_table="corpscout.no_company_domains",
        columns=("a", "b"),
        select_sql="SELECT a, b FROM corpscout.no_websites",
    )
    assert written == 42
    joined = " || ".join(client.statements)
    create = next(i for i, s in enumerate(client.statements) if s.startswith("CREATE TABLE"))
    insert = next(i for i, s in enumerate(client.statements) if s.startswith("INSERT INTO"))
    exchange = next(i for i, s in enumerate(client.statements) if s.startswith("EXCHANGE TABLES"))
    drop = next(i for i, s in enumerate(client.statements) if s.startswith("DROP TABLE"))
    assert create < insert < exchange < drop, joined
    assert "(a, b)" in client.statements[insert]
    assert "AS corpscout.no_company_domains" in client.statements[create]


def test_replace_table_from_select_drops_stage_on_failure():
    class _FailingClient(_FakeStageExchangeClient):
        def execute(self, sql, params=None):
            super().execute(sql, params)
            if str(sql).strip().startswith("INSERT"):
                raise RuntimeError("boom")
            if "count()" in str(sql):
                return [(0,)]
            return []

    client = _FailingClient()
    import pytest

    with pytest.raises(RuntimeError):
        contact_extraction.replace_table_from_select(
            client,
            qualified_table="corpscout.no_company_domains",
            columns=("a",),
            select_sql="SELECT a FROM x",
        )
    assert any(s.startswith("DROP TABLE") for s in client.statements)  # finally-cleanup
```

- [ ] **Step 3: Implement** (mirror `replace_contact_table`'s stage-naming/uuid conventions in the same module; the stage name must be unique per invocation; EXCHANGE atomic; count query after exchange):

```python
def replace_table_from_select(
    clickhouse_client: Any,
    *,
    qualified_table: str,
    columns: Sequence[str],
    select_sql: str,
    log: Callable[..., object] | None = None,
) -> int:
    """Atomically replace a ClickHouse table's contents from a SELECT over
    other ClickHouse tables (stage CREATE AS target -> INSERT SELECT ->
    EXCHANGE -> drop stage). The canonical-pair derivation assets use this;
    per-source SELECTs are duplicated in their backfill migrations by design.
    """
    stage_table = f"{qualified_table}__derive_{uuid.uuid4().hex[:8]}"
    clickhouse_client.execute(f"CREATE TABLE {stage_table} AS {qualified_table}")
    try:
        clickhouse_client.execute(
            f"INSERT INTO {stage_table} ({_column_list(columns)}) {select_sql}"
        )
        clickhouse_client.execute(f"EXCHANGE TABLES {stage_table} AND {qualified_table}")
    finally:
        clickhouse_client.execute(f"DROP TABLE IF EXISTS {stage_table}")
    written = int(clickhouse_client.execute(f"SELECT count() FROM {qualified_table}")[0][0])
    if log is not None:
        log("Replaced %s from select: rows=%s", qualified_table, written)
    return written
```

(Adapt to the module's actual `_column_list` helper and fake-client result shapes — the tests pin the contract.)

- [ ] **Step 4: Verify + commit**

```bash
uv run pytest tests/test_contact_extraction.py -q && uv run ruff check src/dagster_v3/contact_extraction.py tests/test_contact_extraction.py
git add corpscout/docs/superpowers/specs/ corpscout/dagster_v3/src corpscout/dagster_v3/tests
git commit -m "feat(dagster): shared select-derivation replace helper; spec decision-5 correction"
```

---

### Task 2: Three migrations — canonical pairs with backfill

**Files:**
- Create: `0000NN_corpscout_no_canonical_contacts.{up,down}.sql`, `0000NN+1_corpscout_fi_canonical_contacts.{up,down}.sql`, `0000NN+2_corpscout_wikidata_canonical_contacts.{up,down}.sql`
- Modify: `tests/test_clickhouse_migrations.py` (three entries), `tests/test_canonical_contact_migrations.py` (three conformance tests)

**Interfaces:**
- Produces: six live canonical tables, backfilled. Tables: `no_company_contacts`/`no_company_domains`, `fi_company_contacts`/`fi_company_domains`, `wikidata_company_contacts`/`wikidata_company_domains`.

- [ ] **Step 1: Write the migrations.** Each up: `CREATE DATABASE...;` + two canonical CREATEs (from 000088 modulo prefix — real table names, no shadow needed: these are NEW tables) + two backfill `INSERT INTO … SELECT`. Downs: drop both tables. The backfill SELECTs (these exact six queries are ALSO the Task 3/4 asset SELECTs — keep them in lock-step):

Norway facts / domains:

```sql
INSERT INTO corpscout.no_company_contacts (country_iso2, source_slug, source_run_id, source_record_id, registry_id, contact_type, contact_type_raw, contact_value, source_field, is_current, valid_to, source_url, resolved_at)
SELECT 'NO', 'norway_brreg', source_run_id, source_record_id, org_number, 'website', '', website_url, 'hjemmeside', is_current, ended_on, '', now64(3, 'UTC')
FROM corpscout.no_websites;

INSERT INTO corpscout.no_company_domains (country_iso2, source_slug, source_run_id, source_record_id, registry_id, domain, domain_source, validation_method, confidence, website_url, website_normalized_url, website_host, is_current, is_primary, resolved_at)
SELECT 'NO', 'norway_brreg', source_run_id, source_record_id, org_number, root_domain, 'website', '', 1.0, website_url, website_normalized_url, website_host, is_current, is_primary, now64(3, 'UTC')
FROM corpscout.no_websites
WHERE nullIf(trim(root_domain), '') IS NOT NULL;
```

Finland: same shape with `business_id`, `'FI'`, `'finland_ytj'`, `'website'` as source_field, `valid_to = ended_on`, and the domains SELECT uses `ifNull(root_domain, '')`-guarded filter (`root_domain` is Nullable) selecting `root_domain` via `ifNull(root_domain, '')`.

Wikidata (both queries LEFT JOIN companies for country; domains adds dedupe + election):

```sql
INSERT INTO corpscout.wikidata_company_contacts (country_iso2, source_slug, source_run_id, source_record_id, registry_id, contact_type, contact_type_raw, contact_value, source_field, is_current, valid_to, source_url, resolved_at)
SELECT ifNull(companies.headquarters_country_iso2, ''), 'wikidata', websites.source_run_id, websites.source_record_id, websites.wikidata_id, 'website', '', websites.website_url, 'official_website', 1, NULL, '', now64(3, 'UTC')
FROM corpscout.wikidata_company_websites AS websites
LEFT JOIN corpscout.wikidata_companies AS companies ON companies.wikidata_id = websites.wikidata_id;

INSERT INTO corpscout.wikidata_company_domains (country_iso2, source_slug, source_run_id, source_record_id, registry_id, domain, domain_source, validation_method, confidence, website_url, website_normalized_url, website_host, is_current, is_primary, resolved_at)
SELECT country_iso2, 'wikidata', source_run_id, source_record_id, registry_id, domain, 'website', '', 1.0, website_url, website_normalized_url, website_host, 1, if(rn = 1, 1, 0), now64(3, 'UTC')
FROM (
    SELECT
        ifNull(companies.headquarters_country_iso2, '') AS country_iso2,
        websites.source_run_id AS source_run_id,
        websites.source_record_id AS source_record_id,
        websites.wikidata_id AS registry_id,
        websites.root_domain AS domain,
        websites.website_url AS website_url,
        websites.website_normalized_url AS website_normalized_url,
        websites.website_host AS website_host,
        row_number() OVER (PARTITION BY websites.wikidata_id ORDER BY length(websites.root_domain), websites.root_domain, websites.website_normalized_url) AS rn,
        row_number() OVER (PARTITION BY websites.wikidata_id, websites.root_domain ORDER BY websites.website_normalized_url) AS domain_rn
    FROM corpscout.wikidata_company_websites AS websites
    LEFT JOIN corpscout.wikidata_companies AS companies ON companies.wikidata_id = websites.wikidata_id
    WHERE nullIf(trim(websites.root_domain), '') IS NOT NULL
)
WHERE domain_rn = 1;
```

(One row per (wikidata_id, domain) via `domain_rn = 1`; exactly one primary per wikidata_id via `rn = 1` — election rule reduces to shortest/alphabetical since source/current/confidence are uniform. NOTE the subtlety: `rn` is computed over ALL rows but primaries are assigned after the `domain_rn` dedup — verify with a probe that the rn=1 row always survives the domain_rn filter (it does: rn=1 is the minimal (length, domain, url) row, which is also domain_rn=1 within its domain group — reason it through and add a live sanity check in Step 3).)

- [ ] **Step 2: Conformance tests + ledger entries** (three of each; helper against the six real table names in each up file).

- [ ] **Step 3: Live apply + verify.** Counts: `no_company_domains` == `no_websites` rows with non-empty root_domain; `fi_company_domains` == same for fi; facts == full source-table counts; wikidata: domains ≤ websites (dedupe), `countIf(is_primary=1)` == `countDistinct(registry_id)` (run the row-shape validator queries mentally: no empty domains, website columns populated). Paste all before/after numbers.

- [ ] **Step 4: Commit** (`feat(clickhouse): canonical contact/domain pairs for norway, finland, wikidata`).

---

### Task 3: Norway + Finland derivation assets

**Files:**
- Create: `src/dagster_v3/defs/norway_brreg/assets/contacts.py`, `src/dagster_v3/defs/finland_ytj/contacts.py`
- Modify: each source's asset registration + job selections (`norway_brreg/assets/{__init__.py,jobs.py}` — the canonical asset joins BOTH the full-snapshot and entity-updates jobs, deps on BOTH clickhouse assets, mirroring `norway_brreg_translation_load`'s wiring; `finland_ytj/resolved.py` or wherever `finland_ytj_resolved_job` is defined — dep + selection)
- Test: `tests/test_norway_brreg_definitions.py`, `tests/test_finland_ytj_*` (find the finland defs test), plus a new `tests/test_canonical_derivation_assets.py` for the SQL-shape pins

**Interfaces:**
- Consumes: Task 1's `replace_table_from_select`; Task 2's tables.
- Produces: assets `norway_brreg_clickhouse_canonical_contacts`, `finland_ytj_clickhouse_canonical_contacts` — each derives BOTH its tables and returns MaterializeResult metadata `{contacts, domains}`.

- [ ] **Step 1**: each module defines `build_contacts_select()` / `build_domains_select()` returning EXACTLY the migration's SELECT strings (the lock-step contract) and an asset calling `replace_table_from_select` twice (contacts first, domains second — domains-last matters for Phase E ordering consistency). Deps + group per source conventions (read the translation-loader wiring in each source as the template). Job selections: add the asset to `norway_brreg_entities_full_snapshot_job`, `norway_brreg_entity_updates_job`, and `finland_ytj_resolved_job` per each job's existing union style.

- [ ] **Step 2**: tests — SQL-shape pins (fragments: target columns tuple identity with shared tuples; the migration lock-step pin: read the migration file, extract its INSERT SELECT bodies, assert the asset's builder output matches modulo whitespace); job-membership pins updated; `dg check defs`.

- [ ] **Step 3**: live smoke — run both assets' derivations against live CH via a `uv run python` driver (foreground); counts must equal the Task 2 backfill counts (same data, same SELECT).

- [ ] **Step 4: Commit** (`feat(dagster): norway and finland canonical pair derivation assets`).

---

### Task 4: wikidata derivation asset + docs + full verification

**Files:**
- Create: `src/dagster_v3/defs/wikidata/contacts.py` (same shape; both SELECTs incl. the election subquery; asset `wikidata_clickhouse_canonical_contacts`, dep `wikidata_company_seed_clickhouse`, group `wikidata`, added to `wikidata_company_seed_weekly_job`'s selection)
- Modify: wikidata defs registration; `docs/data-source-guidelines.md` §8b (one line: website-field sources derive the canonical pair via `replace_table_from_select`, reference norway/finland/wikidata modules)
- Test: extend `tests/test_canonical_derivation_assets.py` + wikidata defs test pins

- [ ] **Step 1**: implement (lock-step with the wikidata migration SELECTs incl. election; live sanity probe: `countIf(is_primary=1) == countDistinct(registry_id)` and zero duplicate `(registry_id, domain)` pairs).
- [ ] **Step 2**: tests + `dg check defs` + full suite (standard excludes, manifest copied) no new failures + ruff.
- [ ] **Step 3**: live smoke (run the derivation; counts equal backfill).
- [ ] **Step 4: Commit** (`feat(dagster): wikidata canonical pair derivation asset`).

---

## Deployment note (not a code task)

After this phase ALL seven sources have live canonical pairs (cz/lv/br name-extracted; ee reshaped; no/fi/wikidata derived). The legacy websites tables and the domain graph remain untouched and consistent. Phase E then: collapse the graph's five hand-written arms into one templated SELECT over the seven uniform `_company_domains` tables (company_id_type from each source's REGISTRY_ID_TYPE), parity-gate against the current graph output, switch, then deprecate `fi_websites`/`no_websites`/`wikidata_company_websites`/`br_websites` (or demote them to internal stages where they still feed the derivations — decide in E; note no/fi/wikidata canonical pairs DERIVE from those tables, so E must either keep them as internal stages or move the derivation upstream into each source's native pipeline).
