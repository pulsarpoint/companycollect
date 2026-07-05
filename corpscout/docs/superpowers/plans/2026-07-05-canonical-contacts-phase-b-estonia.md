# Canonical Contact/Domain Tables — Phase B (Estonia) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reshape Estonia's existing `ee_company_contacts`/`ee_company_domains` to the canonical standard **without ever presenting the live domain graph an empty or missing table**, finish the denylist consolidation, and land the row-shape validator the Phase A review requested.

**Architecture:** Estonia already has the right table NAMES with old shapes, and `ee_company_domains` feeds the live domain graph — so unlike Phases A/C, the migration is data-preserving: shadow-table + `INSERT SELECT` column mapping + `EXCHANGE TABLES` (repo precedent: migration 000014), keeping the graph fed throughout. The DuckDB writers emit canonical columns (EE contact-type codes map to the closed vocabulary, with the original code preserved in `contact_type_raw`); the internal DuckDB contacts table keeps trailing enrichment columns (domain/domain_source) that `company_domains` consumes but the export doesn't ship. The graph's ee UNION arm gets a two-identifier lockstep rename (`reg_code`→`registry_id`) — NOT the Phase E architecture change. Estonia's local denylist dies; both drift-guard subsets become identities.

**Tech Stack:** Python 3.14 (`uv run`), DuckDB SQL over the ~4.5 GB yldandmed JSON (monthly job), ClickHouse (golang-migrate), truncate+insert exports (Estonia's existing pattern).

**Spec:** `corpscout/docs/superpowers/specs/2026-07-04-company-contacts-domains-standard-design.md` (Estonia row + decisions 1, 2, 6). Canonical DDL reference: `000088_corpscout_cz_canonical_contacts.up.sql`. Data-preserving reshape precedent: `000014_corpscout_fi_names_history_order_key.up.sql`.

## Global Constraints

- Work dir `corpscout/dagster_v3` (`uv run`); migrations in `corpscout/clickhouse/migrations/`. Migration number = highest on disk + 1 AT EXECUTION TIME (000095 at planning, 000093 is a legitimate gap; expect 000096 — this repo has had THREE parallel-session number collisions, re-verify and also confirm the live `schema_migrations` version before applying).
- Estonia mapping (spec decisions): `registry_id = reg_code`; `country_iso2='EE'`, `source_slug='estonia_ar'` (existing values, carried through); `source_field='sidevahendid'` (the register JSON array all contacts come from); `contact_type` mapping WWW→`website`, EMAIL→`email`, TEL→`phone`, MOB→`mobile`, FAX→`fax`, MUU→`other`; `contact_type_raw` = the original EE code (`WWW`, `EMAIL`, …); `valid_to` = old `end_date`; `contact_type_en` dies.
- Domains rows: `validation_method=''` (inference, not validation); `confidence` = `WEBSITE_CONFIDENCE (1.0)` for `domain_source='website'`, `EMAIL_UNIQUE_CONFIDENCE (0.9)` for `'email'` — constants imported from the shared module, interpolated into SQL like Brazil does. Election unchanged (website > is_current > shortest > alphabetical — equals the spec rule since confidence correlates with source here).
- **Graph continuity invariant**: at no point may `corpscout.ee_company_domains` be absent or empty while the old-shape graph SQL could run; the migration EXCHANGE is atomic, and the graph's ee arm is updated in the same branch (lockstep rename only: `websites.reg_code` → `websites.registry_id`; the `company_id_type` literal STAYS `'reg_code'` — it names the id semantics, not the column).
- Denylist consolidation completes: `resources.py` loses `EMAIL_PROVIDER_DENYLIST`/`EMAIL_DOMAIN_MAX_COMPANIES`; imports from `dagster_v3.contact_extraction`. Behavior change (approved, symmetric to Brazil's): Estonia now also rejects the Brazilian portal domains. The drift-guard test becomes two identity assertions.
- Row-shape validator (Phase A review follow-up) lands in `tests/canonical_contact_tables.py` and is exercised by Estonia's tests — Estonia is the first source with website-sourced rows, the case Phase A couldn't test.
- NO full Estonia pipeline re-run required: the migration preserves and reshapes existing live data; the next monthly run (8th, 06:00) writes canonical shapes natively. Exports are truncate+insert into the (reshaped) tables — column lists must match the canonical DDL after this branch.
- Verification per task: `uv run pytest tests/test_estonia_ar_contacts.py tests/test_canonical_contact_migrations.py tests/test_clickhouse_migrations.py tests/test_contact_extraction.py tests/test_domains_assets.py -q` green; `dg check defs` green after asset-touching tasks; ruff clean. Full-suite excludes: `--ignore=tests/test_exchange_rates_v2_dbt.py --ignore=tests/test_finland_xbrl_parsed_assets.py --ignore=tests/test_sweden_company_normalized_duckdb.py`. Worktree quirk: gitignored `exchange_rates_v2/dbt/target/manifest.json` may need copying from the main checkout for full-suite/defs runs.
- Conventional Commits.

---

### Task 1: Data-preserving reshape migration

**Files:**
- Create: `corpscout/clickhouse/migrations/0000NN_corpscout_ee_canonical_contacts.up.sql` + `.down.sql`
- Modify: `tests/test_clickhouse_migrations.py` (EXPECTED entry), `tests/test_canonical_contact_migrations.py` (ee conformance test)

**Interfaces:**
- Produces: live `ee_company_contacts` (canonical 13-col) and `ee_company_domains` (canonical 15-col), BOTH containing the reshaped existing data (domains row count unchanged; graph keeps working on old SQL until Task 3 deploys).

- [ ] **Step 1: Write the up migration**

For EACH table, the 000014 pattern: create shadow with canonical DDL (copied from 000088 modulo `cz_`→`ee_`), `INSERT INTO shadow SELECT <mapping> FROM old`, `EXCHANGE TABLES`, drop shadow. The mappings:

```sql
-- ee_company_contacts__canonical shadow, then:
INSERT INTO corpscout.ee_company_contacts__canonical
SELECT
    country_iso2,
    source_slug,
    source_run_id,
    source_record_id,
    reg_code AS registry_id,
    multiIf(
        contact_type = 'WWW', 'website',
        contact_type = 'EMAIL', 'email',
        contact_type = 'TEL', 'phone',
        contact_type = 'MOB', 'mobile',
        contact_type = 'FAX', 'fax',
        'other'
    ) AS contact_type,
    contact_type AS contact_type_raw,
    contact_value,
    'sidevahendid' AS source_field,
    is_current,
    end_date AS valid_to,
    source_url,
    now64(3, 'UTC') AS resolved_at
FROM corpscout.ee_company_contacts;
```

(The old table's `domain`/`domain_source` columns are simply not selected — they die. The old table has no `resolved_at` — backfill with migration time.)

```sql
-- ee_company_domains__canonical shadow, then:
INSERT INTO corpscout.ee_company_domains__canonical
SELECT
    country_iso2,
    source_slug,
    source_run_id,
    source_record_id,
    reg_code AS registry_id,
    domain,
    domain_source,
    '' AS validation_method,
    multiIf(domain_source = 'website', 1.0, 0.9) AS confidence,
    website_url,
    website_normalized_url,
    website_host,
    is_current,
    is_primary,
    resolved_at
FROM corpscout.ee_company_domains;
```

Down: for each table, shadow with the OLD DDL (contacts: 000027 columns + the 000028 `domain`/`domain_source` columns appended, unversioned `ReplacingMergeTree`; domains: 000029 verbatim), `INSERT SELECT` reverse-mapping what's reversible (`registry_id`→`reg_code`, `contact_type_raw`→`contact_type`, `contact_type_en` rebuilt via `multiIf` on the raw code from the `EE_CONTACT_TYPE_EN_BY_CODE` values, `valid_to`→`end_date`, domains: drop validation_method/confidence; contacts down sets `domain=''`/`domain_source=''` — that enrichment is honestly lost and refills on the next monthly run), EXCHANGE, drop shadow.

- [ ] **Step 2: Conformance test + ledger**

Add `test_ee_canonical_migration_conforms` (helper against `ee_company_contacts`/`ee_company_domains` in the new up file — the helper matches `CREATE TABLE IF NOT EXISTS corpscout.<table>` so the SHADOW create must use the final names… it can't: the shadow is `__canonical`-suffixed. Adjust: the conformance test extracts the shadow CREATE (`ee_company_contacts__canonical`) — extend `_read`/the helper call with the shadow name and assert against the same canonical column spec; the helper's table-name parameter already supports this, pass `"ee_company_contacts__canonical"`). Append the EXPECTED entry; run migration tests.

- [ ] **Step 3: Live apply + verification**

Before: record `count()` of both tables and `SELECT count() FROM corpscout.company_website_domains WHERE source_slug='estonia_ar'`. Apply (`make clickhouse-migrate-up`). After: both tables exist under canonical DDL (`DESCRIBE`), domains count UNCHANGED, contacts count UNCHANGED, spot-check 3 rows (contact_type in vocabulary, contact_type_raw carries EE codes, confidence 1.0/0.9 split matches domain_source). The graph has NOT rebuilt yet (its ee SQL is now stale against the renamed column — acceptable: it only runs with its job, and Task 3 fixes it in this same branch; note the window in the report).

- [ ] **Step 4: Commit**

```bash
git add corpscout/clickhouse/migrations/ corpscout/dagster_v3/tests/
git commit -m "feat(clickhouse): reshape estonia contact/domain tables to canonical, data-preserving"
```

---

### Task 2: Estonia writers → canonical output + denylist identity + row-shape validator

**Files:**
- Modify: `src/dagster_v3/defs/estonia_ar/contacts.py`, `company_domains.py`, `resources.py`, `tables.py`, `clickhouse.py` (export column lists)
- Modify: `tests/canonical_contact_tables.py` (row-shape validator), `tests/test_estonia_ar_contacts.py`, `tests/test_contact_extraction.py` (drift test → identities)

**Interfaces:**
- Consumes: shared `EMAIL_PROVIDER_DENYLIST`, `EMAIL_DOMAIN_MAX_COMPANIES`, `WEBSITE_CONFIDENCE`, `EMAIL_UNIQUE_CONFIDENCE`, `CONTACT_TYPE_VALUES`, `COMPANY_CONTACTS_COLUMNS`, `COMPANY_DOMAINS_COLUMNS`.
- Produces: DuckDB `company_contacts` with the canonical 13 columns FIRST plus trailing internal `domain`, `domain_source` enrichment columns (export ships only the canonical 13); DuckDB `company_domains` with exactly the canonical 15; new helpers `assert_canonical_contact_row(row)` / `assert_canonical_domain_row(row)` in `tests/canonical_contact_tables.py`.

- [ ] **Step 1: resources.py** — delete `EMAIL_PROVIDER_DENYLIST`/`EMAIL_DOMAIN_MAX_COMPANIES` (import from shared at the use sites); replace `EE_CONTACT_TYPE_EN_BY_CODE` with:

```python
# Canonical contact_type by the register's sidevahendid code; the raw code is
# preserved in contact_type_raw (spec: contacts standard).
EE_CONTACT_TYPE_BY_CODE = {
    "WWW": "website",
    "EMAIL": "email",
    "TEL": "phone",
    "MOB": "mobile",
    "FAX": "fax",
    "MUU": "other",
}
```

- [ ] **Step 2: contacts.py** — the build SQL's final select emits, in order: the canonical 13 (with `registry_id`, mapped `contact_type` via a `CASE` built from `EE_CONTACT_TYPE_BY_CODE`, `contact_type_raw` = raw code, `source_field='sidevahendid'`, `valid_to` = old end_date expression, `resolved_at` = `now()`), THEN the existing `domain`, `domain_source` enrichment columns (internal-only). The enrichment CTE logic (root_domain for WWW, email-suffix + uniqueness + denylist for EMAIL) is unchanged except the denylist literal now comes from the shared import. Update `tables.py`: `EE_COMPANY_CONTACTS_EXPORT_COLUMNS = COMPANY_CONTACTS_COLUMNS` (identity), keep an internal `EE_COMPANY_CONTACTS_STAGE_COLUMNS = COMPANY_CONTACTS_COLUMNS + ("domain", "domain_source")` for the DuckDB-side assertions.

- [ ] **Step 3: company_domains.py** — reads the internal enrichment columns as before (adjust column name `reg_code`→`registry_id` where it reads the contacts stage); final select emits the canonical 15 with `''` validation_method and `multiIf`-equivalent DuckDB `CASE` confidence (interpolate the shared constants); election SQL unchanged. `tables.py`: `EE_COMPANY_DOMAINS_EXPORT_COLUMNS = COMPANY_DOMAINS_COLUMNS`.

- [ ] **Step 4: clickhouse.py** — exports select the canonical column lists explicitly (contacts export must NOT ship the trailing internal columns — verify how `export_duckdb_connection_table_to_clickhouse` picks columns; pass the explicit list the way Brazil's exports do).

- [ ] **Step 5: Row-shape validator** in `tests/canonical_contact_tables.py`:

```python
def assert_canonical_contact_row(row) -> None:
    assert len(row) == len(COMPANY_CONTACTS_COLUMNS)
    values = dict(zip(COMPANY_CONTACTS_COLUMNS, row))
    from dagster_v3.contact_extraction import CONTACT_TYPE_VALUES

    assert values["contact_type"] in CONTACT_TYPE_VALUES
    assert values["registry_id"] != ""


def assert_canonical_domain_row(row) -> None:
    assert len(row) == len(COMPANY_DOMAINS_COLUMNS)
    values = dict(zip(COMPANY_DOMAINS_COLUMNS, row))
    from dagster_v3.contact_extraction import (
        DOMAIN_SOURCE_VALUES,
        VALIDATION_METHOD_VALUES,
    )

    assert values["domain_source"] in DOMAIN_SOURCE_VALUES
    assert values["validation_method"] in VALIDATION_METHOD_VALUES
    assert 0.0 < values["confidence"] <= 1.0
    assert values["domain"] != "" and values["registry_id"] != ""
    if values["domain_source"] != "website":
        assert values["website_url"] == ""
        assert values["website_normalized_url"] == ""
        assert values["website_host"] == ""
    assert values["is_primary"] in (0, 1)
```

- [ ] **Step 6: Tests** — `test_estonia_ar_contacts.py` (read it first; it drives `_build_contacts_from_json` with sample JSON): update expectations to canonical columns (mapped types, raw codes, source_field, valid_to), run every produced row through the two validators, assert website-sourced domain rows carry the real URL columns and confidence 1.0 while email rows carry '' and 0.9, and the exported column subset excludes the internal enrichment pair. Drift test in `test_contact_extraction.py`: both country assertions become identity (`is`), rename to `test_shared_denylist_is_single_source_of_truth`.

- [ ] **Step 7: Verify + commit**

```bash
uv run pytest tests/test_estonia_ar_contacts.py tests/test_contact_extraction.py tests/test_canonical_contact_migrations.py -q
uv run ruff check src/dagster_v3/defs/estonia_ar/ tests/
git add src/dagster_v3/defs/estonia_ar/ tests/
git commit -m "feat(dagster): estonia writers emit canonical contact/domain shapes; denylist single-source"
```

---

### Task 3: Graph ee-arm lockstep rename + full verification

**Files:**
- Modify: `src/dagster_v3/defs/domains/assets.py` (ee UNION arm ONLY: `websites.reg_code` → `websites.registry_id` in the two places it appears — the `company_id` select and the synthetic `source_website_id` concat; `company_id_type` literal STAYS `'reg_code'`)
- Modify: `tests/test_domains_assets.py` (pin update)
- Modify: `docs/data-source-guidelines.md` §8b one-liner if it references Estonia's old shape (check)

**Interfaces:**
- Consumes: Task 1's live reshaped table.
- Produces: a domain graph whose ee arm works against the canonical column names; everything else in `domains/assets.py` untouched (Phase E does the real collapse).

- [ ] **Step 1: The rename** — exactly two identifier edits in the ee arm; diff must show nothing else in the file.

- [ ] **Step 2: Tests** — update `test_domains_assets.py`'s ee-branch pin (it asserts generated SQL content); run the domains tests.

- [ ] **Step 3: Live graph verification** — record `count()` of `corpscout.company_website_domains WHERE source_slug='estonia_ar'` and of `corpscout.domains`; then run the domains rebuild end-to-end against live ClickHouse (mirror how `domains_clickhouse` calls `replace_domain_clickhouse_tables` — it's a full recompute over CH tables only, minutes, atomic EXCHANGE; use a `uv run python` driver like previous live re-runs). After: estonia_ar row count within ±1% of before (the underlying ee_company_domains data is identical — expect exact match), `corpscout.domains` total row count within noise of before. Paste before/after. If the runtime environment can't reach ClickHouse or the rebuild is unexpectedly heavy, STOP and report instead of leaving the graph half-verified.

- [ ] **Step 4: Full verify + commit**

```bash
uv run pytest tests/test_estonia_ar_contacts.py tests/test_domains_assets.py tests/test_canonical_contact_migrations.py tests/test_clickhouse_migrations.py tests/test_contact_extraction.py -q
uv run dg check defs 2>&1 | tail -1
uv run pytest --ignore=tests/test_exchange_rates_v2_dbt.py --ignore=tests/test_finland_xbrl_parsed_assets.py --ignore=tests/test_sweden_company_normalized_duckdb.py -q 2>&1 | tail -2
uv run ruff check src/dagster_v3/defs/domains/ tests/
git add src/dagster_v3/defs/domains/ tests/ docs/
git commit -m "feat(dagster): domain graph reads estonia canonical registry_id"
```

---

## Deployment note (not a code task)

The migration reshapes live data in Task 1, so there's a window where the deployed (old) graph SQL references `reg_code` against the renamed column — the graph only runs with its job, fails loudly if triggered, and Task 3 + deploy closes it. Deploy this branch to the dagster box together with everything else pending. Estonia's next monthly run (8th) writes canonical shapes natively; no manual re-run needed. After Phase B: only Phase D (Norway/Finland/wikidata — the `registered_on` decision documented in this session goes in that plan) and Phase E (graph collapse: templated SELECT over uniform tables, "domains swaps last", parity gate) remain.

## PROD DEPLOY RUNBOOK (added post-review — sequence BEFORE the 8th monthly job)

The dagster box's migration ledger is independent of the lab's and sits below 95.
When `make clickhouse-migrate-up` runs there it WILL dirty-fail at 000095
(br_cvm_financial_metrics view SQL is broken: ClickHouse err 184, sum() in
WHERE — confirmed by executing the SELECT body). Recovery (the exact sequence
the lab exercised):

1. Expect the failure at 95; verify the view is simply absent:
   `SELECT name FROM system.tables WHERE database='corpscout' AND name='br_cvm_financial_metrics'` → empty.
2. `make clickhouse-migrate-force VERSION=95`
3. `make clickhouse-migrate-up` → applies 000096 (Estonia reshape; alias-free,
   idempotent via shadow-drop guards; safe against prod's old-shape tables).
4. `br_cvm_financial_metrics` stays ABSENT until the Brazil-CVM workstream
   ships corrected view SQL as a NEW migration (000097+). Do not fix it here.

Deployment-window note: if Estonia's monthly job (8th, 06:00) fires with
mismatched writer/table shapes in either direction, the export fails LOUDLY
at the stage-insert (no corruption; stage dropped in finally) — acceptable,
but sequence the deploy before the 8th to avoid the noise. Historical note:
000095's file was rewritten after the lab force-mark (forward-only-ledger
exception) — justified because no environment ever executed the original SQL.
