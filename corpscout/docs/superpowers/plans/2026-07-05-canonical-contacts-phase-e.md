# Canonical Contact/Domain Tables — Phase E (Graph Switch) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** The domain graph reads only the seven uniform `<src>_company_domains` tables through one templated SELECT — Czech and Latvia enter the graph for the first time, the five hand-written per-source arms disappear, and the legacy websites tables are demoted to documented internal stages (user decision: no drops, no pipeline surgery).

**Architecture:** `_company_website_domains_insert_sql` in `defs/domains/assets.py` becomes a loop over a seven-entry config (`table`, `registry_id_type`, `source_slug`) emitting identical UNION arms — country comes from each table's own `country_iso2` column (the wikidata LEFT JOIN dies; Phase D baked country in), `domain_source` passes through everywhere, and the graph asset's deps repoint to the seven canonical-pair producers. The switch is parity-gated live: a baseline snapshot of today's `(source_slug, company_id, root_domain)` edges must be a subset of the new build, with the only additions being cz/lv's debut (~6.1k edges). The EXCHANGE order is corrected so the links table swaps before the aggregate built from it.

**Tech Stack:** Python 3.14 (`uv run`), ClickHouse SQL, existing stage/EXCHANGE machinery in `domains/assets.py` (unchanged).

**Spec:** `corpscout/docs/superpowers/specs/2026-07-04-company-contacts-domains-standard-design.md` — decision 7 + migration-strategy Phase E bullet (incl. the derivation caveat added in Phase D).

## Global Constraints

- Work dir `corpscout/dagster_v3` (`uv run`). NO new migrations (nothing drops; `company_website_domains`/`domains` schemas unchanged).
- The seven-source config (order = program order; slug/id-type are config literals — data-column slugs are NOT trusted because `ee_company_contacts` carries an outlier slug):

| table | registry_id_type | source_slug | country |
|---|---|---|---|
| cz_company_domains | ico | czech_ares | from column |
| lv_company_domains | regcode | latvia_ur | from column |
| ee_company_domains | reg_code | estonia_ar | from column |
| br_company_domains | cnpj_basico | brazil_rfb | from column |
| no_company_domains | org_number | norway_brreg | from column |
| fi_company_domains | business_id | finland_ytj | from column |
| wikidata_company_domains | wikidata_id | wikidata | from column ('' → NULL) |

- `company_id_type` literals preserve TODAY's graph values (reg_code, business_id, org_number, cnpj_basico, wikidata_id) + the cz/lv module constants (`ico`, `regcode` — cross-check against `czech_ares/contacts.py`/`latvia_ur/contacts.py` `REGISTRY_ID_TYPE` in a test).
- New deps for `domains_clickhouse` (replacing all five current ones): `czech_ares_clickhouse_company_contacts`, `latvia_ur_clickhouse_company_contacts`, `estonia_ar_clickhouse_company_domains`, `brazil_comp_rfb_clickhouse_company_domains`, `norway_brreg_clickhouse_canonical_contacts`, `finland_ytj_clickhouse_canonical_contacts`, `wikidata_clickhouse_canonical_contacts`.
- EXCHANGE-order fix: links table swaps FIRST, aggregate `domains` table second (aggregate computed from links must never be newer than the links readers see) — with a comment stating the requirement.
- **Parity gate (blocking)**: baseline = live `SELECT DISTINCT source_slug, company_id, root_domain FROM corpscout.company_website_domains` snapshotted to a temp table before the switch; after the new build: (a) zero baseline edges missing; (b) additions only under slugs czech_ares/latvia_ur (small drift under other slugs needs explanation from source-table resolved_at, not hand-waving); (c) `corpscout.domains` count movement consistent with new cz/lv domains. Temp table dropped after. If (a) fails → STOP, report, do not leave the new build live without flagging.
- Legacy demotion is DOCUMENTATION ONLY: `fi_websites`/`no_websites`/`wikidata_company_websites` builders+tables keep running (the canonical derivations read them); `br_websites` keeps running but now has ZERO consumers (note it as retire-with-future-migration). No behavior/DDL changes to any of them.
- Verification: `uv run pytest tests/test_domains_assets.py tests/test_canonical_derivation_assets.py -q` + full suite standard excludes (`--ignore=tests/test_exchange_rates_v2_dbt.py --ignore=tests/test_finland_xbrl_parsed_assets.py --ignore=tests/test_sweden_company_normalized_duckdb.py`); `dg check defs` (manifest quirk: copy gitignored `exchange_rates_v2/dbt/target/manifest.json` from main checkout); ruff clean.
- Conventional Commits.

---

### Task 1: Templated graph SQL + deps + exchange order + tests

**Files:**
- Modify: `src/dagster_v3/defs/domains/assets.py`, `src/dagster_v3/defs/domains/tables.py` (config lives here)
- Test: `tests/test_domains_assets.py`

**Interfaces:**
- Produces: `CANONICAL_DOMAIN_SOURCES` config tuple in `domains/tables.py`; the templated `_company_website_domains_insert_sql`; consumed by Task 2's live run.

- [ ] **Step 1: Config in `domains/tables.py`:**

```python
# Phase E: the graph reads ONLY the seven canonical <src>_company_domains
# tables (spec decision 7). slug/id-type are config literals, not data
# columns — ee_company_contacts carries a legacy slug outlier and the
# graph's provenance values must stay stable regardless of source data.
CANONICAL_DOMAIN_SOURCES: tuple[dict[str, str], ...] = (
    {"table": "cz_company_domains", "registry_id_type": "ico", "source_slug": "czech_ares"},
    {"table": "lv_company_domains", "registry_id_type": "regcode", "source_slug": "latvia_ur"},
    {"table": "ee_company_domains", "registry_id_type": "reg_code", "source_slug": "estonia_ar"},
    {"table": "br_company_domains", "registry_id_type": "cnpj_basico", "source_slug": "brazil_rfb"},
    {"table": "no_company_domains", "registry_id_type": "org_number", "source_slug": "norway_brreg"},
    {"table": "fi_company_domains", "registry_id_type": "business_id", "source_slug": "finland_ytj"},
    {"table": "wikidata_company_domains", "registry_id_type": "wikidata_id", "source_slug": "wikidata"},
)
```

- [ ] **Step 2: Replace `_company_website_domains_insert_sql`'s body** — the five hand-written arms become:

```python
def _canonical_domain_arm(source: dict[str, str]) -> str:
    table = source["table"]
    return f"""
        SELECT
            '{table}' AS source_website_table,
            ifNull(
                nullIf(trim(websites.source_record_id), ''),
                concat('{table}:', websites.registry_id, ':', websites.domain)
            ) AS source_website_id,
            nullIf(trim(websites.country_iso2), '') AS country_iso2,
            '{source["source_slug"]}' AS source_slug,
            '{source["registry_id_type"]}' AS company_id_type,
            websites.registry_id AS company_id,
            websites.website_url AS website_url,
            websites.website_normalized_url AS website_normalized_url,
            websites.website_host AS website_host,
            websites.domain AS root_domain,
            websites.domain_source AS domain_source,
            websites.is_current AS is_current,
            websites.is_primary AS is_primary
        FROM {_qualified_table(table)} AS websites
        WHERE nullIf(trim(websites.domain), '') IS NOT NULL"""
```

joined with `UNION ALL` over `tables.CANONICAL_DOMAIN_SOURCES` inside the existing outer INSERT wrapper (which keeps its `now64(3) AS resolved_at`). The wikidata LEFT JOIN and all per-source column-name special-casing are deleted. NOTE the country semantics change for non-wikidata sources: previously hardcoded `'FI'`/`'NO'`/`'EE'`/`'BR'` literals, now `nullIf(trim(country_iso2), '')` from data — the canonical tables carry exactly those values (live-verified in Phase D), so output is identical; wikidata's '' → NULL matches its old join-miss behavior.

- [ ] **Step 3: Deps + EXCHANGE order** — replace the asset's five deps with the seven from Global Constraints. In `replace_domain_clickhouse_tables`, reorder the two EXCHANGE statements: links first, then domains, with the comment `-- links swap first: the aggregate is computed FROM the links and must never be newer than what links readers see`. (Keep stage creation/insert order as-is — inserts already run links-then-domains.)

- [ ] **Step 4: Tests** — rewrite `test_domains_assets.py` pins: generated SQL contains all seven `FROM \`corpscout\`.\`<table>\`` references and ZERO references to `fi_websites`/`no_websites`/`wikidata_company_websites`/`br_websites`/`wikidata_companies`; each config slug/id-type appears; cross-check test importing `REGISTRY_ID_TYPE` from `czech_ares.contacts` and `latvia_ur.contacts` equals the config entries; deps pin = the seven asset keys (set equality); EXCHANGE-order test (fake client records statements; links exchange index < domains exchange index).

- [ ] **Step 5: Verify + commit**

```bash
uv run pytest tests/test_domains_assets.py tests/test_canonical_derivation_assets.py -q && uv run ruff check src/dagster_v3/defs/domains/ tests/test_domains_assets.py
git add src/dagster_v3/defs/domains/ tests/test_domains_assets.py
git commit -m "feat(dagster): domain graph reads the seven canonical company_domains tables"
```

---

### Task 2: Live switch with parity gate

**Files:**
- No code changes (execution + evidence task); appends results to the SDD report file.

**Interfaces:**
- Consumes: Task 1's code; live ClickHouse (creds main checkout `corpscout/dagster_v3/.env`).

- [ ] **Step 1: Baseline snapshot** (before running any new code):

```sql
CREATE TABLE corpscout._parity_baseline_cwd ENGINE = Memory AS
SELECT DISTINCT source_slug, company_id, root_domain
FROM corpscout.company_website_domains;
```

Record: total baseline edges; per-slug counts; `corpscout.domains` count.

- [ ] **Step 2: Run the new build** — drive `replace_domain_clickhouse_tables` via a foreground `uv run python` script (clickhouse_driver.Client, native port), exactly as the asset would.

- [ ] **Step 3: The gate** (all three must hold):

```sql
-- (a) zero missing baseline edges:
SELECT count() FROM corpscout._parity_baseline_cwd AS b
LEFT ANTI JOIN (SELECT DISTINCT source_slug, company_id, root_domain FROM corpscout.company_website_domains) AS n
ON b.source_slug = n.source_slug AND b.company_id = n.company_id AND b.root_domain = n.root_domain;
-- must be 0. If not: STOP, report the missing sample (LIMIT 20), leave everything as-is for triage.

-- (b) additions grouped by slug:
SELECT source_slug, count() FROM (SELECT DISTINCT source_slug, company_id, root_domain FROM corpscout.company_website_domains) AS n
LEFT ANTI JOIN corpscout._parity_baseline_cwd AS b ON b.source_slug = n.source_slug AND b.company_id = n.company_id AND b.root_domain = n.root_domain
GROUP BY source_slug ORDER BY source_slug;
-- czech_ares + latvia_ur expected (~4.6k + ~1.5k); any OTHER slug's additions must be explained
-- against that source's canonical-table resolved_at (fresher data), not accepted silently.

-- (c) aggregate sanity: corpscout.domains count vs baseline count; delta consistent with new
-- cz/lv root_domains (query countDistinct(root_domain) additions).
```

Then `DROP TABLE corpscout._parity_baseline_cwd;`. Paste ALL numbers in the report. Also spot-check 5 new czech_ares edges (join a couple back to cz_companies names, eyeball plausibility).

- [ ] **Step 4:** No commit (evidence-only task; report file carries the gate results).

---

### Task 3: Legacy demotion docs + program close-out

**Files:**
- Modify: builder-file docstrings/comments (one to three lines each): `finland_ytj/dbt/models/fi_websites.sql` (SQL comment), `norway_brreg/assets/entity_normalized.py` (`_no_websites_row` docstring), `wikidata/assets.py` (`_create_wikidata_company_websites_table` comment), `brazil_companies/rfb/contacts.py` (`build_brazil_rfb_websites` docstring: zero consumers, retire with a future migration)
- Modify: `docs/data-source-guidelines.md` §8b/§8c (graph reads only canonical pairs; legacy websites tables are internal stages), the standard spec (Phase E bullet: mark EXECUTED with the demotion outcome; conversion-inventory table: all rows done), the Phase E plan file itself is NOT edited (ledger carries status)
- Test: none new (docs), but full verification battery runs here

- [ ] **Step 1: The demotion comments** — each states: internal stage, the graph no longer reads it, consumed only by <the canonical derivation> (or "no consumers — retire via future migration" for br_websites), do not build new consumers on it.

- [ ] **Step 2: Spec + guidelines updates** per Files.

- [ ] **Step 3: Full verification**

```bash
uv run pytest tests/test_domains_assets.py tests/test_canonical_derivation_assets.py tests/test_canonical_contact_migrations.py tests/test_contact_extraction.py -q
uv run dg check defs 2>&1 | tail -1
uv run pytest --ignore=tests/test_exchange_rates_v2_dbt.py --ignore=tests/test_finland_xbrl_parsed_assets.py --ignore=tests/test_sweden_company_normalized_duckdb.py -q 2>&1 | tail -2
uv run ruff check src/dagster_v3/ 2>&1 | tail -1
```

- [ ] **Step 4: Commit** (`docs(contacts): demote legacy websites tables to internal stages; program close-out`).

---

## Deployment note (not a code task)

The switch is already live after Task 2 (the graph tables are rebuilt from canonical sources on the shared ClickHouse). On the dagster box, deploy picks up the new graph code; the next scheduled `domains_clickhouse` run reproduces the same output. THE CANONICAL-CONTACTS PROGRAM IS COMPLETE after this phase: seven sources, two uniform tables each, one templated graph. Remaining known follow-ups (all optional, tracked in ledgers): br_websites drop migration + export-asset removal; ee_company_contacts source_slug outlier normalization; upstream-move of the no/fi/wikidata derivations if ever desired; doc-relative spec paths sweep.
