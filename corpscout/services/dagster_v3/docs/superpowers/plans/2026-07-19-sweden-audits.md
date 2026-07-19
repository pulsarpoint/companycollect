# Sweden Company Audits Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extract the audit-firm relationship and audit-opinion form from Swedish XBRL facts into `se_company_audits`, and surface "Audited by <firm> · <opinion>" in the backoffice Management card with a warning badge for modified opinions.

**Architecture:** Same shape as `se_company_officers`: a ClickHouse-SQL extraction over `corpscout.se_financial_facts` (no document re-parsing), stage + EXCHANGE + refuse-empty + shrink guard, rebuilt by the standard SE clickhouse job. One row per statement (filing).

**Tech Stack:** ClickHouse migration 000146, dagster module file `sweden_financial/audits.py`, backoffice registry query + Management-card extension.

## Measured source facts (2026-07-19)

- `ValtRevisionsbolagNamn` (184k) + `ValtRevisionsbolagsnamn` (105k) — appointed audit firm name, clean text (e.g. "KPMG AB", "Öhrlings PricewaterhouseCoopers AB"); both spellings must be read (taxonomy-version variants).
- `RevisorspateckningRevisionsberattelseEnligtStandardutformning` (304.8k) — standard/clean opinion endorsement; value is the endorsement DATE.
- `RevisorspateckningRevisionsberattelseAvvikerStandardutformning` (743) — audit report DEVIATES from standard form → modified opinion (distress signal). 720 statements carry more than one Revisorspateckning concept → **Avviker wins** when present.

## Global Constraints

- Stage + `EXCHANGE TABLES`, refuse-empty guard, `guard_against_clickhouse_table_shrink` (0.5, `allow_shrink` default False) — mirror `sweden_financial/officers.py` exactly (structure, %-escaping discipline with params dict, contract tests).
- Migration **000146** (`corpscout/clickhouse/migrations/000146_corpscout_se_company_audits.up.sql`/`.down.sql`); register `"000146_corpscout_se_company_audits"` in `tests/test_clickhouse_migrations.py` `EXPECTED_MIGRATIONS` **and commit that registry line** (insert after the 000145 line; commit only your line even though the file carries unrelated WIP — take HEAD's version, add the line, commit via partial staging of exactly that hunk if possible, else note for the controller to handle). No semicolons inside `--` migration comments. Column order in the Python constant must match the migration.
- Asset `se_company_audits_clickhouse`, deps `["sweden_financial_facts_clickhouse"]`, reuse `SwedenFinancialClickhouseExportConfig`; add it to `SWEDEN_FINANCIAL_CLICKHOUSE_SELECTION` and to `CLICKHOUSE_LEAVES` in `defs/common/clickhouse_checks.py` (max_age=None like officers).
- Backoffice: registry-driven (`auditQuery` in `countries.ts` SE detail), named params, "modified opinion" must be visually distinct (destructive-styled badge).
- `uv run` everything; `dg check defs` green; explicit-path commits with trailer `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`.

---

### Task 1: Migration + audits extraction + asset (dagster)

**Files:**
- Create: `corpscout/clickhouse/migrations/000146_corpscout_se_company_audits.up.sql` / `.down.sql`
- Create: `corpscout/services/dagster_v3/src/dagster_v3/defs/sweden_financial/audits.py`
- Modify: `corpscout/services/dagster_v3/src/dagster_v3/defs/sweden_financial/assets.py` (asset after `se_company_officers_clickhouse`; add to `SWEDEN_FINANCIAL_CLICKHOUSE_SELECTION`)
- Modify: `corpscout/services/dagster_v3/src/dagster_v3/defs/common/clickhouse_checks.py` (`CLICKHOUSE_LEAVES`)
- Modify (registry line only): `corpscout/services/dagster_v3/tests/test_clickhouse_migrations.py`
- Test: `corpscout/services/dagster_v3/tests/test_sweden_financial_audits.py`

**Interfaces:**
- Produces: table `corpscout.se_company_audits`; `replace_se_company_audits_clickhouse(clickhouse, *, source_run_id, resolved_at, log, allow_shrink) -> dict`; asset `se_company_audits_clickhouse`.

- [ ] **Step 1: Migration 000146**

```sql
CREATE DATABASE IF NOT EXISTS corpscout;

CREATE TABLE IF NOT EXISTS corpscout.se_company_audits
(
    company_id String,
    fiscal_year Int32,
    statement_key String,
    audit_firm String,
    opinion_kind LowCardinality(String), -- 'standard' | 'modified' | 'unknown' (firm known, no pateckning fact)
    opinion_date Nullable(Date32),
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY (company_id, fiscal_year, statement_key);
```
`.down.sql`: `DROP TABLE IF EXISTS corpscout.se_company_audits;`

- [ ] **Step 2: failing contract/wiring tests** (mirror `test_sweden_financial_officers.py`: migration-column-order contract, build-SQL content assertions incl. both firm-name spellings and the Avviker-wins rule, %-escaping round-trip tests, asset wiring with dep + selection membership).

- [ ] **Step 3: `audits.py`** — mirror officers.py structure. Core INSERT SELECT:

```sql
SELECT
    company_id,
    toInt32(coalesce(toYear(report_period_end), 0)) AS fiscal_year,
    statement_key,
    coalesce(
        anyIf(trim(coalesce(text_value, raw_value)),
              concept_local_name IN ('ValtRevisionsbolagNamn', 'ValtRevisionsbolagsnamn')),
        ''
    ) AS audit_firm,
    multiIf(
        countIf(concept_local_name = 'RevisorspateckningRevisionsberattelseAvvikerStandardutformning') > 0, 'modified',
        countIf(concept_local_name = 'RevisorspateckningRevisionsberattelseEnligtStandardutformning') > 0, 'standard',
        'unknown'
    ) AS opinion_kind,
    maxIf(date_value,
          concept_local_name IN (
              'RevisorspateckningRevisionsberattelseEnligtStandardutformning',
              'RevisorspateckningRevisionsberattelseAvvikerStandardutformning'
          )) AS opinion_date,
    <resolved_at param, officers.py mechanism> AS resolved_at
FROM corpscout.se_financial_facts
WHERE concept_local_name IN (
    'ValtRevisionsbolagNamn', 'ValtRevisionsbolagsnamn',
    'RevisorspateckningRevisionsberattelseEnligtStandardutformning',
    'RevisorspateckningRevisionsberattelseAvvikerStandardutformning'
)
GROUP BY company_id, fiscal_year, statement_key
HAVING audit_firm != '' OR opinion_kind != 'unknown'
```

Quality metadata: row count, company count, modified_opinion_count, unknown_opinion_count, null_fiscal_year_count (officers pattern).

- [ ] **Step 4:** asset + selection + leaves; `uv run pytest tests/test_sweden_financial_audits.py tests/test_sweden_financial_assets.py -q` green; `uv run dg check defs` green.
- [ ] **Step 5: Commit** — `feat(dagster): se_company_audits with audit firm and opinion form`.

### Task 2: Materialize + backoffice audit line

**Files:**
- Modify: `corpscout/services/backoffice/app/lib/countries.ts` (SE `auditQuery` + `CountryDetailConfig` field)
- Modify: `corpscout/services/backoffice/app/lib/queries.server.ts` (`AuditRow`, wire like officers)
- Modify: `corpscout/services/backoffice/app/components/detail/management-section.tsx` (audit line in the Auditor block)

- [ ] Deploy (light_sync), apply migration 000146 (controller/user runs `make clickhouse-migrate-up`), launch `se_company_audits_clickhouse` via GraphQL; verify counts (≈305k+ rows, modified ≈743) and spot-check 5560003575 fiscal 2023 (KPMG AB expected — verify live).
- [ ] `auditQuery`: `{id:String}` → latest fiscal year's row: `SELECT audit_firm, opinion_kind, toString(opinion_date) AS opinion_date, fiscal_year FROM se_company_audits WHERE company_id = {id:String} ORDER BY fiscal_year DESC, statement_key DESC LIMIT 1`.
- [ ] Management card auditor block gains one muted line: "Audited by <firm>" when firm != ''; opinion badge: standard → outline "standard opinion"; modified → destructive badge "modified opinion"; unknown → no badge.
- [ ] `npm run typecheck` clean; verify live on a KPMG company + one of the 743 modified-opinion companies (find one via query); commit `feat(backoffice): audit firm and opinion in management section`.

## Deferred

- Firm-name → se_companies linkage (name match to orgnr) — with the future identity/linkage layer.
- Historical audit-firm changes view (firm switches are themselves a signal).
