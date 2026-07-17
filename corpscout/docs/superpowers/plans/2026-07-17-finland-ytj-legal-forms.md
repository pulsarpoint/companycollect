# Finland YTJ Legal-Form Backfill Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Populate `fi_companies`' legal-form columns, `trade_register_status`, `raw_status_code`, `last_modified`, `eu_id`, `business_id_registration_date`, and the VAT/employer/prepayment registration flags from YTJ data that is ALREADY ingested — then re-enable Finland's legal-form facet in the backoffice.

**Architecture:** This is dagster_v3 work (`defs/finland_ytj`), following that project's conventions (its CLAUDE.md). The dlt layer already flattens `trade_register_status`, `status`, `last_modified` into `finland_prhytj.all_companies` columns, and keeps each company's full YTJ v3 JSON in `raw_company`; `fi_companies.sql` currently NULL-stubs the legal-form columns and doesn't emit the status/flag columns at all (ClickHouse fills defaults `''`/`0` — exactly the observed empty data). We add two Python JSON-extraction functions (registered as DuckDB UDFs via the existing `dbt_plugin.py` pattern, exactly like `fi_primary_industry_json`), rewrite `fi_companies.sql` to emit all target columns, extend the export column contract + its migration test, re-materialize the `finland_ytj` chain, and finally flip the backoffice registry flag that was parked with a dated comment. **No ClickHouse migration is needed** — every target column already exists (migrations 000005 + 000010).

**Tech Stack:** dagster_v3 (Python 3, dbt-duckdb, dlt, ClickHouse exporter via EXCHANGE TABLES), pytest, `uv run` for everything. Backoffice re-flag is a two-line TypeScript change in `corpscout/services/backoffice`.

## Global Constraints

- dagster_v3 conventions are binding (its CLAUDE.md): `uv run` for `dg`/`pytest`; NO `from __future__ import annotations` in modules defining assets; the migration owns the CH schema (no DDL in Python — and none is needed here); commit by explicit path (shared tree, never `git add -A`); validate with `uv run dg check defs` before done.
- **Language codes in YTJ v3** (verified live against `avoindata.prh.fi/opendata-ytj-api/v3`): `"1"` = Finnish, `"2"` = Swedish, `"3"` = English. `legal_form_description_original` = the Finnish description with `legal_form_description_language = 'fi'`; `legal_form_description_en` = the English description taken DIRECTLY from YTJ (no LLM translation — leave `legal_form_description_translated_at/-_provider/-_model` as null literals).
- **Registered-entries register codes** (verified in live samples): `"1"` = Trade Register (authority 2 = PRH), `"5"` = prepayment register, `"6"` = VAT register, `"7"` = employer register. A registration is CURRENT when the entry has no `endDate` (key absent, null, or empty). Flags `is_vat_registered` / `is_prepayment_registered` / `is_employer_registered` = 1 iff a current entry exists with that register code.
- The current company form = the `companyForms` entry with no `endDate`; if several, the one with the greatest `registrationDate` (tie-break: greatest `version`). `legal_form_code` = its `type` (e.g. `"16"` = Osakeyhtiö, `"17"` = Julkinen osakeyhtiö).
- `trade_register_status` / `raw_status_code` (source key `status`) / `last_modified` come from the ALREADY-FLATTENED `all_companies` columns — do not re-extract them from JSON. `trade_register_status` is `LowCardinality(String)` NON-NULL in CH → coalesce to `''` in the model (CLAUDE.md: non-nullable CH String must get `''`, never NULL — the native driver dies on None).
- `eu_id` ← JSON path `$.euId.value`; `business_id_registration_date` ← `$.businessId.registrationDate` — plain `json_extract_string` in SQL, no UDF. `vat_id` has NO source field in the YTJ v3 payload → keep emitting NULL (do not derive it; logged follow-up).
- UDFs must be NULL-safe (`null_handling="special"` like the existing ones) and never raise on malformed JSON — return NULL and let SQL coalesce.
- The empty-table export guard (`allow_empty*`) must NOT be loosened. `RESOLVED_TABLE_COLUMNS` stays the full migration contract; `RESOLVED_EXPORT_COLUMNS` = minus `CLICKHOUSE_EXCLUDED_COLUMNS` (`source_payload_hash`) — that split and the migration contract test must stay in sync.
- Materialization: run locally via `./scripts/dagster-dev.sh` + `uv run dg launch --assets <FULL EXPLICIT LIST>` — NEVER `+leaf` (this dg build resolves upstream one hop only; dagster_v3 CLAUDE.md Troubleshooting). The YTJ bulk download is large; run in the background and poll.
- Conventional Commits; trailer `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`.
- ClickHouse verification via the HTTP endpoint (`http://companycollect:8123`, user `default`, password in `corpscout/.env`), read-only.

## Ground truth (verified live, 2026-07-17)

- `fi_companies.sql` (`corpscout/services/dagster_v3/src/dagster_v3/defs/finland_ytj/dbt/models/fi_companies.sql`) selects from `source('finland_prhytj', 'all_companies')` and hardcodes `cast(null as varchar)` for all 7 `legal_form_*` columns; it does NOT emit `trade_register_status`/`raw_status_code`/`last_modified`/`eu_id`/`vat_id`/`business_id_registration_date`/`is_*_registered` at all.
- `assets.py:_dlt_company_row` already flattens per company: `last_modified`, `trade_register_status`, `status` (all strings, `''` when absent) — these exist as columns on `all_companies` alongside `raw_company` (the full JSON).
- `fi_industries.sql` proves the raw-JSON UDF pattern: `fi_primary_industry_json(raw_company)` registered in `dbt_plugin.py` via `conn.create_function(..., ["VARCHAR"], "VARCHAR", null_handling="special")`.
- Real YTJ v3 JSON (fetched live, businessId 0112038-9 / Nokia Oyj): top keys `addresses, businessId, companyForms, companySituations, euId, lastModified, mainBusinessLine, names, registeredEntries, registrationDate, status, tradeRegisterStatus, website`. `businessId = {"value","registrationDate","source"}`; `euId = {"value": "FIFPRO.0112038-9", "source"}`; NO `vatId` key. `companyForms: [{type, descriptions:[{languageCode:"1"|"2"|"3", description}], registrationDate, version, source, endDate?}]`; `registeredEntries: [{type, descriptions, registrationDate, endDate?, register:"1"|"4"|"5"|"6"|"7"…, authority:"1"|"2"}]`.
- CH schema: migration `corpscout/clickhouse/migrations/000005_corpscout_fi_companies.up.sql` (base table) + `000010_corpscout_finland_ytj_registry_tables.up.sql` which ALTERs `fi_companies`: `business_id_registration_date Nullable(Date)`, `eu_id Nullable(String)`, `vat_id Nullable(String)` (after `business_id`); `trade_register_status LowCardinality(String)`, `raw_status_code Nullable(String)`, `last_modified Nullable(DateTime64(3,'UTC'))`, `is_vat_registered/is_employer_registered/is_prepayment_registered UInt8 DEFAULT 0` (after `is_active`).
- `resolved_tables.py` → `RESOLVED_TABLE_COLUMNS[FI_COMPANIES_TABLE]` currently lists ONLY the 000005 columns (22 entries) — none of the 000010 additions. The exporter inserts exactly `RESOLVED_EXPORT_COLUMNS`, so CH fills the missing columns with defaults.
- Contract test `tests/test_clickhouse_migrations.py::test_finland_resolved_migrations_cover_exported_columns` maps each table to ONE migration file and asserts `f"    {column_name} "` appears in it. fi_companies columns will now span TWO files, and 000010's lines are `    ADD COLUMN IF NOT EXISTS trade_register_status …` — the 4-space+name pattern will NOT match them. The test needs a files-list per table and a match of `f" {column_name} "` (space-delimited) across the files.
- Downstream unblock: `corpscout/services/backoffice/app/lib/countries.ts` FI `legal_form` column has `filterable` removed with comment `// legal_form_* columns are NULL for all fi_companies rows (pipeline gap, 2026-07-16) — re-flag when populated`.
- Sibling tables (`fi_legal_forms`, `fi_registered_entries`, `fi_tax_registrations`, `fi_company_situations`, `fi_addresses`) exist in CH with 0 rows and no dbt models — OUT OF SCOPE (logged); this pass populates the denormalized `fi_companies` columns only.

---

### Task 1: JSON extraction functions + UDF registration

**Files:**
- Create: `corpscout/services/dagster_v3/src/dagster_v3/defs/finland_ytj/registry.py`
- Modify: `corpscout/services/dagster_v3/src/dagster_v3/defs/finland_ytj/dbt_plugin.py`
- Test: `corpscout/services/dagster_v3/tests/test_finland_ytj_registry.py`

**Interfaces:**
- Produces (Task 2's SQL relies on these UDF names and JSON keys):
  - `legal_form_json(raw: str | None) -> str | None` → JSON `{"code","description_fi","description_sv","description_en","registration_date"}` for the CURRENT company form, or NULL when no forms/unparseable.
  - `registration_flags_json(raw: str | None) -> str | None` → JSON `{"is_vat_registered","is_prepayment_registered","is_employer_registered"}` (0/1 ints), or NULL when unparseable.
  - Registered as DuckDB UDFs `fi_legal_form_json` and `fi_registration_flags_json`.

- [ ] **Step 1: Write the failing tests**

`corpscout/services/dagster_v3/tests/test_finland_ytj_registry.py`:

```python
import json

from dagster_v3.defs.finland_ytj.registry import legal_form_json, registration_flags_json

RAW = json.dumps(
    {
        "businessId": {"value": "0112038-9", "registrationDate": "1978-03-15"},
        "companyForms": [
            {
                "type": "16",
                "descriptions": [
                    {"languageCode": "1", "description": "Osakeyhtiö"},
                    {"languageCode": "2", "description": "Aktiebolag"},
                    {"languageCode": "3", "description": "Limited company"},
                ],
                "registrationDate": "1980-01-01",
                "endDate": "1997-08-31",
                "version": 1,
            },
            {
                "type": "17",
                "descriptions": [
                    {"languageCode": "1", "description": "Julkinen osakeyhtiö"},
                    {"languageCode": "3", "description": "Public limited company"},
                ],
                "registrationDate": "1997-09-01",
                "version": 1,
            },
        ],
        "registeredEntries": [
            {"type": "1", "register": "1", "registrationDate": "1896-12-19", "authority": "2"},
            {"type": "80", "register": "6", "registrationDate": "1994-06-01", "authority": "1"},
            {"type": "55", "register": "5", "registrationDate": "1995-03-01", "authority": "1",
             "endDate": "2020-01-01"},
        ],
    }
)


def test_legal_form_picks_current_form_with_all_languages():
    payload = json.loads(legal_form_json(RAW))
    assert payload["code"] == "17"
    assert payload["description_fi"] == "Julkinen osakeyhtiö"
    assert payload["description_en"] == "Public limited company"
    assert payload["description_sv"] is None  # absent language stays null
    assert payload["registration_date"] == "1997-09-01"


def test_legal_form_prefers_latest_when_multiple_current():
    raw = json.loads(RAW)
    for form in raw["companyForms"]:
        form.pop("endDate", None)
    payload = json.loads(legal_form_json(json.dumps(raw)))
    assert payload["code"] == "17"  # later registrationDate wins


def test_registration_flags_respect_end_dates():
    payload = json.loads(registration_flags_json(RAW))
    assert payload["is_vat_registered"] == 1        # register 6, current
    assert payload["is_prepayment_registered"] == 0  # register 5, END-DATED
    assert payload["is_employer_registered"] == 0    # no register 7 entry


def test_null_and_garbage_inputs():
    assert legal_form_json(None) is None
    assert registration_flags_json(None) is None
    assert legal_form_json("not json") is None
    assert registration_flags_json("not json") is None
    assert legal_form_json("{}") is None  # no companyForms
    assert json.loads(registration_flags_json("{}"))["is_vat_registered"] == 0
```

- [ ] **Step 2: Run to verify failure**

Run: `cd corpscout/services/dagster_v3 && uv run pytest tests/test_finland_ytj_registry.py -q`
Expected: FAIL — `registry` module not found.

- [ ] **Step 3: Implement `registry.py`**

```python
"""JSON extraction helpers for YTJ v3 company payloads (DuckDB UDFs).

Language codes: "1" = Finnish, "2" = Swedish, "3" = English.
Register codes: "1" trade register, "5" prepayment, "6" VAT, "7" employer.
A form/entry is current when it has no endDate.
"""

import json
from typing import Any

_LANGUAGE_KEYS = {"1": "description_fi", "2": "description_sv", "3": "description_en"}
_FLAG_REGISTERS = {
    "6": "is_vat_registered",
    "5": "is_prepayment_registered",
    "7": "is_employer_registered",
}


def _loads(raw: str | None) -> dict[str, Any] | None:
    if not raw:
        return None
    try:
        payload = json.loads(raw)
    except (ValueError, TypeError):
        return None
    return payload if isinstance(payload, dict) else None


def _is_current(item: dict[str, Any]) -> bool:
    return not item.get("endDate")


def legal_form_json(raw: str | None) -> str | None:
    payload = _loads(raw)
    if payload is None:
        return None
    forms = [f for f in payload.get("companyForms") or [] if isinstance(f, dict)]
    current = [f for f in forms if _is_current(f)]
    candidates = current or forms
    if not candidates:
        return None
    picked = max(
        candidates,
        key=lambda f: (str(f.get("registrationDate") or ""), int(f.get("version") or 0)),
    )
    result: dict[str, Any] = {
        "code": picked.get("type"),
        "description_fi": None,
        "description_sv": None,
        "description_en": None,
        "registration_date": picked.get("registrationDate"),
    }
    for description in picked.get("descriptions") or []:
        if not isinstance(description, dict):
            continue
        key = _LANGUAGE_KEYS.get(str(description.get("languageCode")))
        if key is not None and description.get("description"):
            result[key] = description["description"]
    return json.dumps(result, ensure_ascii=False)


def registration_flags_json(raw: str | None) -> str | None:
    payload = _loads(raw)
    if payload is None:
        return None
    flags = {name: 0 for name in _FLAG_REGISTERS.values()}
    for entry in payload.get("registeredEntries") or []:
        if not isinstance(entry, dict) or not _is_current(entry):
            continue
        flag = _FLAG_REGISTERS.get(str(entry.get("register")))
        if flag is not None:
            flags[flag] = 1
    return json.dumps(flags)
```

- [ ] **Step 4: Register the UDFs**

In `dbt_plugin.py`, import `from dagster_v3.defs.finland_ytj.registry import legal_form_json, registration_flags_json` and add two `conn.create_function` calls inside `configure_connection`, identical in shape to the existing `fi_primary_industry_json` one: names `"fi_legal_form_json"` and `"fi_registration_flags_json"`, signature `["VARCHAR"] -> "VARCHAR"`, `null_handling="special"`.

- [ ] **Step 5: Verify + commit**

Run: `uv run pytest tests/test_finland_ytj_registry.py -q` → PASS (4 tests); `uv run dg check defs` → clean.

```bash
git add corpscout/services/dagster_v3/src/dagster_v3/defs/finland_ytj/registry.py corpscout/services/dagster_v3/src/dagster_v3/defs/finland_ytj/dbt_plugin.py corpscout/services/dagster_v3/tests/test_finland_ytj_registry.py
git commit -m "feat(dagster): ytj legal form and registration flag udfs"
```

---

### Task 2: Wire the extraction into `fi_companies.sql` + export contract

**Files:**
- Modify: `corpscout/services/dagster_v3/src/dagster_v3/defs/finland_ytj/dbt/models/fi_companies.sql`
- Modify: `corpscout/services/dagster_v3/src/dagster_v3/defs/finland_ytj/resolved_tables.py:27-51`
- Modify: `corpscout/services/dagster_v3/tests/test_clickhouse_migrations.py` (`test_finland_resolved_migrations_cover_exported_columns`, ~line 756)

**Interfaces:**
- Consumes: `fi_legal_form_json` / `fi_registration_flags_json` UDFs (Task 1); JSON keys exactly as Task 1 produces; flattened `all_companies` columns `trade_register_status`, `status`, `last_modified`, `raw_company`.
- Produces: `fi_companies` model + export contract emitting all 000005+000010 columns. Task 3 materializes it; Task 4 depends on `legal_form_description_en` being populated.

- [ ] **Step 1: Rewrite the model**

Replace `fi_companies.sql` with:

```sql
{{ config(materialized='table') }}

with extracted as (
  select
    *,
    fi_legal_form_json(raw_company) as legal_form_payload,
    fi_registration_flags_json(raw_company) as registration_flags
  from {{ source('finland_prhytj', 'all_companies') }}
)
select
  business_id,
  try_cast(json_extract_string(raw_company, '$.businessId.registrationDate') as date) as business_id_registration_date,
  json_extract_string(raw_company, '$.euId.value') as eu_id,
  cast(null as varchar) as vat_id,
  country_iso2,
  primary_name as name,
  lower(primary_name) as name_normalized,
  try_cast(nullif(registration_date, '') as date) as registration_date,
  try_cast(nullif(end_date, '') as date) as end_date,
  lifecycle_status,
  coalesce(is_active, false) as is_active,
  coalesce(trade_register_status, '') as trade_register_status,
  nullif(status, '') as raw_status_code,
  try_cast(nullif(last_modified, '') as timestamp) as last_modified,
  coalesce(json_extract_string(registration_flags, '$.is_vat_registered') = '1', false) as is_vat_registered,
  coalesce(json_extract_string(registration_flags, '$.is_employer_registered') = '1', false) as is_employer_registered,
  coalesce(json_extract_string(registration_flags, '$.is_prepayment_registered') = '1', false) as is_prepayment_registered,
  json_extract_string(legal_form_payload, '$.code') as legal_form_code,
  json_extract_string(legal_form_payload, '$.description_fi') as legal_form_description_original,
  case when json_extract_string(legal_form_payload, '$.description_fi') is not null then 'fi' end as legal_form_description_language,
  json_extract_string(legal_form_payload, '$.description_en') as legal_form_description_en,
  cast(null as timestamp) as legal_form_description_translated_at,
  cast(null as varchar) as legal_form_description_translation_provider,
  cast(null as varchar) as legal_form_description_translation_model,
  nullif(website_normalized_url, '') as primary_website_url,
  nullif(website_host, '') as primary_website_host,
  source_slug as source_system,
  source_run_id,
  source_record_id,
  source_payload_hash,
  now() as resolved_at
from extracted
where business_id is not null and business_id != ''
```

- [ ] **Step 2: Extend the export contract**

In `resolved_tables.py`, replace the `FI_COMPANIES_TABLE` tuple with (order mirrors physical CH order after 000010's `AFTER` clauses):

```python
    FI_COMPANIES_TABLE: (
        "business_id",
        "business_id_registration_date",
        "eu_id",
        "vat_id",
        "country_iso2",
        "name",
        "name_normalized",
        "registration_date",
        "end_date",
        "lifecycle_status",
        "is_active",
        "trade_register_status",
        "raw_status_code",
        "last_modified",
        "is_vat_registered",
        "is_employer_registered",
        "is_prepayment_registered",
        "legal_form_code",
        "legal_form_description_original",
        "legal_form_description_language",
        "legal_form_description_en",
        "legal_form_description_translated_at",
        "legal_form_description_translation_provider",
        "legal_form_description_translation_model",
        "primary_website_url",
        "primary_website_host",
        "source_system",
        "source_run_id",
        "source_record_id",
        "source_payload_hash",
        "resolved_at",
    ),
```

- [ ] **Step 3: Update the migration contract test**

`fi_companies` columns now span 000005 (base) and 000010 (ALTER `ADD COLUMN IF NOT EXISTS <name> …` — the existing `f"    {column_name} "` pattern misses those lines). In `test_finland_resolved_migrations_cover_exported_columns`, change the mapping to files-tuples and match space-delimited names across them:

```python
    migration_files_by_table = {
        finland_resolved_tables.FI_COMPANIES_TABLE: (
            "000005_corpscout_fi_companies.up.sql",
            "000010_corpscout_finland_ytj_registry_tables.up.sql",
        ),
        finland_resolved_tables.FI_WEBSITES_TABLE: (
            "000006_corpscout_fi_websites.up.sql",
        ),
        finland_resolved_tables.FI_INDUSTRIES_TABLE: (
            "000007_corpscout_fi_industries.up.sql",
        ),
        finland_resolved_tables.FI_NAMES_TABLE: (
            "000010_corpscout_finland_ytj_registry_tables.up.sql",
        ),
    }

    assert set(migration_files_by_table) == set(
        finland_resolved_tables.FINLAND_YTJ_RESOLVED_TABLES
    )

    for table_name, migration_files in migration_files_by_table.items():
        sqls = [_migration_sql(name) for name in migration_files]
        for column_name in finland_resolved_tables.RESOLVED_TABLE_COLUMNS[table_name]:
            assert any(f" {column_name} " in sql for sql in sqls), (
                f"{table_name}.{column_name} not found in {migration_files}"
            )
```

- [ ] **Step 4: Verify + commit**

Run: `uv run pytest tests/test_clickhouse_migrations.py tests/test_finland_ytj_resolved_dbt.py tests/test_finland_ytj_resolved_assets.py -q` → PASS (if a resolved-dbt test pins the old NULL-stub column list, update it to the new expectations — the columns above ARE the spec); `uv run dg check defs` → clean.

```bash
git add corpscout/services/dagster_v3/src/dagster_v3/defs/finland_ytj/dbt/models/fi_companies.sql corpscout/services/dagster_v3/src/dagster_v3/defs/finland_ytj/resolved_tables.py corpscout/services/dagster_v3/tests/test_clickhouse_migrations.py
# add any updated resolved-dbt test file by explicit path too
git commit -m "feat(dagster): extract finland legal form, ids and registration flags into fi_companies"
```

---

### Task 3: Materialize the chain + ClickHouse verification

**Files:** none (operational task; the task report carries the evidence)

- [ ] **Step 1: Launch the chain**

From `corpscout/services/dagster_v3`: start the dev instance in the background (`./scripts/dagster-dev.sh`), discover the finland_ytj asset keys, then launch with the FULL explicit list (download → duckdb load → dbt models → ClickHouse exports) — never `+leaf`:

```bash
uv run dg list defs --json > /tmp/fi_defs.json && python3 -c "
import json
for d in json.load(open('/tmp/fi_defs.json')):
    k = str(d.get('key', ''))
    if 'finland' in k.lower() or k.startswith('fi_'):
        print(k)
"  # fall back to: uv run dg list defs | grep -iE 'finland|fi_'
uv run dg launch --assets <every_finland_ytj_chain_asset_comma_separated>
```

Expect a long run (bulk YTJ snapshot download + parse). Poll to completion; on RUN_EXCEPTION or stuck-QUEUED consult dagster_v3 CLAUDE.md Troubleshooting (leaked pool slots — `uv run python scripts/dagster-health-check.py --fix`).

- [ ] **Step 2: Verify in ClickHouse**

```bash
CH="http://companycollect:8123/?user=default&password=<from corpscout/.env>"
curl -s "$CH" --data "SELECT count(), countIf(legal_form_code IS NOT NULL), countIf(legal_form_description_en IS NOT NULL), countIf(trade_register_status != ''), countIf(is_vat_registered=1), countIf(eu_id IS NOT NULL) FROM corpscout.fi_companies"
# spot-check the live-verified sample company:
curl -s "$CH" --data "SELECT legal_form_code, legal_form_description_original, legal_form_description_en, trade_register_status, is_vat_registered, eu_id, business_id_registration_date FROM corpscout.fi_companies WHERE business_id='0112038-9' FORMAT Vertical"
# legal-form distribution sanity (limited companies should dominate):
curl -s "$CH" --data "SELECT legal_form_description_en, count() c FROM corpscout.fi_companies GROUP BY 1 ORDER BY c DESC LIMIT 8 FORMAT TSV"
```

Expected: legal_form coverage near-total; 0112038-9 shows `17 / Julkinen osakeyhtiö / Public limited company / eu_id FIFPRO.0112038-9 / business_id_registration_date 1978-03-15`; distribution dominated by limited companies and private traders. Record the XBRL-denominator number while here: `SELECT countIf(legal_form_code IN ('16','17')) FROM corpscout.fi_companies` vs the 21,315 XBRL filers.

- [ ] **Step 3: No commit (operational) — write the numbers into the task report.**

---

### Task 4: Backoffice re-flag + gate

**Files:**
- Modify: `corpscout/services/backoffice/app/lib/countries.ts` (FI `legal_form` column: restore `filterable: true`, replace the dated gap comment with `// populated from YTJ companyForms since 2026-07-17`)
- Modify: `corpscout/services/backoffice/README.md` (drop the FI-legal-form line from the pipeline-gaps notes, if present)

- [ ] **Step 1: Flip the flag + comment; README line.**

- [ ] **Step 2: Gate**

From `corpscout/services/backoffice`: `pnpm typecheck && pnpm test` — the live all-countries facet sweep now exercises FI's legal_form facet against the freshly populated data (facet caches are per-process; tests see fresh data immediately). Expected: all green. Then on a THROWAWAY dev port (never touch the user's server on 5183): `/companies` filter sheet → Legal form facet includes Finnish forms (e.g. "Limited company"); a FI detail page shows its legal form instead of a dash.

- [ ] **Step 3: Commit**

```bash
git add corpscout/services/backoffice/app/lib/countries.ts corpscout/services/backoffice/README.md
git commit -m "feat(backoffice): re-enable finland legal form facet"
```

---

## Out of scope (logged)

- `vat_id` — no source field in YTJ v3; deriving `FI` + digits is a business rule needing a decision.
- Sibling tables (`fi_legal_forms`, `fi_registered_entries`, `fi_tax_registrations`, `fi_company_situations`, `fi_addresses`) — dbt models + exports for full fidelity; natural follow-up now that the UDF layer exists.
- FI XBRL coverage reporting (filers ÷ obligated) — the denominator lands here; presentation later.
- Remaining pipeline gaps (LV NACE, SE/SK financial metrics, NO USD/contacts, BR CNAE mapping).
