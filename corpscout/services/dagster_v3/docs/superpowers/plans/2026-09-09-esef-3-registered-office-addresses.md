# ESEF Slice 3: Registered-Office Addresses Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Every Swedish company whose ESEF filing tags `AddressOfRegisteredOfficeOfEntity` gets that registered office as an `esef` address suggestion on the address entity, so the fold publishes it beside the register addresses.

**Architecture:** One more extractor on the address entity's shared extract helper (`se_company_address_suggestions_esef`), reading `se_esef_facts` joined to `se_esef_filings` (both views from slice 1, already Swedish and register-verified). The fact value is a one-line string the parser cannot take as is, so the SQL cleans it (HTML tags, whitespace, trailing punctuation, a trailing country word) and, when it finds a Swedish postcode, re-packs it into the Bolagsverket `street$care-of$town$postcode$country` format the normaliser already parses; a value without a postcode goes into `street_address` alone and the normaliser's own partial rules decide. No normaliser change, no migration, no table change. `esef` joins the source and extractor registries, the extract job, the precedence map (`esef: 500`, display order only) and the backoffice source labels.

**Tech Stack:** Python 3.14 / Dagster / pytest (`uv run pytest`), clickhouse-local (docker) for the extractor integration test, TypeScript / vitest for the backoffice label.

**Spec:** `corpscout/services/dagster_v3/docs/superpowers/specs/2026-09-08-esef-entity-link-people-addresses-design.md` (revised 2026-09-09), section "3. Registered-office addresses"; the address entity's binding design is `docs/superpowers/specs/2026-09-06-se-company-address-entity-design.md`.

## Global Constraints

- **Repo root:** `/Users/graovic/pulsarpoint/ppoint/companycollect`. Dagster commands from `corpscout/services/dagster_v3` with `uv run`; backoffice commands from `corpscout/services/backoffice`. Always `cd` with absolute paths.
- **Branch:** `esef-3-registered-office-addresses` from main. Commit by explicit path only; never stage `corpscout/services/backoffice/app/lib/technology-proposals.server.ts`.
- **Definitions-loading tests** need `WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix`.
- **Source name (verbatim):** `esef`; extractor asset `se_company_address_suggestions_esef`; extractor version `esef-address-v1`; kind `registered`; slot `''`; precedence `esef: 500`.
- **The thirteen select columns, in order (verbatim, from `suggestions.py::ADDRESS_SELECT_COLUMNS`):** `company_id, source, slot, source_record_uid, observed_at, kind, raw_address, care_of, street_address, postal_code, post_town, county, country_code`; every extractor SELECT yields exactly them, binds `company_id IN %(company_ids)s`, and its `current_sql` yields `company_id, observed_at`.
- **Source record uid (verbatim):** `lower(hex(SHA256(concat('company-source-record-v1\nfile\nesef_report_package\n', lowerUTF8(package_sha256)))))` — the uid every `esef_document_*` row carries (`\n` written as `\\n` inside a Python string literal that renders SQL).
- **Packed format the normaliser parses (verbatim):** `street$care-of$town$postcode$country`, five `$`-separated parts, postcode digits only, country `SE` or empty.
- **Views read (slice 1):** `corpscout.se_esef_facts` (columns `company_id` then the facts columns: `lei, fxo_id, period_end, fact_id, concept_qname, concept_namespace, concept_local_name, period_start, period_instant, period_duration_end, unit, currency, value_kind, raw_value, amount_original, decimals, dimensions, language, source_run_id, processed_week, resolved_at`) and `corpscout.se_esef_filings` (`company_id` then `lei, entity_name, fxo_id, country, period_end, date_added, processed_at, json_url, package_url, report_url, viewer_url, package_sha256, ...`); `processed_at` is `Nullable(DateTime64(6))`, `period_end` is `Date32`. A consumer never writes `FINAL` after a view name.
- **Commit footer** on every commit:
  ```
  Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
  Claude-Session: https://claude.ai/code/session_01UJnba4eXZta4f9KaKJxhY9
  ```

---

## File map

| File | Change |
|---|---|
| `corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/address/esef.py` (new) | the extractor: `ESEF_ADDRESS_EXTRACTOR_VERSION`, `esef_current_sql()`, `esef_select_sql()`, `se_company_address_suggestions_esef` |
| `.../se_company/address/tables.py` | `SOURCES` gains `esef` before `reviewer` |
| `.../se_company/address/assets.py` | `EXTRACTOR_SOURCES` gains `esef` (the fold and normalize deps follow) |
| `.../se_company/address/precedence.py` | `ADDRESS_PRECEDENCE["esef"] = 500` |
| `.../se_company/address/jobs.py` | nothing: the extract job selects `EXTRACTOR_ASSET_NAMES` |
| `.../se_company/address/docs/address-design.md`, `docs/superpowers/specs/2026-09-06-se-company-address-entity-design.md` | the `esef` extractor documented; ESEF leaves the out-of-scope line |
| `corpscout/services/dagster_v3/tests/test_se_company_address_extractors_sql.py`, `test_se_company_address_tables.py`, `test_se_company_address_precedence.py`, `test_se_company_address_extractors_clickhouse_local.py`, `tests/fixtures/se_basic_info_source_tables.sql` | re-pinned; the harness gains the two ESEF fixture tables and the two rendered views |
| `corpscout/services/backoffice/app/lib/se-address-fields.ts`, `tests/se-address-fields.test.ts` | `esef` source with label `ESEF` |

---

## Task 1: The extractor and the registries

**Files:**
- Create: `corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/address/esef.py`
- Modify: `.../address/tables.py` (`SOURCES`), `.../address/assets.py` (`EXTRACTOR_SOURCES`), `.../address/precedence.py`
- Test: `tests/test_se_company_address_extractors_sql.py`, `tests/test_se_company_address_tables.py`, `tests/test_se_company_address_precedence.py`

**Interfaces:**
- Consumes: `define_address_suggestion_asset(source=, extractor_version=, current_sql=, select_sql=, deps=, description=)` from `address/suggestions.py` (the same helper `bolagsverket.py` and `ratsit.py` use).
- Produces: `esef.ESEF_ADDRESS_EXTRACTOR_VERSION = "esef-address-v1"`, `esef.ESEF_PACKED_ADDRESS_SQL` (the packing expression, exported for the harness test), `esef.esef_current_sql() -> str`, `esef.esef_select_sql() -> str`, asset `se_company_address_suggestions_esef`.

- [x] **Step 1: Write the failing tests**

Append to `tests/test_se_company_address_extractors_sql.py` (it already imports `bolagsverket`, `ratsit`, `scb`, `assets`, `ADDRESS_SELECT_COLUMNS` and has `_aliases(select_sql)`; import `esef` beside them):

```python
def test_esef_takes_the_registered_office_of_the_newest_filing_and_repacks_it() -> None:
    sql = esef.esef_select_sql()
    assert _aliases(sql) == list(ADDRESS_SELECT_COLUMNS)
    assert "FROM corpscout.se_esef_facts AS facts" in sql
    assert "INNER JOIN corpscout.se_esef_filings AS filings ON filings.fxo_id = facts.fxo_id" in sql
    assert "FINAL" not in sql
    assert "facts.concept_local_name = 'AddressOfRegisteredOfficeOfEntity'" in sql
    assert "'esef' AS source" in sql and "'' AS slot" in sql and "'registered' AS kind" in sql
    assert "'company-source-record-v1\\nfile\\nesef_report_package\\n', lowerUTF8(filings.package_sha256)" in sql
    assert "toDateTime64(filings.processed_at, 3, 'UTC') AS observed_at" in sql
    assert "ORDER BY filings.period_end DESC, filings.processed_at DESC, facts.language DESC, facts.fact_id\nLIMIT 1 BY facts.company_id" in sql
    assert "company_id IN %(company_ids)s" in sql
    # The packed form the normaliser parses; the unparsed remainder goes to street_address.
    assert esef.ESEF_PACKED_ADDRESS_SQL in sql
    assert "AS raw_address" in sql and "AS street_address" in sql
    assert "CAST(NULL AS Nullable(String)) AS care_of" in sql
    assert esef.ESEF_ADDRESS_EXTRACTOR_VERSION == "esef-address-v1"
    assert esef.esef_current_sql() == (
        "SELECT facts.company_id AS company_id, max(toDateTime64(filings.processed_at, 3, 'UTC')) AS observed_at\n"
        "FROM corpscout.se_esef_facts AS facts\n"
        "INNER JOIN corpscout.se_esef_filings AS filings ON filings.fxo_id = facts.fxo_id\n"
        "WHERE facts.concept_local_name = 'AddressOfRegisteredOfficeOfEntity' AND filings.processed_at IS NOT NULL\n"
        "GROUP BY facts.company_id"
    )
```

and change the existing pin `assert assets.EXTRACTOR_SOURCES == ("scb", "bolagsverket", "ratsit")` to `("scb", "bolagsverket", "ratsit", "esef")`. In `test_se_company_address_tables.py` line 95: `tables.SOURCES == ("scb", "bolagsverket", "ratsit", "esef", "reviewer", "reviewer_draft")`. In `test_se_company_address_precedence.py`: the map gains `"esef": 500` and `precedence_rows()` gains `("text", "esef", 500)` between `scb` and `ratsit`.

- [x] **Step 2: Run to verify they fail**

Run: `cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/services/dagster_v3 && uv run pytest tests/test_se_company_address_extractors_sql.py tests/test_se_company_address_tables.py tests/test_se_company_address_precedence.py -q -p no:warnings`
Expected: FAIL (`esef` module missing, pins differ).

- [x] **Step 3: Write `esef.py`**

```python
"""ESEF registered office -> raw address suggestion (spec 2026-09-09, section 3): the tagged
AddressOfRegisteredOfficeOfEntity fact of the company's newest filing, kind registered.

The fact is one line of free text ("Kungsträdgårdsgatan 2, 106 70 Stockholm", sometimes with
HTML spans, a trailing period or ", Sverige"). The normaliser parses either components or the
Bolagsverket packed string, so the SQL cleans the line and, when it finds a Swedish postcode,
re-packs it as street$care-of$town$postcode$country; a line without a postcode is delivered as
street_address alone and the normaliser's partial rules decide."""

import dagster as dg

from dagster_v3.defs.se_company.address.suggestions import define_address_suggestion_asset

ESEF_ADDRESS_EXTRACTOR_VERSION = "esef-address-v1"

ESEF_RECORD_UID_SQL = (
    "lower(hex(SHA256(concat('company-source-record-v1\\nfile\\nesef_report_package\\n', "
    "lowerUTF8(filings.package_sha256)))))"
)

# Tags out, whitespace collapsed, trailing dots/spaces off, then the trailing country word.
_CLEANED_SQL = (
    "trim(replaceRegexpAll(replaceRegexpAll(replaceRegexpAll(facts.raw_value, '<[^>]+>', ' '), "
    "'\\\\s+', ' '), '[\\\\s.,]+$', ''))"
)
_SWEDISH_COUNTRY_SQL = f"match({_CLEANED_SQL}, '(?i)[,\\\\s]+(sverige|sweden)$')"
_BODY_SQL = f"replaceRegexpOne({_CLEANED_SQL}, '(?i)[,\\\\s]+(sverige|sweden)$', '')"
# street part | postcode 3 | postcode 2 | town: the first Swedish postcode splits the line.
_PARTS_SQL = f"extractGroups({_BODY_SQL}, '^(.*?)[,\\\\s]*(?:SE-)?([0-9]{{3}})\\\\s?([0-9]{{2}})\\\\s+(.+)$')"
# No postcode: the part after the last comma is the town ("Ideongatan 1, Lund").
_NO_CODE_PARTS_SQL = f"extractGroups({_BODY_SQL}, '^(.*),\\\\s*([^,]+)$')"

ESEF_PACKED_ADDRESS_SQL = (
    f"if(length({_PARTS_SQL}) = 4, concat(trim({_PARTS_SQL}[1]), '$$', trim({_PARTS_SQL}[4]), '$', "
    f"{_PARTS_SQL}[2], {_PARTS_SQL}[3], '$', if({_SWEDISH_COUNTRY_SQL}, 'SE', '')), "
    "CAST(NULL AS Nullable(String)))"
)
_NO_CODE_STREET_SQL = (
    f"if(length({_PARTS_SQL}) = 4, CAST(NULL AS Nullable(String)), "
    f"if(length({_NO_CODE_PARTS_SQL}) = 2, nullIf(trim({_NO_CODE_PARTS_SQL}[1]), ''), nullIf({_BODY_SQL}, '')))"
)
_NO_CODE_TOWN_SQL = (
    f"if(length({_PARTS_SQL}) = 4 OR length({_NO_CODE_PARTS_SQL}) != 2, CAST(NULL AS Nullable(String)), "
    f"nullIf(trim({_NO_CODE_PARTS_SQL}[2]), ''))"
)


def esef_current_sql() -> str:
    return (
        "SELECT facts.company_id AS company_id, max(toDateTime64(filings.processed_at, 3, 'UTC')) AS observed_at\n"
        "FROM corpscout.se_esef_facts AS facts\n"
        "INNER JOIN corpscout.se_esef_filings AS filings ON filings.fxo_id = facts.fxo_id\n"
        "WHERE facts.concept_local_name = 'AddressOfRegisteredOfficeOfEntity' AND filings.processed_at IS NOT NULL\n"
        "GROUP BY facts.company_id"
    )


def esef_select_sql() -> str:
    return (
        "SELECT\n"
        "    facts.company_id AS company_id,\n"
        "    'esef' AS source,\n"
        "    '' AS slot,\n"
        f"    {ESEF_RECORD_UID_SQL} AS source_record_uid,\n"
        "    toDateTime64(filings.processed_at, 3, 'UTC') AS observed_at,\n"
        "    'registered' AS kind,\n"
        f"    {ESEF_PACKED_ADDRESS_SQL} AS raw_address,\n"
        "    CAST(NULL AS Nullable(String)) AS care_of,\n"
        f"    {_NO_CODE_STREET_SQL} AS street_address,\n"
        "    CAST(NULL AS Nullable(String)) AS postal_code,\n"
        f"    {_NO_CODE_TOWN_SQL} AS post_town,\n"
        "    CAST(NULL AS Nullable(String)) AS county,\n"
        f"    if({_SWEDISH_COUNTRY_SQL}, 'SE', CAST(NULL AS Nullable(String))) AS country_code\n"
        "FROM corpscout.se_esef_facts AS facts\n"
        "INNER JOIN corpscout.se_esef_filings AS filings ON filings.fxo_id = facts.fxo_id\n"
        "WHERE facts.concept_local_name = 'AddressOfRegisteredOfficeOfEntity'\n"
        "  AND filings.processed_at IS NOT NULL\n"
        "  AND facts.company_id IN %(company_ids)s\n"
        # The newest filing wins; inside one filing the Swedish text wins over an English twin.
        "ORDER BY filings.period_end DESC, filings.processed_at DESC, facts.language DESC, facts.fact_id\n"
        "LIMIT 1 BY facts.company_id"
    )


se_company_address_suggestions_esef = define_address_suggestion_asset(
    source="esef",
    extractor_version=ESEF_ADDRESS_EXTRACTOR_VERSION,
    current_sql=esef_current_sql(),
    select_sql=esef_select_sql(),
    deps=[dg.AssetKey("esef_facts_clickhouse"), dg.AssetKey("esef_entity_registry_map_clickhouse")],
    description=(
        "The registered office tagged in the company's newest ESEF filing (se_esef_facts through "
        "the register-verified link) into se_company_address_suggestion, kind registered, slot '': "
        "cleaned and re-packed for the normaliser when a Swedish postcode is found, delivered as a "
        "bare street line otherwise."
    ),
)
```

`facts.language DESC` puts `sv` before `en` (and before `se`, `sv-se`: acceptable, one fact per filing on prod today). The asset key `esef_facts_clickhouse` is the facts output of the `esef_parsing_clickhouse` multi-asset (`esef_filings/partitioned_publish.py`). Then `tables.SOURCES = ("scb", "bolagsverket", "ratsit", "esef", "reviewer", "reviewer_draft")`, `assets.EXTRACTOR_SOURCES = ("scb", "bolagsverket", "ratsit", "esef")`, `ADDRESS_PRECEDENCE = {"reviewer": 20000, "bolagsverket": 1000, "scb": 900, "esef": 500, "ratsit": 300}`. Dagster's autoload picks up module-level asset objects under `defs/` (`ratsit.py` has no `defs =` and no import in `address/__init__.py`), so the new module needs no registration.

The test for the SQL text also asserts the no-postcode split: `assert esef._NO_CODE_TOWN_SQL in sql` is not needed (private); instead assert `"AS post_town" in sql` and that `"[^,]+)$'" in sql` (the last-comma town regex is present).

- [x] **Step 4: Run the tests and the definitions check**

Run: `uv run pytest tests/test_se_company_address_extractors_sql.py tests/test_se_company_address_tables.py tests/test_se_company_address_precedence.py tests/test_se_company_address_assets.py tests/test_se_company_address_jobs.py -q -p no:warnings` and `WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run dg check defs`.
Expected: PASS; definitions load with the new asset in group `se_company_address`, and the extract job's selection includes it.

- [x] **Step 5: Commit**

```bash
git add corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/address/esef.py corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/address/tables.py corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/address/assets.py corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/address/precedence.py corpscout/services/dagster_v3/tests/test_se_company_address_extractors_sql.py corpscout/services/dagster_v3/tests/test_se_company_address_tables.py corpscout/services/dagster_v3/tests/test_se_company_address_precedence.py
git commit -m "feat(se): the address entity takes the ESEF registered office as an esef suggestion"
```

---

## Task 2: The extractor runs in the ClickHouse-local harness

**Files:**
- Modify: `corpscout/services/dagster_v3/tests/fixtures/se_basic_info_source_tables.sql` (adds `esef_filings` and `esef_facts` fixture tables; the map table is already there)
- Modify: `corpscout/services/dagster_v3/tests/test_se_company_address_extractors_clickhouse_local.py`

**Interfaces:**
- Consumes: `esef.esef_current_sql()`, `esef.esef_select_sql()`, `ESEF_ADDRESS_EXTRACTOR_VERSION`; `dagster_v3.defs.esef_filings.country_views.build_se_esef_view_sql` and `tables.SE_ESEF_VIEWS` (slice 1) to render `se_esef_facts` and `se_esef_filings`.

- [x] **Step 1: Write the failing test**

Add fixture tables (column names and types as the live tables: read them with `DESCRIBE` on `http://companycollect:8123/?database=corpscout`, user `default`, password from `corpscout/services/backoffice/.env`, never printed; only the columns the view lists are needed, but the view selects every product column so declare them all):

```sql
CREATE TABLE IF NOT EXISTS corpscout.esef_filings ( ...all 19 columns... ) ENGINE = ReplacingMergeTree(resolved_at) ORDER BY (lei, period_end, fxo_id);
CREATE TABLE IF NOT EXISTS corpscout.esef_facts ( ...all 21 columns... ) ENGINE = ReplacingMergeTree(resolved_at) ORDER BY (processed_week, lei, period_end, fxo_id, fact_id);
```

In the harness `_schema()`, after the fixture statements append the two rendered views:

```python
from dagster_v3.defs.esef_filings import tables as esef_tables
from dagster_v3.defs.esef_filings.country_views import build_se_esef_view_sql

_ESEF_VIEWS = [build_se_esef_view_sql(v) for v in esef_tables.SE_ESEF_VIEWS if v.table in ("esef_facts", "esef_filings")]
```

and a new test module section: seed one map row (`lei 'ESEFLEI0000000000001'`, `country_iso2 'SE'`, `registry_id '5560125220'`, `match_source 'gleif_registered_as'`, `link_status 'register_verified'`), one filing per case and one fact per filing with these `raw_value`s, then run the esef scope, the esef insert (`_insert(esef.esef_select_sql(), [...], extractor_version=esef.ESEF_ADDRESS_EXTRACTOR_VERSION)`), and read back `raw_address, street_address, country_code` per company:

| company_id | raw_value | expected raw_address | street_address | post_town | country_code |
|---|---|---|---|---|---|
| `5560125220` | `Kungsträdgårdsgatan 2, 106 70 Stockholm` | `Kungsträdgårdsgatan 2$$Stockholm$10670$` | NULL | NULL | NULL |
| `5561552760` | `Regeringsgatan 25, 111 53 Stockholm, Sverige` | `Regeringsgatan 25$$Stockholm$11153$SE` | NULL | NULL | `SE` |
| `5562434182` | `<div>Kungsgatan 17</div><div>111 43 Stockholm</div>` | `Kungsgatan 17$$Stockholm$11143$` | NULL | NULL | NULL |
| `5563333333` | `Ideongatan 1, Lund.` | NULL | `Ideongatan 1` | `Lund` | NULL |
| `5564444444` | `Lands vägen 57, Box 1264, 172 25 Sundbyberg` | `Lands vägen 57, Box 1264$$Sundbyberg$17225$` | NULL | NULL | NULL |

(One company per case, each with its own map row and filing. The tag case documents that tags become spaces and whitespace collapses; a tag inside a word, as prod has in "Stockhol<span></span>m", stays split and is out of scope.) Also assert the normalize hand-off: run `changed_rows_sql()` the way the existing `test_normalize_hand_off_gives_the_expected_parse_statuses` does and expect `parsed` for the packed rows and `partial` for the `Ideongatan 1` / `Lund` components row (no postcode); if the normaliser returns another status for that row, pin the actual value with a comment saying why.

Add the esef case to the existing scope-convergence test (`test_all_three_scopes_converge_after_insert` becomes four scopes).

- [x] **Step 2: Run to verify it fails**

Run: `uv run pytest tests/test_se_company_address_extractors_clickhouse_local.py -q -p no:warnings` (docker).
Expected: FAIL (unknown table `se_esef_facts`).

- [x] **Step 3: Implement the harness changes**, then run the same command. Expected: PASS, every test in the module.

- [x] **Step 4: Commit**

```bash
git add corpscout/services/dagster_v3/tests/fixtures/se_basic_info_source_tables.sql corpscout/services/dagster_v3/tests/test_se_company_address_extractors_clickhouse_local.py
git commit -m "test(se): the address extractors harness runs the esef registered-office extractor end to end"
```

---

## Task 3: The backoffice knows the source

**Files:**
- Modify: `corpscout/services/backoffice/app/lib/se-address-fields.ts:7,35-36`
- Test: `corpscout/services/backoffice/tests/se-address-fields.test.ts:12`

- [x] **Step 1:** Re-pin: `expect([...ADDRESS_SOURCES]).toEqual(["scb", "bolagsverket", "ratsit", "esef", "reviewer", "reviewer_draft"])` and `expect(addressSourceLabel("esef")).toBe("ESEF")`. Run `cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/services/backoffice && npx vitest run tests/se-address-fields.test.ts`: FAIL.
- [x] **Step 2:** `ADDRESS_SOURCES = ["scb", "bolagsverket", "ratsit", "esef", "reviewer", "reviewer_draft"] as const;` and `esef: "ESEF"` in `SOURCE_LABELS`. Run `npm run typecheck && npx vitest run tests/se-address-fields.test.ts`: PASS. Grep `app/` for any exhaustive `switch` or `Record<SeAddressSource, ...>` the type widening breaks (`npm run typecheck` reports them) and add the `esef` entry there too.
- [x] **Step 3:** Commit: `git add corpscout/services/backoffice/app/lib/se-address-fields.ts corpscout/services/backoffice/tests/se-address-fields.test.ts && git commit -m "feat(backoffice): the address tab labels the esef source"`.

---

## Task 4: Docs

- [x] **Step 1:** `address/docs/address-design.md`, section "Extractors (slice 1)": a fourth bullet for `esef` (reads `se_esef_facts` joined to `se_esef_filings`, the `AddressOfRegisteredOfficeOfEntity` fact of the newest filing, cleaned and re-packed into the Bolagsverket format when a Swedish postcode is found, otherwise street and town components split at the last comma; kind `registered`, slot `''`; source record uid = the filing package's uid; precedence `esef: 500`). `docs/superpowers/specs/2026-09-06-se-company-address-entity-design.md`: line 41 lists ESEF as a source since slice 3 of the 2026-09-08 ESEF design, line 54 drops "ESEF address extraction" from the out-of-scope list. `docs/superpowers/specs/2026-09-08-esef-entity-link-people-addresses-design.md` section 3, first bullet: after "`raw_address` = the `AddressOfRegisteredOfficeOfEntity` fact" add the sentence "The normaliser's `raw_address` path parses only the Bolagsverket packed form, so the extractor cleans the fact (tags, whitespace, trailing punctuation and country word) and re-packs it when a Swedish postcode is found; a fact without one is delivered as street and town components with `raw_address` NULL." and replace "The normaliser parses the string; ..." with "A trailing "Sverige"/"Sweden" resolves to `SE`."
- [x] **Step 2:** Commit: `git add corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/address/docs/address-design.md corpscout/services/dagster_v3/docs/superpowers/specs/2026-09-06-se-company-address-entity-design.md && git commit -m "docs(se): the address entity's esef extractor"`.

---

## Task 5: Verify and merge

- [x] **Step 1:** `WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run pytest -q -m "not integration" --deselect tests/test_schedule_cron_contracts.py::test_every_schedule_fires_on_a_unique_minute_hour_pair -p no:cacheprovider -p no:warnings --color=no -rf 2>&1 | rg "^FAILED|passed"`. Expected: only the four failures already on main.
- [x] **Step 2:** backoffice `npm run typecheck && npx vitest run tests/se-address-fields.test.ts tests/se-company-tabs.server.test.ts`. Expected: PASS.
- [x] **Step 3:** No migration: merge `--no-ff` into main right away (footer).

---

## Task 6: Rollout

- [ ] **Step 1 (owner):** dbt-state refresh is not needed (no dbt change); light_sync deploy from main.
- [ ] **Step 2:** launch `se_company_address_suggestions_esef` with `execute: true` (preview first with the default `execute: false` and read the count: about 404 companies). Verify `SELECT count(), countIf(raw_address IS NOT NULL), countIf(street_address IS NOT NULL) FROM corpscout.se_company_address_suggestion FINAL WHERE source = 'esef'`.
- [ ] **Step 3:** launch `se_company_address_normalize` (`changed_only: true`, default); verify `SELECT parse_status, count() FROM corpscout.se_company_address_normalized FINAL WHERE source = 'esef' GROUP BY 1`.
- [ ] **Step 4:** fold the touched companies: the fold asset in changed-only mode over the buckets (the address design's "Selection (fold)" section names the asset and its config); verify `SELECT count() FROM corpscout.se_company_address FINAL WHERE has(source_names, 'esef')` (or the fold's equivalent source column) is close to the parsed count, and that Handelsbanken's address tab (`/admin/se/company/5020077862/address`) lists `Kungsträdgårdsgatan 2, 106 70 Stockholm` with the ESEF source badge.
- [ ] **Step 5:** run the geocode warm step if the address design requires it after a fold (its "Warm step" section).
- [ ] **Step 6:** re-run `se_company_address_precedence_clickhouse` (the address design asks for it after a precedence-dictionary change; the fold reads only company override rows from that table, so nothing breaks before it runs, the exported global rows just lack `esef`).
