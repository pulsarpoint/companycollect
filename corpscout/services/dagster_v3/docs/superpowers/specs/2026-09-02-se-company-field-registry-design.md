# SE company field registry, candidates and resolve — design

Date: 2026-09-02. Country: Sweden only. Consumer: the backoffice admin surface.
Supersedes the publisher half of `2026-08-22-sweden-company-source-artifacts-design.md`
(the artifact layer stays) and the resolve half of
`2026-09-01-se-company-info-field-values-design.md` (the decisions table stays).

## 1. Goal

Replace the hand-written publisher of `corpscout.se_company_info` (Python precedence in
`info_rules.py`, LLM merge pass, field-value overrides) with a registry-driven model:

- a **field registry** in code declares every scalar company attribute, the sources that may
  contribute it, a rank per source, and a resolve policy;
- every source writes **candidates** into one long, append-only table;
- reviewer **decisions** stay in `se_company_info_field_value`;
- one **resolve** step picks a value per company and field, writes a long resolved table, and
  regenerates the wide `se_company_info` from it;
- the same generated SQL resolves one company in the backoffice right after a decision.

Cutover is big-bang: one branch replaces the whole publisher and rebuilds all 3.5M rows.
Phase A (data model and publisher) ships first; phase B (registry-driven admin page) follows on
the same design.

## 2. Decisions taken during design

| Question | Decision |
| --- | --- |
| Consumer of the resolved layer | Backoffice admin only for now; public views later |
| Cutover | Big-bang replacement of the publisher |
| Fields in scope | Description (en, sv); industry (SNI + NACE + label); registry facts (legal name, legal form, status, incorporation date); derived scalars (website, employee count, latest revenue with fiscal year) |
| Registry ownership | Code (Python, dagster_v3), exported to ClickHouse; backoffice reads, never edits |
| LLM role | Candidate source for text fields, ranked above other sources, below reviewer |
| Model shape | Long candidates + long resolved + wide projection |
| Registry scope | Generic framework with composite keys; instantiated for `info` now; `financial` and `jobs` registries are separate later specs |
| Policy execution | ClickHouse SQL generated from the registry; run by Dagster in bulk and by the backoffice per company |
| Policy model | One default `source_precedence` policy for every field; optional per-field override implementing the same interface; zero overrides today |
| Orchestration | Dagster for extraction, bulk resolve and triggers. A Temporal + TypeScript resolver was considered and rejected |

## 3. Layers

```
S3 raw  ->  landed copy in ClickHouse  ->  per-source normalized tables / artifacts
        ->  se_company_field_candidate (long, append-only)
        ->  se_company_info_field_value (decisions, latest row wins)          [exists]
        ->  se_company_field (long resolved, one row per company x field)
        ->  se_company_info (wide projection; existing columns + new)         [exists]
        ->  se_companies_serving (refreshable MV)                              [exists]
```

Raw, landed, candidates, decisions and resolved rows are never deleted. Normalized tables,
the wide projection and serving are rebuilt.

## 4. Registry framework

Package `dagster_v3.defs.se_company.fields` (new):

- `registry.py` — the framework types and the `info` instance.
- `policies.py` — the `FieldPolicy` interface, `source_precedence`, and any overrides.
- `export.py` — renders the registry and the generated SQL into ClickHouse rows.

### 4.1 Types

```python
@dataclass(frozen=True)
class FieldSpec:
    name: str            # snake_case, unique within the datatype
    value_type: str      # text | code | date | integer | decimal | url | json
    display_group: str   # identity | activity | scale
    structured: bool     # compare value_json instead of value
    sources: tuple[str, ...]            # precedence order, first wins; no numbers, so inserting
                                        # a new source anywhere is a one-line edit
    policy: str = "source_precedence"   # name in policies.POLICIES
    python_only: bool = False           # resolved by Dagster alone; backoffice shows "next run"

@dataclass(frozen=True)
class DatatypeRegistry:
    datatype: str                 # "info"
    country: str                  # "SE"
    key_columns: tuple[str, ...]  # ("company_id",) for info; composite for financial/jobs later
    fields: tuple[FieldSpec, ...]
    version: str                  # bumped on any field, source, rank or policy binding change
```

Import-time validation: unique field names, no duplicate source within a field, every source
name in `KNOWN_SOURCES`, every policy name in `POLICIES`, `reviewer` never listed. Reviewer
decisions are not a source; they win by construction (section 7.4), and the backoffice renders
them above the candidates list.

### 4.2 The `info` registry (version `se-info-v1`)

| Group | Field | Type | Sources in precedence order (reviewer decisions always first) |
| --- | --- | --- | --- |
| identity | legal_name | text | bolagsverket, scb, wikidata |
| identity | legal_form_code | code | bolagsverket, scb |
| identity | status | code | bolagsverket, scb |
| identity | incorporation_date | date | bolagsverket, scb, wikidata |
| activity | description | text | llm, esef, wikidata, scb |
| activity | description_sv | text | llm, scb |
| activity | primary_sni_code | code | scb, ratsit |
| activity | primary_nace_code | code | scb, ratsit |
| activity | industry_label_en | text | scb, ratsit, wikidata |
| scale | website | url | domains, wikidata |
| scale | employee_count | json (count, as_of, period) | esef, bolagsverket, ratsit, wikidata |
| scale | latest_revenue | json (amount, currency, amount_usd, fiscal_year, period_end) | esef, bolagsverket, ratsit |

`latest_revenue_fiscal_year` and the employee as-of period are members of the JSON value, not
separate fields; the wide projection extracts them into their own columns.

With precedence alone, latest revenue from a rank-1 source wins even when a lower-ranked source
has a newer fiscal year. The ranks above are the initial choice; a policy override exists if it
ever matters.

### 4.3 Export

Asset `se_company_field_registry_clickhouse` (group `se_company_fields`) writes:

```sql
CREATE TABLE corpscout.se_company_field_registry (
    datatype LowCardinality(String), country LowCardinality(String),
    field String, value_type LowCardinality(String), display_group LowCardinality(String),
    structured Bool, python_only Bool,
    sources Array(String),              -- precedence order; position is the rank
    policy_name LowCardinality(String), policy_version String,
    resolve_sql String,                 -- generated statement for this field, see section 7
    registry_version String, version DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(version) ORDER BY (datatype, country, field);
```

The wide projection statement (section 8.3) is exported as one extra row per datatype with
`field = '*'`, `value_type = 'projection'` and the pivot SQL in `resolve_sql`, so both runners
read every statement they need from this one table.

Consumers read with `argMax(..., version)` like `se_code_labels`. The decisions table CHECKs
(`known_field`, `known_source`) are widened by migration to the registry's lists, and a test
pins the migration to the registry so they cannot drift.

## 5. Candidates

### 5.1 Table

```sql
CREATE TABLE corpscout.se_company_field_candidate (
    company_id String,
    field LowCardinality(String),
    source LowCardinality(String),
    source_record_uid String,
    value String,                           -- display form; absent = no row, never empty
    value_json String DEFAULT '',           -- structured payload and compare keys
    observed_at DateTime64(3, 'UTC'),       -- when the source observed it
    extracted_at DateTime64(3, 'UTC'),      -- this extraction run
    extractor_version LowCardinality(String),
    source_run_id String,
    evidence_hash FixedString(64) MATERIALIZED lower(hex(SHA256(concat(
        field, '\n', source, '\n', source_record_uid, '\n', ifNull(value, ''), '\n', value_json)))),
    CONSTRAINT has_company CHECK match(company_id, '^([0-9]{10}|[0-9]{12})$'),
    CONSTRAINT has_value CHECK trim(value) != ''
) ENGINE = ReplacingMergeTree(extracted_at)
ORDER BY (company_id, field, source, source_record_uid);
```

A re-extraction of the same source record replaces its row; a new source record adds one.
Nothing is deleted. The ORDER BY serves both the resolver (company prefix) and the backoffice
candidates list.

`value_json` always carries `compare_key` (normalised form used for agreement counting) and,
per field, the structured members named in section 4.2. Extractors compute everything a policy
would otherwise need code for: parsed dates, converted amounts, detected language.

### 5.2 Extractor assets (group `se_company_fields`)

One asset per source family. Each reads its source tables, emits rows for every field it knows,
and publishes with the existing `publish_with_stage` anti-join on
`(company_id, field, source, source_record_uid, evidence_hash)` so unchanged rows are not
rewritten.

| Asset | Reads | Fields |
| --- | --- | --- |
| `se_company_field_candidates_scb` | `se_company_info_scb`, `se_industries`, `se_company_registry_current` (source = scb), `nace_categories` | description (English translation preferred, Swedish original as description_sv), primary_sni_code, primary_nace_code, industry_label_en, legal_name, legal_form_code, status, incorporation_date |
| `se_company_field_candidates_bolagsverket` | `se_company_registry_current` (source = bolagsverket), `se_financials_bolagsverket_current` | legal_name, legal_form_code, status (with `conflict` in value_json), incorporation_date, employee_count, latest_revenue |
| `se_company_field_candidates_esef` | `se_company_info_esef`, `se_financials_esef_current` | description, employee_count, latest_revenue |
| `se_company_field_candidates_wikidata` | `se_company_info_wikidata`, `wikidata_company_websites` | description, legal_name, incorporation_date, industry_label_en, website, employee_count |
| `se_company_field_candidates_ratsit` | `se_ratsit_company_industry_codes`, `se_ratsit_financial_periods` | primary_sni_code, primary_nace_code, industry_label_en, employee_count, latest_revenue |
| `se_company_field_candidates_domains` | `company_domains` | website (`review_status = confirmed_primary` first, else `suggested_primary` with highest confidence) |
| `se_company_field_candidates_llm` | text candidates above, `se_company_info_enrichment_observation` | description, description_sv |

Extractor rules:

- `source_record_uid` is the source's own record uid (artifact uid, financial source uid,
  domain `evidence_fingerprint`, LLM `suggestion_id`).
- `observed_at` is the source observation time (artifact `observed_at`, financial
  `report_period_end`, domain `last_seen_at`, LLM `created_at`).
- Empty, whitespace-only and known placeholder values are not emitted.
- Each extractor is scoped by `company_ids` and `max_companies` like the current artifacts, and
  by default processes only companies whose source rows changed since the extractor's last run
  (source `observed_at` or `updated_from_raw_at` newer than the candidate's `extracted_at`).

### 5.3 LLM candidates

`se_company_field_candidates_llm` is the current pass-2 code behind the candidate contract:
the same prompt (`se-company-info-description-v3`), the same `input_hash` reuse of stored
observations, the same observation table. Differences: it runs only for companies with two or
more non-LLM text candidates whose set changed since the last LLM candidate; its output is a
candidate row per field with `source = llm`, `source_record_uid = suggestion_id`; it never
writes the published row. Provider and model remain required run config (no default).

## 6. Decisions

`se_company_info_field_value` is unchanged in shape. Its `field` and `source` CHECKs are
widened to the registry lists. A decision's `source` is the candidate source it copied
(`scb`, `esef`, ... , `llm`) or `reviewer` for a typed value; `value IS NULL` releases the field.
Live decision per field = newest `(created_at, toString(value_id))`.

## 7. Policies and generated SQL

### 7.1 Interface

```python
class FieldPolicy(Protocol):
    name: str
    version: str
    def candidate_filter_sql(self, field: FieldSpec) -> str: ...   # WHERE fragment over c.*
    def winner_order_sql(self, field: FieldSpec) -> str: ...       # ORDER BY fragment
    def compare_key_sql(self, field: FieldSpec) -> str: ...        # expression over c.*
```

### 7.2 Default `source_precedence` (version `v1`)

- filter: `c.value IS NOT NULL AND trim(c.value) != ''`
- order: `rank ASC, c.observed_at DESC, c.source_record_uid DESC` where
  `rank = indexOf({sources:Array(String)}, c.source)` and `sources` is the field's precedence
  tuple rendered inline from the registry (a source absent from the tuple is not eligible).
- compare key: `JSONExtractString(c.value_json, 'compare_key')` if present, else
  `lowerUTF8(trim(c.value))`.

### 7.3 Override slot

`FieldSpec.policy` names any entry in `POLICIES`. Zero fields override today. Adding one is a
registry edit plus one policy class with its own tests; the registry version bumps.

### 7.4 Generated statement

For each field the exporter renders one statement, stored in `resolve_sql` and used verbatim by
both runners. Shape:

```sql
INSERT INTO corpscout.se_company_field (...)
WITH
  decision AS (
    SELECT company_id, argMax(value, (created_at, toString(value_id))) AS value,
           argMax(source, (created_at, toString(value_id))) AS source,
           argMax(source_ref, (created_at, toString(value_id))) AS source_ref,
           argMax(value_id, (created_at, toString(value_id))) AS value_id
    FROM corpscout.se_company_info_field_value
    WHERE field = {field:String} AND company_id IN {company_ids:Array(String)}
    GROUP BY company_id),
  eligible AS (
    SELECT c.*, <rank expr> AS rank, <compare key> AS compare_key
    FROM corpscout.se_company_field_candidate AS c
    WHERE c.field = {field:String} AND c.company_id IN {company_ids:Array(String)}
      AND (<candidate_filter_sql>)),
  agreement AS (
    SELECT company_id, compare_key, groupUniqArray(source) AS agreeing_sources
    FROM eligible GROUP BY company_id, compare_key),
  winner AS (
    SELECT * FROM eligible ORDER BY company_id, <winner_order_sql> LIMIT 1 BY company_id)
SELECT ... -- decision row when present and value IS NOT NULL; else winner; no row when neither
```

A released decision (`value IS NULL`) means "use the winner"; a company with no winner and no
decision gets no row (and the wide projection shows NULL for that field).

The rendered SQL is pinned as text in tests and executed in the clickhouse-local harness with
seeded candidates and decisions covering: decision beats winner, release falls back, rank
order, same-rank recency, empty candidates filtered, agreement counting, no-row when nothing.

## 8. Resolve asset and wide projection

### 8.1 Resolved table

```sql
CREATE TABLE corpscout.se_company_field (
    company_id String, field LowCardinality(String),
    value String, value_json String DEFAULT '',
    source LowCardinality(String), source_record_uid String, observed_at DateTime64(3, 'UTC'),
    decision_id Nullable(UUID),
    policy_name LowCardinality(String), policy_version String,
    candidate_count UInt16, agreeing_sources Array(String),
    registry_version String, source_run_id String,
    resolved_at DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(resolved_at) ORDER BY (company_id, field);
```

### 8.2 Asset `se_company_field_resolved_clickhouse` (group `se_company_fields`)

Replaces `se_company_info_clickhouse`. Per run:

1. Select the company set: `company_ids` config, or the changed-company scan (section 8.4),
   or every company when `resolve_all` is set.
2. For each registry field, execute its `resolve_sql` for the set in pages of
   `company_batch_size`.
3. Re-pivot the wide row for the set (section 8.3) and publish it with `publish_with_stage`.
4. Emit metadata: per field, rows resolved, rows from decisions, rows per winning source,
   companies with no row.

The asset keeps `execute` (preview by default) and the `company_ids` / `max_companies` scoping
of the current asset.

### 8.3 Wide projection

`se_company_info` keeps its name, engine and every existing column so `se_companies_serving`
and the backoffice loaders survive the cutover. Column sources after the cutover:

| Column | From |
| --- | --- |
| legal_name, legal_form_code, status, incorporation_date | resolved fields |
| legal_form_label_en/sv | `se_code_labels` lookup on the resolved code (curated, never a candidate) |
| description, description_sv | resolved fields |
| description_language | `value_json.language` of the winning description |
| primary_nace_code, primary_sni_code | resolved fields |
| wikidata_id, lei | kept; taken from the wikidata / esef artifacts as today |
| llm_enhanced | `source = 'llm'` on the description row |
| description_sources, description_source_record_uids, description_source_count | candidates present for `description` |
| suggestion_id, model_provider, model_name, prompt_version | from the LLM candidate's observation when the description source is `llm`; `deterministic` values otherwise |
| correction_ids | decision ids applied across all fields |
| source_record_uids, evidence_hashes | all winning candidates' uids and hashes |
| resolved_at, source_run_id | the run |

New columns (one additive migration): `industry_label_en String DEFAULT ''`,
`website Nullable(String)`, `employee_count Nullable(UInt64)`,
`employee_count_as_of Nullable(Date32)`, `latest_revenue_amount Nullable(Decimal128(2))`,
`latest_revenue_currency LowCardinality(String) DEFAULT ''`,
`latest_revenue_amount_usd Nullable(Decimal128(2))`, `latest_revenue_fiscal_year Nullable(UInt16)`.

The legacy provenance columns are kept for one release and dropped in phase B once the
backoffice reads `se_company_field` directly.

The SCB row remains mandatory for publication: a company without a `legal_name` candidate from
`scb` or `bolagsverket` is not published, as today.

### 8.4 Incrementality

A company is re-resolved when any of these is newer than its `resolved_at` in `se_company_info`:

- a candidate's `extracted_at` for any registry field;
- a decision's `created_at`;
- the registry version or any policy version differs from the one stamped on its resolved rows;
- it has never been published.

The scan is the current `build_changed_companies_sql` with the `artifacts` CTE replaced by a
`candidates` CTE. `se_company_info_field_value_sensor` keeps its cursor and launches this asset.
The weekly schedule launches it with no explicit scope. `resolve_all` forces every company.

## 9. Backoffice resolve after a decision

After `appendSeCompanyInfoFieldValues` inserts the decision rows, the action:

1. reads `resolve_sql` for each decided field from `se_company_field_registry` (cached per
   registry version);
2. executes it with `company_ids = [companyId]`;
3. re-pivots that company's wide row using the projection statement from the registry table's
   `field = '*'` row (one statement, not per field);
4. returns; the loader then shows the resolved value.

The writer role gains `INSERT` on `se_company_field` and `se_company_info`. A `python_only`
field is skipped with the note "applies on next run". The sensor still re-resolves the company
in bulk; the result is identical and lands as a same-value version.

## 10. Serving

`se_companies_serving` keeps its base and gains the new wide columns. The public page's dbt
`company_*_current` tables keep reading the serving view. No `se_company_view_*` tables in this
phase; a future per-audience view is built from `se_company_field`.

## 11. Phase B: the admin Info page

- One `FieldGroupCard` component renders a display group from the registry export: per field the
  resolved value, a source chip with observed date, an expandable candidates list with "Use this"
  per candidate, and Edit / Release per field. The description card becomes the first instance,
  keeping its language toggle.
- Groups: identity, activity, scale. Absent fields render as absent.
- The per-source artifact cards collapse into a "Sources" drawer. The Published version card
  goes. Value history, the pipeline sheet and the LLM suggestions card stay.
- The loader reads `se_company_field` and `se_company_field_candidate` for the company instead
  of the wide row's legacy provenance columns; the legacy columns are then dropped by migration.
- `se-info-field-values.ts` validates against the registry export instead of hard-coded enums.

## 12. Migrations, cutover, backfill, tests

Migrations (additive only, no DROP in the ledger):

1. `se_company_field_registry`, `se_company_field_candidate`, `se_company_field` tables;
   INSERT grants on the latter two for the writer role.
2. New columns on `se_company_info`.
3. Widened CHECKs on `se_company_info_field_value`.

Cutover, on the prod Dagster host:

1. Apply migrations; deploy dagster_v3 (old asset still present, sensor stopped).
2. Materialize the registry export, then every candidates asset in full (`max_companies`
   unbounded), LLM asset last with the required provider config.
3. Run the resolve asset with `resolve_all` and `execute`.
4. Run the parity asset check: for companies whose old `llm_enhanced = false`, new description
   equals old; for `llm_enhanced = true`, new description equals the stored observation's;
   legal facts and codes equal the old row for every company; counts of rows per field per
   source reported.
5. Deploy the branch that deletes `se_company_info_clickhouse`, `info_rules.py` and the old scan;
   start the sensor; enable the weekly schedule on the new asset.
6. Deploy the backoffice (phase A: unchanged page reading the same columns; the resolve-after-
   decision path active).

Rollback within the window: the old asset code is still deployed until step 5; re-running it
republishes from artifacts (the wide table is ReplacingMergeTree, newer `resolved_at` wins).

Tests:

- registry: validation rules; version string changes when any field/rank/policy changes;
  export rows match the module.
- policies: generated SQL pinned as text; executed in the clickhouse-local harness for every
  path listed in 7.4.
- extractors: each asset's SQL pinned and executed against seeded source tables in the harness;
  one row per (field, source) with the documented `source_record_uid` and `observed_at`.
- pivot: the wide row produced from seeded long rows equals a hand-written expected row for the
  Handelsbanken fixture used by the existing harness.
- backoffice: single-company resolve under the existing `VITEST_LIVE` pattern; validator reads
  the registry export.
- parity asset check as above.

## 13. Out of scope, recorded

- Financial and jobs registries: separate specs; same framework, composite keys, their own
  policies (tolerance, unit normalisation, dedup).
- Public per-audience views from `se_company_field`.
- Clock-skew stranding for sensor-launched resolves (created_at from the backoffice clock);
  unchanged from today, mitigated by the synchronous backoffice resolve.
- Other countries: the framework is country-parameterised; only SE is instantiated.

## 14. Naming

- Assets: `se_company_field_registry_clickhouse`, `se_company_field_candidates_<source>`,
  `se_company_field_resolved_clickhouse`; group `se_company_fields`.
- Tables: `se_company_field_registry`, `se_company_field_candidate`, `se_company_field`.
- Registry version strings: `se-info-vN`; policy versions: `<name>-vN`.
