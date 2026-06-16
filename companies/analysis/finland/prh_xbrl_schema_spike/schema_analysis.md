# Finland PRH XBRL Sample Schema Analysis

Date checked: 2026-06-12

## Purpose

This spike samples the PRH digital financial statement API before implementing
the Dagster/Temporal financial ingestion path. The goal is to understand the XML
shape and propose a ClickHouse schema that can store the raw XBRL facts without
losing taxonomy context.

This is not a complete taxonomy implementation. The sample size is intentionally
small: 10 statements from one PRH registration window, enough to design the first
storage shape and identify the important parser risks.

## Inputs

Official PRH endpoints used:

- Discovery:
  `https://avoindata.prh.fi/opendata-xbrl-api/v3/all_financial_statements?registeredDateStart=2025-01-01&registeredDateEnd=2025-01-31&page=1`
- Statement XML:
  `https://avoindata.prh.fi/opendata-xbrl-api/v3/financial?businessId=<business_id>&financialDate=<YYYY-MM-DD>`
- OpenAPI schema:
  `https://avoindata.prh.fi/opendata-xbrl-api/v3/schema?lang=en`

Taxonomy helper workbook:

- `https://fi.xbrl.org/wp-content/uploads/sites/17/2020/09/OYTP_2019-v01.xlsx`

Local artifacts:

- `samples/*.xml` - 10 downloaded XML statement samples.
- `derived/sample_manifest.json` - selected statements and checksums.
- `derived/structure_summary.json` - parser summary from the sample set.
- `derived/concept_inventory.csv` - observed fact concepts.
- `derived/taxonomy_code_matches.csv` - observed taxonomy code labels from the
  workbook.
- `derived/line_item_candidates.csv` - observed line-item candidates with labels
  and counts.

The local parser was run with `lxml 5.4.0` from
`data-pipelines/services/python-worker/.venv`.

## Sample Set

| # | Business ID | Financial date | Registration date | XML bytes |
|---:|---|---|---|---:|
| 1 | 0104539-0 | 2024-10-31 | 2025-01-18 | 43,685 |
| 2 | 0144206-2 | 2024-09-30 | 2025-01-30 | 37,677 |
| 3 | 0152307-3 | 2024-03-31 | 2025-01-10 | 89,073 |
| 4 | 0159948-1 | 2024-12-31 | 2025-01-30 | 43,676 |
| 5 | 0163207-7 | 2024-12-31 | 2025-01-21 | 29,739 |
| 6 | 0176460-0 | 2023-09-30 | 2025-01-23 | 43,703 |
| 7 | 0176460-0 | 2024-09-30 | 2025-01-23 | 42,694 |
| 8 | 0186191-0 | 2024-06-30 | 2025-01-17 | 47,245 |
| 9 | 0200510-4 | 2024-12-31 | 2025-01-30 | 36,610 |
| 10 | 0202458-3 | 2024-09-30 | 2025-01-02 | 61,457 |

Aggregate sample observations:

- XML size: 475,559 bytes.
- XBRL contexts: 828.
- XBRL units: 10, all `iso4217:EUR`.
- Facts: 858 total, 818 numeric and 40 text/date facts.
- Unique fact concepts: 6.
- Unique observed `fi_dim:MCY` line-item members: 74.
- All 10 samples reference the same taxonomy entrypoint:
  `http://www.valtiokonttori.fi/fi/fr/xbrl/crr/fws/oytp/kpl-2016-12/2019-03-28/mod/oytp_gaap_ind.xsd`.

## XML Shape

The 10 samples are XBRL instance documents with an `<xbrl>` root. They are not
HTML inline XBRL documents in this sample set.

Each statement has:

- `link:schemaRef` pointing to the OYTP GAAP individual company entrypoint.
- `xbrli:context` elements with entity identifier, period, and optional scenario
  dimensions.
- `xbrli:unit` elements, observed as EUR.
- Fact elements under `fi_met:*` with `contextRef`, optional `unitRef`, and
  numeric/text/date content.

Important modeling point: financial statement line items are not separate XBRL
element names. Most financial values are facts like `fi_met:mi53` or
`fi_met:md103`, and the actual line item is stored in the referenced context as a
dimension member, mainly `fi_dim:MCY = fi_MC:<code>`.

Observed fact concepts:

| Concept | Meaning inferred from sample/workbook | Facts | Files | Value kind |
|---|---|---:|---:|---|
| `fi_met:mi53` | Book value / balance value (`Kirjanpitoarvo`) | 542 | 10 | numeric EUR |
| `fi_met:md103` | Accounting-period monetary accumulation (`Tilikauden rahallinen kertymä`) | 276 | 9 | numeric EUR |
| `fi_met:di120` | Period start date, inferred from values | 10 | 10 | date text |
| `fi_met:di121` | Period end date, inferred from values | 10 | 10 | date text |
| `fi_met:si168` | Company name, inferred from values | 10 | 10 | text |
| `fi_met:si289` | Business ID, inferred from values | 10 | 10 | text |

All observed contexts use instant periods. Do not assume P&L facts can be
identified by XBRL duration contexts. The accounting period start/end appear as
separate facts (`di120`, `di121`), while P&L rows are distinguished by
`fi_met:md103` and `fi_dim:MCY`.

Observed dimensions:

| Dimension | Meaning | Observation |
|---|---|---|
| `fi_dim:MCY` | Main category / financial line item (`Pääkategoria`) | Present on all 818 numeric facts. |
| `fi_dim:REF` | Reference date or period (`Viite ajankohta tai -kausi`) | Used on comparative contexts. |

Observed `fi_dim:REF` members:

| Member | Meaning |
|---|---|
| `fi_RF:x4` | End of previous accounting year T-1 (`Tilikauden loppussa T-1`) |
| `fi_RF:x53` | Previous accounting period T-1 (`Edellinen tilikausi T-1`) |

## Line-Item Examples

The taxonomy workbook maps observed `fi_MC` codes to Finnish labels. Examples
from the sample:

| Code | Finnish label | Observed facts | Common concept |
|---|---|---:|---|
| `fi_MC:x673` | Liikevaihto | 16 | `fi_met:md103` |
| `fi_MC:x689` | Liikevoitto (-tappio) | 20 | `fi_met:md103` |
| `fi_MC:x740` | Tilikauden voitto (tappio) | 20 | `fi_met:md103` |
| `fi_MC:x360` | Vastaavaa | 20 | `fi_met:mi53` |
| `fi_MC:x376` | Oma pääoma | 20 | `fi_met:mi53` |
| `fi_MC:x424` | Vieras pääoma | 20 | `fi_met:mi53` |
| `fi_MC:x399` | Rahat ja pankkisaamiset | 20 | `fi_met:mi53` |
| `fi_MC:x1768` | Lyhytaikaiset saamiset | 20 | `fi_met:mi53` |
| `fi_MC:x1811` | Lyhytaikainen vieras pääoma | 20 | `fi_met:mi53` |
| `fi_MC:x5` | Henkilöstökulut | 14 | `fi_met:md103` |
| `fi_MC:x6` | Palkat ja palkkiot | 12 | `fi_met:md103` |

The same `fi_MC` code can appear in several report template sheets with
different row numbers. Store the taxonomy code and label, not only the row
number.

## Schema Recommendation

Use a raw-first schema. The first parser should store statement documents,
contexts, units, taxonomy code labels, and facts without discarding dimensions.
Curated metric tables or views should be derived from raw facts after the
mapping is explicit and test-covered.

### `fi_prh_xbrl_statement_documents`

One row per downloaded XML statement.

```sql
CREATE TABLE fi_prh_xbrl_statement_documents (
  statement_key String,
  source_run_id String,
  business_id String,
  financial_date Date,
  registration_date Nullable(Date),
  source_url String,
  xml_object_key String,
  xml_sha256 FixedString(64),
  xml_size_bytes UInt64,
  root_name LowCardinality(String),
  schema_refs Array(String),
  taxonomy_entrypoint String,
  reported_business_id Nullable(String),
  reported_company_name Nullable(String),
  reported_period_start Nullable(Date),
  reported_period_end Nullable(Date),
  contexts_count UInt32,
  units_count UInt32,
  facts_count UInt32,
  parser_version String,
  parsed_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(parsed_at)
ORDER BY (business_id, financial_date, xml_sha256);
```

Use a deterministic `statement_key`, for example
`sha256(business_id + ':' + financial_date + ':' + xml_sha256)`. Keep the
download manifest's `business_id` and `financial_date` as primary metadata, and
also store the values reported inside XML (`si289`, `di121`) for validation.

### `fi_prh_xbrl_contexts`

One row per XBRL context.

```sql
CREATE TABLE fi_prh_xbrl_contexts (
  statement_key String,
  context_id String,
  entity_identifier String,
  entity_scheme String,
  period_type LowCardinality(String),
  instant_date Nullable(Date),
  period_start Nullable(Date),
  period_end Nullable(Date),
  dimensions Array(Tuple(
    dimension_code String,
    member_code String,
    member_label_fi String
  )),
  mcy_member_code Nullable(String),
  mcy_member_label_fi Nullable(String),
  ref_member_code Nullable(String),
  ref_member_label_fi Nullable(String),
  is_comparative UInt8
)
ENGINE = MergeTree
ORDER BY (statement_key, context_id);
```

The denormalized `mcy_*` and `ref_*` columns are query helpers. The full
`dimensions` array remains the source of truth so future taxonomy dimensions are
not lost.

### `fi_prh_xbrl_units`

One row per XBRL unit.

```sql
CREATE TABLE fi_prh_xbrl_units (
  statement_key String,
  unit_id String,
  measures Array(String),
  is_divide UInt8,
  raw_xml String
)
ENGINE = MergeTree
ORDER BY (statement_key, unit_id);
```

The sample only uses EUR, but the parser should support divide units because
XBRL allows them.

### `fi_prh_xbrl_facts_raw`

One row per fact element.

```sql
CREATE TABLE fi_prh_xbrl_facts_raw (
  statement_key String,
  business_id String,
  financial_date Date,
  fact_ordinal UInt32,
  concept_qname LowCardinality(String),
  concept_namespace LowCardinality(String),
  concept_local_name LowCardinality(String),
  context_id String,
  unit_id Nullable(String),
  decimals Nullable(String),
  precision Nullable(String),
  value_kind LowCardinality(String),
  raw_value String,
  numeric_value Nullable(Decimal(38, 6)),
  date_value Nullable(Date),
  text_value Nullable(String),
  mcy_member_code Nullable(String),
  mcy_member_label_fi Nullable(String),
  ref_member_code Nullable(String),
  ref_member_label_fi Nullable(String),
  is_comparative UInt8,
  dimensions Array(Tuple(
    dimension_code String,
    member_code String,
    member_label_fi String
  )),
  parser_version String,
  parsed_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(financial_date)
ORDER BY (business_id, financial_date, concept_qname, mcy_member_code, ref_member_code, fact_ordinal);
```

This table is the durable contract for reprocessing. It lets us add or correct
metric mappings without downloading XML again.

### `fi_prh_xbrl_taxonomy_code_map`

Versioned lookup table loaded from the OYTP workbook and, later, official
taxonomy packages if we ingest them.

```sql
CREATE TABLE fi_prh_xbrl_taxonomy_code_map (
  taxonomy_version String,
  code String,
  code_kind LowCardinality(String),
  namespace_hint Nullable(String),
  label_fi String,
  metric_name_hint Nullable(String),
  template_sheet Nullable(String),
  template_row Nullable(UInt32),
  template_row_text String,
  source_artifact String,
  loaded_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(loaded_at)
ORDER BY (taxonomy_version, code, template_sheet, template_row);
```

Do not hard-code Finnish labels in parser code. Keep them data-driven through
this table or an equivalent versioned JSON artifact checked into the package.

### `fi_prh_xbrl_metrics_long_v1`

Curated derived table or materialized view. One row per selected business metric,
current or comparative.

```sql
CREATE TABLE fi_prh_xbrl_metrics_long_v1 (
  statement_key String,
  business_id String,
  financial_date Date,
  period_start Nullable(Date),
  period_end Nullable(Date),
  metric_key LowCardinality(String),
  metric_label String,
  period_reference LowCardinality(String),
  value Decimal(38, 6),
  currency LowCardinality(String),
  source_concept_qname String,
  source_mcy_member_code String,
  source_ref_member_code Nullable(String),
  source_fact_ordinal UInt32,
  mapping_version String,
  derived_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(derived_at)
ORDER BY (business_id, financial_date, metric_key, period_reference);
```

Initial metric mappings from the sample:

| Metric key | Concept | MCY member | Meaning |
|---|---|---|---|
| `revenue` | `fi_met:md103` | `fi_MC:x673` | Liikevaihto |
| `operating_profit_loss` | `fi_met:md103` | `fi_MC:x689` | Liikevoitto (-tappio) |
| `profit_loss` | `fi_met:md103` | `fi_MC:x740` | Tilikauden voitto (tappio) |
| `total_assets` | `fi_met:mi53` | `fi_MC:x360` | Vastaavaa |
| `equity` | `fi_met:mi53` | `fi_MC:x376` | Oma pääoma |
| `liabilities` | `fi_met:mi53` | `fi_MC:x424` | Vieras pääoma |
| `cash_and_bank` | `fi_met:mi53` | `fi_MC:x399` | Rahat ja pankkisaamiset |
| `current_assets` | `fi_met:mi53` | `fi_MC:x435` | Vaihtuvat vastaavat |
| `current_receivables` | `fi_met:mi53` | `fi_MC:x1768` | Lyhytaikaiset saamiset |
| `current_liabilities` | `fi_met:mi53` | `fi_MC:x1811` | Lyhytaikainen vieras pääoma |
| `personnel_expenses` | `fi_met:md103` | `fi_MC:x5` | Henkilöstökulut |
| `wages_and_salaries` | `fi_met:md103` | `fi_MC:x6` | Palkat ja palkkiot |

The current/comparative distinction should come from `fi_dim:REF`:

- No `fi_dim:REF`: current period/current balance date.
- `fi_RF:x4`: previous balance date.
- `fi_RF:x53`: previous accounting period.

## Parser Rules

1. Parse XML with namespace-aware tooling (`lxml` is installed in the Temporal
   Python worker environment for this spike).
2. Store source document metadata before parsing facts.
3. Parse contexts first and preserve all dimensions.
4. Parse units separately.
5. Parse every fact with `contextRef`; do not filter to known metrics in the raw
   layer.
6. Join facts to contexts during parsing and denormalize the common `MCY` and
   `REF` members for query speed.
7. Extract statement-level metadata from raw facts:
   - `si289` -> reported business ID.
   - `si168` -> reported company name.
   - `di120` -> reported period start.
   - `di121` -> reported period end.
8. Validate XML metadata against discovery metadata, but do not discard the row
   on mismatch. Store validation warnings in a parse audit table or Dagster
   metadata.
9. Load taxonomy labels as data, versioned by taxonomy artifact/version.
10. Derive business metrics only from explicit mapping rows
    (`concept_qname + mcy_member_code + optional ref_member_code`).

## Dagster Asset Shape

For the first full workflow, the asset chain should be:

```text
remote PRH XBRL API
  -> raw XML snapshot / manifest in RustFS
  -> parsed statement documents
  -> parsed contexts
  -> parsed units
  -> raw facts
  -> taxonomy code map
  -> curated metrics long v1
  -> optional wide explorer/cache tables
```

The raw facts asset should be the main lineage boundary. The curated metrics
asset can be rebuilt without re-downloading XML.

## Open Questions

- The sample only covers one registration month and one taxonomy entrypoint.
  Before broad production use, repeat the scan across more dates and at least
  several hundred statements.
- This sample did not include inline HTML XBRL. The parser should either support
  it explicitly or reject it with a clear parse status.
- The taxonomy workbook is useful for labels, but a production taxonomy loader
  should prefer official taxonomy package files if available and fall back to
  workbook-derived labels only when necessary.
- Numeric precision policy needs a final decision. `Decimal(38, 6)` is enough
  for observed EUR values, but we should keep `raw_value`, `decimals`, and
  `precision` for auditability.

## Recommendation

Proceed with the raw-first schema. Do not build only a wide financial metrics
table from the XML parser. The PRH files encode most semantics through XBRL
dimensions, so the durable table must preserve contexts, dimensions, facts, and
taxonomy codes. A wide table is useful later for UI/query convenience, but it
should be generated from `fi_prh_xbrl_facts_raw` and mapping data.
