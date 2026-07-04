# Brazil CVM DFP DuckDB Parser Design

## 1. Source Overview

- **Country / registry**: Brazil - CVM DFP annual financial statements for public/open companies.
- **Module**: `defs/brazil_financial/cvm/` · DuckDB file `data/brazil_cvm_source.duckdb` · pool `brazil_fin_cvm_duckdb`.
- **Existing raw asset**: `brazil_fin_cvm_dfp_raw_archives_s3`.
- **New parser asset**: `brazil_fin_cvm_dfp_raw_duckdb`.
- **Later export asset**: `brazil_fin_cvm_dfp_raw_clickhouse`.
- **ClickHouse migration**: `000087_corpscout_br_cvm_dfp_tables`.
- **ClickHouse tables**:
  - `corpscout.br_cvm_dfp_documents`
  - `corpscout.br_cvm_dfp_statement_rows`
  - `corpscout.br_cvm_dfp_capital_composition`
  - `corpscout.br_cvm_dfp_auditor_reports`

Datasets used:

| dataset | url/object | format | cadence | auth? |
|---|---|---|---|---|
| CVM DFP yearly archive | `source-brazil-cvm/brazil_cvm/dfp/raw_archives/year=<year>/archive.zip` | ZIP of semicolon CSV files | source refreshes when CVM republishes annual archive | no |
| CVM DFP source URL | `https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/DFP/DADOS/dfp_cia_aberta_<year>.zip` | ZIP | yearly partition, currently 2010-2026 | no |

Entity keys:

- primary company identifier in CVM rows: `CNPJ_CIA`
- normalized join key to Brazil RFB establishments: digits-only full `cnpj`
- normalized join key to `br_companies`: first 8 digits `cnpj_basico`
- CVM issuer code: `CD_CVM`

## 2. Ingest Mode

Chosen mode: **bulk file by natural source partition**.

The source publishes one full ZIP per DFP year. The upstream archive is already
partitioned by year, and the existing raw asset stores each year under a
deterministic S3/RustFS object key. The parser should keep the same year
partitioning so a failed parse for one year does not require reprocessing the
whole historical corpus.

This is a deliberate deviation from the normal "bulk file full-refresh,
non-partitioned" rule. Here the source itself is a historical archive family,
not a single current snapshot. Year partitions match the upstream object and
keep backfills small.

Format notes:

- ZIP contains semicolon-delimited CSV files.
- Real samples show Windows-1252/Latin-1 text; parser should read with
  `encoding='latin-1'` or `encoding='windows-1252'`, not default UTF-8.
- Decimal values use dot decimal separator, for example `2148915.0000000000`.
- Dates are ISO `YYYY-MM-DD`.
- `composicao_capital` exists in recent years such as 2026, but not all
  historical years such as 2010.

## 3. Raw ZIP Layout

The yearly archive is named:

```text
dfp_cia_aberta_<year>.zip
```

Expected CSV families:

| Source file pattern | Target DuckDB table | Target ClickHouse table |
|---|---|---|
| `dfp_cia_aberta_<year>.csv` | `brazil_cvm.dfp_documents` | `br_cvm_dfp_documents` |
| `dfp_cia_aberta_BPA_con_<year>.csv` | `brazil_cvm.dfp_statement_rows` | `br_cvm_dfp_statement_rows` |
| `dfp_cia_aberta_BPA_ind_<year>.csv` | `brazil_cvm.dfp_statement_rows` | `br_cvm_dfp_statement_rows` |
| `dfp_cia_aberta_BPP_con_<year>.csv` | `brazil_cvm.dfp_statement_rows` | `br_cvm_dfp_statement_rows` |
| `dfp_cia_aberta_BPP_ind_<year>.csv` | `brazil_cvm.dfp_statement_rows` | `br_cvm_dfp_statement_rows` |
| `dfp_cia_aberta_DRE_con_<year>.csv` | `brazil_cvm.dfp_statement_rows` | `br_cvm_dfp_statement_rows` |
| `dfp_cia_aberta_DRE_ind_<year>.csv` | `brazil_cvm.dfp_statement_rows` | `br_cvm_dfp_statement_rows` |
| `dfp_cia_aberta_DFC_MD_con_<year>.csv` | `brazil_cvm.dfp_statement_rows` | `br_cvm_dfp_statement_rows` |
| `dfp_cia_aberta_DFC_MD_ind_<year>.csv` | `brazil_cvm.dfp_statement_rows` | `br_cvm_dfp_statement_rows` |
| `dfp_cia_aberta_DFC_MI_con_<year>.csv` | `brazil_cvm.dfp_statement_rows` | `br_cvm_dfp_statement_rows` |
| `dfp_cia_aberta_DFC_MI_ind_<year>.csv` | `brazil_cvm.dfp_statement_rows` | `br_cvm_dfp_statement_rows` |
| `dfp_cia_aberta_DMPL_con_<year>.csv` | `brazil_cvm.dfp_statement_rows` | `br_cvm_dfp_statement_rows` |
| `dfp_cia_aberta_DMPL_ind_<year>.csv` | `brazil_cvm.dfp_statement_rows` | `br_cvm_dfp_statement_rows` |
| `dfp_cia_aberta_DRA_con_<year>.csv` | `brazil_cvm.dfp_statement_rows` | `br_cvm_dfp_statement_rows` |
| `dfp_cia_aberta_DRA_ind_<year>.csv` | `brazil_cvm.dfp_statement_rows` | `br_cvm_dfp_statement_rows` |
| `dfp_cia_aberta_DVA_con_<year>.csv` | `brazil_cvm.dfp_statement_rows` | `br_cvm_dfp_statement_rows` |
| `dfp_cia_aberta_DVA_ind_<year>.csv` | `brazil_cvm.dfp_statement_rows` | `br_cvm_dfp_statement_rows` |
| `dfp_cia_aberta_composicao_capital_<year>.csv` | `brazil_cvm.dfp_capital_composition` | `br_cvm_dfp_capital_composition` |
| `dfp_cia_aberta_parecer_<year>.csv` | `brazil_cvm.dfp_auditor_reports` | `br_cvm_dfp_auditor_reports` |

Unknown CSV members should fail the parser by default. Missing
`composicao_capital` should not fail the parser because it is absent in older
archives.

## 4. Loading Into DuckDB

Reader: DuckDB `read_csv` over extracted temporary CSV files.

Do not parse rows in Python. The implementation should:

1. Download the raw ZIP from `ObjectStoreResource` to a temporary file.
2. Extract CSV members into a temporary directory.
3. For each expected member, use DuckDB `read_csv` with:
   - `delim=';'`
   - `header=true`
   - `all_varchar=true`
   - `encoding='latin-1'` or `encoding='windows-1252'`
   - `ignore_errors=false`
   - explicit source member list, not wildcard-only matching
4. Load source columns as text into temporary raw tables.
5. Use set-based DuckDB SQL to normalize and replace the partition rows in
   final DuckDB tables.

DuckDB schema:

```text
brazil_cvm
```

DuckDB tables:

```text
brazil_cvm.dfp_documents
brazil_cvm.dfp_statement_rows
brazil_cvm.dfp_capital_composition
brazil_cvm.dfp_auditor_reports
brazil_cvm.dfp_parse_runs
```

`dfp_parse_runs` records one row per parsed year/run with counts by table,
archive key, archive SHA-256 when available, source run id, started/finished
timestamps, and parser version.

Replacement rule:

- Partition key is `dfp_year`.
- Each parser materialization deletes/replaces rows for only that `dfp_year`.
- This keeps year backfills idempotent and prevents a failed current year parse
  from touching historical years.

## 5. Statement Family Mapping

The parser should derive these fields from file name:

| File token | Meaning | Normalized field |
|---|---|---|
| `BPA` | balance sheet assets | `statement_code='BPA'`, `statement_name='balance_sheet_assets'` |
| `BPP` | balance sheet liabilities/equity | `statement_code='BPP'`, `statement_name='balance_sheet_liabilities_equity'` |
| `DRE` | income statement | `statement_code='DRE'`, `statement_name='income_statement'` |
| `DFC_MD` | cash flow direct method | `statement_code='DFC_MD'`, `statement_name='cash_flow_direct'` |
| `DFC_MI` | cash flow indirect method | `statement_code='DFC_MI'`, `statement_name='cash_flow_indirect'` |
| `DMPL` | changes in equity | `statement_code='DMPL'`, `statement_name='changes_in_equity'` |
| `DRA` | comprehensive income | `statement_code='DRA'`, `statement_name='comprehensive_income'` |
| `DVA` | value added statement | `statement_code='DVA'`, `statement_name='value_added_statement'` |
| `_con_` | consolidated | `consolidation_type='consolidated'` |
| `_ind_` | individual | `consolidation_type='individual'` |

Statement columns:

- BPA/BPP do not have `DT_INI_EXERC`; set `period_start_date = NULL`.
- DRE/DFC/DMPL/DRA/DVA have both `DT_INI_EXERC` and `DT_FIM_EXERC`.
- DMPL has `COLUNA_DF`; map it to `equity_column`.
- Non-DMPL statements should set `equity_column=''`.

## 6. DuckDB To ClickHouse Shape

The DuckDB final tables should already match the ClickHouse export columns in
`dagster_v3.defs.brazil_financial.cvm.tables`.

### `dfp_documents`

Grain: one CVM DFP document/version row.

Important columns:

- `country_iso2='BR'`
- `source_slug='brazil_cvm_dfp'`
- `source_record_id`: deterministic hash or joined natural key from
  `dfp_year`, `cnpj`, `reference_date`, `version`, and `document_id`
- `cnpj`: digits-only `CNPJ_CIA`
- `cnpj_basico`: first 8 digits of `cnpj`
- `cvm_code`: `CD_CVM`
- `document_id`: `ID_DOC`
- `document_url`: `LINK_DOC`

### `dfp_statement_rows`

Grain: one account row per document/version/statement/consolidation/period.

Important columns:

- `statement_code`, `statement_name`, and `consolidation_type` from file name
- `grupo_dfp`: original `GRUPO_DFP`
- `currency`: original `MOEDA`
- `scale`: original `ESCALA_MOEDA`
- `original_order`: original `ORDEM_EXERC`
- `account_code`: original `CD_CONTA`
- `account_description_original`: original `DS_CONTA`
- `amount_original`: cast from `VL_CONTA` to `Decimal(38, 10)`
- `fixed_account_flag`: original `ST_CONTA_FIXA`

### `dfp_capital_composition`

Grain: one capital composition row per document/version.

Historical handling:

- If the CSV member is absent for a year, write zero rows for that year and
  record `capital_composition_row_count=0` in parse metadata.

### `dfp_auditor_reports`

Grain: one text report row per document/version/report item.

Text field:

- `report_text_original` from `TXT_PARECER_DECL`

Do not translate report text in this asset. Translation can be evaluated later
because these texts are long and not needed for first financial metrics.

## 7. ClickHouse Export

Export target tables are already migration-owned:

```text
corpscout.br_cvm_dfp_documents
corpscout.br_cvm_dfp_statement_rows
corpscout.br_cvm_dfp_capital_composition
corpscout.br_cvm_dfp_auditor_reports
```

The export asset should assert tables exist through
`assert_clickhouse_tables_exist`, then replace rows. Preferred implementation:

- first implementation can truncate and replace each full table after all
  historical years are parsed;
- later implementation can use staging tables and partition/year-scoped
  replacement if full-table size becomes too large.

Do not export raw payload JSON or source payload hashes to ClickHouse. Keep
those only in DuckDB if needed.

## 8. Currency

Raw CVM rows carry:

- `currency`: usually `REAL`
- `scale`: usually `MIL`
- `amount_original`: value as published in the CSV

This parser asset must not perform USD conversion. It should preserve the source
amount and scale exactly. A later metrics asset should:

1. map account rows to canonical metrics;
2. apply `scale` to produce native BRL amounts;
3. convert selected metrics to USD using the shared exchange-rate flow keyed on
   `period_end_date`.

## 9. Translation

Base parser/export tables carry Portuguese text as `*_original` fields:

- `account_description_original`
- `report_text_original`

Do not add `_en` columns to these base tables. If translation is needed later,
add a separate translation loader and translated view backed by
`corpscout.text_translations`.

Initial recommendation:

- translate account descriptions only after metrics mapping proves we need them
  in product/UI;
- do not translate `report_text_original` in the first implementation because
  auditor/director texts are long and high-volume.

## 10. Contacts And NACE

This financial source does not add company contacts or NACE categories. Contacts
and industry data remain owned by `brazil_rfb` and the Brazil CNAE-to-NACE
mapping.

The parser should still preserve `cnpj`, `cnpj_basico`, `company_name`, and
`cvm_code` so DFP rows can join to:

- `corpscout.br_establishments` on full `cnpj`
- `corpscout.br_companies` on `cnpj_basico`
- a future CVM issuer cadastro table on `cvm_code`

## 11. Asset And Job Wiring

Planned assets:

```text
brazil_fin_cvm_dfp_raw_archives_s3
  -> brazil_fin_cvm_dfp_raw_duckdb
  -> brazil_fin_cvm_dfp_raw_clickhouse
```

`brazil_fin_cvm_dfp_raw_duckdb`:

- partitions: same static year partitions as `brazil_fin_cvm_dfp_raw_archives_s3`
- pool: `brazil_fin_cvm_duckdb`
- resources: `object_store`, `brazil_fin_cvm_dfp`, `brazil_fin_cvm_duckdb`
- input: S3/RustFS archive key for `context.partition_key`
- output: rows replaced for `dfp_year=context.partition_key`

`brazil_fin_cvm_dfp_raw_clickhouse`:

- first version can be non-partitioned and export all parsed DuckDB rows;
- later version can become partition-aware if row volume requires year-scoped
  replacement.

Jobs:

```text
brazil_fin_cvm_dfp_raw_backfill_job
```

should eventually select all three raw-stage assets for historical backfill.

## 12. Validation And Tests

Unit tests:

- filename parser maps statement family and consolidation type correctly;
- synthetic ZIP with document, DRE, BPA, DMPL, parecer, and optional/missing
  composicao_capital loads expected row counts;
- Latin-1/Windows-1252 text is decoded into readable Portuguese, not mojibake;
- re-materializing one year replaces only that `dfp_year`;
- invalid unknown CSV member fails loudly;
- ClickHouse export columns match `brazil_cvm.tables`.

Dagster tests:

- `brazil_fin_cvm_dfp_raw_duckdb` has same partitions as raw archive asset;
- asset uses `context.partition_key` as `dfp_year`;
- job selection includes archive -> DuckDB -> ClickHouse chain.

Live validation:

1. Apply migration `000087`.
2. Materialize a small year such as `2026` through archive and DuckDB parser.
3. Compare DuckDB row counts to ZIP CSV row counts.
4. Spot-check known rows:
   - DRE `3.01` revenue rows exist.
   - BPA `1` total assets rows exist.
   - DMPL rows preserve `COLUNA_DF` in `equity_column`.
   - `parecer` rows preserve long text.
5. Export to ClickHouse and verify counts by `dfp_year`, `statement_code`, and
   `consolidation_type`.

## 13. Out Of Scope

- ITR quarterly filings.
- Derived financial metrics.
- USD conversion.
- Translation loader/view.
- CVM issuer cadastro support table.
- Current-year weekly freshness/versioned replacement logic.
