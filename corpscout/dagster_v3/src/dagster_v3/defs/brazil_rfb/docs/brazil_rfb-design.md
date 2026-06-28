# brazil_rfb design doc

Ingest the Brazil Receita Federal Dados Publicos CNPJ bulk registry into DuckDB
and ClickHouse. This phase covers the national company registry, establishment
contacts, and CNAE-to-NACE industry mapping. CVM listed-company financials are a
separate later phase because they have different cadence, schema, and financial
statement grain.

## 1. Source overview

- **Country / registry**: Brazil - Receita Federal Dados Publicos CNPJ, published
  by Receita Federal do Brasil / SERPRO.
- **Module**: `defs/brazil_rfb/` - stage-specific DuckDB files under `data/`
  with one writer pool per stage.
- **Related reference module**: `defs/brazil_cnae/`, which publishes
  `corpscout.br_cnae_to_nace` from curated fixture data. The registry industry
  build depends on `brazil_cnae_to_nace_clickhouse`.
- **ClickHouse tables planned**:
  - `corpscout.br_companies`: one row per legal entity (`cnpj_basico`).
  - `corpscout.br_establishments`: one row per full 14-digit establishment CNPJ.
  - `corpscout.br_company_contact_info`: one row per establishment contact.
  - `corpscout.br_websites`: deduped company-domain feeder for the common
    domain graph.
  - `corpscout.br_industries`: one row per deduped legal-entity CNAE activity,
    mapped to one or more NACE categories through `br_cnae_to_nace`.
- **Datasets used**:

  | dataset | URL | format | size | cadence | auth? |
  |---|---|---|---|---|---|
  | Empresas | `https://dados-abertos-rf-cnpj.casadosdados.com.br/arquivos/{YYYY-MM-DD}/` mirror of RFB CNPJ ZIPs | ZIP CSV, split files | large | monthly | no |
  | Estabelecimentos | same monthly CNPJ snapshot | ZIP CSV, split files | very large | monthly | no |
  | Simples | same monthly CNPJ snapshot | ZIP CSV | large | monthly | no |
  | Reference tables (`Cnaes`, `Naturezas`, `Municipios`, `Paises`, `Qualificacoes`, `Motivos`) | same monthly CNPJ snapshot | ZIP CSV | small | monthly | no |

- **Deferred from this phase**: `Socios` because it mixes corporate partners with
  natural-person partner names and masked CPF personal data under LGPD. Do not
  commit samples from this file. A later restricted partner-enrichment design
  should decide how to ingest it, separating corporate partners from natural
  persons and defining access, minimization, retention, and redaction rules.
- **Entity key**: `cnpj_basico` is the legal entity key. Full CNPJ
  (`cnpj_basico` + `cnpj_ordem` + `cnpj_dv`) identifies an establishment.
- **Expected volume**: tens of millions of legal entities and 50M+ establishment
  records in the national registry.

## 2. Ingest mode - snapshot-keyed bulk full-refresh

- **Chosen**: monthly Dagster partitions over complete RFB full snapshots. The
  partition key selects the source snapshot month; `2026-05-01` resolves the
  `2026-05` RFB snapshot.
- **Why**: the official source publishes full ZIP CSV snapshots. A paginated API is
  not needed and would be slower and less reproducible. Each partition is still a
  complete registry snapshot, not an incremental month-over-month delta.
- **Access caveat**: the historical
  `arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/` path returns
  404 from this environment. The default fetch path uses the Casa dos Dados
  open mirror (`https://dados-abertos-rf-cnpj.casadosdados.com.br/arquivos/`),
  which exposes the same RFB CNPJ ZIP layout through simple directory listings.
  The mirror publishes dated directories such as `2026-05-10/`; the partition
  month (`2026-05`) resolves to the latest dated directory for that month.
- **Format**: ZIP files containing Latin-1-compatible, semicolon-delimited CSV
  with no header. Extraction preserves the raw member and writes a UTF-8
  normalized `.utf8.csv` artifact for DuckDB, replacing dirty control bytes that
  appear in some registry rows. RFB uses fixed published column order per file
  family. Dates are `YYYYMMDD`. Monetary values such as `capital_social` use
  Brazilian decimal formatting.
- **Dagster partitioning**: every `brazil_rfb` asset uses monthly partitions from
  `2024-01-01`. Stage files are stored under
  `data/brazil_rfb/<YYYY-MM>/` so different snapshots cannot share anonymous
  DuckDB filenames. ClickHouse exports still replace current-state serving tables
  for the selected partition.

## 3. Loading

- **Download boundary**: a dlt-bounded bulk download asset resolves the monthly
  snapshot file list from the asset partition key and downloads ZIP files with
  retry/backoff. It records the source URLs, file hashes, byte sizes, and retrieved
  timestamp.
- **Launch config**: launch `brazil_rfb_resolve_job` for one monthly partition.
  Valid partition examples are `2024-01-01`, `2026-05-01`, and `2026-06-01`.

  Override the base URL only for tests or if the official RFB host becomes
  directly browsable again:

  ```yaml
  ops:
    brazil_rfb_snapshot_files_duckdb:
      config:
        snapshot_base_url: "https://dados-abertos-rf-cnpj.casadosdados.com.br/arquivos/"
  ```
- **DuckDB reader**: use DuckDB `read_csv` over UTF-8 normalized extracted files
  with explicit column lists, `all_varchar=true`, `header=false`, and
  `delim=';'`. Do not parse rows in Python; Python only performs streaming byte
  normalization because DuckDB rejects some dirty Latin-1 control bytes before
  row parsing.
- **File-family checkpoints**:
  - `brazil_rfb_empresas_duckdb`
  - `brazil_rfb_estabelecimentos_duckdb`
  - `brazil_rfb_simples_duckdb`
  - `brazil_rfb_reference_duckdb`
- **DuckDB stage artifacts**:
  - `data/brazil_rfb/<YYYY-MM>/manifest.duckdb`: dlt snapshot manifest only.
  - `data/brazil_rfb/<YYYY-MM>/empresas.duckdb`: `empresas_raw`.
  - `data/brazil_rfb/<YYYY-MM>/estabelecimentos.duckdb`: `estabelecimentos_raw`.
  - `data/brazil_rfb/<YYYY-MM>/simples.duckdb`: `simples_raw`.
  - `data/brazil_rfb/<YYYY-MM>/reference.duckdb`: code-list raw tables.
  - `data/brazil_rfb/<YYYY-MM>/companies.duckdb`: normalized `companies` and
    `establishments`.
  - `data/brazil_rfb/<YYYY-MM>/contact_info.duckdb`: normalized `company_contact_info`.
  - `data/brazil_rfb/<YYYY-MM>/websites.duckdb`: email-derived `websites`.
- **Staging tables**: `empresas_raw`, `estabelecimentos_raw`, `simples_raw`,
  `cnaes_raw`, `naturezas_raw`, `municipios_raw`, `paises_raw`,
  `qualificacoes_raw`, `motivos_raw`. Raw provenance and `source_payload_hash`
  stay in DuckDB only.
- **Empty input rule**: every file-family asset refuses to replace its staging
  table on zero rows.
- **Concurrency model**: each writable DuckDB stage has its own pool. Raw family
  loads read the manifest database through a read-only `ATTACH`, then write only
  their own stage database. Downstream transforms attach completed upstream
  databases read-only and write to their own output database. ClickHouse exports
  read completed stage databases and do not use a DuckDB writer pool.
- **DuckDB spill settings**: the company/establishment transform applies
  `preserve_insertion_order=false`, `threads`, `temp_directory`, and
  `max_temp_directory_size` before running the large window/join SQL. Defaults
  are `DUCKDB_THREADS=4`, `DUCKDB_MAX_TEMP_DIRECTORY_SIZE=100GiB`, and a
  project-local temp directory beside the DuckDB database
  (`data/duckdb_tmp`) when `DUCKDB_TEMP_DIRECTORY` is not set. Set
  `DUCKDB_MEMORY_LIMIT` only when the worker needs an explicit memory cap. These
  `DUCKDB_*` environment variables are shared knobs intended for any large
  DuckDB-based country source.

## 4. Transform

- **Mechanism**: set-based DuckDB SQL. No dbt in this phase; the transforms are
  joins, casts, code-list resolution, contact unpivoting, and CNAE unnesting.
- **Stage boundaries**: company/establishment SQL writes to
  `<YYYY-MM>/companies.duckdb` after attaching raw-family databases read-only.
  Contact SQL writes to `<YYYY-MM>/contact_info.duckdb` after attaching
  `<YYYY-MM>/companies.duckdb` read-only. Website SQL writes to
  `<YYYY-MM>/websites.duckdb` after attaching `<YYYY-MM>/contact_info.duckdb`
  read-only.
- **Company grain**: `br_companies` is one row per `cnpj_basico` legal entity.
  It joins `Empresas` to the headquarters establishment (`cnpj_ordem='0001'`) for
  company-facing status, address, trade name, and primary CNAE. If a headquarters
  row is missing, use a deterministic fallback: active establishment with the
  smallest `cnpj_ordem`, then smallest full CNPJ.
- **Establishment grain**: `br_establishments` keeps one row per full 14-digit CNPJ
  so branch-level status, address, and activity are not lost.
- **Contacts**: unpivot `correio_eletronico`, `ddd_1/telefone_1`,
  `ddd_2/telefone_2`, and `ddd_fax/fax` from `Estabelecimentos` into normalized
  contact rows. Blank and malformed values are dropped.
- **Industries**:
  - Parse `cnae_fiscal_principal` as the establishment primary CNAE.
  - Split `cnae_fiscal_secundaria` into zero or more secondary CNAEs.
  - Deduplicate to `br_industries` by `(cnpj_basico, source_industry_code)`.
  - `is_primary=1` only for the headquarters primary CNAE selected for the company
    row. Other mapped activities remain searchable but not primary.
  - Join to `corpscout.br_cnae_to_nace` on normalized CNAE code. The join is
    many-to-many: one CNAE can yield multiple NACE rows and multiple CNAEs can
    map to the same NACE.
- **Mapping coverage**: the current `br_cnae_to_nace` fixture is a seed, not a
  complete production mapping. Before running full Brazil industry materialization,
  expand the fixture or allow unmapped CNAEs to land with
  `nace_mapping_status='unmapped'`. The asset should emit coverage metadata:
  distinct CNAE codes, mapped count, unmapped count, and mapped company count.

## 5. ClickHouse schema and DDL deviations

- **`br_companies`**: one row per `cnpj_basico`.
  - Core columns: `cnpj_basico`, `headquarters_cnpj`, `legal_name`,
    `trade_name`, `legal_nature_code`, `legal_nature_description_pt`,
    `legal_nature_description_en`, `company_size_code`, `company_size_en`,
    `share_capital_amount_original`, `share_capital_amount_usd`,
    `fx_rate_to_usd`, `fx_rate_date`, `fx_source`, `status_code`, `status_en`,
    `is_active`, `status_date`, `activity_start_date`, address columns,
    `is_simples`, `is_mei`, source columns.
  - Engine: `ReplacingMergeTree(resolved_at)`.
  - `ORDER BY (cnpj_basico)`.
- **`br_establishments`**: one row per full CNPJ.
  - Core columns: `cnpj`, `cnpj_basico`, `cnpj_ordem`, `cnpj_dv`,
    `is_headquarters`, `trade_name`, `status_code`, `status_en`,
    `status_date`, `status_reason_code`, `status_reason_en`,
    `activity_start_date`, address columns, `primary_cnae_code`,
    `secondary_cnae_codes`, source columns.
  - `ORDER BY (cnpj_basico, cnpj)`.
- **`br_company_contact_info`**: one row per normalized contact.
  - Core columns: `cnpj_basico`, `cnpj`, `contact_type`, `contact_type_en`,
    `contact_value`, `root_domain`, `domain_source`, `is_current`, source columns.
  - `domain_source` is `email` when a unique company email domain is accepted,
    otherwise empty. There is no website field in RFB CNPJ.
  - `ORDER BY (cnpj_basico, contact_type, contact_value, cnpj)`.
- **`br_websites`**: one row per `(cnpj_basico, root_domain)` for the shared
  `company_website_domains` graph.
  - Email-derived domains are accepted only when the suffix belongs to one
    distinct `cnpj_basico`, with the same `EMAIL_DOMAIN_MAX_COMPANIES=1` rule used
    by Estonia. A small provider denylist remains a backstop.
  - RFB does not publish official website URLs, so first-version rows use
    `domain_source='email'` and leave website URL/host fields empty.
  - `ORDER BY (cnpj_basico, root_domain)`.
- **`br_industries`**: one row per company CNAE to NACE edge.
  - Core columns: `cnpj_basico`, `source_industry_code`,
    `source_industry_code_set='CNAE_2_0'`, `description_original`,
    `description_en`, `nace_revision`, `nace_code`, `nace_normalized_code`,
    `nace_mapping_method`, `nace_mapping_status`, `is_primary`,
    `establishment_count`, source columns.
  - `nace_mapping_method='br_cnae_to_nace_fixture'` for mapped rows and empty for
    unmapped rows.
  - `ORDER BY (cnpj_basico, source_industry_code, nace_revision, nace_normalized_code)`.
- **DDL deviations**:
  - Brazil needs both legal-entity and establishment tables because the official
    registry separates `Empresas` and `Estabelecimentos`; collapsing everything
    to one company row would lose branch status, contact, and activity data.
  - Share capital has no effective date in RFB CNPJ. USD conversion uses the
    snapshot publish/retrieval date as `fx_rate_date`; document this in metadata.
  - `Socios` is deferred to a separate restricted partner-enrichment design
    because phase 1 does not define the privacy controls needed for
    natural-person partner data.
- **Export subset**: `*_EXPORT_COLUMNS` drops `raw_*` and `source_payload_hash`.

## 6. Translation

| field | original | mechanism | notes |
|---|---|---|---|
| legal nature | `legal_nature_description_pt` | static fixture map | finite RFB code list |
| company size | `porte` | static map | micro, small, other |
| establishment status | `situacao_cadastral` | static map | active, suspended, inactive, closed |
| status reason | `motivo_situacao` | static fixture map | finite RFB reason code list |
| contact type | RFB phone/email/fax fields | static map | Email, Phone, Fax |
| CNAE description | `Cnaes` / `br_cnae_to_nace` | curated mapping fixture | English from CNAE/NACE mapping where available |

- **No LLM translation in this phase**. The source has finite code lists and proper
  nouns. Company names, trade names, municipalities, street names, and addresses
  are not translated.
- Unknown finite-code translations land as empty strings and should be counted in
  materialization metadata.

## 6b. Contacts - mandatory assessment

- **Contact data found**: yes, in `Estabelecimentos`.
- **Types present**: email, phone 1, phone 2, fax. No official website URL exists
  in RFB CNPJ.
- **Storage**: `br_company_contacts`, one row per establishment contact.
- **Domain extraction**: email suffix only, accepted when unique to a single
  `cnpj_basico`. This feeds `br_company_domains`, then the shared
  `company_website_domains` / `domains` graph with `domain_source='email'`.

## 6c. Industry / NACE

- **Source classification**: CNAE 2.0, from `cnae_fiscal_principal` and
  `cnae_fiscal_secundaria` in `Estabelecimentos`.
- **Unified mapping**: `br_industries` joins `corpscout.br_cnae_to_nace` on
  `cnae_normalized_code`. It then joins `corpscout.nace_categories` using
  `nace_revision` and `nace_normalized_code`.
- **Many-to-many behavior**: keep all mapping edges. The UI can filter companies
  by NACE by joining through `br_industries`, while a future Brazil-specific UI
  can expose native CNAE categories.
- **Unmapped handling**: keep source CNAE rows with `nace_mapping_status='unmapped'`
  so coverage gaps are visible and searchable by native code later.

## 7. Currency

- **Native currency**: BRL.
- **RFB monetary field**: `capital_social` only.
- **USD conversion**: `share_capital_amount_original` is parsed from RFB decimal
  text into BRL. `share_capital_amount_usd`, `fx_rate_to_usd`, `fx_rate_date`, and
  `fx_source` are populated by a separate `apply_brazil_rfb_usd_conversion` step
  using the shared `ExchangeRateClient`.
- **FX date**: because RFB does not provide a share-capital effective date, use
  the snapshot publish/retrieval date. This is less semantically precise than
  financial statement conversion and must be documented in asset metadata.
- **CVM financials**: deferred. When implemented, use statement period end date
  and BRL scaling rules from the CVM design.

## 8. Scheduling

- **`brazil_rfb_resolve_job`**: manual monthly-partitioned full-refresh job for
  all `brazil_rfb` assets. Run `domains_clickhouse` separately after the selected
  Brazil partition exports `br_websites`.
- **Backfills**: each month is a full registry snapshot. Backfill only when the
  operator explicitly wants to persist separate snapshot-stage artifacts; the
  ClickHouse export still replaces current-state serving tables for each selected
  partition.
- **Manual full refresh**: group-level job for all `brazil_rfb` assets. Ensure
  upstream `brazil_cnae_to_nace_clickhouse` and `nace_categories_clickhouse` have
  been materialized before the Brazil run.
- **Cron staggering**: choose a different hour/day from Estonia/France/UK monthly
  jobs because Brazil's `Estabelecimentos` file family is large.

## 9. Issues expected during processing

- **RFB host discovery**: the historical RFB host can return 404/timeout to
  headless clients. The resolver therefore defaults to the Casa dos Dados mirror
  and supports dated monthly directories (`YYYY-MM-DD/`) plus direct month
  directories (`YYYY-MM/`) for future host changes.
- **Large split files**: `Estabelecimentos` is the heavy part. Load by file family
  and checkpoint before transformations.
- **Latin-1-compatible source and no headers**: normalize extracted source CSVs
  to UTF-8 before DuckDB ingestion, use explicit schemas, and never infer columns
  from the first row.
- **Brazilian decimal text**: normalize `capital_social` carefully before casting.
- **Secondary CNAE list**: split the source list set-based in DuckDB and handle
  empty values.
- **LGPD**: defer `Socios` from this phase and never commit partner-name samples.
  A future partner design should treat corporate partners differently from
  natural-person partners and should document purpose, access, minimization,
  retention, and redaction rules before ingestion.
- **Mapping coverage**: the current CNAE-to-NACE fixture is only a seed. The full
  registry run must report unmapped CNAE coverage and should not silently drop
  unmapped companies.

## 10. Verification

- **Tests**:
  - `tests/test_brazil_rfb_resources.py`: URL resolver, CNPJ normalization, date
    parsing, capital parsing, no-header schemas.
  - `tests/test_brazil_rfb_tables.py`: ClickHouse DDL and export columns.
  - `tests/test_brazil_rfb_transforms.py`: company fallback selection, contacts
    unpivot, email-domain uniqueness, CNAE split/dedup.
  - `tests/test_brazil_rfb_assets.py`: asset dependencies, empty-input refusal,
    ClickHouse table preflight.
  - `tests/test_clickhouse_migrations.py`: migration registration and schema
    shape.
- **Dagster checks**: `uv run dg check defs`.
- **Live validation**:
  - Run ClickHouse migrations.
  - Materialize `nace_categories_clickhouse`, `brazil_cnae_to_nace_clickhouse`,
    then the `brazil_rfb` group.
  - Spot-check counts for `br_companies`, `br_establishments`,
    `br_company_contacts`, `br_company_domains`, and `br_industries`.
  - Verify known rows by CNPJ formatting, status translation, share-capital cast,
    email-domain rule, and CNAE-to-NACE mapping.
  - Confirm `br_industries` reports mapped/unmapped coverage and that mapped rows
    join `corpscout.nace_categories`.
