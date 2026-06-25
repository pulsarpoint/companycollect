# brazil_rfb design doc

Ingest the Brazil Receita Federal Dados Publicos CNPJ bulk registry into DuckDB
and ClickHouse. This phase covers the national company registry, establishment
contacts, and CNAE-to-NACE industry mapping. CVM listed-company financials are a
separate later phase because they have different cadence, schema, and financial
statement grain.

## 1. Source overview

- **Country / registry**: Brazil - Receita Federal Dados Publicos CNPJ, published
  by Receita Federal do Brasil / SERPRO.
- **Module**: `defs/brazil_rfb/` - DuckDB `data/brazil_rfb_source.duckdb` - pool
  `brazil_rfb_duckdb`.
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
  | Empresas | `https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/{YYYY-MM}/` or SERPRO+ equivalent | ZIP CSV, split files | large | monthly | no |
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

## 2. Ingest mode - bulk full-refresh

- **Chosen**: non-partitioned bulk full-refresh from the monthly CNPJ snapshot.
- **Why**: the official source publishes full ZIP CSV snapshots. A paginated API is
  not needed and would be slower, less reproducible, and harder to backfill.
- **Access caveat**: the historical RFB directory URL is cataloged, but the files
  are now also served through a SERPRO+ JavaScript portal. The implementation
  should have a resolver that can use the normal monthly directory when available
  and a configured snapshot manifest/base URL when the JS portal blocks headless
  discovery. This is a source-access issue, not a reason to switch to per-record
  API ingestion.
- **Format**: ZIP files containing Latin-1, semicolon-delimited CSV with no header.
  RFB uses fixed published column order per file family. Dates are `YYYYMMDD`.
  Monetary values such as `capital_social` use Brazilian decimal formatting.
- **Partitioning**: none. The snapshot is replaced atomically after all required
  files are staged and transformed.

## 3. Loading

- **Download boundary**: a dlt-bounded bulk download asset resolves the monthly
  snapshot file list and downloads ZIP files with retry/backoff. It records the
  `snapshot_year_month`, source URLs, file hashes, byte sizes, and retrieved timestamp.
- **Launch config**: `snapshot_year_month` is required and must use `YYYY-MM`.
  Valid examples are `2026-05` and `2026-06`. Invalid examples are `202605`,
  `2026/05`, and `05-2026`.

  ```yaml
  ops:
    brazil_rfb_snapshot_files_duckdb:
      config:
        snapshot_year_month: "2026-05"
  ```

  Override the base URL only for mirrors or tests:

  ```yaml
  ops:
    brazil_rfb_snapshot_files_duckdb:
      config:
        snapshot_year_month: "2026-05"
        snapshot_base_url: "https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/"
  ```
- **DuckDB reader**: use DuckDB `read_csv` over unzipped files with explicit
  column lists, `all_varchar=true`, `header=false`, `delim=';'`, and Latin-1
  decoding. Do not parse rows in Python.
- **File-family checkpoints**:
  - `brazil_rfb_empresas_duckdb`
  - `brazil_rfb_estabelecimentos_duckdb`
  - `brazil_rfb_simples_duckdb`
  - `brazil_rfb_reference_duckdb`
- **Staging tables**: `empresas_raw`, `estabelecimentos_raw`, `simples_raw`,
  `cnaes_raw`, `naturezas_raw`, `municipios_raw`, `paises_raw`,
  `qualificacoes_raw`, `motivos_raw`. Raw provenance and `source_payload_hash`
  stay in DuckDB only.
- **Empty input rule**: every file-family asset refuses to replace its staging
  table on zero rows.
- **Single writer**: every asset that writes `brazil_rfb_source.duckdb` uses
  `pool="brazil_rfb_duckdb"`.

## 4. Transform

- **Mechanism**: set-based DuckDB SQL. No dbt in this phase; the transforms are
  joins, casts, code-list resolution, contact unpivoting, and CNAE unnesting.
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

- **`brazil_rfb_resolve_job`**: manual full-refresh job for all `brazil_rfb`
  assets plus `domains_clickhouse`, so `br_websites` is pushed through
  `company_website_domains` and the shared `domains` aggregate in the same run.
  Keep it unscheduled until the source resolver and first full materialization
  are validated.
- **Manual full refresh**: group-level job for all `brazil_rfb` assets plus
  upstream `brazil_cnae_to_nace_clickhouse` and `nace_categories_clickhouse`.
- **Cron staggering**: choose a different hour/day from Estonia/France/UK monthly
  jobs because Brazil's `Estabelecimentos` file family is large.

## 9. Issues expected during processing

- **SERPRO+ portal discovery**: the open data may not expose a simple browsable
  directory to headless clients. Build the resolver with a configured snapshot URL
  override instead of blocking the transform design on the portal UI.
- **Large split files**: `Estabelecimentos` is the heavy part. Load by file family
  and checkpoint before transformations.
- **Latin-1 and no headers**: use explicit schemas and encoding. Never infer
  columns from the first row.
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
