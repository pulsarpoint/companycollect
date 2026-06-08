# Corpscout ClickHouse Company Store Design

## Purpose

Corpscout should not keep the high-volume company/source-record store in
PostgreSQL. The expected corpus is larger than 50 million company records, and
the main product workflows need fast filtering, search, aggregation, and detail
composition over imported source data. Direct Parquet exploration already shows
that columnar access is a better fit for this data shape.

Use ClickHouse for imported company facts and searchable projections. Keep
PostgreSQL for workflow control, source registry metadata, curated identity
objects, brands, and relationship state.

## Decision

Use a two-store model:

```text
PostgreSQL
  small transactional control plane and curated identity graph

ClickHouse
  large source-record fact store and search/detail read model

Parquet/object files
  immutable source exports and replay archive
```

PostgreSQL remains authoritative for user-authored and workflow state.
ClickHouse is authoritative for imported source company facts and derived
search/detail projections.

## Alternatives Considered

### A. PostgreSQL for all company data

This is the simplest operationally, but it is the weakest fit for the expected
volume. Even with partitioning and indexes, the source-record tables would become
large, expensive to search, and difficult to evolve. This was the previous
direction and should be replaced.

### B. ClickHouse for all company and identity data

This gives fast scans, but it makes curated identity work harder. Company
relationships, brands, manual merges, review state, and workflow metadata need
transactional updates, constraints, and clean referential behavior. ClickHouse is
not the right primary store for those edits.

### C. PostgreSQL identity graph plus ClickHouse company facts

This is the recommended design. PostgreSQL stores small stable identities and
relationships. ClickHouse stores imported source facts and merged projections
optimized for search, filtering, and detail pages.

## Data Ownership

### PostgreSQL Owns

- source registry rows
- Temporal source run and export metadata
- central company identity rows
- brand identity rows
- company-company relationships
- brand-company and brand-legal-entity relationships
- manual merge decisions and review state
- stable IDs used by APIs and UI links

### ClickHouse Owns

- imported source company records
- source-specific identifiers
- names and translated names
- addresses
- contacts
- industries
- domains and websites observed in source records
- source payload hashes and source payload references
- flattened searchable company projections
- source coverage summaries for detail pages

### Parquet/Object Files Own

- immutable source export files
- original source export manifests
- replayable source snapshots

ClickHouse tables may contain compact raw JSON payloads when useful, but large
or rarely viewed original payloads should be referenced by export path, file
name, row group, row number, source record ID, or payload hash.

## PostgreSQL Model

PostgreSQL should be much smaller than the earlier all-Postgres design.

### `registry.sources`

One row per source executable.

Important columns:

```text
id
slug
display_name
description
coverage_scope
default_country_id
executable_path
working_directory
default_args
environment_contract
output_contract_version
enabled
schedule_enabled
schedule_kind
schedule_expression
metadata
created_at
updated_at
```

### `registry.source_runs`

One Temporal execution of a source.

Important columns:

```text
id
source_id
temporal_workflow_id
temporal_run_id
command
args
trigger_type
status
started_at
finished_at
exit_code
stdout_result
error_message
metadata
created_at
```

### `registry.source_exports`

One manifest produced by a source run.

Important columns:

```text
id
source_id
source_run_id
manifest_path
manifest_sha256
export_kind
schema_version
run_key
created_at_source
records_seen
records_exported
decode_errors
metadata
created_at
```

### `identity.companies`

Small central company row. This is the stable object used by product APIs,
relationship editing, brand links, and manual curation.

Important columns:

```text
id
canonical_name
canonical_name_normalized
display_name
primary_country_id
status
clickhouse_company_key
metadata
created_at
updated_at
```

`clickhouse_company_key` points to the ClickHouse merged projection key. It may
equal the Postgres `id` once a company has been resolved, but it should remain an
explicit column so unresolved and batch-derived projections can exist before a
Postgres central company is created.

### `identity.brands`

First-class brand rows, separate from legal entities and central companies.

Important columns:

```text
id
canonical_name
canonical_name_normalized
display_name
owner_company_id
status
metadata
created_at
updated_at
```

### Relationship Tables

Keep these in PostgreSQL:

```text
identity.company_relationships
identity.brand_relationships
identity.brand_company_links
identity.company_clickhouse_links
identity.review_decisions
```

`identity.company_clickhouse_links` maps one central company to one or more
ClickHouse projections/source-record groups:

```text
company_id
clickhouse_company_key
link_type
confidence
status
reviewed_by
reviewed_at
metadata
created_at
updated_at
```

This table allows a central company to connect to many source-derived groups and
lets operators split or merge groups without rewriting the curated identity row.

## ClickHouse Model

Use ClickHouse tables for wide, append-friendly, source-owned data and derived
read models.

Do not start with one unified source-record schema. The first ClickHouse source
tables should be source-specific, because Corpscout needs to ingest roughly 20
country/source exports before deciding which structures are truly common. The
generic read projections can exist from the beginning, but they are derived from
source-specific tables.

## ClickHouse Schema Migrations

Use `golang-migrate` for ClickHouse schema migrations, matching the migration
tool already used by Corpscout PostgreSQL.

Keep PostgreSQL and ClickHouse migrations in separate directories:

```text
corpscout/database/migrations
  PostgreSQL migrations for registry, workflow, identity, brands, and curated
  relationships

corpscout/clickhouse/migrations
  ClickHouse migrations for source-specific fact tables and projection tables
```

Example ClickHouse migration files:

```text
000001_create_sources_database.up.sql
000001_create_sources_database.down.sql
000002_create_finland_prhytj_tables.up.sql
000002_create_finland_prhytj_tables.down.sql
000003_create_company_projection_tables.up.sql
000003_create_company_projection_tables.down.sql
```

ClickHouse migrations are for DDL only:

- create databases
- create source-specific tables
- create projection tables
- create materialized views when needed
- add schema indexes/order keys

They must not load large Parquet exports. Data import is a separate idempotent
job executed after the table schema exists.

The first Makefile/API shape should be:

```text
make clickhouse-migrate-up
make clickhouse-migrate-down
```

The ClickHouse DSN should be configured separately from `DATABASE_URL`, for
example:

```text
CLICKHOUSE_URL=clickhouse://default:password@localhost:9000/corpscout_sources
```

Use the `golang-migrate` ClickHouse database driver:

```text
github.com/golang-migrate/migrate/v4/database/clickhouse
```

Avoid Atlas for the first implementation. Atlas can manage ClickHouse schemas,
but it adds more process and licensing/plan considerations than needed while the
source table shapes are still being learned.

### Parquet DDL Generator

Do not hand-write every source-specific ClickHouse table. Build a small
deterministic generator that reads source export Parquet schemas and produces
reviewable `golang-migrate` SQL files.

Create:

```text
corpscout/clickhouse/tools/parquetddl
corpscout/clickhouse/sources/finland_prhytj.yaml
```

Example command:

```text
go run ./clickhouse/tools/parquetddl \
  --source finland_prhytj \
  --database corpscout_sources \
  --export-dir ../companies/data/finland/countrydata/sources/prhytj/exports/20260607T205519Z-prhytj \
  --config clickhouse/sources/finland_prhytj.yaml \
  --out clickhouse/migrations/000002_create_finland_prhytj_tables.up.sql
```

The generator must produce deterministic output:

- stable table ordering
- stable column ordering
- stable type mapping
- stable injected metadata columns
- stable `ENGINE`, `ORDER BY`, and optional partition settings from config

Use ClickHouse schema inference instead of a custom Parquet parser when possible.
The equivalent inspection query is:

```sql
DESCRIBE TABLE file('/absolute/path/to/companies.parquet', Parquet);
```

The source config supplies decisions that Parquet cannot infer:

```yaml
database: corpscout_sources
source_prefix: fi_prhytj
tables:
  companies:
    parquet: companies.parquet
    table: fi_prhytj_companies
    engine: ReplacingMergeTree
    order_by:
      - business_id
      - source_run_id
    inject_columns:
      source_export_id: UUID
      ingested_at: "DateTime64(3, 'UTC')"
  registered_entries:
    parquet: registered_entries.parquet
    table: fi_prhytj_registered_entries
    engine: ReplacingMergeTree
    order_by:
      - business_id
      - register_code
      - entry_type_code
      - registered_on
      - source_item_hash
    inject_columns:
      source_export_id: UUID
      ingested_at: "DateTime64(3, 'UTC')"
```

Generation workflow:

```text
Parquet export
  -> parquetddl reads schemas
  -> parquetddl applies source config
  -> migration SQL is generated
  -> migration is reviewed and committed
  -> golang-migrate applies DDL
  -> importer loads Parquet data
```

The generator is allowed to generate `CREATE TABLE` DDL and matching `DROP TABLE`
down migrations. It must not import data.

### Finland PRH YTJ Source Tables

Create the first source-specific tables for the Finland PRH YTJ export:

```text
corpscout_sources.fi_prhytj_raw_records
corpscout_sources.fi_prhytj_companies
corpscout_sources.fi_prhytj_company_names
corpscout_sources.fi_prhytj_legal_forms
corpscout_sources.fi_prhytj_industries
corpscout_sources.fi_prhytj_addresses
corpscout_sources.fi_prhytj_registered_entries
corpscout_sources.fi_prhytj_tax_registrations
corpscout_sources.fi_prhytj_websites
```

These tables mirror:

```text
companies/data/finland/countrydata/sources/prhytj/exports/<run_id>/*.parquet
```

The current pilot export is:

```text
companies/data/finland/countrydata/sources/prhytj/exports/20260607T205519Z-prhytj
```

#### `fi_prhytj_raw_records`

One full-fidelity row per PRH YTJ API company response. This table is required
because the normalized Parquet files currently keep `source_payload_hash`, but
not the full API payload.

```text
country_iso2
source_slug
source_run_id
source_export_id
source_record_id
business_id
source_payload_hash
snapshot_path
snapshot_sha256
snapshot_line_number
raw_payload_json
schema_version
exported_at
ingested_at
```

Suggested engine:

```text
ReplacingMergeTree
ORDER BY (source_run_id, business_id, source_payload_hash)
```

#### `fi_prhytj_companies`

One row per company record from `companies.parquet`.

```text
country_iso2
source_slug
source_run_id
source_export_id
source_record_id
source_native_id
source_payload_hash
source_updated_at
exported_at
schema_version
business_id
vat_id
euid
legal_name
legal_name_normalized
lifecycle_status
is_active
legal_form_code
legal_form_label
legal_form_label_en
primary_industry_code
primary_industry_code_set
primary_industry_label
primary_industry_label_en
primary_nace_code
primary_nace_revision
website_url
website_normalized_url
website_host
ingested_at
```

Suggested engine:

```text
ReplacingMergeTree
ORDER BY (business_id, source_run_id)
```

#### `fi_prhytj_company_names`

One row per source name from `company_names.parquet`.

```text
country_iso2
source_slug
source_run_id
source_export_id
source_record_id
source_item_hash
business_id
source_position
name
name_type_code
registered_on
ended_on
is_current
is_primary
ingested_at
```

Suggested engine:

```text
ReplacingMergeTree
ORDER BY (business_id, source_position, source_item_hash)
```

#### `fi_prhytj_legal_forms`

```text
country_iso2
source_slug
source_run_id
source_export_id
source_record_id
source_item_hash
business_id
legal_form_code
legal_form_label
legal_form_label_en
legal_form_label_fi
legal_form_label_sv
registered_on
ended_on
ingested_at
```

Suggested engine:

```text
ReplacingMergeTree
ORDER BY (business_id, registered_on, legal_form_code, source_item_hash)
```

#### `fi_prhytj_industries`

```text
country_iso2
source_slug
source_run_id
source_export_id
source_record_id
source_item_hash
business_id
source_industry_code
source_industry_code_set
source_industry_label
source_industry_label_en
source_industry_label_fi
source_industry_label_sv
mapped_nace_code
nace_revision
is_primary
ingested_at
```

Suggested engine:

```text
ReplacingMergeTree
ORDER BY (business_id, source_industry_code, source_item_hash)
```

#### `fi_prhytj_addresses`

```text
country_iso2
source_slug
source_run_id
source_export_id
source_record_id
source_item_hash
business_id
source_position
address_type_code
address_type
street
building_number
entrance
apartment_number
post_office_box
co
post_code
city_fi
city_sv
municipality_code
country
registered_on
ingested_at
```

Suggested engine:

```text
ReplacingMergeTree
ORDER BY (business_id, address_type_code, source_position, source_item_hash)
```

#### `fi_prhytj_registered_entries`

```text
country_iso2
source_slug
source_run_id
source_export_id
source_record_id
source_item_hash
business_id
register_code
register_label
authority
entry_type_code
entry_type_label
entry_type_label_en
registered_on
ended_on
is_current
ingested_at
```

Suggested engine:

```text
ReplacingMergeTree
ORDER BY (business_id, register_code, entry_type_code, registered_on, source_item_hash)
```

#### `fi_prhytj_tax_registrations`

```text
country_iso2
source_slug
source_run_id
source_export_id
source_record_id
source_item_hash
business_id
registration_type
register_code
current_registered
first_registered_on
ended_on
ingested_at
```

Suggested engine:

```text
ReplacingMergeTree
ORDER BY (business_id, registration_type, register_code, source_item_hash)
```

#### `fi_prhytj_websites`

```text
country_iso2
source_slug
source_run_id
source_export_id
source_record_id
source_item_hash
business_id
url
normalized_url
host
path
registered_on
ended_on
is_current
is_primary
ingested_at
```

Suggested engine:

```text
ReplacingMergeTree
ORDER BY (host, business_id, source_item_hash)
```

### Future Source Consolidation

After roughly 20 country/source imports, revisit whether source-specific tables
should feed a shared fact schema. Until then, avoid premature universal tables
such as `source_company_records` or `source_company_attributes`.

### `company_search_projection`

Flattened read model for search and list pages.

Important columns:

```text
clickhouse_company_key
postgres_company_id
canonical_name
canonical_name_en
canonical_name_normalized
country_iso2s
source_slugs
registration_numbers
domains
websites
industry_codes
active_status
source_count
field_coverage_json
best_source_slug
updated_at
```

`postgres_company_id` is nullable. Many imported groups will exist before a
central identity row is curated in PostgreSQL.

### `company_detail_projection`

Pre-aggregated detail payload used by `GET /companies/{id}` and source coverage
panels.

Important columns:

```text
clickhouse_company_key
postgres_company_id
merged_profile_json
source_coverage_json
source_records_json
identifiers_json
addresses_json
contacts_json
industries_json
websites_json
raw_payload_refs_json
updated_at
```

The detail projection should include enough source-level evidence to show how
many sources contributed data and what each source knows about the company.

## Data Flow

```text
source binary
  |
  | Temporal executes configured command
  v
Parquet export + manifest
  |
  | Corpscout records run/export metadata in PostgreSQL
  v
ClickHouse import
  |
  | source-specific Finland PRH YTJ tables inserted
  v
ClickHouse resolver/projection job
  |
  | deterministic source groups and searchable projections
  v
PostgreSQL identity curation
  |
  | optional central company, brand, and relationship decisions
  v
API detail composition
  |
  | PostgreSQL identity graph + ClickHouse detail projection
  v
UI
```

## API Composition

### Source Pages

`GET /sources` reads from PostgreSQL registry tables and shows run/export status.
Counts and observed countries come from ClickHouse aggregates keyed by
`source_slug` and `source_export_id`.

### Company Search

`GET /companies/search` reads primarily from ClickHouse
`company_search_projection`. Filters should be pushed to ClickHouse:

- country
- source
- active status
- name text
- registration number
- domain or website
- industry code

### Company Detail

`GET /companies/{id}` uses the Postgres company ID as the stable API ID.

The handler reads:

```text
PostgreSQL:
  identity.companies
  identity.brands
  identity.company_relationships
  identity.brand_company_links
  identity.company_clickhouse_links

ClickHouse:
  company_detail_projection for linked clickhouse_company_key values
```

The response combines curated identity state with source-derived detail and
coverage evidence.

## Merge And Resolution Rules

Deterministic source grouping should happen in ClickHouse-oriented jobs because
the inputs are large. The resolver produces `clickhouse_company_key` values from
stable source facts such as:

- country and registration number
- jurisdiction-specific identifiers
- LEI or other global identifiers
- normalized legal name plus strong address/domain evidence

PostgreSQL should not store every intermediate match candidate. It stores only
curated decisions, overrides, and central company links.

## Error Handling And Logging

Go code follows the repository logging rules:

- lower-level source import and ClickHouse clients wrap and return errors with
  `github.com/cockroachdb/errors`
- Temporal activities and HTTP handlers log once with `log/slog`
- external API responses use safe messages
- source command stdout/stderr and payload references must not leak secrets

## Testing

Initial tests should cover:

- PostgreSQL migration creates only control-plane and identity graph tables
- old POC company/source-record tables are dropped
- ClickHouse DDL creates Finland PRH YTJ source tables and projection tables
- Parquet import maps Finland source export rows into ClickHouse records
- search queries filter by country, source, name, registration number, and domain
- company detail composition merges Postgres identity graph and ClickHouse
  projection data

## Migration Impact

The previous clean Postgres replacement plan should be retired before execution.
Its useful parts remain:

- source registry
- source run/export metadata
- source executable activity
- central identity and brand relationships

Its large `source_records`, `entities`, and web fact tables should move to
ClickHouse projections or be reduced to small Postgres link/curation tables.
