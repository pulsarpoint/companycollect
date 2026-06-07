# Corpscout Company Identity Clean Replacement Schema

## Purpose

Corpscout needs a clean replacement for the proof-of-concept source, scheduler,
and company tables. The new model must support:

- source binaries that produce Parquet exports
- sources that cover one country, many countries, or global data
- deterministic merging of multiple source records for the same registered entity
- companies registered in many countries
- brands as first-class identities
- parent/child and ownership relationships at company, legal-entity, and brand
  levels
- source-by-source evidence on company and brand detail pages

PostgreSQL remains the system of record. Relationship tables should be shaped like
a graph so a graph database can be added later as a read model, but the first
implementation should not require Neo4j, Memgraph, Kuzu, or similar infrastructure.

## Core Decision

Use four durable layers:

```text
registry
  what source executables exist, how Temporal runs them, and what exports they
  produced

source_records
  normalized source facts imported from Parquet, preserving original payload and
  evidence

entities
  deterministic legal entities, usually one registered company in one
  jurisdiction, merged from one or more source records

identity
  central companies, brands, and graph-shaped relationships between companies,
  legal entities, and brands
```

This is intentionally not country-schema-first. Country is a strong filter and
resolution dimension, but not the storage boundary for source ingestion. Some
sources will be global or multi-country, so source imports must be modeled
independently from country pages.

## Data Flow

```text
source binary
  e.g. companies/finland/cmd/finland-countrydata
        sync-source --source prhytj --build-export --data-dir ...
      |
      | Temporal activity executes configured binary
      v
Parquet export + manifest
      |
      | Corpscout generic importer
      v
source_records.*
      |
      | deterministic resolver
      v
entities.legal_entities
entities.legal_entity_source_links
      |
      | manual review, automatic matching, imported relationships
      v
identity.companies
identity.brands
identity.relationships
```

Source binaries own source-specific download, parsing, and export logic. Corpscout
owns execution, import, resolution, identity links, and API composition.

## Registry Schema

Use one schema for source execution and export metadata:

```sql
CREATE SCHEMA registry;
```

### `registry.sources`

One row per source producer. A producer may cover one country, multiple countries,
or global records.

Important columns:

```sql
id UUID PRIMARY KEY
slug TEXT UNIQUE NOT NULL
display_name TEXT NOT NULL
description TEXT
coverage_scope TEXT NOT NULL
  CHECK (coverage_scope IN ('single_country', 'multi_country', 'global', 'unknown'))
default_country_id UUID REFERENCES countries(id)
executable_path TEXT NOT NULL
working_directory TEXT
default_args JSONB NOT NULL DEFAULT '{}'::jsonb
environment_contract JSONB NOT NULL DEFAULT '{}'::jsonb
output_contract_version TEXT NOT NULL
enabled BOOLEAN NOT NULL DEFAULT true
schedule_enabled BOOLEAN NOT NULL DEFAULT false
schedule_kind TEXT NOT NULL DEFAULT 'manual'
schedule_expression TEXT
metadata JSONB NOT NULL DEFAULT '{}'::jsonb
created_at TIMESTAMPTZ NOT NULL DEFAULT now()
updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
```

Examples:

- `finland_prh_ytj`
- `united_states_sec_edgar`
- `gleif`
- `wikidata`
- `opencorporates`

For a source like Finland PRH YTJ, `coverage_scope = 'single_country'` and
`default_country_id` points to Finland. For GLEIF or Wikidata,
`coverage_scope = 'global'`.

### `registry.source_countries`

Optional declared coverage for source pages and filtering.

```sql
source_id UUID NOT NULL REFERENCES registry.sources(id) ON DELETE CASCADE
country_id UUID NOT NULL REFERENCES countries(id)
coverage_status TEXT NOT NULL
  CHECK (coverage_status IN ('declared', 'observed', 'disabled'))
evidence JSONB NOT NULL DEFAULT '{}'::jsonb
PRIMARY KEY (source_id, country_id)
```

For global sources this table can be sparse and populated from observed imported
records.

### `registry.source_runs`

One Temporal execution of one source command.

```sql
id UUID PRIMARY KEY
source_id UUID NOT NULL REFERENCES registry.sources(id)
temporal_workflow_id TEXT
temporal_run_id TEXT
command TEXT NOT NULL
args JSONB NOT NULL DEFAULT '{}'::jsonb
trigger_type TEXT NOT NULL
  CHECK (trigger_type IN ('manual', 'scheduled', 'retry', 'backfill'))
status TEXT NOT NULL
  CHECK (status IN ('running', 'succeeded', 'failed', 'cancelled'))
started_at TIMESTAMPTZ NOT NULL
finished_at TIMESTAMPTZ
exit_code INTEGER
stdout_result JSONB NOT NULL DEFAULT '{}'::jsonb
error_message TEXT
metadata JSONB NOT NULL DEFAULT '{}'::jsonb
created_at TIMESTAMPTZ NOT NULL DEFAULT now()
```

Boundary layers log failures once. Lower-level source import code wraps and
returns errors with `github.com/cockroachdb/errors`.

### `registry.source_exports`

One manifest produced by a successful source run.

```sql
id UUID PRIMARY KEY
source_id UUID NOT NULL REFERENCES registry.sources(id)
source_run_id UUID REFERENCES registry.source_runs(id)
manifest_path TEXT NOT NULL
manifest_sha256 TEXT
export_kind TEXT NOT NULL
  CHECK (export_kind IN ('source', 'final', 'snapshot', 'other'))
schema_version TEXT NOT NULL
run_key TEXT NOT NULL
created_at_source TIMESTAMPTZ
records_seen BIGINT NOT NULL DEFAULT 0
records_exported BIGINT NOT NULL DEFAULT 0
decode_errors BIGINT NOT NULL DEFAULT 0
metadata JSONB NOT NULL DEFAULT '{}'::jsonb
created_at TIMESTAMPTZ NOT NULL DEFAULT now()
UNIQUE (source_id, run_key)
```

### `registry.source_export_files`

One row per Parquet file in a manifest.

```sql
id UUID PRIMARY KEY
source_export_id UUID NOT NULL REFERENCES registry.source_exports(id) ON DELETE CASCADE
file_name TEXT NOT NULL
file_path TEXT NOT NULL
row_count BIGINT NOT NULL DEFAULT 0
sha256 TEXT
schema_hash TEXT
metadata JSONB NOT NULL DEFAULT '{}'::jsonb
UNIQUE (source_export_id, file_name)
```

## Source Records Schema

Use shared source-record tables instead of one table per country. These tables are
the normalized import target for Parquet exports. They preserve source identity,
country/jurisdiction, and original payload references.

```sql
CREATE SCHEMA source_records;
```

### `source_records.companies`

One company-like record as asserted by one source.

```sql
id UUID PRIMARY KEY
source_id UUID NOT NULL REFERENCES registry.sources(id)
source_export_id UUID REFERENCES registry.source_exports(id)
source_record_id TEXT NOT NULL
source_native_id TEXT
country_id UUID REFERENCES countries(id)
jurisdiction_code TEXT
registration_number TEXT
legal_name TEXT
legal_name_normalized TEXT
lifecycle_status TEXT
is_active BOOLEAN
primary_website TEXT
source_updated_at TIMESTAMPTZ
source_payload_hash TEXT
record_hash TEXT NOT NULL
raw_payload JSONB NOT NULL DEFAULT '{}'::jsonb
normalized_payload JSONB NOT NULL DEFAULT '{}'::jsonb
evidence JSONB NOT NULL DEFAULT '{}'::jsonb
first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now()
last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now()
created_at TIMESTAMPTZ NOT NULL DEFAULT now()
updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
UNIQUE (source_id, source_record_id)
```

`country_id` should be set when the source record has a clear legal jurisdiction.
For global sources that describe a multinational group rather than a registered
entity, `country_id` may be null and the record can link directly to an identity
company or brand.

### Source Child Tables

Core source facts should use shared child tables:

```text
source_records.company_names
source_records.identifiers
source_records.addresses
source_records.industries
source_records.websites
source_records.source_evidence
```

Each child row should include:

```sql
id UUID PRIMARY KEY
source_company_id UUID NOT NULL REFERENCES source_records.companies(id) ON DELETE CASCADE
source_id UUID NOT NULL REFERENCES registry.sources(id)
source_export_id UUID REFERENCES registry.source_exports(id)
source_item_hash TEXT NOT NULL
raw_item_payload JSONB NOT NULL DEFAULT '{}'::jsonb
evidence JSONB NOT NULL DEFAULT '{}'::jsonb
created_at TIMESTAMPTZ NOT NULL DEFAULT now()
updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
```

Optional capabilities can be added as shared tables when needed:

```text
source_records.officers
source_records.beneficial_owners
source_records.filings
source_records.financial_statements
source_records.branches
source_records.licenses
```

Unknown optional Parquet files should be recorded in `registry.source_export_files`
and import metadata, not silently discarded.

## Legal Entity Schema

Legal entities are deterministic merged rows for registered companies in a
jurisdiction. They are the layer that solves: multiple sources found the same
company in the same country, and Corpscout needs one stable row for that legal
entity.

```sql
CREATE SCHEMA entities;
```

### `entities.legal_entities`

```sql
id UUID PRIMARY KEY
country_id UUID REFERENCES countries(id)
jurisdiction_code TEXT
registration_number TEXT
lei TEXT
tax_identifier TEXT
vat_identifier TEXT
euid TEXT
canonical_name TEXT NOT NULL
canonical_name_normalized TEXT
lifecycle_status TEXT
is_active BOOLEAN
primary_website TEXT
resolution_key TEXT NOT NULL
resolution_strategy TEXT NOT NULL
profile_hash TEXT
confidence REAL CHECK (confidence IS NULL OR confidence BETWEEN 0 AND 1)
review_status TEXT NOT NULL DEFAULT 'auto_resolved'
  CHECK (review_status IN ('auto_resolved', 'needs_review', 'confirmed', 'rejected'))
evidence JSONB NOT NULL DEFAULT '{}'::jsonb
metadata JSONB NOT NULL DEFAULT '{}'::jsonb
created_at TIMESTAMPTZ NOT NULL DEFAULT now()
updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
UNIQUE (country_id, registration_number)
UNIQUE (lei)
UNIQUE (resolution_key)
```

`country_id` is nullable for rare legal-entity records where jurisdiction is not
known yet. It should be populated for normal country registry records.

### `entities.legal_entity_source_links`

Links one legal entity to all source records that support it.

```sql
id UUID PRIMARY KEY
legal_entity_id UUID NOT NULL REFERENCES entities.legal_entities(id) ON DELETE CASCADE
source_company_id UUID NOT NULL REFERENCES source_records.companies(id) ON DELETE CASCADE
source_id UUID NOT NULL REFERENCES registry.sources(id)
match_method TEXT NOT NULL
match_confidence REAL NOT NULL CHECK (match_confidence BETWEEN 0 AND 1)
is_primary BOOLEAN NOT NULL DEFAULT false
field_coverage JSONB NOT NULL DEFAULT '{}'::jsonb
evidence JSONB NOT NULL DEFAULT '{}'::jsonb
created_at TIMESTAMPTZ NOT NULL DEFAULT now()
updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
UNIQUE (legal_entity_id, source_company_id)
```

`field_coverage` lets the detail API show how much each source knows:

```json
{
  "identifiers": true,
  "names": true,
  "addresses": true,
  "websites": false,
  "industries": true,
  "officers": false,
  "financials": false
}
```

### `entities.legal_entity_relationships`

Country/jurisdiction-level relationships between registered entities.

```sql
id UUID PRIMARY KEY
subject_legal_entity_id UUID NOT NULL REFERENCES entities.legal_entities(id)
predicate TEXT NOT NULL
object_legal_entity_id UUID NOT NULL REFERENCES entities.legal_entities(id)
ownership_percentage NUMERIC(5,2)
valid_from DATE
valid_to DATE
source_id UUID REFERENCES registry.sources(id)
confidence REAL CHECK (confidence IS NULL OR confidence BETWEEN 0 AND 1)
status TEXT NOT NULL DEFAULT 'active'
  CHECK (status IN ('active', 'needs_review', 'rejected', 'superseded'))
evidence JSONB NOT NULL DEFAULT '{}'::jsonb
created_at TIMESTAMPTZ NOT NULL DEFAULT now()
updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
CHECK (subject_legal_entity_id <> object_legal_entity_id)
```

Predicates should include:

```text
direct_parent
ultimate_parent
subsidiary_of
branch_of
same_legal_group
merged_into
acquired_by
other
```

## Identity Schema

Identity tables model central companies and brands. They are not source records
and not always registered legal entities.

```sql
CREATE SCHEMA identity;
```

### `identity.companies`

A central company is the resolved operating/corporate identity that can connect to
many legal entities across countries.

```sql
id UUID PRIMARY KEY
canonical_name TEXT NOT NULL
canonical_name_normalized TEXT NOT NULL
display_name TEXT
company_kind TEXT NOT NULL DEFAULT 'operating_company'
  CHECK (company_kind IN ('operating_company', 'corporate_group', 'holding_company', 'unknown'))
home_country_id UUID REFERENCES countries(id)
primary_legal_entity_id UUID REFERENCES entities.legal_entities(id)
primary_website TEXT
description TEXT
profile_hash TEXT
review_status TEXT NOT NULL DEFAULT 'needs_review'
  CHECK (review_status IN ('auto_resolved', 'needs_review', 'confirmed', 'rejected'))
evidence JSONB NOT NULL DEFAULT '{}'::jsonb
metadata JSONB NOT NULL DEFAULT '{}'::jsonb
created_at TIMESTAMPTZ NOT NULL DEFAULT now()
updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
```

### `identity.company_legal_entity_links`

Links central companies to legal entities.

```sql
id UUID PRIMARY KEY
company_id UUID NOT NULL REFERENCES identity.companies(id) ON DELETE CASCADE
legal_entity_id UUID NOT NULL REFERENCES entities.legal_entities(id) ON DELETE CASCADE
link_type TEXT NOT NULL
  CHECK (link_type IN ('same_company', 'registered_entity', 'branch', 'subsidiary', 'representative_office', 'other'))
is_primary BOOLEAN NOT NULL DEFAULT false
confidence REAL CHECK (confidence IS NULL OR confidence BETWEEN 0 AND 1)
source_id UUID REFERENCES registry.sources(id)
evidence JSONB NOT NULL DEFAULT '{}'::jsonb
created_at TIMESTAMPTZ NOT NULL DEFAULT now()
updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
UNIQUE (company_id, legal_entity_id, link_type)
```

This is how one company can connect to registered companies in many countries.

### `identity.brands`

A brand is a market-facing identity. It can be owned by a company, operated by a
legal entity, used by several entities, or be a sub-brand of another brand.

```sql
id UUID PRIMARY KEY
canonical_name TEXT NOT NULL
canonical_name_normalized TEXT NOT NULL
display_name TEXT
brand_kind TEXT NOT NULL DEFAULT 'brand'
  CHECK (brand_kind IN ('brand', 'product_brand', 'service_brand', 'marque', 'trade_name', 'unknown'))
primary_website TEXT
description TEXT
review_status TEXT NOT NULL DEFAULT 'needs_review'
  CHECK (review_status IN ('auto_resolved', 'needs_review', 'confirmed', 'rejected'))
evidence JSONB NOT NULL DEFAULT '{}'::jsonb
metadata JSONB NOT NULL DEFAULT '{}'::jsonb
created_at TIMESTAMPTZ NOT NULL DEFAULT now()
updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
```

Examples:

- Volkswagen as a brand or marque
- Audi as a brand or marque
- Škoda as a brand or marque
- Volkswagen Group as a central company or corporate group
- Volkswagen AG as a legal entity and likely also the primary company identity

### Brand Link Tables

```text
identity.brand_company_links
identity.brand_legal_entity_links
identity.brand_source_links
```

Use separate tables rather than one nullable-target table. This keeps foreign keys
clear and avoids rows that accidentally point at multiple target types.

`identity.brand_company_links`:

```sql
id UUID PRIMARY KEY
brand_id UUID NOT NULL REFERENCES identity.brands(id) ON DELETE CASCADE
company_id UUID NOT NULL REFERENCES identity.companies(id) ON DELETE CASCADE
link_type TEXT NOT NULL
confidence REAL CHECK (confidence IS NULL OR confidence BETWEEN 0 AND 1)
source_id UUID REFERENCES registry.sources(id)
evidence JSONB NOT NULL DEFAULT '{}'::jsonb
created_at TIMESTAMPTZ NOT NULL DEFAULT now()
updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
UNIQUE (brand_id, company_id, link_type)
```

`identity.brand_legal_entity_links`:

```sql
id UUID PRIMARY KEY
brand_id UUID NOT NULL REFERENCES identity.brands(id) ON DELETE CASCADE
legal_entity_id UUID NOT NULL REFERENCES entities.legal_entities(id) ON DELETE CASCADE
link_type TEXT NOT NULL
confidence REAL CHECK (confidence IS NULL OR confidence BETWEEN 0 AND 1)
source_id UUID REFERENCES registry.sources(id)
evidence JSONB NOT NULL DEFAULT '{}'::jsonb
created_at TIMESTAMPTZ NOT NULL DEFAULT now()
updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
UNIQUE (brand_id, legal_entity_id, link_type)
```

`identity.brand_source_links`:

```sql
id UUID PRIMARY KEY
brand_id UUID NOT NULL REFERENCES identity.brands(id) ON DELETE CASCADE
source_company_id UUID NOT NULL REFERENCES source_records.companies(id) ON DELETE CASCADE
link_type TEXT NOT NULL
confidence REAL CHECK (confidence IS NULL OR confidence BETWEEN 0 AND 1)
source_id UUID REFERENCES registry.sources(id)
evidence JSONB NOT NULL DEFAULT '{}'::jsonb
created_at TIMESTAMPTZ NOT NULL DEFAULT now()
updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
UNIQUE (brand_id, source_company_id, link_type)
```

Valid brand/company link types:

```text
owned_by
operated_by
licensed_to
used_by
formerly_owned_by
other
```

### Graph-Shaped Relationships

Use explicit relationship tables for common high-value relationships and one
generic graph edge table for cross-type links that do not deserve their own table.

Recommended explicit tables:

```text
identity.company_relationships
identity.brand_relationships
entities.legal_entity_relationships
```

Recommended generic table:

```sql
CREATE TABLE identity.relationship_edges (
  id UUID PRIMARY KEY,
  subject_type TEXT NOT NULL
    CHECK (subject_type IN ('company', 'brand', 'legal_entity', 'source_record')),
  subject_id UUID NOT NULL,
  predicate TEXT NOT NULL,
  object_type TEXT NOT NULL
    CHECK (object_type IN ('company', 'brand', 'legal_entity', 'source_record')),
  object_id UUID NOT NULL,
  source_id UUID REFERENCES registry.sources(id),
  confidence REAL CHECK (confidence IS NULL OR confidence BETWEEN 0 AND 1),
  valid_from DATE,
  valid_to DATE,
  status TEXT NOT NULL DEFAULT 'active'
    CHECK (status IN ('active', 'needs_review', 'rejected', 'superseded')),
  evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CHECK (subject_type <> object_type OR subject_id <> object_id)
);
```

Use explicit tables when foreign keys matter. Use `relationship_edges` for mixed
or less-common links that need graph behavior. If a generic edge becomes important
to product behavior, promote it to an explicit FK-backed table.

## Company Detail API

Company detail should assemble a coherent profile while preserving source truth.

Endpoint:

```text
GET /companies/{company_id}
```

Response shape:

```json
{
  "company": {},
  "brands": [],
  "legal_entities": [],
  "relationships": [],
  "source_coverage": [],
  "resolved_profile": {},
  "source_records": []
}
```

Load order:

1. Load `identity.companies`.
2. Load linked `entities.legal_entities`.
3. Load legal-entity relationships and central company relationships.
4. Load linked brands and brand relationships.
5. Load source records through `entities.legal_entity_source_links`.
6. Compute source coverage by field family.
7. Produce `resolved_profile` from deterministic precedence rules.
8. Include original normalized and raw source payload references.

The resolved profile should be evidence-backed. It should not erase source
conflicts. Conflicting source values should appear in the source panels and in a
conflict section when confidence is low.

## Brand Detail API

Endpoint:

```text
GET /brands/{brand_id}
```

Response shape:

```json
{
  "brand": {},
  "owners": [],
  "operators": [],
  "related_brands": [],
  "legal_entities_using_brand": [],
  "source_records": []
}
```

This keeps a brand like Volkswagen separate from legal entities such as
Volkswagen AG and from country registrations or subsidiaries.

## `/sources` UI

`/sources` should remain source-oriented because sources may be global or
multi-country.

Recommended routes:

```text
/sources
  source list with coverage scope, countries observed, latest run, latest export

/sources/:source_slug
  executable path, configured commands, schedules, runs, exports, imported files,
  observed countries, and source-record counts

/sources/countries/:country_iso2
  country-filtered source view showing all sources that contribute records for the
  selected country, plus resolved legal entities and unresolved duplicate groups
```

For the current local URL, `http://localhost:8094/sources` can first show the
source list with a country filter. A dedicated country route can be added after
the source list is stable.

## Resolver Rules

Initial legal-entity resolution should be deterministic and conservative:

- exact `(country_id, registration_number)` match
- exact LEI match
- exact EUID match
- exact source-provided stable identifier match
- no fuzzy merge without review

Automatic central-company linking should be more conservative than legal-entity
resolution. It can start with:

- one central company per confirmed legal entity
- manual or reviewed links across countries
- source-supported group relationships from high-confidence sources

This avoids accidental global merges between unrelated companies with similar
names.

## Graph Database Position

Do not introduce a graph database in the clean replacement.

Postgres should own:

- source imports
- legal entity resolution
- central identity
- brands
- relationships
- evidence and audit history

Relationship tables should be graph-shaped so a graph read model can be built
later. If traversal becomes central to the product, project Postgres relationships
to a graph database. The graph database should be rebuildable and not become the
first source of truth.

## Migration Stance

This is a clean replacement target. The current POC tables can be dropped or
recreated during implementation:

- old `data_sources`
- old `source_pull_runs`
- old central `companies` with direct `country_id`
- old country/source-specific raw schemas that duplicate the new source-record
  contract

Implementation can still be phased, but the target schema should not preserve old
POC assumptions such as one country per central company.
