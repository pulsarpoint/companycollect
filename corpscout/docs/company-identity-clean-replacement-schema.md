# Corpscout Company Identity Clean Replacement Schema

## Purpose

Corpscout needs a clean replacement for the proof-of-concept source, scheduler,
and company tables. The new model must support:

- source binaries that produce Parquet exports
- sources that cover one country, many countries, or global data
- deterministic merging of multiple source records for the same registered entity
- companies registered in many countries
- brands as first-class identities
- domains and websites that can connect to many companies, legal entities, brands,
  and source records
- parent/child and ownership relationships at company, legal-entity, and brand
  levels
- source-by-source evidence on company and brand detail pages

PostgreSQL remains the system of record. Relationship tables should be shaped like
a graph so a graph database can be added later as a read model, but the first
implementation should not require Neo4j, Memgraph, Kuzu, or similar infrastructure.

## Core Decision

Use five durable layers:

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

web
  domains, websites, URLs, and evidence-backed links between web properties and
  companies, legal entities, brands, and source records
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
      |
      | web presence links
      v
web.domains
web.websites
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

## Web Presence Schema

Domains and websites are first-class records. A domain can be used by many legal
entities, companies, and brands over time. A website can represent a specific URL
or site surface on a domain. Source records may assert web properties, but source
assertions should link to shared web records instead of being copied only as text.

```sql
CREATE SCHEMA web;
```

### `web.domains`

```sql
id UUID PRIMARY KEY
domain TEXT UNIQUE NOT NULL
registrable_domain TEXT
public_suffix TEXT
normalized_domain TEXT UNIQUE NOT NULL
first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now()
last_seen_at TIMESTAMPTZ
metadata JSONB NOT NULL DEFAULT '{}'::jsonb
created_at TIMESTAMPTZ NOT NULL DEFAULT now()
updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
```

Examples:

- `volkswagen.com`
- `vw.com`
- `volkswagen.fi`
- `audi.com`

### `web.websites`

One row per normalized website URL or site surface.

```sql
id UUID PRIMARY KEY
domain_id UUID NOT NULL REFERENCES web.domains(id) ON DELETE CASCADE
url TEXT UNIQUE NOT NULL
normalized_url TEXT UNIQUE NOT NULL
url_kind TEXT NOT NULL DEFAULT 'website'
  CHECK (url_kind IN ('website', 'landing_page', 'support', 'investor_relations', 'careers', 'social_profile', 'other'))
title TEXT
language_code TEXT
first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now()
last_seen_at TIMESTAMPTZ
metadata JSONB NOT NULL DEFAULT '{}'::jsonb
created_at TIMESTAMPTZ NOT NULL DEFAULT now()
updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
```

`web.domains` answers ownership and domain-level questions. `web.websites`
answers page/site-level questions. A company can have multiple websites on the
same domain, and a domain can host many related brands or country-specific legal
entities.

### Web Link Tables

Use separate FK-backed link tables for the important targets:

```text
web.company_website_links
web.legal_entity_website_links
web.brand_website_links
web.source_record_website_links
```

Recommended columns for company links:

```sql
id UUID PRIMARY KEY
company_id UUID NOT NULL REFERENCES identity.companies(id) ON DELETE CASCADE
website_id UUID NOT NULL REFERENCES web.websites(id) ON DELETE CASCADE
domain_id UUID NOT NULL REFERENCES web.domains(id) ON DELETE CASCADE
relationship_type TEXT NOT NULL
  CHECK (relationship_type IN ('official_site', 'brand_site', 'country_site', 'investor_site', 'careers_site', 'support_site', 'candidate', 'old_site', 'other'))
status TEXT NOT NULL DEFAULT 'needs_review'
  CHECK (status IN ('active', 'needs_review', 'rejected', 'superseded'))
confidence REAL CHECK (confidence IS NULL OR confidence BETWEEN 0 AND 1)
source_id UUID REFERENCES registry.sources(id)
valid_from DATE
valid_to DATE
evidence JSONB NOT NULL DEFAULT '{}'::jsonb
created_at TIMESTAMPTZ NOT NULL DEFAULT now()
updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
UNIQUE (company_id, website_id, relationship_type)
```

`web.legal_entity_website_links`, `web.brand_website_links`, and
`web.source_record_website_links` use the same shape with their target FK:

```text
legal_entity_id -> entities.legal_entities(id)
brand_id -> identity.brands(id)
source_company_id -> source_records.companies(id)
```

Source-record links preserve the exact website assertion from a source. Company,
legal-entity, and brand links are resolved web presence. This lets the detail API
show both “PRH says this is the website” and “Corpscout resolved this as the
official brand site.”

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
web.company_website_links
web.legal_entity_website_links
web.brand_website_links
```

Recommended generic table:

```sql
CREATE TABLE identity.relationship_edges (
  id UUID PRIMARY KEY,
  subject_type TEXT NOT NULL
    CHECK (subject_type IN ('company', 'brand', 'legal_entity', 'source_record', 'domain', 'website')),
  subject_id UUID NOT NULL,
  predicate TEXT NOT NULL,
  object_type TEXT NOT NULL
    CHECK (object_type IN ('company', 'brand', 'legal_entity', 'source_record', 'domain', 'website')),
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
  "websites": [],
  "domains": [],
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
5. Load linked websites and domains for the company, legal entities, brands, and
   source records.
6. Load source records through `entities.legal_entity_source_links`.
7. Compute source coverage by field family.
8. Produce `resolved_profile` from deterministic precedence rules.
9. Include original normalized and raw source payload references.

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
  "websites": [],
  "domains": [],
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

## Existing Corpscout Migration Tables To Preserve

This section refers to the current Corpscout application migrations:

```text
/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/database/migrations
```

These migrations contain several generations of POC tables. The clean replacement
should preserve durable concepts and data contracts, not old table names where the
model has changed.

### Preserve Mostly As-Is

These tables are useful reference or service configuration data and do not depend
on the old company identity model:

```text
countries
nace_classifications
nace_codes
nace_code_aliases
nace_source_files
nace_import_runs
exchange_rate_source_files
exchange_rate_sheets
exchange_rates
exchange_rate_sync_runs
llm_providers
temporal_schedule_metadata
```

`countries` remains the shared country reference. NACE remains the industry
taxonomy reference used by source records and legal entities. Exchange rates
remain useful for normalized financials. `llm_providers` and
`temporal_schedule_metadata` are scheduler/service configuration tables and can
stay outside identity resolution.

The implementation may move these into clearer schemas later, for example
`reference.*`, `finance.*`, or `scheduler.*`, but their data model should be
preserved.

### Replace With `registry.*`

The old source registry and run tables should be replaced by the new registry
schema:

```text
data_sources
source_pull_runs
source_processor_states
source_sync_checkpoints
temporal_executions
```

Preserve these concepts:

- enabled source definitions
- source coverage and capabilities
- schedule settings
- last started/success/failed timestamps
- source markers/checkpoints
- Temporal workflow IDs and run status
- run counts, errors, and metadata

Do not preserve the old `data_sources.input_table_name`,
`pull_task_type`, or `processor_task_type` model as the primary contract. The new
contract is executable source producers plus Parquet manifests:

```text
registry.sources
registry.source_countries
registry.source_runs
registry.source_exports
registry.source_export_files
```

### Replace With `source_records.*`

The old source-specific raw/profile tables should be folded into the shared source
record model:

```text
gleif_company_raw_inputs
companies_house_company_raw_inputs
brreg_company_raw_inputs
ai_company_profile_raw_inputs
domain_discovery_raw_inputs
cvr_company_raw_inputs
ariregister_company_raw_inputs
brreg_enhanced_raw_inputs

brreg_source.*
ariregister_source.*
france_source.*

countrydata_finland_prh_ytj.*
countrydata_united_states_colorado_entities.*
countrydata_united_states_irs_eo_bmf.*
countrydata_united_states_sam_gov_entity.*
countrydata_united_states_sec_edgar.*
```

Preserve the field families these tables already proved useful:

- source-native identifiers
- country and jurisdiction codes
- legal names and normalized names
- lifecycle/status fields
- legal forms
- addresses and geocode state
- contacts
- websites and domains
- industries and NACE mappings
- capital, annual reports, financial statements, and financial periods
- people, organizations, roles, and shareholdings where sources provide them
- establishments/branches for France-like datasets
- raw payloads, normalized payloads, payload hashes, source item hashes, evidence,
  metadata, first/last seen timestamps, and row status

The new shared target should be:

```text
source_records.companies
source_records.company_names
source_records.identifiers
source_records.addresses
source_records.contacts
source_records.industries
source_records.websites
source_records.source_evidence
```

Add optional capability tables only when imported Parquet needs them:

```text
source_records.establishments
source_records.financial_statements
source_records.financial_periods
source_records.capital
source_records.people
source_records.roles
source_records.shareholdings
source_records.filings
```

### Replace With Generic Workflow/Import Audit

The old source-specific workflow schemas are useful, but should not remain
source-specific:

```text
brreg_workflow.*
ariregister_workflow.*
cvr_workflow.*
france_workflow.*
se_workflow.*
```

Preserve these workflow concepts:

- workflow/import runs
- source files and bulk snapshots
- raw records with current-row tracking
- task attempts
- per-record task states
- translation/domain/financial result summaries
- retry metadata, error category, error code, and result summaries

In the clean replacement, the source binary owns source-specific download and
normalization. Corpscout only needs generic execution/import audit:

```text
registry.source_runs
registry.source_exports
registry.source_export_files
registry.import_runs
registry.import_run_files
```

If Corpscout later needs per-record background tasks again, add a generic task
state table keyed by `source_records.companies.id`, not a source-specific workflow
schema.

### Replace Central Company Tables With `entities.*`, `identity.*`, And `web.*`

The old central company model should not be preserved as-is because `companies`
contains `country_id` directly and therefore models one country per central
company:

```text
companies
company_aliases
company_locations
company_addresses
company_phones
company_emails
company_industries
company_markets
company_services
company_financials
domains
company_domains
company_domain_reviews
company_relationships
```

Preserve the concepts, but move them to the new layers:

```text
entities.legal_entities
entities.legal_entity_source_links
entities.legal_entity_relationships

identity.companies
identity.company_legal_entity_links
identity.company_relationships
identity.brands
identity.brand_company_links
identity.brand_legal_entity_links
identity.brand_relationships

web.domains
web.websites
web.company_website_links
web.legal_entity_website_links
web.brand_website_links
web.source_record_website_links
```

Do not keep `companies.country_id` in the central identity table. Country belongs
on legal entities and source records. A central company can link to many legal
entities across countries.

### Preserve Review Workflow Concepts

The current suggestion tables contain useful review mechanics:

```text
suggestions
suggestion_source_links
suggestion_company_profiles
suggestion_company_domains
suggestion_company_locations
suggestion_company_emails
suggestion_company_phones
suggestion_company_financials
suggestion_company_industries
suggestion_company_markets
suggestion_company_services
suggestion_company_relationships

company_suggestions
organization_suggestions
open_source_project_suggestions
company_domain_suggestions
company_contact_suggestions
company_location_suggestions
company_status_suggestions
company_relationship_suggestions
```

Do not preserve both generations. Preserve one review model with:

- source link to the record or evidence that caused the suggestion
- target type and target ID
- operation: add, update, remove, replace, merge, split
- pending/applied/rejected/superseded status
- confidence
- reviewer, reviewed timestamp, and review note
- proposed JSON payload and optional field-level proposed rows

The new review model should target the new tables: legal entities, identity
companies, brands, web links, and relationships.

### Optional Or Product-Specific

These tables are not part of the core company identity replacement. Keep only if
the product still needs those features:

```text
organizations
open_source_projects
cpe_entity_link_suggestions
cpe_entity_links
cve_entity_link_suggestions
cve_entity_links
companies_house_sic_codes
domain_import_batches
domain_crawl_jobs
domain_crawl_job_pages
translation_cache
source_translation.*
brreg_source.translation_terms
ariregister_source.translation_terms
brreg_source.translation_queue_entries
ariregister_source.translation_queue_entries
brreg_source.action_tasks
ariregister_source.action_tasks
france_source.action_tasks
```

For the clean replacement, translation and action queues should be generic or
source-record based rather than hard-coded per source.

### Views And Materialized Views

Most current views are tied to old table shapes:

```text
v_companies
v_company_sources
v_company_domains
v_domains
v_resolved_entities
v_company_locations
v_company_phones
v_company_emails
v_company_industries
v_company_markets
v_company_services
brreg_source.mv_company_explorer
ariregister_source.mv_company_explorer
france_source views
```

Rebuild views after the new base tables exist. Preserve the product surfaces, not
the old SQL definitions:

- source list and source detail
- country-filtered source records
- legal entity detail
- central company detail
- brand detail
- source coverage summaries
- unresolved duplicate/review queues

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
