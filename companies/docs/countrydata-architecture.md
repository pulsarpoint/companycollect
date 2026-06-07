# Countrydata Architecture

## Purpose

Countrydata packages collect company registry data country by country, outside
the Corpscout scheduler codebase. Each country package is a standalone Go
module that can be built as a binary, run in a container, and orchestrated later
by Corpscout, Temporal, or another central workflow system.

## Core Shape

```text
companies/
  common/                 shared source-agnostic Go helpers
  {country_slug}/          one standalone Go module per country
  data/{country_slug}/     generated snapshots, exports, and manifests
  analysis/{country_slug}/ tracked source discovery and data-model artifacts
  docs/                    architecture and implementation standards
```

Each country module owns its source packages, country CLI, source parquet
exports, and final country parquet exports. It must not import `corpscout`,
`scheduler`, sqlc, or database-specific types.

## Data Flow

```text
official source
  -> source snapshot
  -> source parquet export + manifest
  -> final country parquet export + manifest
  -> central loader imports parquet into Corpscout storage
```

Snapshots are raw source inputs. Source exports preserve source-specific
richness in parquet. Final exports combine one or more source exports into the
country-level model defined by `companies/analysis/{country_slug}/data_model`.

## Boundaries

- Discovery and data-model analysis live in `companies/analysis/{country_slug}`.
- Runtime outputs live in `companies/data/{country_slug}/countrydata`.
- Shared helpers live in `companies/common` only when they are source-agnostic.
- Country packages expose CLIs and parquet manifests as the integration
  boundary.
- Corpscout should orchestrate country binaries or containers and consume
  manifests/parquet files. It should not import country modules for new work.

## Why This Shape

Countries differ heavily in company identifiers, legal forms, languages,
addresses, industries, financial disclosures, filings, ownership, source access,
and data freshness. Keeping country packages independent lets each country
model its real source data without forcing one global table too early.

The central system can still build a shared search/index layer later by loading
final country parquet files and mapping common fields such as company name,
country, identifiers, status, industry codes, addresses, websites, and source
evidence.

## Operational Rules

- Use one `go.mod` per country.
- Do not create `companies/go.mod`.
- Keep generated data out of country module folders.
- Keep source download/process/export commands source-specific.
- Keep final country export construction country-specific.
- Add live tests for real-world source shape validation, gated by env vars.
- Preserve source lineage and file hashes in manifests.
- Log errors once at CLI, worker, or orchestration boundaries.
