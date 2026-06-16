# NACE Reference Categories Design

## Goal

Add a shared Dagster section for NACE category reference data. This section is independent from Finland YTJ and XBRL assets because the NACE list should be reusable by many company sources and countries.

The first implementation should pull official NACE classification data, process it into a stable analytical table, and make it available for later company-to-industry mapping. NACE is small shared reference data, so it should load directly from dlt to ClickHouse instead of creating a staging DuckDB asset.

## Source

Use Eurostat / EU Publications Office classification data as the authoritative source. Eurostat documents NACE Rev. 2.1 as the current version for European statistics from 2025 onward and NACE Rev. 2 as the version used for 2008-2024 European statistics. Eurostat also points to ShowVoc / EU vocabularies as machine-readable access for classifications.

The implementation should prefer a machine-readable source that can be fetched regularly without browser automation. Candidate formats are CSV, XML, RDF, or SPARQL output from EU vocabularies / ShowVoc. The source URL used for each load must be stored in output metadata.

## Assets

Create a new Dagster definitions section under:

`src/dagster_v3/defs/nace/`

Use Dagster group name:

`nace`

Initial asset:

`nace_categories`

This is a ClickHouse-backed reference table containing both NACE Rev. 2 and NACE Rev. 2.1. The asset should ensure the ClickHouse database/table exists, truncate the table, then run a dlt pipeline that appends the current official snapshot into the canonical table. If the selected official endpoint requires separate pulls, each revision can be pulled separately and loaded into the same normalized table.

Expected columns:

- `classification_version`: examples: `NACE_REV_2`, `NACE_REV_2_1`
- `code`: raw NACE code, preserving section letters and dotted class notation if provided
- `normalized_code`: code normalized for joins, for example removing punctuation from numeric classes
- `parent_code`: parent category code when available
- `level`: hierarchy level such as section, division, group, or class
- `section_code`: top-level section code
- `description_en`: English category description
- `concept_uri`: official concept URI
- `parent_concept_uri`: official parent concept URI when available
- `source_scheme_uri`: official scheme URI
- `valid_from`: first date this classification version should be used
- `valid_to`: nullable end date
- `is_current`: current-version flag
- `source_url`: URL used to pull the record
- `source_payload_hash`: hash of the source payload used for change detection
- `source_run_id`: Dagster run id
- `pulled_at`: UTC timestamp of the load

Optional columns can be added if they are directly available from the source without extra inference, such as multilingual labels or explanatory notes.

## Data Flow

The source fetch step downloads the official classification payloads and records source URLs and payload hashes. The transform step parses the payloads into a canonical NACE table with stable column names and explicit versioning. The load step writes the result directly to ClickHouse through dlt.

Later source-specific company assets should join their raw activity codes against `nace_categories` using `classification_version` and `normalized_code`. For Finland YTJ, the raw code set must still be stored because Finnish registry data may expose national TOL / TOIMI codes that are NACE-compatible but not always identical in naming or versioning.

## ClickHouse

The `nace_categories` asset owns this ClickHouse table:

`reference.nace_categories`

The ClickHouse schema should keep `classification_version` and `normalized_code` as first-class join keys.

The asset should run idempotent DDL before loading:

- `CREATE DATABASE IF NOT EXISTS reference`
- `CREATE TABLE IF NOT EXISTS reference.nace_categories (...)`
- `TRUNCATE TABLE reference.nace_categories`

Use dlt `write_disposition="append"` after truncation so ClickHouse DDL remains the schema contract and the table still represents the latest official snapshot.

## Automation

This dataset changes slowly. In production, schedule it weekly or monthly. For the local dev spike, no schedule is required until the asset is verified.

If scheduled later, the schedule should materialize only the NACE reference asset, not the Finland YTJ or XBRL assets.

## Error Handling And Observability

The asset should fail if the official source is unreachable, malformed, or produces zero categories. Materialization metadata should include row counts by classification version, source URLs, payload hashes, and the number of section, division, group, and class rows.

If only one classification revision is available from the selected endpoint, the asset should materialize that revision and report the missing revision clearly in metadata. It should not silently pretend both revisions were loaded.

## Tests

Add focused tests for:

- parsing a small representative source payload into versioned category rows
- preserving parent-child hierarchy
- normalizing join codes without losing the raw code
- rejecting empty parsed results
- producing stable row counts and metadata

Avoid testing the live Eurostat endpoint directly in unit tests. Use fixtures for parser tests and keep live endpoint checks as optional manual verification.
