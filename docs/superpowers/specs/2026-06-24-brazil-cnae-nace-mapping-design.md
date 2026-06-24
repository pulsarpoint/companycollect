# Brazil CNAE to NACE Mapping Design

## Goal

Add a Brazil-specific, curated many-to-many mapping table between Brazilian CNAE
industry codes and NACE categories before implementing Brazil company ingestion.
The first application UI will continue to use NACE as the primary industry
exploration taxonomy, while Brazil source data keeps the original CNAE values so
a later country-specific CNAE view can be added without re-ingesting companies.

## Context

Existing country industry tables in `dagster_v3` keep the source industry code
and mapped NACE columns in the same per-country table. Examples include
`ee_industries`, `fr_industries`, `cz_industries`, `sk_industries`,
`gb_industries`, `fi_industries`, and `no_industries`.

Brazil is different from EU-derived NACE sources. Receita Federal publishes CNAE
codes in the CNPJ establishment data. CNAE is the authoritative Brazilian source
classification, but the UI needs NACE filtering for cross-country exploration.
The mapping is not guaranteed to be one-to-one, so it must be modeled as edges:
one CNAE can map to multiple NACE categories, and one NACE category can receive
many CNAE codes.

## Table

Create `corpscout.br_cnae_to_nace` as a ClickHouse reference table owned by a
Dagster source under `defs/brazil_cnae`.

Columns:

```text
cnae_version
cnae_code
cnae_normalized_code
cnae_description_pt
cnae_description_en
nace_revision
nace_code
nace_normalized_code
nace_description_en
mapping_source
source_url
source_payload_hash
source_run_id
pulled_at
```

The table key/order should allow duplicate CNAE and duplicate NACE values:

```text
(cnae_version, cnae_normalized_code, nace_revision, nace_normalized_code)
```

No `mapping_status`, `mapping_method`, or `mapping_confidence` columns are needed
in the first version. Rows in this table are accepted mapping edges from curated
fixtures. Missing mappings should be detected by checks, not represented as
placeholder rows.

## Fixture Source

The mapping asset should load versioned fixture files committed with the source.
The fixture represents the accepted CNAE-to-NACE mapping edges. If the mapping is
derived through an intermediate classification such as ISIC Rev. 4, that derivation
belongs in the fixture-generation notes and `mapping_source`, not in runtime
company ingestion.

Expected fixture properties:

- stable text format such as CSV;
- one row per mapping edge;
- explicit CNAE version and NACE revision;
- original Portuguese CNAE description;
- English CNAE description, maintained as curated reference text;
- NACE English description copied from or validated against `nace_categories`;
- source URL and payload hash for provenance.

## Function Boundary

The mapping logic should be a plain function that can be tested without Dagster:

```text
build_br_cnae_to_nace(
    duckdb_path,
    fixture_path,
    source_run_id,
) -> dict[str, int]
```

The function owns the actual work:

- create or replace the DuckDB staging table;
- load fixture rows;
- normalize CNAE and NACE codes;
- validate required fields and duplicate mapping edges;
- validate target NACE codes against available `nace_categories` data when that
  dependency is supplied to the test or materialization context;
- return row counts and validation counts for asset metadata.

Keep this function source-specific and direct. Do not introduce a generic fixture
loader, mapping service, interface, or config object unless another real source
needs the same behavior later.

## Dagster Flow

Use the normal `dagster_v3` reference-data flow:

```text
fixture files -> brazil_cnae_source.duckdb -> corpscout.br_cnae_to_nace
```

The source should be small and direct:

- one source module: `defs/brazil_cnae`;
- one DuckDB file: `data/brazil_cnae_source.duckdb`;
- one source-owned pool for writes to that DuckDB file;
- the materialization function only calls `build_br_cnae_to_nace(...)`, logs the
  returned counts, and returns them as metadata;
- direct DuckDB load and validation inside the function, no row-by-row Python
  transform;
- migration-owned ClickHouse DDL and atomic replacement on export.

## Brazil Company Usage

When Brazil CNPJ ingestion is implemented, `br_industries` should keep CNAE as
source data and use `br_cnae_to_nace` to populate the NACE columns:

```text
RFB Estabelecimentos.cnae_fiscal_principal
  -> normalize CNAE code
  -> join corpscout.br_cnae_to_nace
  -> emit one br_industries row per CNAE-to-NACE edge
```

If one company CNAE maps to three NACE categories, `br_industries` emits three
rows for that source CNAE. UI filtering by NACE must deduplicate companies after
the industry join.

`br_industries` should mirror the existing per-country industry shape:

```text
company_id / cnpj
source_industry_code
source_industry_code_set = BR_CNAE
description_original
description_language = pt
description_en
nace_revision
nace_code
nace_normalized_code
is_primary
source_system
source_run_id
source_record_id
resolved_at
```

## Checks

Add checks at the mapping layer:

- every fixture row has non-empty CNAE and NACE normalized codes;
- fixture primary edge key is unique;
- every mapped NACE code joins `corpscout.nace_categories`;
- descriptions are populated for both CNAE and NACE;
- mapping export refuses to replace ClickHouse with zero rows.

Add checks at the later Brazil CNPJ layer:

- every distinct CNAE code used by RFB establishments has at least one mapping row;
- `br_industries` emits at least one mapped NACE row for companies with non-empty
  primary CNAE;
- NACE UI queries deduplicate companies after joining `br_industries`.

## Non-Goals

Do not create a generic `industry_code_mappings` table yet. A generic table may
be justified later if multiple country-specific mapping tables converge on the
same structure, but Brazil should not pay that abstraction cost before we have
other real examples.

Do not replace country-specific classification support with NACE. CNAE remains
the source classification for Brazil and should be available for a later
Brazil-specific category UI.

Do not store unmapped CNAE rows in `br_cnae_to_nace`. Missing mappings are data
quality failures or review items, not mapping facts.

## Testing

Focused tests should cover:

- fixture parsing and normalization;
- many-to-many behavior, including one CNAE mapping to multiple NACE rows;
- duplicate edge rejection;
- NACE join validation against a small fake `nace_categories` table;
- ClickHouse export uses the migration-owned table and atomic replacement helper.

The first Brazil CNPJ implementation should add separate tests proving that
company industry rows expand through the mapping table and that NACE filtering
can return Brazil companies through the same fields used by existing sources.
