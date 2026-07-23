# Eurostat source design

## Scope

- Module: `defs/eurostat/`
- Raw bucket: `source-eurostat`
- DuckDB file: `data/eurostat_source.duckdb`
- DuckDB pool: `eurostat_duckdb`
- ClickHouse migration: `000159_corpscout_eurostat`
- Historical window: fixed at 2010-present

The source covers the confirmed annual country-overview and company-economy
datasets:

- `nama_10_gdp`
- `nama_10_pc`
- `nama_10_a10`
- `gov_10dd_edpt1`
- `gov_10a_main`
- `prc_hicp_aind`
- `une_rt_a`
- `demo_gind`
- `bd_size`
- `bd_hg`
- `sbs_ovw_act`
- `sbs_sc_ovw`

The dataset registry is an explicit product boundary. Geography values are
source-driven: every geography and aggregate in the selected Eurostat files is
retained without a configured country list.

## Asset chain

1. `eurostat_snapshot_s3`
   downloads one compressed TSV and one SDMX structure/codelist XML document per
   dataset. Objects are content-addressed and the run manifest is written only
   after all 24 files validate.
2. `eurostat_observations_duckdb`
   downloads and verifies every S3 object before opening the persistent DuckDB
   file. DuckDB SQL splits series dimensions, unpivots time columns, parses
   values and flags, filters to 2010-present, and transactionally replaces the
   normalized tables.
3. `eurostat_observations_clickhouse`
   validates DuckDB contracts and atomically replaces all five migration-owned
   ClickHouse tables.

The raw asset uses dlt's retrying HTTP client. It does not use a dlt pipeline:
the source is already a durable wide TSV bulk file and DuckDB is the required
normalization engine.

## Normalized model

- `eurostat_datasets`: current dataset metadata and raw-object lineage
- `eurostat_dimension_values`: dimension and codelist labels from the SDMX DSD
- `eurostat_series`: natural Eurostat series keys and promoted common dimensions
- `eurostat_series_dimensions`: one row per series dimension
- `eurostat_observations`: annual numeric or flagged-missing observations

Plain `:` cells are omitted. Flagged missing cells such as `: @C` are retained
with a null value, and numeric statuses such as `p` or `b` are preserved
verbatim. The pipeline does not infer whether a Eurostat geography is a country;
consumers join `geo_code` to the canonical country registry.

ClickHouse represents the current Eurostat state. Weekly content-addressed S3
snapshots preserve the raw files needed for audit or future revision-history
processing.

## Validation

The materialization fails before publication when:

- the manifest differs from the configured dataset set;
- TSV dimensions differ from the matching DSD;
- series keys are malformed or duplicated;
- a selected dataset contains non-annual series;
- observation values cannot be parsed;
- a series dimension value is absent from its SDMX codelist;
- an output table is empty;
- duplicate observations exist; or
- observations before 2010 survive normalization.

## Automation

`eurostat_refresh_job` selects the complete three-asset chain through
`AssetSelection.assets(...).upstream()`.

`eurostat_weekly_schedule` is registered for Sundays at 05:55
`Europe/Belgrade`, after World Bank and IMF. It remains stopped until the first
full live materialization and ClickHouse validation succeed.
