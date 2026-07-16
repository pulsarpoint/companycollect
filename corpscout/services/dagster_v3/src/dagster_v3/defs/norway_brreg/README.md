# Norway BRREG Company Data

This package owns Norway company/entity data from Bronnoysundregistrene's
Enhetsregisteret API. Financial-account data is intentionally separate in
`dagster_v3.defs.norway_brreg_financial`.

## Source API

The Dagster resource is `NorwayBrregApiResource` in `resources.py`.

Default configuration:

- `base_url`: `https://data.brreg.no/enhetsregisteret/api`
- `user_agent`: `corpscout-dagster-v3-dev/0.1`
- `timeout_seconds`: `120`
- `update_page_size`: `10000`

The resource uses a single `requests.Session` with the configured user agent.
Tests can inject a fake session through `NorwayBrregApiResource(session=...)`.

## Full Snapshot

Full company bootstrap uses:

```text
GET /enheter/lastned
GET /enheter/lastned/csv
```

In code this is `NorwayBrregApiResource.entries_snapshot()`. The method checks
the requested S3 object first. If it already exists, it returns without calling
BRREG. If it does not exist, it streams the gzip response body from BRREG
directly into the provided Dagster S3 resource at the requested bucket/key.

The endpoint returns a gzip-compressed JSON array. We stream the archive with
`ijson` instead of loading decoded JSON all at once. The parquet conversion
wraps each source entity as a uniform record:

```text
org_number
change_type = snapshot
source_change_type = snapshot
updated_at = null
update_id = null
entity_url
entity
raw_update = null
```

The CSV archive is retained separately because it contains `epostadresse`, which
is not exposed by the entity JSON endpoint. DuckDB reads the compressed CSV and
produces the complete contact and address snapshot without loading the archive
into Python memory.

Raw backup Dagster asset:

```text
norway_brreg_entries_snapshot_raw_s3
norway_brreg_entries_snapshot_csv_raw_s3
```

Raw backup object:

```text
bucket: source-norway-brreg
key: norway_brreg/entities/raw/snapshot/entities.json.gz
key: norway_brreg/entities/raw/snapshot/entities.csv.gz
```

Parquet Dagster asset:

```text
norway_brreg_entities_snapshot_s3
```

The asset reads the raw gzip JSON array object from S3, streams the entities
into normalized parquet, and writes the parquet object back to S3:

```text
bucket: source-norway-brreg
key: norway_brreg/entities/snapshot/entities.parquet
```

This full raw snapshot is a one-time/bootstrap style asset. If the raw object
already exists on S3, the raw asset reuses it and skips downloading from BRREG.
When the parquet asset materializes, it always regenerates and overwrites the
parquet object from the raw gzip.

## Daily Updates

Daily company updates use:

```text
GET /oppdateringer/enheter
```

For a Dagster partition `YYYY-MM-DD`, the request window is:

```text
dato=YYYY-MM-DDT00:00:00.000Z
updatedBefore=YYYY-MM-DDT23:59:59.999Z
size=10000
page=0
sort=id,ASC
```

In code this is `NorwayBrregApiResource.iter_updated_entities(start=..., end=...)`.

Dagster asset:

```text
norway_brreg_entity_updates_s3
```

The asset writes one parquet object per day:

```text
bucket: source-norway-brreg
key: norway_brreg/entities/updates/date=YYYY-MM-DD/entities.parquet
```

Daily update objects are partition outputs. They are not skipped if rerun,
because a rerun is expected to replace that day's partition output.

## Pagination And Large Windows

The BRREG update endpoint has a practical result-window limit of `10000`.

The resource starts with the requested window and page size. On page 0 it reads
`page.totalElements`. If the source reports more than `10000` items for the
window, the resource recursively splits the time window in half and reads the
two smaller windows instead.

This avoids requesting pages past the source window limit. If a window cannot
be split further without going below one millisecond, the resource raises.

Duplicate update rows are guarded by `oppdateringsid` when present. If that id
is missing, the fallback key is:

```text
organisasjonsnummer + dato + endringstype
```

## Entity Hydration

The update endpoint returns update metadata. For non-removal updates we hydrate
the current entity record through:

```text
GET /enheter/{organisasjonsnummer}
```

That hydrated entity is stored in the uniform update record under `entity`.

For removal updates (`Fjernet` or `Slettet`), no hydration is needed and the
record is emitted with:

```text
change_type = removed
entity = null
raw_update = original update payload
```

If hydration returns HTTP 410, the resource treats the company as removed and
emits the same tombstone shape.

## Normalized Parquet

Raw snapshot and update parquet records are normalized into the same table
shapes:

```text
no_companies
no_company_contacts
no_company_addresses
no_websites
no_industries
affected_orgs
removed_orgs
```

Snapshot normalized outputs:

```text
norway_brreg/entities/normalized/snapshot/no_companies.parquet
norway_brreg/entities/normalized/snapshot/no_company_contacts.parquet
norway_brreg/entities/normalized/snapshot/no_company_addresses.parquet
norway_brreg/entities/normalized/snapshot/no_websites.parquet
norway_brreg/entities/normalized/snapshot/no_industries.parquet
norway_brreg/entities/normalized/snapshot/affected_orgs.parquet
norway_brreg/entities/normalized/snapshot/removed_orgs.parquet
```

Daily update normalized outputs:

```text
norway_brreg/entities/normalized/updates/date=YYYY-MM-DD/no_companies.parquet
norway_brreg/entities/normalized/updates/date=YYYY-MM-DD/no_company_contacts.parquet
norway_brreg/entities/normalized/updates/date=YYYY-MM-DD/no_company_addresses.parquet
norway_brreg/entities/normalized/updates/date=YYYY-MM-DD/no_websites.parquet
norway_brreg/entities/normalized/updates/date=YYYY-MM-DD/no_industries.parquet
norway_brreg/entities/normalized/updates/date=YYYY-MM-DD/affected_orgs.parquet
norway_brreg/entities/normalized/updates/date=YYYY-MM-DD/removed_orgs.parquet
```

The normalized company tables are then published to ClickHouse:

```text
corpscout.no_companies
corpscout.no_company_contacts
corpscout.no_company_addresses
corpscout.no_websites
corpscout.no_industries
```

The full snapshot path replaces those tables. The daily update path deletes
affected org numbers from ClickHouse and inserts replacement rows from the
partition parquet files.

`no_company_contacts` stores one row per nonblank `hjemmeside`, `epostadresse`,
`telefon`, or `mobil`. `no_company_addresses` stores separate `business`
(`forretningsadresse`) and `postal` (`postadresse`) rows with address lines,
postal town/code, municipality name/code, and country name/code. Daily JSON
updates preserve bulk-only email rows unless the company is removed or the
update source supplies a replacement email.

## Dagster Jobs

Manual full snapshot job:

```text
norway_brreg_entities_full_snapshot_job
```

This job pulls/reuses the full company snapshot, normalizes it, publishes the
company tables to ClickHouse, and runs the translation loader
(`norway_brreg_translation_load`), which enqueues untranslated text to the Go
translator service and waits until translated output is flushed. Queue failures
or a processing timeout fail the Dagster job.

Daily update job:

```text
norway_brreg_entity_updates_job
```

This job pulls one daily update partition, normalizes it, and applies it to
ClickHouse.

Daily schedule:

```text
norway_brreg_entity_updates_schedule
```

The schedule is built from the partitioned update job.

## Important Boundaries

- `norway_brreg` is company/entity data only.
- `NorwayBrregApiResource` does not call the BRREG financial-account endpoint.
- Norway financial assets, raw financial fetch storage, parsing, FX conversion,
  and ClickHouse financial statement publishing live in `norway_brreg_financial`.
