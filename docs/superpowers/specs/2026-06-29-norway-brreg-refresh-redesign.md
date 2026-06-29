# Norway Brreg Refresh Redesign

## Goal

Refactor the Norway Brreg Dagster source so external IO is explicit, repeatable,
and cheap to refresh. The source should download raw Brreg data through a
source-specific API resource, persist raw or parquet artifacts through native S3
access in assets, compute diffs before downstream processing, and avoid
refetching financial accounts that are already known for the same company and
filing year.

The first implementation should keep the existing DuckDB and dbt resolved flow
where it reduces migration risk. Moving all staging to parquet can happen after
the source boundaries are clean.

## Current Issues

`norway_brreg_entities_duckdb` currently downloads the entity archive directly
inside the dlt resource and writes to DuckDB. The source boundary, persistence
boundary, and staging table are coupled.

`norway_brreg_financial_fetches_duckdb` currently reads companies from DuckDB,
calls `https://data.brreg.no/regnskapsregisteret/regnskap/{org_number}`, and
stores fetch outcomes in DuckDB. This mixes API calls, caching, and staging.

The financial candidate filter requires a non-empty website. Website presence
has no accounting meaning, so this excludes companies that may have financial
accounts. The normal filter should keep active companies with a non-empty
`sisteInnsendteAarsregnskap` signal, but should not require a website.

`norway_resolved` is a transformation layer over Norway Brreg data, not an
independent source. Keeping it in a separate Dagster package makes the Norway
graph harder to reason about.

## Brreg Source Model

The full entity archive endpoint is a bootstrap and full-refresh source:

```text
GET /enhetsregisteret/api/enheter/lastned
```

Use it for initial load, storage recovery, schema rebuilds, and explicit audits.
It should not be the normal daily refresh path.

The normal entity refresh source is the Brreg update feed:

```text
GET /enhetsregisteret/api/oppdateringer/enheter
```

Use `dato` and `updatedBefore` to build deterministic daily partitions. The
`includeChanges` parameter adds the `endringer` field, which describes the
reason an update was published. Treat this as optional optimization metadata,
not as the canonical entity state.

The canonical current entity record should still come from:

```text
GET /enhetsregisteret/api/enheter/{org_number}
```

Financial account data remains per company:

```text
GET /regnskapsregisteret/regnskap/{org_number}
GET /regnskapsregisteret/regnskap/{org_number}?år={year}
```

There is no all-companies financial delta endpoint in the current source model.
Financial refresh should therefore be driven by entity updates and stored
financial keys.

## Resources

Create one source-specific configurable API resource:

```text
NorwayBrregApiResource
  download_entities_snapshot() -> bytes
  iter_entity_updates(start, end) -> Iterator[dict]
  get_entity(org_number) -> dict
  get_financial_accounts(org_number, year=None, account_type=None) -> list[dict]
```

The API resource only talks to Brreg. It must not write to S3, DuckDB, or
ClickHouse.

Use native S3 support from the existing Dagster/AWS stack for object storage.
Do not add a `NorwayBrregStorageResource` unless S3 path and manifest behavior
becomes complex enough to justify a real abstraction.

Use source-specific path helper functions for deterministic S3 keys, for
example:

```text
norway_brreg/entities/snapshot/run_id={run_id}/entities.json.gz
norway_brreg/entities/updates/date={partition_date}/updates.parquet
norway_brreg/entities/current/date={partition_date}/entities.parquet
norway_brreg/financial_accounts/org_prefix={prefix}/org_number={org_number}/year={year}/accounts.parquet
```

## Asset Flow

Bootstrap flow:

```text
norway_brreg_entities_snapshot_s3
  -> norway_brreg_entities_current
  -> downstream resolved/company assets
```

Daily flow:

```text
norway_brreg_entity_updates_parquet
  -> norway_brreg_changed_entities_parquet
  -> norway_brreg_financial_fetch_plan
  -> norway_brreg_financial_accounts_parquet
  -> norway_brreg_financial_statements_duckdb
  -> norway_brreg_resolved_dbt
  -> norway_brreg_clickhouse
```

The asset performs persistence:

```text
asset calls NorwayBrregApiResource
asset writes S3 object through native S3 resource
asset returns row counts, object keys, and hashes as metadata
```

This keeps side effects visible in Dagster materializations and keeps resources
focused on external system boundaries.

## Entity Refresh

The daily update asset should fetch update events for its partition window and
write the events as parquet.

The changed-entities asset should extract changed organization numbers, call
`get_entity(org_number)` for current state, and write only changed current
entity rows after comparing source payload hashes with the previous current
state.

If Brreg marks an entity as removed from Open Data, the current entity layer
should emit a deletion/tombstone record or update lifecycle status according to
the response semantics. It should not silently keep stale active data.

## Financial Refresh

Normal financial candidates are current entities where:

```text
is_active = true
last_submitted_accounts_year is not null
```

Do not require `website`.

The fetch plan should derive a key:

```text
(org_number, last_submitted_accounts_year)
```

If that key already exists in financial-account parquet with a stored payload
hash, skip the Brreg financial call in normal daily refresh.

If the key is missing, call Regnskapsregisteret for that company and year, store
the response as parquet, and include the payload hash. Empty arrays and 404-like
no-account responses should be represented explicitly so they are not retried
forever in normal refresh.

If a new fetch produces the same payload hash as the stored record, downstream
processing can skip the company. If the hash differs, downstream financial
normalization should process that company and replace affected financial rows.

## Correction Handling

`sisteInnsendteAarsregnskap` is a cheap trigger, but it may not change when a
past filing is corrected for the same year. Add a separate maintenance job after
the first redesign:

```text
norway_brreg_financial_recent_year_recheck_job
```

That job should recheck the latest one or two filing years for companies with
financial accounts, compare payload hashes, and only pass changed rows
downstream. This keeps daily refresh cheap while still catching source
corrections.

## DuckDB And dbt

Keep the existing one-file Norway DuckDB and dbt resolved models in the first
implementation. This limits the blast radius while source IO and refresh logic
are cleaned up.

DuckDB should become a staging/query engine over parquet or an intermediate
resolved build target, not the source cache. The durable source state should be
S3 objects with deterministic keys and payload hashes.

Move the `norway_resolved` assets, dbt project, and ClickHouse export into the
`norway_brreg` package because they are source-specific transformations over
Brreg data.

Suggested package shape:

```text
defs/norway_brreg/
  resources.py
  paths.py
  assets/
    entities.py
    entity_updates.py
    financial_accounts.py
    financial_statements.py
    resolved.py
    clickhouse.py
    jobs.py
  dbt/
    models/
      no_companies.sql
      no_websites.sql
      no_industries.sql
      no_financial_statements.sql
```

## Non-Goals

Do not migrate all Norway staging from DuckDB to parquet in the first pass.

Do not add a custom storage resource before native S3 usage proves insufficient.

Do not remove the `last_submitted_accounts_year` financial candidate filter from
normal refresh. A broader active-company probe can be added as an explicit audit
or historical job later.

Do not add PDF annual-account download/parsing in this redesign. The first
financial improvement should use the existing JSON account endpoint and better
refresh semantics.

## Testing

Focused tests should cover:

- `NorwayBrregApiResource` builds the expected Brreg URLs and accepts injectable
  HTTP sessions;
- entity snapshot asset writes an S3 object and returns object metadata;
- entity update asset writes a daily parquet partition;
- changed-entity logic fetches current state for updated organization numbers
  and emits only hash changes;
- financial candidate filter removes the website requirement but keeps active
  and `last_submitted_accounts_year`;
- financial fetch plan skips existing `(org_number, year)` keys;
- empty or no-account financial results are persisted as terminal outcomes for
  normal refresh;
- changed financial payload hashes trigger downstream normalization;
- `norway_resolved` assets are registered under the `norway_brreg` package and
  keep the existing ClickHouse output contract.

## Rollout

Implement in small steps:

1. Remove the website filter from financial candidates and keep existing DuckDB
   flow working.
2. Add `NorwayBrregApiResource` and move Brreg HTTP calls into it.
3. Add S3/parquet assets for entity snapshot and daily entity updates.
4. Add changed-entity and financial fetch-plan assets.
5. Write financial account parquet before normalization.
6. Move `norway_resolved` into `norway_brreg` without changing ClickHouse table
   contracts.
7. Add the recent-year recheck job after the normal refresh path is stable.
