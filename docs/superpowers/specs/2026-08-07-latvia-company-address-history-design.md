# Latvia Company Address History Design Proposal

**Status:** Implemented through expand/read cutover; physical-column cleanup intentionally deferred

## Decision

Store every Latvian company address state in one authoritative ClickHouse table,
`corpscout.lv_company_addresses`. Do not keep authoritative address values in
`corpscout.lv_companies`.

`lv_companies` remains one row per company and contains company facts. Current-address
reads are provided by views:

- `lv_company_addresses_current`: the latest observed address state per `regcode`.
- `lv_companies_current`: `lv_companies` joined to `lv_company_addresses_current` for
  consumers that want the existing company-plus-current-address shape.

The views do not duplicate stored data. They keep address history and company identity at
their natural grains while allowing simple current-company queries.

## Why This Model

The current Latvia pipeline replaces both the DuckDB `latvia_ur.entities` table and
ClickHouse `corpscout.lv_companies` on every run. Consequently, an address disappears as
soon as the next source snapshot contains a different address.

Making `lv_companies` itself append-only would preserve the address, but it would also
duplicate every company attribute and change the table from one row per company to one row
per company version. Counts, joins, translation, classification, procurement matching, and
company-page queries would then all need latest-row logic.

An address table represents the actual one-to-many relationship directly:

```text
lv_companies             1 row per company
lv_company_addresses     1 row per observed address-data change
```

Only a change is appended. An unchanged daily snapshot does not create another row.

## Timestamp Meaning

Use `observed_at`, not `inserted_at` or `valid_from`.

`register.csv` tells us the address present in the downloaded snapshot. It does not tell us
the legal date on which that address became effective. Therefore:

- `observed_at` means when our pipeline first observed this address state;
- it must be stamped once in UTC for the entire source run, not once per insert batch;
- it must not be presented as the company's legal move date;
- the next observation can be used as an inferred upper bound, not a proven `valid_to` date.

## Proposed ClickHouse Schema

The exact migration number should be allocated when implementation begins. At the time of
this proposal, `000254` is the next available number.

```sql
CREATE TABLE corpscout.lv_company_addresses
(
    country_iso2 LowCardinality(String),
    source_slug LowCardinality(String),
    source_run_id String,
    source_url String,

    regcode String,
    address String,
    postal_code String,
    address_id String,
    region_code LowCardinality(String),
    city_code LowCardinality(String),
    atvk_code LowCardinality(String),

    vzd_address_text Nullable(String),
    vzd_address_postal_code Nullable(String),
    vzd_address_status LowCardinality(Nullable(String)),
    address_city_name Nullable(String),
    address_municipality_name Nullable(String),
    address_latitude Nullable(Float64),
    address_longitude Nullable(Float64),

    has_address UInt8,
    address_fingerprint FixedString(64),
    observation_fingerprint FixedString(64),
    observed_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(observed_at)
PARTITION BY toYear(observed_at)
ORDER BY (regcode, observed_at, observation_fingerprint);
```

`address_fingerprint` is a stable SHA-256 over the normalized UR-owned fields:

```text
address, postal_code, address_id, region_code, city_code, atvk_code
```

`observation_fingerprint` covers both the UR fields and the VZD enrichment fields. This
allows a corrected city name or coordinate to become the current observation without
incorrectly treating it as a company move. Consecutive rows with the same
`address_fingerprint` are enrichment-only changes.

An all-empty address is still a valid state with `has_address = 0`. Recording it prevents a
removed address from leaving an older address incorrectly marked as current.

Do not add stored `is_current` or `observed_to` columns. Both would require mutating older
rows and could disagree with the append-only event sequence.

## Current Views

`lv_company_addresses_current` selects the complete latest tuple, rather than independently
selecting each field:

```sql
CREATE VIEW corpscout.lv_company_addresses_current AS
SELECT
    regcode,
    current.1 AS address,
    current.2 AS postal_code,
    -- remaining address fields
    current.15 AS address_fingerprint,
    current.16 AS observation_fingerprint,
    current.17 AS observed_at
FROM
(
    SELECT
        regcode,
        argMax(
            tuple(
                address,
                postal_code,
                address_id,
                region_code,
                city_code,
                atvk_code,
                vzd_address_text,
                vzd_address_postal_code,
                vzd_address_status,
                address_city_name,
                address_municipality_name,
                address_latitude,
                address_longitude,
                has_address,
                address_fingerprint,
                observation_fingerprint,
                observed_at
            ),
            tuple(observed_at, source_run_id)
        ) AS current
    FROM corpscout.lv_company_addresses
    GROUP BY regcode
);
```

The implementation should use named tuple fields or explicit aliases if supported by the
deployed ClickHouse version; the numbered tuple members above only illustrate the selection
rule.

`lv_companies_current` joins the current address view to `lv_companies` and exposes the
legacy address column names. This is the serving relation for the backoffice and ad hoc
queries that need the current address.

## Dagster Data Flow

Keep `register.csv` ingestion as a full replacement in DuckDB. It is the current source
snapshot, not the history store.

During the existing Latvia register job:

1. Load `register.csv` and the VZD reference CSVs into DuckDB as today.
2. Build a company projection without address fields.
3. Build an address-candidate projection with normalized fields, both fingerprints,
   `context.run_id`, and one run-level `observed_at` value.
4. Load address candidates into a temporary ClickHouse staging table.
5. Append candidates whose `observation_fingerprint` differs from the corresponding row in
   `lv_company_addresses_current`, or whose company has no address observation yet.
6. Replace `corpscout.lv_companies` from the company projection.
7. Drop temporary staging tables and report materialization metadata.

This produces approximately one initial row per Latvian company and thereafter only actual
address-data changes. It avoids roughly 485,000 duplicate observations on every daily run.

The existing `latvia_ur_clickhouse_companies` operation should become a non-subsettable
Dagster multi-asset with these retained/new asset keys:

- `latvia_ur_clickhouse_companies`
- `latvia_ur_clickhouse_company_addresses`

One operation is appropriate because both outputs come from the same source snapshot and
share the ClickHouse publication boundary. Downstream dependencies on the existing company
asset key can remain unchanged.

ClickHouse cannot transactionally combine a full table exchange with an append into another
table. Publish address changes first and company facts second. This protects the initial cutover:
the legacy current address is not cleared until its authoritative history row exists. If the
company exchange then fails, the current view can temporarily show the newer address beside the
older company facts; the run fails and a rerun repairs the company snapshot.

## Compatibility And Rollout

Use an expand/read/contract rollout so existing readers are not broken.

### Release 1: Expand and seed

- Create `lv_company_addresses`, `lv_company_addresses_current`, and
  `lv_companies_current`.
- Add the history writer to the Latvia register job.
- Keep fallback reads from the legacy address columns until the first history materialization.
- The first successful run seeds the current state for every company, then stops populating the
  legacy physical address columns.
- Compare legacy current values with `lv_company_addresses_current` during cutover.

Temporary duplication is intentional only during rollout and provides a safe comparison and
rollback window.

### Release 2: Move readers (implemented with Release 1)

- Change the Latvia backoffice `companiesTable` to `lv_companies_current`.
- Change its detail and address queries to read the current-address view.
- Recreate `lv_companies_translated` over `lv_companies_current` if detail records must retain
  the legacy current-address shape.
- Keep procurement, signals, translation input, and classification readers on physical
  `lv_companies`; they only need company facts and `regcode`.
- Run at least one complete scheduled refresh and validate parity.

### Release 3: Contract

- Confirm the UR and VZD address fields remain absent from `LV_COMPANIES_EXPORT_COLUMNS`.
- Drop the physical address columns from `corpscout.lv_companies`.
- Recreate dependent views with explicit column lists.
- Remove temporary parity checks against the legacy columns.

After Release 3, address values exist physically only in `lv_company_addresses`.

## Backfill Boundary

The first seed records the address visible on the deployment date. It does not reconstruct
earlier history. The open `register.csv` contains only the current address, and the current
pipeline deletes its temporary download and replaces its DuckDB table.

A separate, optional backfill can inspect dated ClickHouse backups or preserved Dagster
artifacts. For every usable dated snapshot:

1. Restore it to an isolated database.
2. Extract the address fields with the backup timestamp as `observed_at`.
3. Process snapshots oldest to newest.
4. Append only fingerprint changes.
5. Label the timestamp as backup observation time, never legal effective time.

Without such a snapshot, this change cannot recover the former `Palangas iela 22` address
for `42103035600`. It will make future changes locally discoverable and auditable.

## Query Examples

Current address:

```sql
SELECT *
FROM corpscout.lv_company_addresses_current
WHERE regcode = '42103035600';
```

Observed history:

```sql
SELECT
    observed_at,
    address,
    postal_code,
    address_city_name,
    address_fingerprint
FROM corpscout.lv_company_addresses
WHERE regcode = '42103035600'
ORDER BY observed_at;
```

Find companies ever observed at an address:

```sql
SELECT
    a.regcode,
    any(c.legal_name) AS legal_name,
    a.address,
    min(a.observed_at) AS first_observed_at,
    max(a.observed_at) AS last_observed_at
FROM corpscout.lv_company_addresses AS a
INNER JOIN corpscout.lv_companies AS c USING (regcode)
WHERE positionCaseInsensitiveUTF8(a.address, 'Palangas iela 22') > 0
GROUP BY a.regcode, a.address
ORDER BY first_observed_at;
```

## Materialization Metadata And Checks

Report at least:

- company rows replaced;
- address candidates observed;
- address observations inserted;
- first observations for new companies;
- source-address changes;
- enrichment-only changes;
- current empty-address states;
- the run-level `observed_at` value.

Add checks that:

- `lv_companies` remains unique by `regcode`;
- `lv_company_addresses_current` remains unique by `regcode`;
- every company has one current address state, including an explicit empty state;
- after a successful run, candidate and current `observation_fingerprint` values match;
- an unexpectedly large change count is visible in metadata before a hard threshold is
  introduced from real operating data.

## Required Tests

- The initial run inserts one address state per company.
- An unchanged rerun inserts zero rows.
- A changed address inserts exactly one row.
- An `A -> B -> A` sequence retains all three observations.
- Clearing an address creates an explicit empty current state.
- A VZD-only correction keeps the same `address_fingerprint` but changes the
  `observation_fingerprint`.
- A retry after partial insertion does not duplicate already-current observations.
- The current view returns all fields from one latest tuple.
- Backoffice list, detail, and address queries compile against the new views.
- The Latvia register job contains both ClickHouse asset outputs.

Verification commands:

```bash
cd corpscout/services/dagster_v3
uv run pytest tests/test_latvia_ur_resources.py tests/test_latvia_ur_tables.py \
  tests/test_latvia_ur_assets.py tests/test_latvia_ur_address_history.py -q
uv run dg check defs

cd ../backoffice
npm test -- countries.test.ts queries.server.test.ts
```

## Expected File Changes

- `corpscout/clickhouse/migrations/000254_corpscout_lv_company_addresses.*.sql`
- a later contract migration that removes address columns from `lv_companies`
- `corpscout/services/dagster_v3/src/dagster_v3/defs/latvia_ur/tables.py`
- `corpscout/services/dagster_v3/src/dagster_v3/defs/latvia_ur/clickhouse.py`
- `corpscout/services/dagster_v3/src/dagster_v3/defs/latvia_ur/assets.py`
- `corpscout/services/dagster_v3/src/dagster_v3/defs/latvia_ur/docs/latvia_ur-design.md`
- `corpscout/services/dagster_v3/tests/test_latvia_ur_address_history.py`
- existing Latvia table, asset, and backoffice query tests
- `corpscout/services/backoffice/app/lib/countries.ts`

## Non-Goals

- Inferring a legal move date from the first local observation.
- Creating a generic cross-country address-history framework.
- Appending an unchanged row for every company every day.
- Importing authenticated Latvian UR history data.
- Changing the existing current-company key or downstream company joins.

## Acceptance Criteria

The implementation is accepted when:

1. Address data in the ClickHouse serving layer is physically stored only in
   `lv_company_addresses` after contract rollout; DuckDB may retain the current source fields
   as staging input.
2. `lv_companies` still has exactly one row per `regcode`.
3. Current company and address pages remain query-compatible through views.
4. Re-running an unchanged source snapshot adds no history rows.
5. Every future address-data change remains queryable with its first local observation time.
6. Documentation explicitly distinguishes observation time from legal effective time.
