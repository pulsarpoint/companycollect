# Finland PRH YTJ Normalized ClickHouse Design

## Purpose

Finland PRH YTJ should become the reference pattern for source-owned
ClickHouse ingestion in Corpscout. The source package should parse PRH raw
records into typed Go structures, normalize every known source field into
source-specific ClickHouse tables, and import those tables in batches.

ClickHouse should not contain a raw JSON table or a generic company summary
table for this source. The downloaded `source.ndjson` file and `manifest.json`
remain the replay/audit boundary for original source payloads.

## Decisions

- Remove the current `fi_prhytj_raw_records` table from active ClickHouse schema.
- Remove the current `fi_prhytj_companies` table from active ClickHouse schema.
- Stop importing those two tables from `finland/prhytj` code.
- Keep original JSON only in the source run folder, not duplicated in ClickHouse.
- Add a replacement migration that creates only normalized PRH YTJ tables.
- Keep manual ClickHouse SQL migrations as the schema authority.
- Keep Go row definitions, column lists, and ClickHouse type definitions
  source-specific and tested against the migration schema.
- Use one `NormalizedEntry` per parsed source record so all rows derived from
  that source record stay grouped until the insert boundary.
- Build the importer around batches of `[]NormalizedEntry` so Temporal and CLI
  paths can call the same Go API.
- Use native Go ClickHouse insertion through `clickhouse-go/v2`; do not keep the
  old Docker/clickhouse-local importer as an active ingestion path.
- Delete stale pilot config/tooling that targets the old tables:
  `corpscout/clickhouse/sources/finland_prhytj.yaml` and
  `corpscout/clickhouse/tools/chimport`.

Because `000002_create_finland_prhytj_tables` has already been applied to the
remote ClickHouse server, implementation should add a new migration rather than
editing applied history. The new migration should drop old PRH tables and create
the replacement normalized schema.

## Source Input Model

The existing PRH input structs remain the typed source format:

- `CompanyRecord`
- `Identifier`
- `Name`
- `BusinessLine`
- `Website`
- `CompanyForm`
- `CompanySituation`
- `RegisteredEntry`
- `Address`
- `PostOffice`
- `Description`

The parser still reads `source.ndjson` line by line and computes:

- source run ID from the run directory
- source line number from the NDJSON line
- source payload hash from the raw line bytes

The payload hash is lineage metadata only. The raw payload bytes should not be
inserted into ClickHouse.

## Common Columns

Every normalized table should carry enough lineage to trace a row back to the
downloaded source file:

```text
country_iso2          String / Nullable(String), always FI
source_slug           String / Nullable(String), always prhytj
source_run_id         String
source_record_id      String, PRH business ID
business_id           String, PRH business ID
source_line_number    Int64
source_payload_hash   String
source_item_hash      String where the row represents a nested/repeated item
source_position       Int32 where source order matters
source_export_id      UUID
ingested_at           DateTime64(3, 'UTC')
```

`source_item_hash` should be deterministic from stable source values:

```text
source_run_id + business_id + section name + source_position + key source fields
```

This gives ClickHouse replacing keys stable identities without needing a raw
record table.

## Normalized Tables

The replacement Finland PRH schema should contain these tables.

### `fi_prhytj_identifiers`

Stores the top-level business ID, optional EUID, and entries from
`identifiers[]`.

Columns:

```text
common lineage columns
identifier_scope       String, one of business_id, euid, identifier
identifier_type        String
identifier_value       String
registered_on          String
ended_on               String
source                 String
is_primary_business_id Bool
```

### `fi_prhytj_statuses`

Stores top-level lifecycle and registry status fields. This replaces the status
parts that were previously embedded in `fi_prhytj_companies`.

Columns:

```text
common lineage columns without source_item_hash/source_position
trade_register_status  String
status                 String
registration_date      String
end_date               String
last_modified          String
lifecycle_status       String
is_active              Bool
```

### `fi_prhytj_names`

Stores each item in `names[]`.

Columns:

```text
common lineage columns
name                   String
name_type_code         String
version                Int32
registered_on          String
ended_on               String
is_current             Bool
is_primary             Bool
```

### `fi_prhytj_business_lines`

Stores the top-level `mainBusinessLine` object.

Columns:

```text
common lineage columns without source_position
business_line_type       String
business_line_code_set   String
registered_on            String
source                   String
is_primary               Bool
```

### `fi_prhytj_business_line_descriptions`

Stores every `mainBusinessLine.descriptions[]` entry without collapsing
languages into fixed columns.

Columns:

```text
common lineage columns
business_line_item_hash String
language_code           String
description             String
```

### `fi_prhytj_websites`

Stores the top-level website object.

Columns:

```text
common lineage columns
url                    String
normalized_url         String
host                   String
path                   String
registered_on          String
ended_on               String
is_current             Bool
is_primary             Bool
```

### `fi_prhytj_company_forms`

Stores each item in `companyForms[]`.

Columns:

```text
common lineage columns
form_type_code         String
version                Int32
registered_on          String
ended_on               String
source                 String
is_current             Bool
```

### `fi_prhytj_company_form_descriptions`

Stores every `companyForms[].descriptions[]` entry.

Columns:

```text
common lineage columns
company_form_item_hash String
language_code          String
description            String
```

### `fi_prhytj_company_situations`

Stores each item in `companySituations[]`.

Columns:

```text
common lineage columns
situation_type_code    String
registered_on          String
ended_on               String
is_current             Bool
```

### `fi_prhytj_company_situation_descriptions`

Stores every `companySituations[].descriptions[]` entry.

Columns:

```text
common lineage columns
company_situation_item_hash String
language_code               String
description                 String
```

### `fi_prhytj_registered_entries`

Stores each item in `registeredEntries[]`.

Columns:

```text
common lineage columns
entry_type_code        String
register_code          String
authority              String
registered_on          String
ended_on               String
is_current             Bool
```

### `fi_prhytj_registered_entry_descriptions`

Stores every `registeredEntries[].descriptions[]` entry.

Columns:

```text
common lineage columns
registered_entry_item_hash String
language_code              String
description                String
```

### `fi_prhytj_addresses`

Stores each item in `addresses[]`.

Columns:

```text
common lineage columns
address_type_code      Int32
street                 String
post_code              String
building_number        String
entrance               String
apartment_number       String
post_office_box        String
co                     String
country                String
registered_on          String
source                 String
```

### `fi_prhytj_address_post_offices`

Stores every `addresses[].postOffices[]` entry.

Columns:

```text
common lineage columns
address_item_hash      String
language_code          String
city                   String
municipality_code      String
```

## Migration Shape

Add a new migration after the current highest ClickHouse migration, for example:

```text
000004_replace_finland_prhytj_normalized_tables.up.sql
000004_replace_finland_prhytj_normalized_tables.down.sql
```

The `up` migration should:

1. Drop old PRH tables, including `fi_prhytj_raw_records` and
   `fi_prhytj_companies`.
2. Drop old PRH side tables whose shape no longer matches the normalized
   replacement.
3. Create the normalized table set above.

The `down` migration should:

1. Drop the new normalized table set.
2. Recreate the prior PRH tables only if we need reversible local development.
   Remote production rollback should be treated carefully because old raw/company
   data is intentionally not part of the new model.

Implementation must not rewrite already-applied migration files unless we
explicitly reset ClickHouse in a development environment.

## Go Package Shape

The Finland PRH package should move from two `map[string]any` row builders to
typed normalized row builders:

```text
types.go          source API payload structs
parser.go         streaming NDJSON parser
rows.go           typed ClickHouse row structs and table column lists
normalize.go      CompanyRecord -> normalized table row groups
import.go         batch import orchestration
```

The main normalization API should preserve source-record boundaries:

```go
type NormalizedEntry struct {
    Identifiers                  []IdentifierRow
    Status                       *StatusRow
    Names                        []NameRow
    BusinessLine                 *BusinessLineRow
    BusinessLineDescriptions     []BusinessLineDescriptionRow
    Website                      *WebsiteRow
    CompanyForms                 []CompanyFormRow
    CompanyFormDescriptions      []CompanyFormDescriptionRow
    CompanySituations            []CompanySituationRow
    CompanySituationDescriptions []CompanySituationDescriptionRow
    RegisteredEntries            []RegisteredEntryRow
    RegisteredEntryDescriptions  []RegisteredEntryDescriptionRow
    Addresses                    []AddressRow
    AddressPostOffices           []AddressPostOfficeRow
}

func NormalizeParsedRecord(run RunContext, record ParsedRecord) NormalizedEntry
```

`ParsedRecord` should contain the typed PRH record plus line-level lineage:

```go
type ParsedRecord struct {
    LineNumber  int64
    PayloadHash string
    Record      CompanyRecord
}
```

The importer should stream `source.ndjson`, normalize each parsed record into one
`NormalizedEntry`, collect up to `BatchSize` entries, flatten entries to table
row arrays only inside the insert function, and insert each non-empty table batch
into ClickHouse.

## Insert Contract

The ClickHouse writer should use native `clickhouse-go/v2` batches. Each
normalized table should expose explicit columns and matching ClickHouse type
metadata from the source package:

```go
var IdentifierColumns = []string{...}
var IdentifierColumnTypes = map[string]string{...}
func (r IdentifierRow) ClickHouseRow() map[string]any
```

Tests should assert:

- column lists and ClickHouse type definitions match the corresponding migration
  table columns
- every non-empty PRH nested structure produces at least one row
- all description arrays preserve every language entry
- no importer references `fi_prhytj_raw_records`
- no importer references `fi_prhytj_companies`
- `source_payload_hash` and `source_line_number` are present on every row
- no `NormalizedBatch` shape is introduced between normalization and import

## Import Result

The import result should report all tables written:

```json
{
  "ImportedTables": [
    "fi_prhytj_identifiers",
    "fi_prhytj_statuses",
    "fi_prhytj_names",
    "fi_prhytj_business_lines",
    "fi_prhytj_business_line_descriptions",
    "fi_prhytj_websites",
    "fi_prhytj_company_forms",
    "fi_prhytj_company_form_descriptions",
    "fi_prhytj_company_situations",
    "fi_prhytj_company_situation_descriptions",
    "fi_prhytj_registered_entries",
    "fi_prhytj_registered_entry_descriptions",
    "fi_prhytj_addresses",
    "fi_prhytj_address_post_offices"
  ],
  "ImportedRows": 100
}
```

`ImportedRows` should remain the number of source records processed, not the sum
of normalized child rows.

## What This Does Not Build Yet

This design does not build a central company projection, brand graph, or
cross-source merge table. Those should be derived later from normalized
source-specific ClickHouse tables after more sources are loaded.

This design also does not require a graph database. Parent/child company and
brand relationships can remain Postgres relationships until there is a proven
query that ClickHouse/Postgres cannot support.

## Verification

Implementation should verify:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/companysources/finland/prhytj ./internal/clickhouse ./cmd/corpscout-source -count=1

cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout
make -n clickhouse-migrate-up
make source-import-run SOURCE_LIMIT=100 SOURCE_BATCH_SIZE=50
```

After the limited import, remote ClickHouse should show rows in normalized PRH
tables and no active `fi_prhytj_raw_records` or `fi_prhytj_companies` tables.
