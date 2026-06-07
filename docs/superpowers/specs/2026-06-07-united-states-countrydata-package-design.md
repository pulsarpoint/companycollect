# United States Countrydata Package Design

## Summary

Create a standalone United States countrydata package under:

```text
companies/united_states
```

The package follows the Finland countrydata architecture: one country-owned Go
module, one country CLI, one package per source, source parquet exports, final
country parquet exports, manifests, and runtime data under
`companies/data/united_states/countrydata`.

The USA package will include three ready sources, implemented one at a time:

1. `secedgar` from `sec_edgar`
2. `irseobmf` from `irs_eo_bmf`
3. `coloradoentities` from `colorado_business_entities`

The first implementation phase is `secedgar`. It establishes the USA module,
CLI, source status/export layout, and a final export that can work with whichever
ready source manifests exist. Later phases add IRS EO BMF and Colorado Business
Entities without changing the country-level architecture.

## Inputs

The implementation must use the existing upstream analysis artifacts:

```text
companies/analysis/united_states/source_inventory.json
companies/analysis/united_states/data_model/company_data_analysis.md
companies/analysis/united_states/data_model/country_company_profile.schema.json
companies/analysis/united_states/data_model/country_company_profile.example.json
companies/analysis/united_states/data_model/country_company_profile_mapping.md
companies/analysis/united_states/data_model/common_field_mapping_suggestions.md
companies/analysis/united_states/data_model/sources/sec_edgar/*
companies/analysis/united_states/data_model/sources/irs_eo_bmf/*
companies/analysis/united_states/data_model/sources/colorado_business_entities/*
```

Blocked/planning-only sources are not implemented in this package phase:

- `sam_gov_entity`: blocked by required API key and unverified response shape.
- `opencorporates`: blocked by license/commercial-use uncertainty.
- `state_sos_registries`: not one source; each state must become its own source.

## Architecture

Repository layout:

```text
companies/united_states/
  go.mod
  go.sum
  README.md
  paths.go
  status.go
  types.go
  export.go
  cmd/
    united-states-countrydata/
      main.go
  secedgar/
  irseobmf/
  coloradoentities/
```

Runtime layout:

```text
companies/data/united_states/countrydata/
  sources/
    secedgar/
      snapshots/
      exports/{run_id}/
    irseobmf/
      snapshots/
      exports/{run_id}/
    coloradoentities/
      snapshots/
      exports/{run_id}/
  final/
    exports/{run_id}/
```

The package must not import `corpscout`, `scheduler`, sqlc, or database types.
Shared source-agnostic behavior comes from:

```text
github.com/pulsarpoint/companycollect/companies/common/countryimport
```

## CLI Contract

The country binary is:

```text
united-states-countrydata
```

Commands:

```text
sync-source --source secedgar
sync-source --source irseobmf
sync-source --source coloradoentities
status-source --source <source>
export-source --source <source>
status
build-export
sync --source <source> --build-export
```

Common flags:

```text
--env <path>
--data-dir <path>
--source <source>
--snapshot-path <path>
--run-id <id>
--max-pages <n>
--chunk-size <n>
--build-export
```

Default country data root:

```text
../data/united_states/countrydata
```

Every command writes a JSON result map to stdout and logs operation-level errors
once with `log/slog` at the CLI boundary.

## Source API

Each source package exposes the same concrete API shape:

```go
func NewSource(cfg Config) *Source
func ConfigFromEnv() Config
func (s *Source) Download(ctx context.Context, opts countryimport.DownloadOptions) (countryimport.DownloadResult, error)
func (s *Source) Process(ctx context.Context, opts countryimport.ProcessOptions) (countryimport.ProcessResult, error)
func (s *Source) Store(ctx context.Context, records []SourceRecord) (countryimport.StoreResult, error)
func (s *Source) Export(ctx context.Context, opts ExportOptions) (ExportResult, error)
```

Each source owns source-specific config, state, parsing, fixtures, live tests,
export rows, parquet writer tests, and README notes.

## Phase 1: SEC EDGAR

Source package:

```text
companies/united_states/secedgar
```

Input source:

```text
https://www.sec.gov/files/company_tickers.json
```

Important source rules:

- Access is public, but SEC requires a descriptive `User-Agent`.
- Default `SEC_EDGAR_USER_AGENT` must be configurable through env.
- The top-level JSON object is keyed by integer-like strings. The implementation
  must iterate values, not decode as an array.
- Each record contains `cik_str`, `ticker`, and `title`.
- CIK must be preserved and also exportable as a zero-padded 10 digit string.

Source snapshot:

- Preserve the downloaded JSON file as the raw snapshot.
- Compute SHA-256 and byte size.
- Support local/fixture download tests with `httptest.Server`.

Source export:

Expected source parquet tables:

```text
companies.parquet
identifiers.parquet
tickers.parquet
source_evidence.parquet
```

Final export contribution:

- One company per CIK.
- `primary_id = "CIK:{zero_padded_cik}"`.
- `legal_name = title`.
- `identifiers.cik = zero_padded_cik`.
- `identifiers.ticker = ticker`.
- `entity_classification.is_public_company = true`.
- Source evidence points to `sec_edgar`.

## Phase 2: IRS EO BMF

Source package:

```text
companies/united_states/irseobmf
```

Input source:

```text
https://www.irs.gov/pub/irs-soi/eo1.csv
https://www.irs.gov/pub/irs-soi/eo2.csv
https://www.irs.gov/pub/irs-soi/eo3.csv
https://www.irs.gov/pub/irs-soi/eo4.csv
```

Important source rules:

- Download all four CSV files into one source snapshot run.
- Preserve each raw CSV file and record its hash.
- Process rows as CSV with headers.
- Preserve EIN as a 9 digit string.
- Parse nonprofit financial values as whole USD integers when present.

Final export contribution:

- One company per EIN when not already matched by a stronger identifier.
- `primary_id = "EIN:{ein}"` unless joined to an existing company.
- Set nonprofit classification and IRS status fields.
- Add IRS mailing address and nonprofit financials where present.

## Phase 3: Colorado Business Entities

Source package:

```text
companies/united_states/coloradoentities
```

Input source:

```text
https://data.colorado.gov/resource/4ykn-tg5h.json
```

Important source rules:

- Use Socrata SODA `$limit` and `$offset` pagination.
- Default page size is `1000`.
- Optional `COLORADO_BUSINESS_ENTITIES_APP_TOKEN` may set `X-App-Token`.
- Order by `entityid` for stable pagination when supported.
- Support `--max-pages` for smoke tests.

Final export contribution:

- One company per `CO:{entityid}` unless joined to a federal identifier later.
- State registration goes into `identifiers.state_registrations`.
- Colorado `entitystatus` is authoritative for corporate standing.
- Colorado `entityformdate` is authoritative formation date.
- Registered agent is service-of-process data, not ownership data.

## Final Export Design

The final USA export must read source manifests, not raw snapshots. It should
build from whichever ready source manifests exist, so phase 1 can produce a
valid final export with only SEC EDGAR.

Expected final parquet tables:

```text
companies.parquet
company_names.parquet
identifiers.parquet
addresses.parquet
status.parquet
classifications.parquet
nonprofit_financials.parquet
registered_agents.parquet
source_evidence.parquet
```

Tables with no rows for a phase may be omitted only if the manifest makes that
explicit. Prefer stable file names once emitted.

Join and precedence rules come from
`companies/analysis/united_states/data_model/country_company_profile_mapping.md`:

- `primary_id`: CIK > EIN > UEI > `state_code:entity_id`.
- Legal name: state register > SEC title > IRS `NAME`.
- Corporate standing: state status only; IRS/SAM statuses do not overwrite it.
- Formation date: state `entityformdate`; IRS `RULING` is not formation.
- EIN is the strongest future cross-source key when available.

## Testing

Use TDD for each source phase.

Phase 1 required tests:

- USA layout defaults to `../data/united_states/countrydata`.
- CLI parses all commands and rejects unknown sources.
- SEC config honors env overrides and requires/configures User-Agent.
- SEC decode handles object-keyed `company_tickers.json`.
- SEC download writes snapshot metadata, hash, and byte count.
- SEC export writes expected parquet files and manifest.
- SEC final export produces CIK-keyed companies and source evidence.
- Status skips incomplete newer export directories.

Phase 2 required tests:

- Multi-file CSV download preserves file-level metadata.
- CSV processing preserves EIN strings and parses financial values.
- Bad CSV rows are counted/logged and do not stop processing.
- IRS source export and final contribution match the mapping report.

Phase 3 required tests:

- Socrata pagination handles `$limit`, `$offset`, empty page, and `--max-pages`.
- Optional app token is sent only when configured.
- Colorado source export preserves entity status, form date, addresses, and
  registered agent.
- Final contribution maps state registration and corporate standing correctly.

Live tests are gated and skipped by default:

```sh
COUNTRYDATA_SEC_EDGAR_LIVE=1 GOWORK=off go test -tags=integration ./secedgar/... -run TestLive -count=1 -v
COUNTRYDATA_IRS_EO_BMF_LIVE=1 GOWORK=off go test -tags=integration ./irseobmf/... -run TestLive -count=1 -v
COUNTRYDATA_COLORADO_BUSINESS_ENTITIES_LIVE=1 GOWORK=off go test -tags=integration ./coloradoentities/... -run TestLive -count=1 -v
```

## Verification

After each source phase:

```sh
cd companies/common
GOWORK=off go test ./... -count=1

cd companies/united_states
GOWORK=off go test ./... -count=1
GOWORK=off go build -o ./bin/united-states-countrydata ./cmd/united-states-countrydata
rm -f ./bin/united-states-countrydata
rmdir ./bin 2>/dev/null || true
GOWORK=off go run ./cmd/united-states-countrydata status-source --source secedgar
```

When phase 2 or 3 is completed, also run the corresponding `status-source`
command for that source.

## Scope Boundaries

This design does not implement SAM.gov, OpenCorporates, or all state Secretary
of State registries. Those require separate approval because they have
authentication, license, or per-jurisdiction transport blockers.

This design does not add Corpscout database loading. The integration boundary is
the country binary plus parquet manifests.
