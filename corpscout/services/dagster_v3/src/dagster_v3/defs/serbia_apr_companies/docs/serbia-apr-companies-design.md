# Serbia APR companies open-data design

## 1. Source overview

- **Country / registry**: Serbia — Agencija za privredne registre (APR),
  Companies Register open data.
- **Module**: `defs/serbia_apr_companies/`.
- **Endpoint**: `https://openapi.apr.gov.rs/api/opendata/companies`.
- **Format**: one complete JSON snapshot, public GET without credentials.
- **License**: Serbian Open Data Licence (`sodl`).
- **Cadence**: APR snapshot date (`DatumPreseka`) is currently monthly.
- **Entity key**: eight-digit matični broj, represented as the key of each
  member inside the top-level `Podaci` object.

The live response verified on 2026-08-25 contained 133,634 companies and was
57,673,691 bytes. Its source snapshot date was 2026-07-31. APR does not support
`HEAD` for this endpoint and its `GET` response supplied neither
`Content-Length` nor `Last-Modified`.

APR also omits its leaf certificate's issuer intermediate from the TLS
handshake. The asset pins the advertised SSL2BUY EMEA intermediate (SHA-256
fingerprint `58:F0:F7:56:75:8E:93:FB:0B:6B:17:A3:6A:38:50:47:5D:68:BC:0D:6C:99:CB:E2:2A:1B:18:35:1C:89:FF:1F`), whose signature was verified against
Certifi's trusted Sectigo Public Server Authentication Root R46. TLS and
hostname verification remain enabled; the asset never uses `verify=False`.

## 2. Ingest mode and why

This is a non-partitioned full-snapshot download. One request returns the whole
register, so API partitions and pagination would add bookkeeping without
reducing source work.

The source now has two implemented durable boundaries:

```text
APR complete JSON GET -> validated temporary file -> content-addressed S3 object
content-addressed S3 object -> typed Arrow batches -> three atomic DuckDB tables
```

The DuckDB load is one non-subsettable multi-asset because one validation and
parse operation produces a snapshot catalog, historical observations, and the
current population together. ClickHouse company-table publication is not
implemented in this pass. The separately implemented paid representative and
beneficial-owner pipelines remain different source boundaries.

## 3. Download, validation, and object storage

The asset uses the dlt retrying HTTP client and streams the body to a temporary
file while computing SHA-256. A whole-download retry loop covers mid-stream
disconnects that request-level retries cannot recover from.

Because the server publishes no useful freshness or length headers, the asset
validates the complete JSON stream with `ijson` before uploading it:

- the body must be at least 10,000,000 bytes;
- `DatumPreseka` must exist and be an ISO date;
- `Podaci` must be a JSON object;
- at least 100,000 direct company keys must be present; and
- the parser must reach the valid end of the document.

Objects are immutable and content-addressed:

```text
s3://source-serbia-apr-companies/
  serbia_apr_companies/raw/
    snapshot_date=<DatumPreseka>/
      sha256=<payload-sha256>/companies.json
```

A per-run immutable manifest records the source URL, license, retrieval time,
snapshot date, object key, SHA-256, byte size, company count, and whether the
content-addressed body was newly uploaded or reused. Re-running the same APR
payload writes a new audit manifest but does not duplicate the raw object.

## 4. DuckDB model and loading contract

The non-partitioned database is `data/serbia_apr_companies_source.duckdb`, with
schema `serbia_apr_companies`. Every asset that opens it uses the single-writer
pool `serbia_apr_companies_duckdb`. The file stem intentionally differs from
the schema name so DuckDB does not encounter a catalog/schema collision.

The non-subsettable `serbia_apr_companies_duckdb_load` multi-asset produces:

- `serbia_apr_company_snapshot_runs_duckdb` -> `snapshot_runs`: accepted raw
  manifest, source-object integrity, row count, schema fingerprint, and load
  timestamps;
- `serbia_apr_company_observations_duckdb` -> `company_observations`: complete
  typed history, idempotently replaceable by APR snapshot date; and
- `serbia_apr_companies_current_duckdb` -> `companies_current`: the complete
  company population from the selected snapshot.

The loader selects manifests by `(snapshot_date, retrieved_at)`, so a newly
written manifest for an older source snapshot cannot regress current data. A
later correction for the same snapshot date wins. It downloads the referenced
object and independently checks its byte count and SHA-256 before parsing.

Direct `ijson` to typed Arrow batches is used instead of a second dlt extraction
boundary: the raw S3 asset already owns extraction, while APR's dynamic
`Podaci.<matični broj>` JSON map needs strict key preservation and whole-file
validation. Batches contain at most 50,000 records and are inserted with a
registered Arrow relation and `INSERT SELECT`.

The map key is preserved as `company_id`, `registration_number`,
`source_record_id`, and as part of `source_record_uid`. The source mapping is:

| APR field | DuckDB field |
| --- | --- |
| map key under `Podaci` | `company_id`, `registration_number` |
| `PoslovnoIme` | `legal_name` |
| `SifraOpstine` | `municipality_code` |
| `NazivOpstine` | `municipality_name_original` |
| `NazivStatus` | `source_status_original`, canonical `status`, `is_active` |
| `DatumOsnivanja` | `incorporation_date` |
| `NazivPravneForme` | `legal_form_original` |
| `SifraDelatnosti` | `primary_activity_code` |

Each company row also records the source run/object, record ordinal, raw JSON,
payload hash, stable source-record UID, business-state fingerprint, snapshot
date, retrieval time, and DuckDB load time. Raw JSON and hashes deliberately
remain in this source database; a later ClickHouse publication can expose only
the serving fields it needs.

Before publication the loader rejects invalid identifiers/codes, missing or
empty required fields, unknown APR status labels, future incorporation dates,
duplicate company keys, inconsistent municipality code/name mappings, row
count differences, and snapshot-date differences. Staging is validated first;
all three durable tables then commit or roll back together. Re-running one raw
manifest replaces that snapshot's observations instead of duplicating them.

ClickHouse DDL and publication remain a later migration-owned pass.

The raw open feed contains company identity, municipality, current status,
incorporation date, legal form, and primary KD2010 activity code. It does not
contain PIB/VAT, street address, email, phone, representatives, founders, or
beneficial owners.

## 5. Cross-cutting assessments

- **Contacts**: none in this endpoint. Canonical contact/domain tables belong
  in the future normalized company pass and will be empty unless a separate
  official APR contact source is added.
- **Industry**: `SifraDelatnosti` is a KD2010 code aligned with NACE Rev. 2 and
  must be connected to the shared NACE dimension downstream.
- **Translation**: status, legal-form, and municipality labels are Serbian;
  translation/static mappings belong after normalized ClickHouse publication.
- **Currency**: this source contains no monetary amounts.
- **Personal data**: the open company snapshot contains organization records,
  not officers or beneficial owners; the asset is tagged `personal_data=false`.

## 6. Scheduling and deployment

No schedule is registered yet. Once the full S3-to-DuckDB chain has been
manually materialized in the deployed environment, the complete refresh job
should run monthly after APR publishes a new `DatumPreseka`, with the schedule
initially stopped.

## 7. Verification

- Unit tests cover content-addressing, S3 reuse, immutable run manifests,
  streaming JSON validation, population guards, whole-download retry, newest
  manifest selection, typed parsing, Cyrillic and leading-zero preservation,
  idempotency, rollback on source drift, and Dagster lineage/pool registration.
- Definition loading is checked with `uv run dg check defs`.
- Live verification of the DuckDB parser uses the downloaded 133,634-company
  object and checks all three table counts and representative typed values.
