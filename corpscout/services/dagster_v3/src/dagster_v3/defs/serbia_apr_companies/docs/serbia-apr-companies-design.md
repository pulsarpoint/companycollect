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

## 2. Ingest mode and why

This is a non-partitioned full-snapshot download. One request returns the whole
register, so API partitions and pagination would add bookkeeping without
reducing source work.

The first implemented boundary is the durable raw object:

```text
APR complete JSON GET -> validated temporary file -> content-addressed S3 object
```

DuckDB parsing and the ClickHouse company-table publication are deliberately
not implemented in this pass. They will depend on this raw asset in the next
pass. The separately implemented paid representative and beneficial-owner
pipelines remain different source boundaries.

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

## 4. Future DuckDB and ClickHouse steps

The next implementation pass will add a non-partitioned DuckDB file and one
single-writer pool. It will select the newest accepted raw manifest, download
the referenced JSON object, preserve the matični broj map key, and create typed
current and observation tables with source provenance. ClickHouse DDL and
publication will remain migration-owned and atomically replaced.

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

No schedule is registered yet. The asset is being manually materialized after
deployment first. Once live behavior and a later DuckDB chain are validated,
the complete refresh job should run monthly after APR publishes a new
`DatumPreseka`, with the schedule initially stopped.

## 7. Verification

- Unit tests cover content-addressing, S3 reuse, immutable run manifests,
  streaming JSON validation, population guards, whole-download retry, and
  Dagster asset registration.
- Definition loading is checked with `uv run dg check defs`.
- Live completion requires a deployed Dagster materialization whose metadata
  reports the current snapshot date, record count, SHA-256, byte size, S3 object
  key, and manifest key.
