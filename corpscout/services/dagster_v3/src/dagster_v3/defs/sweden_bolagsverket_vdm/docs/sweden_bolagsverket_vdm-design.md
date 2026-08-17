# Sweden Bolagsverket VDM targeted refresh design

## Purpose and boundary

This source refreshes a small, explicitly configured set of Swedish company
identities through Bolagsverket's Valuable Datasets API. It is an asynchronous
Dagster job, not a company-page dependency and not a registry crawler.

The source accepts 1–100 unique identities per run. It has no schedule. A caller
launches `sweden_bolagsverket_vdm_targeted_refresh_job` with `company_ids` after
identifying stale or missing source data.

## Asset chain

```text
POST /organisationer + POST /dokumentlista
  -> immutable JSON in RustFS/S3 + manifest written last
  -> typed run-local DuckDB observations
  -> append-only source-specific ClickHouse tables
```

The raw asset writes both exact HTTP response bodies beneath a run-specific key:

```text
sweden_bolagsverket_vdm/raw/run_id=<run>/identity_sha256=<hash>/organisationer.json
sweden_bolagsverket_vdm/raw/run_id=<run>/identity_sha256=<hash>/dokumentlista.json
sweden_bolagsverket_vdm/raw/run_id=<run>/manifest.json
```

The manifest records SHA-256, size, response status and client-generated request
ID for every stored body. It is written last so downstream assets never treat a
partial batch as complete. Identity values are retained inside the protected raw
payload and manifest but are hashed in object keys so personal identities do not
leak through routine object-store access logs.

## Authentication and failure behavior

`BolagsverketVdmResource` uses OAuth2 client credentials from
`BOLAGSVERKET_VDM_CLIENT_ID` and `BOLAGSVERKET_VDM_CLIENT_SECRET`. The access token
is cached for the lifetime of the resource and renewed 60 seconds before expiry.

HTTP retries are bounded and apply only to transient failures handled by dlt's
request client. `Retry-After` is respected. Each data request carries a fresh
`X-Request-ID`. Errors include only endpoint, request ID and HTTP status; response
bodies, credentials, bearer tokens and company identities are not logged.

## Stored semantics

Company observations preserve:

- the explicit identity type code and original label;
- `verksamOrganisation.kod`, its producer, the derived nullable active flag and
  its observation time;
- Bolagsverket organisation registration date separately from
  `organisationsdatum.infortHosScb`;
- name-protection sequence for registrations that share an identity;
- the number of digital annual-report documents observed, including confirmed
  zero-document checks.

Document observations preserve `dokumentId`, report-period end,
`registreringstidpunkt` as `filing_registered_on`, and `filformat` as
`source_file_format`.

The ClickHouse tables are source-specific. Targeted API refreshes do not mutate
or replace the full bulk Sweden company snapshot, and a later bulk-table swap
cannot erase these observations.

## Operations

Example launch configuration:

```yaml
ops:
  sweden_bolagsverket_vdm_raw_responses_s3:
    config:
      company_ids:
        - "5562434182"
        - "5560187493"
      request_delay_seconds: 0.25
```

Before the first run, apply ClickHouse migration
`000279_corpscout_se_bolagsverket_vdm`. The two destination tables are:

- `corpscout.se_bolagsverket_vdm_company_observations`
- `corpscout.se_bolagsverket_vdm_financial_report_documents`
