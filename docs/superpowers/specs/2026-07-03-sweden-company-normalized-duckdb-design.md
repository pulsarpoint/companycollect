# Sweden Company Normalized DuckDB Design

## Summary

Add a raw-to-normalized DuckDB asset for the Sweden company pipeline. The asset should consume the already materialized `sweden_company_raw_duckdb` tables and produce deterministic normalized tables for company identity, addresses, and industry codes.

Heuristic contact extraction should remain out of this asset. Domains, emails, and phone numbers are not structured source fields in the current Sweden bulk files, so they should be modeled later as candidate/enrichment tables with confidence metadata.

## Current Pipeline

Current durable assets:

```text
sweden_company_raw_snapshot_s3
  -> sweden_company_raw_duckdb
```

Current raw DuckDB database:

```text
data/sweden_company_source.duckdb
```

Current raw schema:

```text
sweden_company.raw_files
sweden_company.bolagsverket_raw
sweden_company.scb_raw
```

## Proposed Pipeline

Add one downstream asset:

```text
sweden_company_raw_snapshot_s3
  -> sweden_company_raw_duckdb
    -> sweden_company_normalized_duckdb
```

The normalized asset should use the same DuckDB resource and writer pool as the raw DuckDB asset. It should rebuild normalized tables as full replacements, because the upstream sources are full snapshots and do not provide incremental company changes.

## Normalized Tables

### `sweden_company.companies`

One row per normalized company identifier.

Primary source precedence:

1. Bolagsverket for legal identity, legal form, registration/deregistration dates, activity description, and postal address.
2. SCB as fallback and enrichment source for statistical status, SNI/activity codes, and address fields.

Expected columns:

```text
company_id
registration_number
bolagsverket_company_id_raw
scb_company_id_raw
legal_name
legal_name_raw
legal_form_code
status
status_reason
incorporation_date
dissolution_date
activity_description
source_run_id
bolagsverket_source_record_id
scb_source_record_id
bolagsverket_source_payload_hash
scb_source_payload_hash
updated_from_raw_at
```

Rules:

- Normalize Swedish organization identifiers to digits only.
- Treat Bolagsverket `organisationsidentitet` as the preferred company id when present.
- Use SCB `PeOrgNr` to join/enrich and to create rows that are absent from Bolagsverket.
- Parse Bolagsverket packed suffixes without discarding raw values.
- Derive status conservatively:
  - `inactive` when Bolagsverket has `avregistreringsdatum`.
  - `active` when no deregistration date is present.
  - Preserve raw reason/status fields for later refinement.

### `sweden_company.company_addresses`

One or more address rows per company.

Expected columns:

```text
company_id
address_type
source
raw_address
street_address
care_of
postal_code
post_town
country_code
source_run_id
source_record_id
source_payload_hash
```

Rules:

- Parse Bolagsverket `postadress` as the preferred postal address source.
- Use SCB `COAdress`, `Gatuadress`, `PostNr`, and `PostOrt` as fallback/enrichment.
- Preserve raw address strings because Bolagsverket uses packed `$`-delimited values and some records may not parse cleanly.

### `sweden_company.company_industry_codes`

One row per non-empty SCB `Ng1`..`Ng5` value.

Expected columns:

```text
company_id
sequence
is_primary
sni_code
nace_rev2_class_code
source_field
source_run_id
source_record_id
source_payload_hash
```

Rules:

- `Ng1` is sequence `1` and `is_primary = true`.
- `Ng2`..`Ng5` are secondary activity codes.
- Store the raw 5-digit SNI code as `sni_code`.
- Derive `nace_rev2_class_code` as the first four digits of the SNI code when the value is five digits and not `00000`.
- Treat `00000`, blank, and malformed values as unknown/unclassified and do not emit them into `company_industry_codes` unless a later reference-code mapping requires preserving unknown codes.
- Do not call the raw 5-digit value a NACE code. SNI 2007 corresponds to NACE Rev. 2 through the 4-digit class level; the 5th digit is Sweden-specific detail.

## Contact Candidate Tables

Do not include contact extraction in `sweden_company_normalized_duckdb`.

Later add:

```text
sweden_company_contact_candidates_duckdb
```

Candidate table shape:

```text
company_id
candidate_type
candidate_value
normalized_value
source_table
source_field
source_record_id
source_payload_hash
confidence
extraction_rule
```

Scope for that later asset:

- domains from free-text fields such as `verksamhetsbeskrivning`;
- emails if a valid email pattern appears in source text;
- phone numbers only if a Sweden-specific phone parser/validator is used.

Candidate extraction must not populate canonical company website/email/phone fields directly.

## Error Handling And Observability

The normalized asset should fail on schema drift that removes required raw columns. It should not fail because individual rows have malformed packed fields; malformed values should produce null parsed columns while preserving raw values.

Materialization metadata should include:

```text
company_count
address_count
industry_code_count
bolagsverket_company_count
scb_company_count
companies_with_sni_count
unknown_sni_count
```

## Testing

Add focused unit tests with small DuckDB fixtures:

- Bolagsverket suffix parsing for organization id and legal name.
- Bolagsverket date parsing.
- Bolagsverket postal-address parsing with raw fallback.
- SCB join by normalized `PeOrgNr`.
- SCB-only company row creation.
- `Ng1`..`Ng5` expansion into ordered SNI rows.
- `00000` and malformed SNI handling.
- Dagster asset registration and job selection.

## Out Of Scope

- ClickHouse export.
- Translation of Swedish free text.
- SNI reference-code title lookup.
- Contact/domain/email/phone canonicalization.
- Financial annual-report parsing.
- Incremental processing.
