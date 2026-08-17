# Bolagsverket Värdefulla datamängder API gap analysis

Checked: 2026-08-17

## Conclusion

The authenticated API is useful as a targeted freshness and document-discovery
source, but it should not replace the public Bolagsverket and SCB bulk feeds.
Most company facts in `POST /organisationer` already exist in those bulk files.

The investigation found two high-priority gaps in the current model:

1. The API contract confirms that one identity can own several separately
   registered businesses, distinguished by `namnskyddslopnummer`. The current
   Sweden normalizer partitions only by identity and retains `company_rank = 1`,
   collapsing the other businesses.
2. `POST /dokumentlista` supplies an official `dokumentId` and
   `registreringstidpunkt` for each digitally filed annual report. Neither is
   present in `se_financial_reports`.

The original supplied OAuth client was rejected with HTTP 401 `invalid_client`.
A replacement client supplied on 2026-08-17 authenticated successfully and all
bounded data-plane probes returned HTTP 200. No token or secret was
printed or persisted.

## API contract verified

Official API: `VärdefullaDatamängder`, version `v1`, published by Bolagsverket.

Production server:

```text
https://gw.api.bolagsverket.se/vardefulla-datamangder/v1
```

| Endpoint | Purpose | Required scope |
|---|---|---|
| `GET /isalive` | Availability check | `vardefulla-datamangder:ping` |
| `POST /organisationer` | Company/register profile | `vardefulla-datamangder:read` |
| `POST /dokumentlista` | Available digital annual reports | `vardefulla-datamangder:read` |
| `GET /dokument/{dokumentId}` | Download one annual-report ZIP | `vardefulla-datamangder:read` |

The OpenAPI operations declare an unlimited throttling tier. Treat that as a
portal policy description, not permission for unbounded polling.

## Calls attempted

| Call | Result |
|---|---|
| OAuth token, Basic client authentication | HTTP 401 `invalid_client` |
| OAuth token, form client authentication | HTTP 401 `invalid_client` |
| OAuth token, replacement client with Basic authentication | HTTP 200; token issued with read and ping scopes |
| Same token attempts on the gateway host | HTTP 404; token service is on the portal host |
| `GET /isalive` without a token | HTTP 401, WSO2 code `900902` |
| `POST /organisationer` for `5562434182` without a token | HTTP 401, WSO2 code `900902` |
| `POST /dokumentlista` for `5562434182` without a token | HTTP 401, WSO2 code `900902` |
| Data-plane HTTP Basic / `ApiKey` header probes | HTTP 401 |
| `GET /isalive` with replacement-client bearer token | HTTP 200, `OK` |
| `POST /organisationer` for `5562434182` with bearer token | HTTP 200; one organisation |
| `POST /dokumentlista` for `5562434182` with bearer token | HTTP 200; zero digital reports |
| `POST /dokumentlista` for positive-control `5560187493` | HTTP 200; one digital report |

The API metadata and OpenAPI specification were publicly retrievable without
authentication. Authentication failures were sanitized because the token
error description echoes the client identifier. The successful access token
was kept only in process memory.

## Live result for `5562434182`

The authenticated company response confirms:

| Field | Official API value | Current coverage |
|---|---|---|
| Organisation registration date | `1984-05-03` | Already in Bolagsverket bulk data |
| Name registration date | `1984-11-08` | Already packed in `legal_name_raw`; should be surfaced |
| Introduced at SCB | `1984-08-19` | Present from SCB, but should not be modeled as incorporation |
| Active | `JA` | Current status is derived; API provides a useful conflict check |
| SNI | `41000` — building construction | Already in the industry tables |
| Postal address | `Åbyvägen 215`, `23173 ANDERSLÖV` | Already in address observations |
| Business description | Building and related activity | Already in Bolagsverket bulk data |
| Digital annual reports | none | Confirms the current database result for the API's digital-only scope |

The API therefore does not explain the broader Ratsit presentation gap for
this company. It confirms that the UI can expose more of the data already
collected, while wording financial absence specifically as no digitally filed
annual report found.

## What is useful that we do not model correctly

### 1. Multiple businesses per identity — high priority

The API documentation explicitly states that `namnskyddslopnummer` separates
multiple businesses for organisation types that can share an identity, notably
sole traders. Its example returns two organisation objects for the same personal
identity with sequence numbers 1 and 2.

The current normalizer does this before retaining one row:

```text
partition by normalized identity
order by source record metadata
company_rank = 1
```

It does not include `namnskyddslopnummer` in the partition key. A bounded
DuckDB analysis of the local Bolagsverket bulk snapshot found:

| Measure | Count |
|---|---:|
| Parsed source records | 2,958,806 |
| Rejected malformed physical lines | 4,617 |
| Distinct identities | 2,850,061 |
| Identities with multiple registrations | 90,419 |
| Extra registrations collapsed by an identity-only key | 108,745 |
| Maximum registrations under one identity | 25 |
| Multi-registration identities with multiple sequence values | 90,419 |

90,389 of the 90,419 affected identities have organisation form `E-ORGFO`
(sole trader).
This is not data exclusive to the API—the bulk file already includes the
sequence—but the API contract makes the identity semantics unambiguous.

Recommended identity:

```text
registration_key = (identitetsbeteckning, namnskyddslopnummer)
```

Keep the normalized identity separately for person/entity linkage. Do not put
12-digit personal identities in public URLs or logs without a privacy review.

### 2. Official annual-report document identity and filing date — high priority

`POST /dokumentlista` returns:

```text
dokumentId
filformat
rapporteringsperiodTom
registreringstidpunkt
```

The current `se_financial_reports` table has report period, archive key, nested
ZIP name, XHTML object key and hashes, but no Bolagsverket document ID and no
official filing-registration date.

Useful additions:

```text
bolagsverket_document_id
filing_registered_on
source_file_format
```

The API would also support a targeted backfill or freshness check for one
company without searching the complete archive partitioning scheme.

For `5562434182`, both the current database and the live API contain zero
digital annual reports. A positive-control call for `5560187493`, selected from
an existing public annual-report bulk archive, returned one ZIP document for
period end `2025-06-30`, registered `2025-12-29`. This verifies that the API
exposes an official document ID and filing-registration date when a digital
report exists. It does not solve the separate paper/scanned-report gap visible
on aggregators.

### 3. Separate organisation registration from SCB introduction — medium priority

The API keeps two dates under `organisationsdatum`:

```text
registreringsdatum
infortHosScb
```

The current normalizer maps SCB `RegDatKtid` into the generic
`incorporation_date`. For `5562434182`, the source-specific dates differ:

```text
Bolagsverket registration date: 1984-05-03
SCB source date:              1984-08-19
```

The API contract shows why these should not be merged under one meaning.
Preserve a separate `introduced_at_scb` field and only use it as an
incorporation-date fallback when the semantics are explicitly accepted.

### 4. Explicit identity type — medium priority and privacy-sensitive

`organisationsidentitet.typ` distinguishes organisation numbers, personal
identity numbers, coordination numbers and GD numbers. The current model keeps
the normalized digits but not the declared identity type; it relies on length
and prefixes.

The live `se_companies` table currently contains 1,313,022 twelve-digit IDs,
including 926,682 rows whose selected legal form is `E-ORGFO`. Preserve the API
type if targeted responses are ingested. Any compound-key repair for sole
traders also needs an explicit decision about storage, search, URLs, logs and
external display of personal identifiers.

### 5. Explicit active flag and field-level producer — lower priority

The API supplies `verksamOrganisation.kod` (`JA`/`NEJ`) and a
`dataproducent` on most field envelopes. Current data derives active status
from deregistration data and keeps Bolagsverket/SCB as row-level sources.

The explicit flag is useful for conflict checks. Field-level producer metadata
is useful if API responses are ingested because one response merges facts from
Bolagsverket and SCB. It is not a major new company-data category.

## What the API does not add materially

These API groups substantially overlap data already collected:

| API group | Existing coverage |
|---|---|
| Names, name types, name registration dates, special-name scope | Packed in `legal_name_raw`; the detail query already parses secondary/foreign names and scope |
| Registration country | `se_company_registry_current.registration_country_code` |
| Organisation form | Bolagsverket `organisationsform` stored as the primary code |
| Tax/statistical legal form | SCB `JurForm` stored on the SCB source row; labels exist in `se_code_labels` |
| Deregistration date and reason | `se_companies` and registry history |
| Liquidation/restructuring proceedings | Parsed into `se_company_proceedings_current` |
| SNI codes | `se_company_industry_current` and `se_industries` |
| Postal address | Bolagsverket and SCB observations plus canonical/geocoded address tables |
| Activity description | Bolagsverket bulk and translated description layer |
| Marketing block | SCB `Reklamsparrtyp` retained in registry history, though not promoted to the company shell |
| Digital report ZIP contents | Already obtained from public annual-report bulk archives and parsed as XHTML/iXBRL |

The API does not expose owners, beneficial owners, a general officer register,
or paper annual reports. Officers currently shown by Corpscout are extracted
from digital annual-report signatures, not from this company endpoint.

## Recommendation

1. Fix the Sweden company key before adding the API: preserve one registration
   per `(identity, namnskyddslopnummer)` and quantify downstream URL/privacy
   implications for sole traders.
2. Split `introduced_at_scb` from legal registration/incorporation date.
3. Move the working client into environment/secret storage and remove/reissue
   plaintext credentials from `docs/` after confirming deployment access.
4. Continue bounded fixture probes for:
   - a sole trader with multiple sequence values,
   - an inactive company with proceedings,
   - a company known to have a recent digital annual report.
5. Use the API as a targeted delta/document catalog. Keep weekly bulk files as
   the full-universe source.

## Saved evidence

- Public OpenAPI specification:
  `companies/data/sweden/raw/api/bolagsverket_vardefulla_datamangder_openapi_20260817.json`
- Public developer-portal metadata:
  `companies/data/sweden/raw/api/bolagsverket_vardefulla_datamangder_metadata_20260817.json`
- Public developer-portal HTML shell:
  `companies/data/sweden/raw/pages/bolagsverket_vardefulla_datamangder_devportal_20260817.html`
- Sanitized probe results:
  `companies/data/sweden/raw/api/bolagsverket_vdm_probe_20260817.json`
- Authenticated probe metadata and raw responses:
  `companies/data/sweden/raw/api/bolagsverket_vdm_authenticated_probe_metadata_20260817.json`,
  `bolagsverket_vdm_organisation_5562434182_20260817.json`, and the two
  `bolagsverket_vdm_documents_*.json` responses
- Source catalog and implementation handoff:
  `companies/analysis/sweden/data_model/sources/bolagsverket_vardefulla_datamangder_api/`
