# Sweden — company open-data investigation

## Goal

Document the practical open-data path for Swedish company data and annual-report financial data.

## Headline conclusion

Use **public downloadable bulk files**, not the authenticated API, as the primary ingestion source.

Bolagsverket publishes high-value company datasets directly as ZIP files:

```text
https://vardefulla-datamangder.bolagsverket.se/scb/scb_bulkfil.zip
https://vardefulla-datamangder.bolagsverket.se/bolagsverket/bolagsverket_bulkfil.zip
```

Annual reports are also published as downloadable archives under:

```text
https://vardefulla-datamangder.bolagsverket.se/arsredovisningar/
```

The bulk files are free and the user confirmed Bolagsverket allows downloading the free bulk files every
**7 days**. The authenticated API exists, but it requires authentication/registration using EU identity
documentation/eID, so it should not be the default implementation path.

## Source 1 — Bolagsverket legal-register bulk file

Direct URL:

```text
https://vardefulla-datamangder.bolagsverket.se/bolagsverket/bolagsverket_bulkfil.zip
```

Local extracted sample:

```text
data_model/bolagsverket_bulkfil.txt
```

Observed properties:

- UTF-8 text.
- Semicolon-separated CSV.
- Header + ~2,963,424 data lines in the local sample.
- Large file: extracted sample is roughly 944 MB.
- Direct URL returned HTTP 200 during verification on 2026-07-02.
- Response headers showed `content-type: application/zip`, `content-length: 248608278`, and
  `last-modified: Mon, 29 Jun 2026 01:27:14 GMT`.

Observed columns:

```text
organisationsidentitet
namnskyddslopnummer
registreringsland
organisationsnamn
organisationsform
avregistreringsdatum
avregistreringsorsak
pagandeAvvecklingsEllerOmstruktureringsforfarande
registreringsdatum
verksamhetsbeskrivning
postadress
```

This is the best source for legal-register facts: company identity, name, legal form, registration
country, registration/deregistration dates, deregistration reason, business description, and postal
address.

## Source 2 — SCB/FDB bulk file

Direct URL:

```text
https://vardefulla-datamangder.bolagsverket.se/scb/scb_bulkfil.zip
```

Local extracted sample:

```text
data_model/scb_bulkfil_JE_20260629T055245_80.txt
```

Observed properties:

- Latin-1 / ISO-8859 text.
- Tab-separated file.
- Header + ~1,816,509 data lines in the local sample.
- Extracted sample is roughly 256 MB.
- Direct URL returned HTTP 200 during verification on 2026-07-02.
- Response headers showed `content-type: application/zip`, `content-length: 70747583`, and
  `last-modified: Mon, 29 Jun 2026 13:04:12 GMT`.

Observed columns:

```text
ForAndrTyp
COAdress
Foretagsnamn
FtgStat
Gatuadress
JEStat
JurForm
Namn
Ng1
Ng2
Ng3
Ng4
Ng5
PeOrgNr
PostNr
PostOrt
RegDatKtid
Reklamsparrtyp
mCOAdress
mForetagsnamn
mFtgStat
mGatuadress
mJEStat
mJurForm
mNamn
mNg1
mNg2
mNg3
mNg4
mNg5
mPostNr
mPostOrt
mRegDatKtid
mReklamsparrtyp
```

This is the best source for SCB/statistical business-register fields such as company names, addresses,
legal form code, status flags, organization/person number (`PeOrgNr`), registration date (`RegDatKtid`),
and SNI/activity codes (`Ng1`..`Ng5`).

## Source 3 — Annual-report bulk archives

Directory:

```text
https://vardefulla-datamangder.bolagsverket.se/arsredovisningar/
```

Local sample archive:

```text
data_model/01_1.zip
```

Extracted local sample:

```text
data_model/annual_reports_01_1/
```

Observed properties:

- `01_1.zip` contains 1,512 nested per-company ZIP files in the local sample.
- Nested ZIP names follow this pattern:

```text
<org_number>_<financial_period_end>.zip
```

Example:

```text
5560187493_2025-06-30.zip
```

- Each inspected nested ZIP contains one or more `.xhtml` files.
- The XHTML files contain inline XBRL namespaces and concepts.
- Example observed concepts:

```text
se-cd-base:RakenskapsarForstaDag
se-cd-base:RakenskapsarSistaDag
se-cd-base:Organisationsnummer
se-cd-base:ForetagetsNamn
se-gen-base:Nettoomsattning
```

This is the source for Swedish financial metrics. The parser should work from raw ZIPs and preserve:

- outer archive key/name,
- nested company ZIP name,
- organization number,
- financial period end,
- XHTML file name,
- raw concept QName,
- context/unit/decimals,
- raw value and parsed numeric/text/date value.

## Authenticated API status

The API should be documented as a targeted fallback/document-discovery source,
not the initial full-universe ingestion path.

Known API surface:

```text
https://gw.api.bolagsverket.se/vardefulla-datamangder/v1
```

Known endpoints from previous investigation:

```text
GET  /isalive
POST /organisationer
POST /dokumentlista
GET  /dokument/{id}
```

The official public OpenAPI specification was retrieved successfully on
2026-08-17. It documents OAuth2 client credentials, read/ping scopes, field-level
producer/error envelopes, and an unlimited portal throttling tier.

The original client returned HTTP 401 `invalid_client`. A replacement client
then authenticated successfully using HTTP Basic OAuth client authentication.
Authenticated health, company, and document-list calls returned HTTP 200. No
credential or token was persisted in the investigation artifacts.

The contract and live responses identify two important modeling gaps:

- `namnskyddslopnummer` is part of a business-registration identity when
  several registrations share one personal identity. The current normalizer
  partitions only by identity and collapses 108,745 extra registrations in the
  inspected local bulk snapshot.
- `/dokumentlista` supplies official `dokumentId` and
  `registreringstidpunkt`; the current Sweden report table stores neither.

For `5562434182`, the live response returned one active organisation and no
digital annual reports. It also confirmed that organisation registration
(`1984-05-03`), SCB introduction (`1984-08-19`), and name registration
(`1984-11-08`) are separate dates. A positive-control company returned one
digital report with a document ID and filing-registration date.

See `bolagsverket-vdm-api-gap-analysis.md` for the field-by-field comparison.

## Recommended ingestion architecture

Company data:

```text
download raw ZIPs every 7 days
-> store raw ZIPs in S3/object storage with checksum and retrieved_at
-> extract/parse text files
-> normalize to Swedish company/register tables
-> load ClickHouse
```

Annual reports:

```text
download annual-report ZIP batches
-> store raw ZIPs in S3/object storage
-> enumerate nested company ZIPs
-> parse XHTML/iXBRL documents
-> map concepts to financial metrics
-> load ClickHouse
```

## Open questions / risks

- The annual-report directory listing format should be scripted and verified; direct `HEAD` to
  `arsredovisningar/01_1.zip` returned HTTP 500 during one check, while the local sample exists and
  the directory is public.
- Exact license/reuse wording for annual-report documents should be captured from Bolagsverket's
  dataset terms before redistribution of raw documents.
- The SCB file is Latin-1; parser must explicitly decode it.
- Bolagsverket fields include encoded suffixes inside values, for example
  `8888006577$ORGNR-IDORG`; parser must split and preserve both raw and normalized values.
- Annual-report concept mapping needs a Sweden taxonomy mapping layer, not just generic XHTML parsing.
