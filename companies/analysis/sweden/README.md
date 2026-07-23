# Company data sources for Sweden

Deep analysis of the cross-country list signals requested on 2026-07-23 is in:

```text
company-signals-analysis.md
```

It covers financial-data availability, public-procurement awards (including
Upphandlingsmyndigheten and TED), current public listings, tri-state UI/filter
semantics, empirical source coverage, and the recommended ingestion order.

## Status

- Official bulk data: **found** — Bolagsverket publishes downloadable company bulk files directly on the
  public high-value-datasets host. The files are free and are refreshed roughly every **7 days**.
- Official API: **found but not recommended for ingestion now** — the API exists, but access requires
  authenticated registration with EU identity documentation/eID. Use public bulk files instead.
- Financial data: **found** — annual reports are available as public ZIP files under the
  `arsredovisningar/` directory. The observed sample archive contains per-company ZIPs with XHTML/iXBRL
  annual-report documents.
- Public procurement: **found** — use Upphandlingsmyndigheten's national
  supplier-award open data as primary and enable Sweden in the existing TED
  eForms pipeline as the EU-threshold/current complement.
- Current public listings: **operational source found; official validation
  still to add** — EODHD already supplies global active/delisted symbols,
  instrument types, ISINs, MIC candidates, and prices. Add the open GLEIF
  ISIN-to-LEI relationship file and use existing GLEIF `registered_as` data
  to resolve Swedish organisation numbers. Then add ESMA FIRDS for exact EEA
  venues, regulatory classification, admission/termination history, and a
  defensible negative result. EODHD's separate ID Mapping API returned HTTP
  402 under the current subscription.
- Open data portal/source page: **found** — Bolagsverket documents downloadable files on its
  "Nedladdningsbara filer" page.
- License: **open/high-value dataset, but exact reuse wording should still be recorded per dataset**.
- Recommended ingestion path: **bulk files first**. Download the company bulk files and annual-report
  archives from the public URLs, store immutable raw ZIPs, then parse locally.

## Best source

Use the public Bolagsverket high-value-dataset bulk files:

```text
Company/statistical register bulk:
https://vardefulla-datamangder.bolagsverket.se/scb/scb_bulkfil.zip

Bolagsverket legal register bulk:
https://vardefulla-datamangder.bolagsverket.se/bolagsverket/bolagsverket_bulkfil.zip

Annual reports directory:
https://vardefulla-datamangder.bolagsverket.se/arsredovisningar/
```

The Bolagsverket landing page is:

```text
https://bolagsverket.se/apierochoppnadata/hamtaforetagsinformation/nedladdningsbarafiler.2517.html
```

That page may present anti-bot/JavaScript verification to automated clients, but the direct ZIP URLs are
publicly reachable.

## Local files inspected

The sample files are in:

```text
/Users/graovic/pulsarpoint/ppoint/companycollect/companies/analysis/sweden/data_model/
```

Observed files:

| File | Observed format | Notes |
|---|---|---|
| `bolagsverket_bulkfil.txt` | UTF-8 semicolon CSV, 11 columns, ~2,963,424 data lines | Legal-register bulk file extracted from `bolagsverket_bulkfil.zip`. |
| `scb_bulkfil_JE_20260629T055245_80.txt` | Latin-1 tab-separated text, 35 columns, ~1,816,509 data lines | SCB/FDB statistical register file extracted from `scb_bulkfil.zip`. |
| `01_1.zip` | Annual-report ZIP, ~1,512 nested company report ZIPs in the sample | Extracted to `data_model/annual_reports_01_1/`. Each nested company ZIP contains one or more XHTML/iXBRL files. |

## Observed company bulk schemas

`bolagsverket_bulkfil.txt` columns:

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

`scb_bulkfil_JE_20260629T055245_80.txt` columns:

```text
ForAndrTyp, COAdress, Foretagsnamn, FtgStat, Gatuadress, JEStat, JurForm, Namn,
Ng1, Ng2, Ng3, Ng4, Ng5, PeOrgNr, PostNr, PostOrt, RegDatKtid, Reklamsparrtyp,
mCOAdress, mForetagsnamn, mFtgStat, mGatuadress, mJEStat, mJurForm, mNamn,
mNg1, mNg2, mNg3, mNg4, mNg5, mPostNr, mPostOrt, mRegDatKtid, mReklamsparrtyp
```

## Annual reports

Annual reports are available under:

```text
https://vardefulla-datamangder.bolagsverket.se/arsredovisningar/
```

The inspected `01_1.zip` sample contains 1,512 per-company ZIP files named like:

```text
5560187493_2025-06-30.zip
```

Inside a company report ZIP, the documents are XHTML files with inline XBRL namespaces. Example observed
concepts include:

```text
se-cd-base:Organisationsnummer
se-cd-base:RakenskapsarForstaDag
se-cd-base:RakenskapsarSistaDag
se-gen-base:Nettoomsattning
```

So the recommended financial pipeline is to store the raw annual-report ZIPs, explode nested company ZIPs,
parse XHTML/iXBRL, and map concepts to company financial metrics.

## Recommended ingestion approach

1. Download `scb_bulkfil.zip` and `bolagsverket_bulkfil.zip` on a 7-day cadence.
2. Keep raw ZIP files immutable in object storage with retrieval date and checksum.
3. Extract and normalize:
   - Bolagsverket file for legal identity, legal form, status, registration/deregistration dates,
     business description, and registered postal address.
   - SCB file for statistical/company universe, SNI (`Ng1`..`Ng5`), address and status flags.
4. Download annual-report ZIP batches from `arsredovisningar/`, store raw archives, then parse nested
   XHTML/iXBRL files for financial metrics.
5. Treat the authenticated API as a fallback/enrichment path only after credentials are available.

## Next action

Build the Sweden source around public bulk files, not the authenticated API:

```text
raw ZIP on S3 -> extracted text/iXBRL parser -> DuckDB/Parquet normalized tables -> ClickHouse
```

The first parser should support the three local samples already present in `data_model/`.
