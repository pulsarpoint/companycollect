# Sweden — license & terms notes

## Current practical position

Bolagsverket publishes company/register bulk files and annual-report archives as public high-value
datasets under:

```text
https://vardefulla-datamangder.bolagsverket.se/
```

The user confirmed that these bulk files are free to download and may be downloaded on a **7-day**
cadence. The first Sweden ingestion should therefore use the public bulk files directly.

## Public bulk files

Recommended source URLs:

```text
https://vardefulla-datamangder.bolagsverket.se/scb/scb_bulkfil.zip
https://vardefulla-datamangder.bolagsverket.se/bolagsverket/bolagsverket_bulkfil.zip
https://vardefulla-datamangder.bolagsverket.se/arsredovisningar/
```

Access characteristics:

- No API token required for the direct ZIP downloads.
- No payment required.
- Files are public high-value datasets.
- Exact formal license/reuse text should still be saved from the Bolagsverket/dataportal metadata before
  redistribution of raw data.

## Annual reports

The annual-report archives contain public filings in XHTML/iXBRL form. Treat the structured facts as
open company filing data, but verify and record any specific wording that applies to redistribution of
the raw annual-report document files.

## Authenticated API

The Bolagsverket API exists, but it is not the preferred full-universe ingestion path:

- requires authentication/registration,
- appears to require EU identity documentation/eID,
- adds operational complexity that the public bulk files avoid.

Working OAuth access was verified on 2026-08-17. Use the API for bounded
targeted enrichment and digital-report discovery while keeping public bulk
files as the universe source.

The public developer portal describes the API as a high-value dataset and the
API is not monetized, but the exact redistribution wording should still be
archived before storing API responses for production reuse. The original client
returned `invalid_client`, but its replacement authenticated successfully;
bounded company and document payloads were collected as raw investigation
evidence.

Keep OAuth client credentials in environment/secret storage. Matching local
credential filenames are ignored by Git; the plaintext files should be removed
from the docs tree after the working secret is provisioned for deployment.

## Uncertainty / to confirm

- [ ] Exact formal license string for each direct bulk dataset.
- [ ] Exact reuse wording for annual-report raw XHTML/iXBRL documents.
- [ ] Whether annual-report archive names and directory partitioning are stable enough to use as raw
      object keys directly.

## Upphandlingsmyndigheten procurement data

The official open-data page says its data is free to use. Attribution must
include source, date, and the period covered. When Corpscout processes the
statistics, the attribution must identify Corpscout as the processor rather
than implying that Upphandlingsmyndigheten produced the derived result.

Source:

```text
https://www.upphandlingsmyndigheten.se/om-oss/var-oppna-data/
```

Recommended attribution pattern:

```text
Data: Upphandlingsmyndigheten
Processing: Corpscout
Source snapshot: <date>
Coverage: <period>
```

## TED

TED's legal notice says procurement notices in the Supplement to the Official
Journal may be freely reused for commercial or non-commercial purposes unless
otherwise noted. SIMAP metadata is dedicated to the public domain under CC0;
editorial website content is CC BY 4.0.

```text
https://ted.europa.eu/en/legal-notice
```

Keep EU source attribution and do not reuse the TED/SIMAP logos.

## GLEIF

GLEIF and ANNA publish the ISIN-to-LEI relationship file as an open daily
mapping. The initiative is voluntary at the national-numbering-agency level,
so license openness does not imply complete global ISIN coverage.

```text
https://www.gleif.org/en/lei-data/lei-mapping/download-isin-to-lei-relationship-files
```

## EODHD

EODHD is a commercial authenticated data provider, not an official exchange or
regulatory source. The current Corpscout subscription successfully supports the
exchange list, active/delisted symbol lists, and price endpoints already used
by the EODHD pipeline. A 2026-07-23 probe of the documented ID Mapping API
returned HTTP 402 Payment Required.

Before making that endpoint a required identity source:

- confirm the required subscription tier and API-call allowance;
- confirm internal storage and user-facing display rights for identifiers,
  listing status, and prices;
- keep EODHD provenance visible and do not describe vendor activity status as
  official legal admission status.

The open GLEIF ISIN-to-LEI file avoids making the paid EODHD ID Mapping endpoint
a hard dependency for symbols that already have ISINs.

## Exchange/venue data

Do not assume that a publicly reachable market-data endpoint grants production
storage or redistribution rights.

- Nasdaq publishes formal European data policies and sells Nordic equity
  reference-data files. Confirm whether the proposed boolean/list display is
  covered before using the public screener API in production.
- NGM states that its market-data policy governs display, non-display, and
  redistribution use. Its Data API should be treated as licensed access.
- Spotlight exposes organisation numbers and LEIs on public company pages,
  but automated collection and republication terms still need review.

Use EODHD for the operational ticker/price layer, ESMA FIRDS plus GLEIF for the
reusable regulatory identity/status spine, then use venue data under an
appropriate agreement for ticker/status enrichment.
