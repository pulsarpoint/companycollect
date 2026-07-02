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

The Bolagsverket API exists, but it is not the preferred ingestion path now:

- requires authentication/registration,
- appears to require EU identity documentation/eID,
- adds operational complexity that the public bulk files avoid.

Use the API only for future targeted enrichment if credentials are available.

## Uncertainty / to confirm

- [ ] Exact formal license string for each direct bulk dataset.
- [ ] Exact reuse wording for annual-report raw XHTML/iXBRL documents.
- [ ] Whether annual-report archive names and directory partitioning are stable enough to use as raw
      object keys directly.
