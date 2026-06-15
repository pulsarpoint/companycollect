# Company data sources for New Zealand

## Status

- Official bulk data: **not found** (no free full-register bulk download)
- Official API: **found** (NZBN API — authoritative entity data — free subscription key required)
- Open data portal: data.govt.nz exists but is bot-protected and does not host the company register openly
- License: **known-ish** — NZBN publicly available data is Crown-copyright, free to reuse; Companies/Disclose registers are public
- Recommended ingestion path: **API** (NZBN API) behind a free subscription key, with the Companies Register + Disclose Register for documents/financials

## Best source

**NZBN API** (`https://api.business.govt.nz/gateway/nzbn/v5/entities`) — run by the
Companies Office (MBIE). The **NZBN (New Zealand Business Number, 13-digit GLN)** is
the universal identifier for every NZ business entity (companies, sole traders,
partnerships, trusts, government). The API returns the **publicly available NZBN
data**: entity name, type, status, registration date, source register +
identifier (e.g. the Companies Register **company number**), addresses, trading
names, contacts, and ANZSIC industry classifications. It requires a **free
subscription key** (registration on the api.business.govt.nz developer portal) —
verified: the gateway returns **HTTP 401 "missing subscription key"** without one.

## Financial data

NZ has **no general public financial-statement filing** for ordinary companies.
Only **FMC reporting entities** (issuers, large companies, large overseas-owned
companies, managed investment schemes, etc.) must file audited financial
statements. Those are public on:

- the **Companies Register** (filed documents for entities required to file), and
- the **Disclose Register** (FMA, `disclose-register.companiesoffice.govt.nz`) for
  FMC offers / managed investment schemes — financial statements + offer documents.

So financials are **available only for the FMC-reporting subset**, as downloadable
documents (PDF; some XBRL) — not a bulk dataset for all companies.

## Caveats

- **No free bulk** dump of the full register; access is the NZBN API (free key) or
  per-entity search. Companies Register help mentions no API/bulk/extract.
- **Tax id (IRD number) and GST number are not public.** NZ has **GST, not VAT**.
- The committed normalized sample is **schematic** (the API is key-gated; no real
  record was fetched).

## Next action

Register for a free NZBN API subscription key, ingest entities by NZBN, and layer
FMC financial statements from the Companies/Disclose registers for the entities
that file.
