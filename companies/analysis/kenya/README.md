# Company data sources for Kenya

## Status

- Official bulk data: **not open** — BRS has no open bulk register
- Official API: **not open** — BRS company search/documents are via eCitizen
  (login + paid)
- Open data portal: `opendata.go.ke` (KODI) is reachable but exposes **no accessible
  company-register dataset**
- License: BRS data is paid/restricted; NSE listed data is public
- Recommended ingestion path: **NSE** for listed companies (open) + BRS documents
  (paid, per-company via eCitizen) for the rest

## Best source

The official registry is the **BRS — Business Registration Service** (Office of the
Registrar of Companies). Company **search** and **documents** are delivered through
the **eCitizen** platform:

- **`brs.ecitizen.go.ke`** / `businessregistration.ecitizen.go.ke` — company/business
  name search and document requests (**CR12** = directors & shareholders, status
  report, certified extracts) — **login-gated and paid per transaction** (returned
  403 to automated requests).

There is **no open bulk register or open API**. The one genuinely **open** source is
the **NSE (Nairobi Securities Exchange)** listed-company directory.

## Financial data

**NSE** (`nse.co.ke/listed-companies/`) publishes the **listed-company directory**
(public) — **verified live**: real issuers incl. **Absa Bank Kenya PLC**, **Stanbic
Holdings Plc**, **Standard Chartered Bank**, **Diamond Trust Bank Kenya**, **Sasini
Ltd**, **Williamson Tea Kenya**, **Car & General (K) Ltd**, **Kapchorua/Limuru Tea**.
NSE also publishes listed-company **announcements / financial results** and market
statistics. **Private-company financials** are filed with BRS (annual returns /
accounts) and obtained **per company for a fee** — **not open**.

## Identifiers & tax

- **Company registration number** — BRS-issued (old formats `C.NNNNN` / `CPR/2015/
  NNNNNN`; new eCitizen format e.g. `PVT-XXXXXXX`).
- **BN number** — Business Name registration (sole proprietors / partnerships).
- **KRA PIN** — Kenya Revenue Authority tax id (e.g. `P051234567X`). Companies and
  individuals.
- **VAT** — VAT obligation is registered **under the KRA PIN** (no separate VAT
  number).
- Currency **KES**. Language: English (+ Swahili).

## Next action

Use the **NSE** listed-company directory + announcements for listed companies
(open); buy **BRS** documents (search, CR12, status report) per company via eCitizen
(login + paid) for the rest. There is **no open bulk register**. CR12 directors/
shareholders are personal data (Kenya Data Protection Act 2019) — redact.
