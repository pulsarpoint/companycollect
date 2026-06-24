# Company data sources for Philippines

## Status

- Official bulk data: **not open** — SEC sells documents per company; no open bulk
  register
- Official API: **not open** — eFAST/eSPARC are login portals; documents via paid
  SEC Express
- Open data portal: `data.gov.ph` exists but is a JS SPA with **no accessible
  company dataset**
- License: SEC documents are paid; PSE listed disclosures are public
- Recommended ingestion path: **PSE EDGE** for listed companies (open) + **paid SEC
  Express** documents (GIS/AFS) per company for the rest

## Best source

**SEC — Securities and Exchange Commission** is the official registry for
corporations and partnerships, but its company documents are **paid**:

- **eFAST** (`efast.sec.gov.ph`) — the login portal where companies file the
  **General Information Sheet (GIS)** (directors, officers, stockholders, capital)
  and **Audited Financial Statements (AFS)**.
- **eSPARC** (`esparc.sec.gov.ph`) — company **registration** (login; incl. One
  Person Corporation).
- **SEC Express System** (`secexpress.ph`) — order **GIS / AFS / Articles of
  Incorporation / certificates** for delivery — **paid per document**.

The SEC main site (`sec.gov.ph`) is WAF-blocked. There is **no open bulk company
register**.

## Financial data

- **PSE EDGE** (`edge.pse.com.ph`) — **open** for **listed companies**: company
  directory (name, symbol, sector, subsector, listing date) and disclosures /
  financial reports. **Verified live** (e.g. **PLDT Inc.**, symbol **TEL**, Services
  / Telecommunications, listed 1953-09-17).
- **Private-company financials (AFS)** are filed with the SEC via eFAST and obtained
  through **SEC Express (paid)** — **not open**.

## Identifiers & tax

- **SEC Registration Number** — corporate registration id (e.g. `CSNNNNNNNN` or older
  `ANNNNNNNN` formats).
- **TIN (Tax Identification Number)** — BIR tax id (9-digit + 3-/5-digit branch).
- **DTI BN number** — business name registration for **sole proprietorships** (DTI
  BNRS, free name search).
- **VAT** — VAT-registered businesses use the **TIN** (no separate VAT number).
- Currency **PHP**. Language: English (+ Filipino).

## Next action

Use **PSE EDGE** for listed companies (open); buy **SEC Express** documents (GIS/AFS)
per company for identity + financials; use **DTI BNRS** for sole-proprietor name
checks. There is **no open bulk register and no open private financials**. GIS
directors/officers/stockholders are personal data (Data Privacy Act 2012) — redact.
