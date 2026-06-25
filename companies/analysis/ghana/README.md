# Company data sources for Ghana

## Status

- Official bulk data: **not open** — ORC has no open bulk register
- Official API: **not open** — ORC company search/documents are via eServices (paid)
- Open data portal: `data.gov.gh` **unreachable** (firewalled) at investigation time
- License: ORC data is paid/restricted; GSE listed data is public
- Recommended ingestion path: **GSE** for listed companies (open) + ORC documents
  (paid/per-company) for the rest
- **Environment note:** the .gov.gh hosts (`orc.gov.gh`, `rgd.gov.gh`, `gra.gov.gh`,
  `data.gov.gh`) **resolved via DNS but were firewalled (TCP/HTTP timeout)** from this
  environment — documented from public knowledge for those; GSE was reachable.

## Best source

The official company registry is the **ORC — Office of the Registrar of Companies**
(formerly the **Registrar General's Department, RGD**), which runs company
registration and search via the **eServices** portal (`eservices.rgd.gov.gh`).
Company **search** and **documents** (incorporation, status, annual returns) are
delivered there, **paid per transaction**. The ORC/RGD hosts were **firewalled** from
this environment. There is **no open bulk register or open API**.

The one genuinely **open** source is the **Ghana Stock Exchange (GSE)** for listed
companies.

## Financial data

**GSE** (`gse.com.gh`) — **open**: the listed-company directory
(`/listed-companies/`, `/profile-of-listed-companies/`) and **financial statements**
(`/financial-statements/`). **Verified live**: real listed companies incl. **Ecobank
Ghana PLC**, **GCB Bank** (Ghana Commercial Bank), **AngloGold Ashanti Plc**,
**CalBank PLC**, **Standard Chartered Bank Ghana PLC**, **Guinness Ghana Breweries
Plc**, **Fan Milk Limited**, **Enterprise Group PLC**. **Private-company financials**
(annual returns filed with the ORC) are **not open** (paid, per company). Currency
**GHS**.

## Identifiers & tax

- **Company registration number** — ORC-issued (new CIN-style format).
- **TIN — Tax Identification Number** — Ghana Revenue Authority (businesses;
  individuals now use the **Ghana Card PIN**).
- **Business registration** — for sole proprietorships / partnerships.
- Currency **GHS** (Ghana cedi). Language: English.

## Next action

Use **GSE** (open) for listed companies + financials; use **ORC eServices** (paid,
per company) for the rest once reachable from an unblocked network. There is **no
open bulk register and no open private financials**. Directors/shareholders are
personal data (Data Protection Act 2012, Act 843) — redact if obtained.
