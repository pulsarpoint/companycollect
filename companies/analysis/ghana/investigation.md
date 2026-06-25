# Ghana — company data investigation

## Goal

Find official/open sources for **company registry data** and **financial data** for
companies registered in Ghana, download/sample where allowed, and document a
reproducible trail.

## What was found

### 1. ORC / RGD — Office of the Registrar of Companies (official registry; firewalled + paid)

- The **ORC** (Office of the Registrar of Companies, formerly the **Registrar
  General's Department, RGD**) is the official registrar for companies, business
  names, and partnerships. Registration and **company search / documents** are
  delivered via the **eServices** portal (`eservices.rgd.gov.gh`) — **paid per
  transaction** (incorporation, status report, annual returns).
- **Access (verified):** `orc.gov.gh`, `rgd.gov.gh`, `eservices.rgd.gov.gh`,
  `gra.gov.gh`, and `data.gov.gh` **resolved via DNS** (e.g. `orc.gov.gh` →
  `197.253.124.98`) but **every HTTP request timed out** from this environment — a
  **network-level block**, not a site outage. No open bulk/API was reachable.

### 2. GSE — Ghana Stock Exchange (listed companies + financials) — OPEN

- **`gse.com.gh`** is open and reachable. The **listed-company directory**
  (`/listed-companies/`, `/profile-of-listed-companies/`) and **financial statements**
  (`/financial-statements/`, HTTP 200) are public. **Verified live** — real listed
  companies include **Access Bank Ghana Plc**, **Agricultural Development Bank**,
  **AngloGold Ashanti Plc**, **Benso Oil Palm Plantation Ltd**, **CalBank PLC**,
  **Camelot Ghana Ltd**, **Clydestone (Ghana) Ltd**, **Cocoa Processing Company**,
  **Ecobank Ghana PLC**, **Enterprise Group PLC**, **Fan Milk Limited**, **GCB / Ghana
  Commercial Bank**, **Guinness Ghana Breweries Plc**, **Mega African Capital Ltd**,
  **Standard Chartered Bank Ghana PLC**.
- NSE also lists market reports and an OTC market. **Listed companies only** (~35).
  Private-company financials are not here.

### 3. GRA — Ghana Revenue Authority (tax; firewalled)

- The **GRA** issues the **TIN** (Tax Identification Number) for businesses
  (individuals now use the **Ghana Card PIN**). `gra.gov.gh` was firewalled from this
  environment. Per-company; not open bulk.

### 4. data.gov.gh — open-data portal (firewalled)

- **`data.gov.gh`** was **unreachable** (firewalled) at investigation time. No
  company-register dataset could be confirmed.

## Conclusion

Ghana's official registry (**ORC/RGD**) is delivered through the **eServices** portal
— company search and documents are **paid**, with no open bulk/API — and the ORC/RGD/
GRA/data.gov.gh hosts were **firewalled** from this environment (DNS resolves; TCP/
HTTP blocked). The one genuinely **open** source is the **Ghana Stock Exchange**
(listed-company directory + financial statements) — **verified live**. So there is
**no open bulk corporate register and no open private financials** — ingestion is
`blocked_payment` (ORC) + open-for-listed (GSE). Identifiers: **company registration
number** (ORC), **TIN** (GRA). Currency **GHS**. Directors/shareholders are personal
data (Data Protection Act 2012, Act 843) — redact. No access controls were bypassed;
the sample uses **GSE-verified + public-knowledge listed companies with null ORC
identifiers** (nothing fabricated).
