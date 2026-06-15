# India Company Data Investigation

## Conclusion

India has a **genuinely open company identity register** plus **paid/listed-only
financials**:

- **Identity (open via API):** the Ministry of Corporate Affairs (MCA) **Company
  Master Data** is republished on **data.gov.in** (Open Government Data Platform)
  under **GODL-India** and served through the **OGD REST API**
  (`api.data.gov.in/resource/{resource_id}`). Every company is keyed by its
  **CIN (Corporate Identification Number, 21-char)**. The data includes name,
  status, class/category, **authorized & paid-up capital**, principal business
  activity, registrar (RoC), and registered address.
- **Financials (paid / listed-only):** full annual financial statements (AOC-4 /
  XBRL) are filed with MCA but are **pay-per-document** on the MCA21 portal — no
  open bulk. The master data carries only capital figures and **latest filing-year
  markers**, not the statements. For **listed** companies (CIN starting `L`),
  financials are openly available from **BSE/NSE/SEBI** disclosures.

## What was verified live

- **OGD API works.** Using the documented public sample key, enumerated **128
  "Company Master Data" resources** (state × year, snapshots 2015–2021) under org
  "Ministry of Corporate Affairs", and fetched real records:
  - 2015 schema (Nagaland): `L20101NL1985PLC002284` — SANGRAHALAYA TIMBER AND
    CRAFTS LTD, ACTIVE, Public, authorized & paid-up ₹200,100,000.
  - 2021 schema (Mizoram): `U01100MZ2016PTC013293` — ZO THLAI THAR PRODUCER
    COMPANY LIMITED, Active, Private, with `industrial_class`, `latest_year_ar`,
    `latest_year_bs`, and a contact email.
- **MCA portal** (mca.gov.in) is **WAF-blocked (403)** to automated clients — its
  free per-CIN master-data lookup and paid document downloads were not fetched.

## Identifiers

- **CIN (Corporate Identification Number)** — 21 characters, highly structured:
  `L20101NL1985PLC002284` decodes as
  - `L` — listing status (L = listed, U = unlisted)
  - `20101` — 5-digit industry code (MCA/NIC-derived)
  - `NL` — state code (Nagaland)
  - `1985` — year of incorporation
  - `PLC` — ownership/type (PLC public ltd, PTC private ltd, etc.)
  - `002284` — 6-digit RoC registration sequence
- The CIN is the universal **join key**. India company identity has **no VAT/tax
  id in this dataset**; the corporate tax id is the **PAN** (10-char, issued by the
  Income Tax dept) and GST registration is **GSTIN** (15-char, = state code + PAN +
  entity/check) — neither appears in the open master data.

## Schema variants

The 2015-era resources and the 2021-era resources differ slightly:

- **Stable**: CIN, company_name, company_status, company_class, company_category,
  authorized capital, paidup_capital, date_of_registration, registered_state,
  registrar_of_companies, principal_business_activity, registered_office_address.
- **2021 adds**: `company_sub_category` (own field), `industrial_class` (4-digit),
  `email_addr` (contact email — personal data), `latest_year_ar` (latest annual
  return year), `latest_year_bs` (latest balance-sheet filing year).

## What is NOT openly available

- **Actual financial statements** (P&L, balance sheet, turnover, net worth) — paid
  MCA documents, or listed-company disclosures only.
- **Directors / officers (DIN)** and **charges** — on the MCA portal (free lookup,
  not in the open bulk); director data is personal data.
- **PAN / GSTIN** — not in the open company master data.
- **A live feed** — data.gov.in is point-in-time snapshots (latest 2021).

## Recommended ingestion

1. **OGD API** — iterate the 128 MCA Company Master Data resources (state × year),
   keyed on CIN, with a free api-key. Prefer the newest snapshot per state.
2. Layer **listed-company financials** from BSE/NSE separately (CIN starts `L`).
3. Treat MCA paid documents (AOC-4/XBRL) as an out-of-scope paid enrichment.
4. Redact the contact email (personal data) in any stored/shared output.
