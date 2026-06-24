# Company data sources for Turkey

## Status

- Official bulk data: **not found (open)** — the central registry (MERSIS) is free per-company search, no open bulk
- Official API: **partial** — KAP (listed companies/financials) is publicly browsable; MERSIS query is a JS form
- Open data portal: no national portal hosts the company register openly
- License: free per-company access; bulk not open
- Recommended ingestion path: **per-company lookup** (MERSIS/gazette) + KAP for listed financials

## Best source

**MERSIS** (Merkezi Sicil Kayıt Sistemi, `mersis.ticaret.gov.tr`), the central
company registry run by the **Ministry of Trade**. Every company has a **MERSIS
number (16-digit)**, and the system holds title, **VKN (tax id)**, trade-registry
number, NACE activity, address, type, and status. Access is a **free per-company
query** (by MERSIS no / VKN / title); there is **no open bulk/API**. The **Trade
Registry Gazette** (Türkiye Ticaret Sicili Gazetesi, `ticaretsicil.gov.tr`)
publishes registrations/changes, searchable per company.

## Financial data — listed only (open)

**KAP — Kamuyu Aydınlatma Platformu** (Public Disclosure Platform, `kap.org.tr`) is
the open source of **listed-company** disclosures and **financial statements**
(mali tablolar). The member list and per-company pages are public; financial
reports are filed and openly viewable. Private-company financials are **not
public**.

Verified live: extracted **808** listed companies from the KAP BIST list — e.g.
**ADEL KALEMCİLİK A.Ş.** (KAP id 832), **ACISELSAN A.Ş.** (1626), **ADESE GYO**
(1560), **Yapı ve Kredi Bankası A.Ş.** (2429).

## Identifiers & tax

- **MERSIS no** — 16-digit Central Registry System number (company id).
- **Ticaret Sicil No** — trade-registry number (per registry office).
- **VKN (Vergi Kimlik Numarası)** — 10-digit tax id. Turkey has **VAT (KDV)** but
  **no separate VAT number** — the VKN is the tax id.
- **NACE** activity code; KAP id for listed companies.

## Next action

Use the free MERSIS per-company query (keyed on MERSIS no / VKN) for identity and
the Trade Registry Gazette for company events; use **KAP** for listed-company
financials. There is no open bulk register. Sample uses the real KAP listed list.
