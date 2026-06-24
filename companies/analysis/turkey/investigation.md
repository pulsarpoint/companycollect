# Turkey Company Data Investigation

## Conclusion

Turkey has a **free per-company registry lookup** but **no open bulk register**,
and **public financials only for listed companies** (KAP):

- **Identity (free per-company):** **MERSIS** (Merkezi Sicil Kayıt Sistemi,
  Ministry of Trade). Every company has a **16-digit MERSIS number**; the system
  holds title (unvan), **VKN (tax id)**, trade-registry number, NACE activity,
  address, type, status. Free per-company query (MERSIS no / VKN / title); **no
  open bulk/API**.
- **Company events:** the **Trade Registry Gazette** (Türkiye Ticaret Sicili
  Gazetesi, TOBB) publishes registrations/amendments/dissolutions, searchable.
- **Financials (listed, open):** **KAP** (Public Disclosure Platform) — listed
  companies + their financial statements (mali tablolar), public. Private-company
  financials are not public.
- **Tax:** **VKN** (10-digit) is the tax id; Turkey has **VAT (KDV)** but no
  separate VAT number.

## What was verified live

- **MERSIS / Ticaret Sicil Gazetesi / KAP** reachable (HTTP 200). MERSIS home is a
  JS app (query via form). KAP is a Next.js app.
- **KAP listed companies extracted**: the BIST member list page yielded **808**
  distinct companies with their KAP id + name — e.g. **ADEL KALEMCİLİK A.Ş.** (KAP
  832), **ACISELSAN A.Ş.** (1626), **ADESE GYO A.Ş.** (1560), **Yapı ve Kredi
  Bankası A.Ş.** (2429). Per-company pages at
  `/tr/sirket-bilgileri/ozet/{kapId}-{slug}`.
- KAP's JSON API endpoints have moved (several `/tr/api/...` paths 404); the company
  entities are public via the site and per-company pages.

## Identifiers

- **MERSIS no** — **16-digit** Central Registry System number (company id).
- **Ticaret Sicil No** — trade-registry number (assigned by the local registry
  office; not globally unique without the office).
- **VKN (Vergi Kimlik Numarası)** — **10-digit** tax id. Turkey has **VAT (KDV)**;
  the VKN serves as the tax/VAT identifier — **no separate VAT number**.
- **KAP id** — KAP's internal id for listed companies; **NACE** activity code.

## What is NOT openly available

- **A bulk company register** — MERSIS is per-company query only; no open bulk/API.
- **Private-company financials** — only listed companies via KAP.
- **Directors / shareholders** — in the gazette / trade-registry records
  (per-company; personal data).

## Recommended ingestion

1. **MERSIS** per-company query (keyed on MERSIS no / VKN) for identity.
2. **Trade Registry Gazette** for company events (registration/changes).
3. **KAP** for listed-company financial statements + disclosures.
4. Redact directors/shareholders (personal data, KVKK) in shared samples.
