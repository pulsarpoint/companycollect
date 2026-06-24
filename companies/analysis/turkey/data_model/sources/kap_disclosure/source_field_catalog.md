# KAP — Public Disclosure Platform Field Catalog

## Source Summary

- Country: Turkey
- Source type: financial_disclosure
- Organization: MKK / Borsa İstanbul (Public Disclosure Platform)
- URL: https://www.kap.org.tr/tr/bist-sirketler
- License: public disclosure
- Access: public (no key)
- Freshness: event-driven / quarterly
- Record shape: listed-company page + financial statements
- Primary keys: kap_id
- Join keys: kap_id, vkn, company_name

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| company.kap_id | KAP id | KAP company id | string | identifier | 832, 2429 | listed only |
| company.name | Unvan | Company title | string | legal_name | ADEL KALEMCILIK ... A.Ş. | real |
| company.ticker | BIST kodu | Ticker | string | identifier | | per-company page |
| company.city / sector | Şehir / Sektör | City / sector | string | geography/activity | | |
| financials.period | Dönem | Period | string | date | | |
| financials.balance_income | Bilanço / Gelir Tablosu | Balance + income (TRY) | object | financial | | TFRS taxonomy |
| disclosures.ozel_durum | Özel Durum Açıklamaları | Disclosures | array | document | | |

## Interpretation Notes

- **Verified from real data**: the KAP BIST member list page yielded **808**
  distinct listed companies (KAP id + name) — e.g. ADEL KALEMCİLİK A.Ş. (832),
  ACISELSAN A.Ş. (1626), ADESE GYO (1560), Yapı ve Kredi Bankası A.Ş. (2429). Per-
  company pages at `/tr/sirket-bilgileri/ozet/{kapId}-{slug}`.
- KAP publishes **listed-company** financial statements (Bilanço balance sheet,
  Gelir Tablosu income statement) under Turkish IFRS (TFRS/TMS), TRY, and material-
  event disclosures (özel durum açıklamaları).
- **Coverage**: listed companies only (~800). Private-company financials are not
  public.
- **Join**: by KAP id / company name (and VKN where shown) to MERSIS identity.
- The KAP JSON API endpoints have moved; the company entities are public via the
  site and per-company pages.
