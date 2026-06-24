# GİB — VKN / Tax-Number Lookup Field Catalog

> **PLANNING-ONLY.** The Revenue Administration (GİB) provides VKN (tax-number)
> verification / VAT (KDV) taxpayer lookup per company. No open bulk. Cataloged
> from public docs.

## Source Summary

- Country: Turkey
- Source type: tax_registry
- Organization: Gelir İdaresi Başkanlığı (GİB)
- URL: https://www.gib.gov.tr/
- License: free per-company
- Access: public per-company lookup
- Freshness: live
- Record shape: per-company VKN lookup
- Primary keys: vkn
- Join keys: vkn

## Fields

| Path | Source field | Meaning | Type | Semantic type | Notes |
|---|---|---|---|---|---|
| gib.vkn | VKN | Tax id (10-digit) | string | identifier | join key |
| gib.unvan | Unvan | Title | string | legal_name | |
| gib.vergi_dairesi | Vergi Dairesi | Tax office | string | geography | |
| gib.kdv_mukellef | KDV Mükellefiyeti | VAT taxpayer status | string | license_or_terms | VKN = VAT id |

## Interpretation Notes

- The **VKN** (10-digit) is the tax id; Turkey has **VAT (KDV)** but **no separate
  VAT number** — the VKN serves as the VAT identifier. GİB offers per-company VKN /
  KDV taxpayer verification; no open bulk. Join on VKN to MERSIS. No raw sample.
