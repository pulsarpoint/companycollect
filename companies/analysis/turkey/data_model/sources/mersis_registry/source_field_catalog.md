# MERSIS — Central Registry System Field Catalog

> **PLANNING-ONLY (per-company query).** MERSIS (Ministry of Trade) is the central
> company registry, accessed via a **free per-company query** (by MERSIS no / VKN /
> title). There is **no open bulk/API**. Cataloged from public documentation / the
> query model — no enumeration of all companies.

## Source Summary

- Country: Turkey
- Source type: official_registry
- Organization: Ministry of Trade (Ticaret Bakanlığı)
- URL: https://mersis.ticaret.gov.tr/
- License: free per-company / no open bulk
- Access: public per-company search
- Freshness: live register
- Record shape: per-company query result
- Primary keys: mersis_no (16-digit)
- Join keys: mersis_no, vkn

## Fields

| Path | Source field | Meaning | Type | Semantic type | Notes |
|---|---|---|---|---|---|
| mersis.mersis_no | MERSIS No | 16-digit id | string | identifier | company id; join key |
| mersis.unvan | Unvan | Title | string | legal_name | |
| mersis.vkn | VKN | Tax id (10-digit) | string | identifier | no separate VAT no |
| mersis.ticaret_sicil_no | Ticaret Sicil No | Trade-registry no | string | identifier | per office |
| mersis.nace | NACE | Activity | string | activity | NACE Rev.2 |
| mersis.adres | Adres | Address | string | address | |
| mersis.sirket_turu | Şirket Türü | Company type | string | legal_form | A.Ş. / Ltd. Şti. |
| mersis.durum | Durum | Status | string | status | active/dissolved |

## Interpretation Notes

- The **MERSIS number** (16-digit) is the authoritative company id and the universal
  join key (to the gazette, GİB, and KAP via VKN/name).
- The **VKN** (10-digit) is the tax id; Turkey has **VAT (KDV)** but **no separate
  VAT number**.
- **Access**: free per-company query (JS form); **no open bulk/API** — there is no
  way to enumerate all companies openly. Lookups need a seed (MERSIS no / VKN /
  title), e.g. from KAP or a procurement list.
- **Personal data**: directors/shareholders are not in the basic query result; they
  appear in the gazette / trade-registry records (personal data, KVKK). No raw
  sample record (per-company query, no bulk).
