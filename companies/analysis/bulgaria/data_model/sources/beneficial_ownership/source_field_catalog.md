# Регистър на действителните собственици (Beneficial Ownership) — Field Catalog

> **PLANNING-ONLY (restricted).** Beneficial-ownership declarations filed within the commercial register;
> **access conditions apply** (legitimate interest post-CJEU). Filed as documents; not open bulk. Cataloged
> from public documentation only; no records/values copied; sensitive PII.

## Source Summary

- Country: Bulgaria
- Source type: beneficial_ownership_register
- Organization: Агенция по вписванията
- URL: https://portal.registryagency.bg/
- License: filed in the register; access conditions apply — planning-only
- Access: restricted
- Freshness: continuous
- Record shape: per-company beneficial-owner declaration (document)
- Primary keys: eik + beneficial_owner
- Join keys: eik

## Fields

| Path | Source field (BG) | Meaning | Type | Semantic type | Notes |
|---|---|---|---|---|---|
| eik | ЕИК | Entity | string | identifier | join |
| deystvitelen_sobstvenik.ime | действителен собственик | BO name | string | person | **sensitive PII** |
| deystvitelen_sobstvenik.kontrol | вид/размер на контрола | Control | string | ownership | |
| data | дата на деклариране | Declaration date | date | date | |

## Interpretation Notes

- **Restricted, not open** (post-CJEU): access conditions / legitimate interest. Filed as documents within
  the commercial register. Sensitive PII (natural persons) → GDPR; do not ingest as open.
- Note: the **commercial register itself already exposes capital partners / sole owner** (`съдружници /
  едноличен собственик`) openly — a partial ownership signal distinct from this restricted BO register.
- Joins via **EIK** if/when lawfully obtained. No `sample_record.json`.
