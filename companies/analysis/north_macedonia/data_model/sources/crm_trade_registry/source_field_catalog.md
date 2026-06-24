# Central Registry — Trade Registry (Трговски регистар) Field Catalog

## Source Summary

- Country: North Macedonia
- Source type: official_registry
- Organization: Централен регистар на РСМ (Central Registry, CRM)
- URL: https://www.crm.com.mk/
- License: commercial distribution by the registry
- Access: **free basic per-company search; paid bulk/detailed data**
- Freshness: live register
- Record shape: per-company record (free basic search; paid detail)
- Primary keys: ЕМБС
- Join keys: ЕМБС, ЕДБ

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| EMBS | ЕМБС | Entity registration number (7-digit) | string | identifier |  | primary id/join key |
| EDB | ЕДБ | Tax number (13-digit) | string | identifier |  | tax id → UJP |
| naziv | Назив / Име | Business name | string | legal_name |  | Cyrillic/Latin/Albanian |
| pravna_forma | Правна форма | Legal form | string | legal_form | ДОО/ДООЕЛ/АД | |
| status | Статус | Status | string | status | активен | |
| sediste_adresa | Седиште / Адреса | Registered seat | string | address |  | |
| dejnost_NKD | Дејност (НКД) | Activity code | string | activity |  | NKD ~NACE |
| upraviteli_osnovaci | Управители / Основачи | Managers / founders | array | ownership |  | PERSONAL DATA — redact; paid |
| osnovna_glavnina | Основна главнина | Registered capital | decimal | financial |  | MKD; paid |

## Interpretation Notes

- The **Central Registry (CRM)** is the official company register. It offers a
  **free public search** (Пребарување) for basic identity/status, and is the
  official **commercial distributor** — **bulk extracts and detailed data
  (managers/founders, capital, full history) are paid** (subscription /
  per-document via the e-distribution service).
- **Identifiers**: **ЕМБС** (7-digit) is the entity registration number and primary
  key; **ЕДБ** (13-digit) is the tax number (links to UJP / VAT).
- **Access constraint (verified)**: the `crm.com.mk` host **resolved via DNS**
  (`92.55.95.145`) but **TCP/HTTP timed out from this environment** (network
  block), so this catalog is documented from **public documentation**; **no live
  values** were captured (example values empty).
- **Personal data**: managers/founders are personal data when natural persons (NM
  Law on Personal Data Protection) — redact. Currency **MKD**; Cyrillic + Albanian.
