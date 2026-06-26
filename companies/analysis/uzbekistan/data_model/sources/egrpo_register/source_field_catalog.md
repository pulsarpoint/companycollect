# EGRPO — Unified State Register of Enterprises and Organizations Field Catalog

## Source Summary

- Country: Uzbekistan
- Source type: official_registry
- Organization: Statistics Agency (stat.uz) / open-data portal data.egov.uz
- URL: https://data.egov.uz/
- License: unknown (portal firewalled — not confirmable)
- Access: **firewalled from this environment** (data.egov.uz / data.gov.uz timeout/refused)
- Freshness: periodic
- Record shape: per-entity register record (planning-only)
- Primary keys: stir_inn
- Join keys: stir_inn, egrpo_code, legal_name

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| stir_inn | СТИР/ИНН | 9-digit taxpayer id | string | identifier |  | primary + join key |
| egrpo_code | ОКПО / EGRPO code | Statistical register code | string | identifier |  | alt id |
| legal_name | Наименование | Entity name (UZ/RU) | string | legal_name |  | Latin/Cyrillic + Russian |
| legal_form | Ташкилий-хукукий шакли | Legal form | string | legal_form |  | MCHJ/AJ/YaTT |
| status | Холати | Status | string | status |  | active/liquidated |
| registration_date | Рўйхатдан ўтган сана | Registration date | date | date |  | Gregorian |
| registered_address | Манзил | Legal address | string | address |  | |
| oked_activity | ОКЭД | Economic activity | string | activity |  | OKED classifier |

## Interpretation Notes

- The **EGRPO** (Unified State Register of Enterprises and Organizations) is the
  **authoritative** Uzbek company register, maintained by the **Statistics Agency** (stat.uz)
  and published via the national **open-data portal data.egov.uz**. Per public knowledge it
  carries **STIR/INN, EGRPO code, name, legal form, status, registration date, registered
  address, and OKED activity** (a director/head field may also be present — uncertain; treat
  as personal data and redact if present).
- **Access**: from this environment `data.egov.uz` and `data.gov.uz` are **firewalled**
  (HTTPS timeout / HTTP connection refused) — the register was **not reachable**. All fields
  here are **planning-only**, documented from public knowledge — **no values captured**. The
  firewall is **environmental**, not a real-world block; re-check from an unblocked network.
- **Identifier**: the **STIR/INN (9-digit)** is the primary key and the universal join key
  (to the tax committee). **OKED** is the UZ economic-activity classifier. **Language**: Uzbek
  (Latin and Cyrillic) + Russian.
- No `sample_record.json`: source firewalled / not captured.
