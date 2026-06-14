# Registar stvarnih vlasnika (Beneficial Ownership) — Field Catalog

> **PLANNING-ONLY (restricted).** Croatian beneficial-ownership register (FINA). **Access conditions apply**
> (legitimate interest post-CJEU). Not open bulk. Cataloged from public documentation only; no records/values
> copied; sensitive PII. Note: the Sudski registar already exposes **members/owners openly** (osobe) — this
> BO register is the restricted layer.

## Source Summary

- Country: Croatia
- Source type: beneficial_ownership_register
- Organization: FINA / Ministarstvo financija
- URL: https://www.fina.hr/
- License: access conditions apply — planning-only
- Access: restricted
- Freshness: continuous
- Record shape: per-company beneficial-owner record
- Primary keys: oib + beneficial_owner
- Join keys: oib

## Fields

| Path | Source field (HR) | Meaning | Type | Semantic type | Notes |
|---|---|---|---|---|---|
| oib | OIB | Entity | string | identifier | join |
| stvarni_vlasnik.ime | stvarni vlasnik | BO name | string | person | **sensitive PII** |
| stvarni_vlasnik.udio | udio/kontrola | Ownership/control | string | ownership | |
| stvarni_vlasnik.drzavljanstvo | državljanstvo | Citizenship | string | person | PII |

## Interpretation Notes

- **Restricted, not open** (post-CJEU): legitimate-interest access. Sensitive PII (natural persons) → GDPR;
  do not ingest as open. Joins via **OIB** if/when lawfully obtained.
- The **Sudski registar `osobe`** already gives **open** members/owners + management — the partial open
  ownership signal; this RSV register is the restricted beneficial-ownership layer. No `sample_record.json`.
