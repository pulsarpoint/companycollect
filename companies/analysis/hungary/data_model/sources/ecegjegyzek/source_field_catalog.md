# e-cégjegyzék / Cégszolgálat — free company information Field Catalog

> Field model documented from the portal. **No `sample_record.json`**: full data is paid and no per-company
> record was copied. Officer/owner fields are **paid + planning-only**.

## Source Summary

- Country: Hungary
- Source type: official_registry
- Organization: Igazságügyi Minisztérium — Céginformációs Szolgálat
- URL: https://www.e-cegjegyzek.hu/
- License: free basic info; certified/full data paid
- Access: public (basic free; full paid)
- Freshness: real-time
- Record shape: company info page (free basic fields; full extract paid)
- Primary keys: `cegjegyzekszam`
- Join keys: `cegjegyzekszam`, `adoszam`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| cegjegyzekszam | cégjegyzékszám | Registration number | string | identifier | (not copied) | NN-NN-NNNNNN |
| name | cégnév | Legal name | string | legal_name | (not copied) | |
| legal_form | cégforma | Legal form | string | legal_form | (not copied) | Kft/Zrt/Nyrt/Bt |
| status | állapot | Status | string | status | (not copied) | bejegyezve/törölve |
| registered_seat | székhely | Registered seat | string | address | (not copied) | |
| main_activity | főtevékenység (TEÁOR) | Main activity | string | activity | (not copied) | TEÁOR |
| officers_owners[] | képviselők / tulajdonosok | Officers + owners | array | person | (paid) | **PAID; PII** |

## Interpretation Notes

- **The register-side identity.** e-cégjegyzék ("Cégszolgálat Ingyenes Céginformáció") provides **free basic**
  company info (name, cégjegyzékszám, seat, status, legal form, main TEÁOR activity). The **full/certified
  extract** (cégkivonat) — including **officers (képviselők), owners (tulajdonosok), share capital and history**
  — is **paid** via the Céginformációs Szolgálat or commercial resellers. Officer/owner fields are therefore
  **paid + planning-only** and **personal data (GDPR)**.
- **Identifiers.** `cégjegyzékszám` (NN-NN-NNNNNN) embeds the court and legal-form codes; `adószám` (8-digit
  base) is the cross-source join stem.
