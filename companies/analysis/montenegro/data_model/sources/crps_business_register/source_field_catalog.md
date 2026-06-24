# CRPS — Central Registry of Business Entities Field Catalog

## Source Summary

- Country: Montenegro
- Source type: official_registry
- Organization: Uprava prihoda i carina (Revenue and Customs Administration), tax.gov.me
- URL: https://eprijava.tax.gov.me/TaxisPortal (currently HTTP 503)
- License: not stated
- Access: **UNAVAILABLE** — current portal returns 503; legacy crps.me is parked
- Freshness: live when available
- Record shape: per-company lookup (no open bulk/API)
- Primary keys: PIB
- Join keys: PIB, registarski_broj

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| PIB | PIB | Tax id (8-digit) | string | identifier |  | primary id/join key |
| registarski_broj | Registarski/Matični broj | Registration number | string | identifier |  |  |
| PDV_broj | PDV broj | VAT number | string | identifier |  | separate |
| naziv | Naziv | Business name | string | legal_name |  |  |
| oblik_organizovanja | Oblik organizovanja | Legal form | string | legal_form | DOO/AD | DOO/AD/OD/KD |
| status | Status | Status | string | status |  | aktivno/likvidacija/stečaj/brisano |
| datum_registracije | Datum registracije | Registration date | date | date |  | dd.mm.yyyy |
| sjediste | Sjedište | Registered seat | string | address |  |  |
| sifra_djelatnosti | Šifra djelatnosti | Activity code | string | activity |  | KD ~NACE |
| osnivaci | Osnivači | Founders | array | ownership |  | PERSONAL DATA — redact |
| finansijski_izvjestaji | Finansijski izvještaji | Financial statements | array | financial |  | filed at CRPS; NOT open; EUR |

## Interpretation Notes

- **CRPS** is the authoritative Montenegro company register, run by the **Revenue
  and Customs Administration** (`crps@tax.gov.me`). It holds identity (name, **PIB**,
  registration number, legal form, status, founders) and is where companies file
  **annual financial statements**.
- **Access (verified live)**: the legacy domain **`crps.me`** serves a
  **domain-parking page**, and the current portal **`eprijava.tax.gov.me/TaxisPortal`**
  returned **HTTP 503 "Service Unavailable"** on every attempt. There is **no open
  bulk/API**. So the field model here is documented from public knowledge of CRPS;
  **no live values were captured** (example values empty).
- **Identifiers**: **PIB** (8-digit) is the tax id and primary key; **registarski
  broj** is the registration number; **PDV broj** (VAT) is separate.
- **Financials** are filed at CRPS but **not published as open data** — planning-only.
- **Personal data**: founders/representatives are personal data (Montenegro Law on
  Personal Data Protection) — redact. Currency **EUR**.
