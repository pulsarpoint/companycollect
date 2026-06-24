# Company data sources for Montenegro

## Status

- Official bulk data: **not found (open)** — the company register (CRPS) has no
  open bulk; its public portal was **down (503)** at investigation time and the
  legacy domain is parked
- Official API: **not found (open)** — no open company API
- Open data portal: **working** (`data.gov.me`, CKAN) but **does not host the
  company register** — only statistics, niche registers, and a public-enterprises
  list
- License: CRPS terms not stated; data.gov.me datasets are openly published
- Recommended ingestion path: **manual / per-company lookup via CRPS once the
  portal is back**; data.gov.me for a small public-enterprise list only

## Best source

**CRPS — Centralni registar privrednih subjekata** (Central Registry of Business
Entities), operated by the **Uprava prihoda i carina** (Revenue and Customs
Administration, `tax.gov.me`; contact `crps@tax.gov.me`). It is the authoritative
company register (identity, **PIB**, registration number, legal form, status,
founders, and the place where companies file **annual financial statements**).
However: the **legacy portal `crps.me` is now a parked domain**, and the current
portal **`eprijava.tax.gov.me/TaxisPortal` returned HTTP 503 (Service
Unavailable)** throughout this investigation. So there is **no working open access
point** right now and **no open bulk/API**.

## Financial data

Montenegrin companies file **annual financial statements** with the tax
administration (CRPS), but these are **not published as open data**. There is **no
open financial dataset**; `data.gov.me` carries only statistical aggregates
(construction activity, average wages, agriculture census).

## Open data that does exist

`data.gov.me` (CKAN, working) hosts a **"Javna preduzeća"** dataset (public/state
enterprises) from the Ministry of Public Administration — real entities with name,
status, type, founder, address, website (e.g. **Investiciono-razvojni fond Crne
Gore A.D.**, **Crnogorski elektroprenosni sistem AD**, **Luka Bar AD**, **Pošta
Crne Gore AD**, **Plantaže AD**). It is **not** the full register.

## Identifiers & tax

- **PIB — Poreski identifikacioni broj** — tax id (**8-digit**).
- **Registarski / Matični broj** — CRPS registration number.
- **PDV broj** — VAT number (separate; for VAT-registered entities).
- Currency: **EUR** (Montenegro uses the euro).

## Next action

Use **CRPS** per-company lookup once `eprijava.tax.gov.me/TaxisPortal` is back
online (keyed on PIB / name / registration number); there is no open bulk/API and
no open financials. Use `data.gov.me` only for the public-enterprises list. Treat
founders/owners as personal data and redact.
