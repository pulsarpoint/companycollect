# Source inventory — Montenegro

| Source | Type | Org | Access | Formats | Company-level | Status |
|---|---|---|---|---|---|---|
| CRPS (`eprijava.tax.gov.me/TaxisPortal`) | Official registry | Uprava prihoda i carina | Portal 503 (down); legacy crps.me parked | html | yes | unavailable |
| data.gov.me — Javna preduzeća | Public-enterprises list | Min. javne uprave | Public XLSX/CKAN | xlsx | yes (public enterprises only) | useful_secondary_source |
| data.gov.me portal (CKAN) | Open-data portal | Gov of Montenegro | Public | xlsx/csv/json | no (statistics) | useful_secondary_source |
| MONSTAT | Statistics | Uprava za statistiku | Public | xlsx/pdf | no (aggregate) | useful_secondary_source |

## Identifiers

- **PIB — Poreski identifikacioni broj** — tax id (8-digit).
- **Registarski / Matični broj** — CRPS registration number.
- **PDV broj** — VAT number (separate; if VAT-registered).

## Key facts

- The official register (**CRPS**) is the only company-level source, but its portal
  was **down (503)** and the legacy domain is **parked** — no open access at
  investigation time; **no open bulk/API**.
- `data.gov.me` is a **working** CKAN portal but **does not host the register** —
  only statistics, niche registers, and a **public-enterprises** list (real,
  openly licensed; used for the sample).
- **No open financials** — annual statements are filed at CRPS, not published.
- Currency **EUR**. Founders/owners are personal data → redact.
