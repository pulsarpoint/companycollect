# Kazakhstan Company Profile — Mapping

Kazakhstan keys everything on the **12-digit BIN (Business Identification Number)**. The
authoritative **open register** is **`gbd_ul`** on data.egov.kz (BIN, name, registration date,
legal address, OKED activity, director) — served via the data.egov.kz API but gated by a
**free API key**. The **KGD** (State Revenue Committee) adds **tax/VAT status** by BIN/IIN
(browser-public). **KASE** lists companies by **ISIN**. No registry per-company values were
captured (no key).

## Identifiers

- **BIN** — 12-digit; registration number, tax id, and universal join key (gbd_ul ↔ KGD).
- **ISIN** — `KZxxxxxxxxxx`; KASE listed securities (listed only).

## Mapping table

| Profile path | Source | Source path | Join key | Precedence | Notes |
|---|---|---|---|---|---|
| registration.bin | egov_gbd_ul | bin | yes | gbd_ul | free-API-key-gated |
| legal_identity.legal_name | egov_gbd_ul | name | no | gbd_ul > KGD | RU/KZ; KGD taxpayer_name alt |
| status.registration_date | egov_gbd_ul | registration_date | no | gbd_ul | Gregorian |
| status.taxpayer_status | kgd_taxpayer | taxpayer_status | no | KGD | TAX status, not registration |
| status.vat_registration | kgd_taxpayer | vat_registration | no | KGD | VAT (НДС) status |
| activity.oked_activity | egov_gbd_ul | oked_activity | no | gbd_ul | OKED classifier |
| registered_location.legal_address | egov_gbd_ul | legal_address | no | gbd_ul | at registration |
| officers[] | egov_gbd_ul | director_full_name | no | gbd_ul | **PERSONAL DATA — REDACT** |
| listing.ticker / listing.isin | kase_listed | ticker / isin | no | KASE | listed only; SPA |
| source_provenance[] | all | n/a | n/a | n/a | per-section provenance |

## Precedence and joins

- **Core identity / registration / activity / address / officers**: from **`gbd_ul`**
  (authoritative open register; free-API-key-gated). **Tax/VAT status**: from **KGD**
  (browser-public). **Listing**: from **KASE** (by ISIN).
- **Join**: gbd_ul ↔ KGD on the **BIN** (both use it); **KASE** joins by **name** (no BIN on
  the page). The **BIN** is canonical.
- **Keep two statuses clear**: gbd_ul provides **registration data** (registration date);
  liquidation/active-status and VAT come from **KGD** (`taxpayer_status` is tax status).
- **Language** Russian (+ Kazakh); **currency** KZT; **activity** OKED classifier.

## Missing / restricted

- **`gbd_ul` requires a free API key** → registration values are not captured here (obtain a
  key to ingest). The dataset is genuinely open, just registration-gated.
- **KGD** is **per-BIN search / per-list** (no single clean API) and includes individuals
  (personal data — redact).
- **KASE** populated listings are not cleanly available (SPA; no API confirmed).
- **Director name** (gbd_ul) and **individual taxpayers** (KGD) are personal data — redact.
- The **BIN** is public (registration/tax id), not personal data.
