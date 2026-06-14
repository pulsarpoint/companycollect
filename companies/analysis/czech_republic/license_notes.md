# Czech Republic — License & Terms Notes

> Czech company identity and structure are genuinely open data; financial statements are public and free but
> document-based (PDF). Confirm exact open-data terms per source and treat officer/shareholder DOB as personal data.

## ARES (Ministry of Finance)
- ARES publishes economic-subject data as **otevřená data** (open data) via the REST API and a bulk export on
  the MF open-data portal (`data.mf.gov.cz/topics/ares`). Reuse is intended to be open — **confirm the exact
  open-data terms / attribution** on the ARES open-data page before redistribution.
- **Rate limits**: documented in the range of tens of thousands of queries/day; aggressive use can get you
  blocked. For full-population needs prefer the bulk export over per-IČO API calls.

## Veřejný rejstřík — Justice open data (dataor.justice.cz)
- The Ministry of Justice publishes the public register as **open data** (CKAN portal): full and "actual"
  (Platný výpis) XML/CSV dumps per legal form / court / year. Downloading and reuse appear open.
- **Caveat**: the CKAN package `license_id` / `license_title` fields were **empty** for the inspected package
  — the licence is not machine-declared. **Confirm the reuse terms** with the Ministry of Justice / on the
  portal before redistribution.

## Sbírka listin — financial statements (účetní závěrka)
- Documents filed into the public register's collection of deeds are **public and free to view** at
  `or.justice.cz`. They are mostly **PDF** (native or scanned) — **not structured open data**. Bulk
  redistribution of the documents is not implied; treat figures as requiring OCR/parsing.

## ČSÚ RES
- The Czech Statistical Office's register of economic subjects is open data; attribute ČSÚ.

## Registr DPH / VIES
- VAT-payer lookup and VIES validate a supplied DIČ and expose the "unreliable payer" flag and registered bank
  accounts. **Validation/enrichment only** — not redistributable as a bulk list.

## NKOD / data.gov.cz
- Licence is **per dataset** (DCAT-AP). Check each dataset's licence field before reuse.

## Personal data / GDPR
- The Justice VR dumps include **officers** and (for a.s.) **shareholders** with **date of birth** —
  personal data. Apply a GDPR lawful basis + retention policy before persisting; do **not** reuse for direct
  marketing. Beneficial-ownership data (Evidence skutečných majitelů) is a separate, access-controlled register.

## Summary recommendation
- **Free to use (confirm exact open terms + attribute)**: ARES API/bulk, Justice VR bulk, ČSÚ RES, RŽP.
- **Free to view but PDF (not structured)**: Sbírka listin financial statements.
- **Validation only**: Registr DPH / VIES.
- **GDPR care**: officer/shareholder DOB in the VR dumps.
