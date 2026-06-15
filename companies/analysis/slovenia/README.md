# Company data sources for Slovenia (SI)

## Status

- Official bulk data: **found** — AJPES PRS (identity) + FURS tax-payer list (tax/VAT/activity), both open CSV.
- Official API: **found but credentialed** — AJPES `restPrsInfo` (registration required; not for mass download).
- Open data portal: **found** — OPSI / podatki.gov.si (AJPES + FURS publishers).
- License: **known** — both open datasets are **CC-BY 4.0**.
- Recommended ingestion path: **bulk CSV (PRS + FURS), joined on matična številka**.

## Best source

Two free official open datasets combine into a solid identity + tax profile,
joined on the **matična številka** (registration number):

1. **AJPES PRS — Poslovni register Slovenije** (via OPSI). The national business
   register: matična številka, full name, legal form, registrar, full address.
   **293,222** entities (all forms incl. s.p., associations). CSV (UTF-16),
   refreshed twice monthly. **CC-BY 4.0**.
2. **FURS — Seznam davčnih zavezancev (legal entities)** (Financial
   Administration, via OPSI). Adds **davčna številka** (tax number), **VAT
   status**, **SKD activity code**, name, address, tax office. **144,537** legal
   entities. CSV (ZIP, daily). **CC-BY 4.0**. VAT id = `SI` + davčna.

## Financial data

**Not available as open structured data.** AJPES **JOLP** publishes annual
reports (balance sheet, income statement, ~last 5 years) **free to view per
company**, but there is **no open bulk/API** for structured financials. The
structured financial database (**Fi=Po**) and **S.BON** credit ratings are
**paid** products. So financials are *publicly viewable but not openly
downloadable* — treat as view-only / paid.

## Next action

Ingest PRS + FURS CSVs (join on matična številka) for identity + tax/VAT/activity.
For financials, either use JOLP per-company (view-only, no bulk) or license Fi=Po.
The credentialed `restPrsInfo` API adds status/activity/history if AJPES access is
obtained.
