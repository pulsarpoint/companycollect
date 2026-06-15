# Canada — Schema Notes

## Identifiers

- **Corporation number** — federal corporation id (7-digit), assigned by
  Corporations Canada. The federal primary key.
- **Business Number (BN)** — CRA 9-digit tax id; the closest to a universal
  Canadian business identifier (corporate income tax, GST/HST, payroll). GST/HST
  registration = **BN + RT** program-account suffix.
- **No separate VAT number** — GST/HST via the BN/RT account.
- Provincial corporations have their own provincial registry numbers (and may also
  have a BN).

## Corporations Canada — Federal Corporations CSV (17 columns)

| Column | Meaning |
|---|---|
| Corporation number | Federal corporation id (7-digit) — primary key |
| Business number (BN) | CRA 9-digit tax id (join/tax key) |
| Corporate name - form 1 | Primary corporate name (often English) |
| Corporate name - form 2 | Alternate corporate name (often French) |
| Governing legislation | e.g. "Canada Business Corporations Act" (CBCA) |
| Status | Active / (Inactive/Dissolved in the other files) |
| Anniversary date | Anniversary/incorporation reference date (YYYY-MM-DD) |
| Year of last annual filing | Compliance signal |
| Date of last annual meeting | YYYY-MM-DD |
| Street, Street 2, City/town, Province/territory, Country, Postal code | Full registered office address |
| Minimum/Maximum number of directors | Director counts (not names) |

Files: active/inactive × CBCA/non-CBCA × EN/FR (8 CSVs). Active CBCA = 642,720.
Covers **federally-incorporated** corporations only (CBCA business corps + non-CBCA
NFP/cooperatives/boards of trade).

## Corporations Canada API (adds)

Director **names**, registered office, status, corporate history — per corporation
number (real-time). Director names are personal data.

## SEDAR+ financials

Reporting-issuer financial statements / annual reports (PDF), per issuer. CAD;
IFRS / ASPE. Reporting issuers only.

## Mapping to internal model

| Internal | Canada source |
|---|---|
| company_id | Corporation number (federal) / provincial registry number |
| registration_number | Corporation number |
| tax_id | Business Number (BN) |
| vat_id | not_available (GST/HST via BN+RT) |
| legal_name | Corporate name - form 1 (or form 2) |
| company_type / legal_form | Governing legislation (CBCA = federal business corp) |
| status | Status (Active / Inactive / Dissolved) |
| incorporation_date | Anniversary date (≈) — exact via API/registry |
| dissolution_date | from the inactive/dissolved files / API |
| registered_address | Street + City + Province + Postal code + Country |
| activity_code | not_available (no NAICS in this dataset) |
| financials | SEDAR+ (reporting issuers); not in the register |
| officers | Corporations Canada API directors (PII; redact) |
| owners | not_available (no public beneficial-ownership register; ISED BO registry is being established) |

## Gotchas

- **Federal-only** coverage — provincially-incorporated companies need provincial
  registries (separate sources; ~13 jurisdictions).
- **Bilingual** names (form 1 / form 2).
- **No NAICS / activity code** in the federal dataset; **no financials**.
- **No VAT** — GST/HST via BN+RT.
- Director names (API) are personal data — redact.
