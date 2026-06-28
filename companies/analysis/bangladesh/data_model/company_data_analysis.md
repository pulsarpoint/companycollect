# Company Data Analysis For Bangladesh

## Summary

Bangladesh is a **hybrid**: an **open listed layer** plus a **gated/paid full register**. The
**Dhaka Stock Exchange (DSE)** is the cleanest open source — `company_listing.php` is a plain
parseable HTML index of **~640 listed instruments** (637 verified), and each company's
`displayCompany.php?name=<CODE>` page exposes Trading Code, Scrip Code, Sector, Authorized
Capital (mn), Paid-up Capital (mn), Listing Year, Market Category, and Type of Instrument —
keyed on the **DSE trading code** (currency BDT). The authoritative **full** register is
**RJSC** (Registrar of Joint Stock Companies and Firms), keyed on the **RJSC registration
number** — name search free, particulars/documents **pay-per-use** (planning-only; the site
had a TLS cert issue). The **NBR** adds tax identity (**BIN** for VAT, **e-TIN** for income
tax) via per-ID verification. There is **no single national identifier** shared across all
sources — they join by **name**. A rich **listed-company** profile is buildable openly; the
full company population requires RJSC (paid). Nothing was bypassed; the DSE sample uses real
data and no identifiers were fabricated.

## Sources Analyzed

| Source slug | Source name | Status | Access | License | Role |
|---|---|---|---|---|---|
| dse_listed | Dhaka Stock Exchange (DSE) | ready | browser-public (parseable HTML) | public disclosure | Listed companies: trading code, sector, capital, listing year |
| rjsc_register | RJSC Registrar | blocked_payment | name search free; documents paid | restricted | Authoritative register: RJSC no., entity type, status, directors |
| nbr_tax | National Board of Revenue (NBR) | insufficient_transport_info | per-BIN/TIN verification | restricted | Tax identity: BIN (VAT), e-TIN (income tax) |

(`cse_listed` overlaps DSE; `data_gov_bd` is statistics — not modeled as primary sources.)

## What Each Source Contributes

- **DSE** — the open, free layer: trading code, scrip code, company name, sector, authorized
  & paid-up capital (BDT mn), listing year, market category, instrument type. ~640 instruments
  (filter `type_of_instrument = Equity` for operating companies). No personal data.
- **RJSC** — the authoritative register: RJSC registration number, entity name, entity type
  (company/firm/society/trade org), status, registration (incorporation) date, registered
  address, authorized/paid-up capital, directors. Name search free; documents paid;
  planning-only. Directors are personal data — redact.
- **NBR** — **BIN** (VAT) and **e-TIN** (income tax) plus VAT status, by per-ID verification;
  covers individuals (personal data).

## Proposed Country Company Profile

A multi-identifier object (`rjsc_registration_number`, `dse_trading_code`, `bin`, `e_tin`)
with sections: `legal_identity` (name, entity type), `status` (RJSC registration_status +
NBR vat_status + RJSC incorporation_date + DSE listing_year), `activity` (DSE sector),
`registered_location` (RJSC), `officers` (RJSC, redacted), `listing` (DSE), and
`financial_statements` (DSE capital, BDT mn), each with `source_provenance`. The example is
anchored on a **real DSE-listed company** (The ACME Laboratories Limited / ACMELAB) with RJSC
fields null (paid) and the director `[REDACTED-PII]`.

## Join And Precedence Rules

- **No single national identifier**: listed companies use the **DSE trading code**, the
  register uses the **RJSC registration number**, tax uses **BIN/e-TIN**. All three join by
  **company name**; the RJSC number is canonical once obtained.
- **Precedence**: DSE for listed identity/sector/capital/listing (open); RJSC for full registry
  identity/status/officers (paid); NBR for tax identity.
- **Do not conflate dates**: DSE `listing_year` ≠ RJSC `incorporation_date`.
- **Language** English + Bangla; **currency** BDT (DSE capital in millions).

## Missing Or Restricted Data

- **The full register (RJSC) is gated/paid** → entity type, status, incorporation date,
  registered office, directors, all entity types, and the broader company population are
  **planning-only** (only the free name search is open).
- **DSE** covers **listed companies only** (~640).
- **NBR** is **per-BIN/TIN verification** (no bulk) and covers individuals (personal data — redact).
- **data.gov.bd** has no company register (statistics only).
- **Directors** (RJSC) and **individual taxpayers** (NBR) are personal data — redact.

## Common Mapper Notes

`company_id`/`registration_number` → RJSC number (paid); `tax_id` → e-TIN, `vat_id` → BIN
(NBR per-ID); `legal_name`/`activity_code`/`financials` → DSE (open, listed). `legal_form`,
`status`, `incorporation_date`, `officers`, `owners` are RJSC-paid. Only DSE is `ready`; RJSC
is `blocked_payment`; NBR is `insufficient_transport_info`.
