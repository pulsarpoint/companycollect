# Malta — Schema Notes

No per-company open record was lawfully downloadable in bulk (MBR registry portals + data.gov.mt WAF-blocked;
documents/API paid). Fields below are documented from the MBR data model and the annual accounts. Join on the
**registration number** across sources.

## Identifiers
- **Registration number** — e.g. `C 12345`; the **prefix encodes the entity class** (**C** = companies / limited
  liability; partnerships and other forms use other prefixes). The register-side join key (numeric part after the
  letter).
- **VAT number** — `MT` + 8 digits; separate from the registration number (VIES/CFR).
- **Income Tax Registration Number / TIN** — separate tax id (not in the free register data).
- Language: **English** (Malta's administrative language).

## MBR company record — documented fields
```
registration_number  - C... (company id); prefix = entity class
name                 - company name
company_type         - e.g. Private Limited Liability Company (Ltd), Public (plc), partnership
status               - Active / Struck off / Liquidated / Dissolved
registration_date    - date of registration / incorporation
registered_address   - registered office address
officers             - directors and company secretary [PII; paid]
shareholders         - name, share type, degree of control [PII; paid]
financial_info       - annual accounts + annual return [paid]
```

## Annual accounts / annual return — document-based (paid)
```
balance sheet ; profit and loss account ; notes ; directors' report ; auditor's report
annual return (capital, shareholders, officers snapshot)
standard: IFRS, or GAPSME for small companies ; currency EUR ; abridged accounts for small companies
```
- Public but accessed as PAID documents (EUR 5-25), usually PDF. Structured figures need OCR/parse, the paid MBR
  API, or a commercial provider. Join on registration number.

## Mapping to internal company model
```
company_id          <- registration_number (C...)
registration_number <- registration_number
tax_id              <- income-tax TIN (separate; not in free register)
vat_id              <- MT + 8 digits (VIES/CFR; separate)
legal_name          <- name
company_type        <- company_type (Ltd/plc/partnership)
status              <- status (Active/Struck off/Liquidated)
incorporation_date  <- registration_date
registered_address  <- registered_address
municipality        <- from address (locality)
activity_code       <- not_available (no public NACE in the free register data)
officers[]          <- directors + secretary [PII; paid]
shareholders[]      <- shareholders (name, share type, control) [PII; paid]
financials[]        <- annual accounts (IFRS/GAPSME) + annual return [paid PDF; parse | API | provider] [EUR]
beneficial_owners[] <- UBO register (restricted; legitimate interest) [PII]
country             <- "Malta"
source_url/name/at, raw_record
```
See `companies/data/malta/normalized/companies.sample.jsonl` (schematic — no per-company open record was lawfully
downloadable in bulk here).
