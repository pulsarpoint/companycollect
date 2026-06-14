# Cyprus — Schema Notes

No per-company open record was downloadable here (data.gov.cy resource URL unresolved; financials paid);
fields below are from the documented DRCIP eSearch / open CSV (confirmed via OpenSanctions) and the HE32 +
audited financial statements. Join on the HE registration number.

## Identifiers
- **Registration number** — the company id; the **prefix encodes the entity type**:
  - **HE / ΗΕ** — εταιρεία (company, limited) — the main one (e.g. `HE 123456`).
  - **BN / ΕΕ** — business name (Εμπορική Επωνυμία).
  - **EE / ΟΕ / ΕΕ** — partnership (ομόρρυθμη / ετερόρρυθμη).
  - **AE / ΑΕΖ / S** — overseas company / other.
- **TIC** — Tax Identification Code (tax id), issued by the Tax Department.
- **VAT** — `CY` + 8 digits + a letter (e.g. `CY12345678X`) — **separate** from the HE number and TIC.
- Names are in **Greek and/or English** (the register holds both where available).

## DRCIP register (eSearch / open CSV) — documented fields
```
registration_number   - HE... (company id); prefix = entity type
name                  - organisation name (Greek/English)
type                  - company / business name / partnership / overseas company
status                - operational / struck-off / dissolved / under liquidation / ...
registration_date     - date of registration
registered_address    - registered office address
officers              - directors, secretary (named in the open CSV) [PII]
```
- The open data.gov.cy CSV set covers all registered organisations (~567k companies; ~2.75M entities incl.
  officers). It **names officers but not shareholders**.

## HE32 + audited financial statements — DOCUMENT-based (paid PDF)
The HE32 annual return + audited financial statements (scanned PDF, via the €10 detailed search) contain:
```
HE32 annual return:
  shareholders, directors, secretary, registered office, share capital (snapshot)
audited financial statements:
  balance sheet (assets, equity, liabilities), income statement (revenue, profit/loss),
  notes; auditor's report
fiscal year (accounting reference date)
```
- **NOT structured open data** — figures are inside scanned PDFs → require **OCR/parsing**, or a commercial
  provider. Currency **EUR**. Access is **paid (€10/detailed search)**.

## Mapping to internal company model
```
company_id          <- registration_number (HE...)
registration_number <- registration_number
tax_id              <- TIC (separate; not in the open CSV)
vat_id              <- CY VAT (separate; validate via VIES)
legal_name          <- name (keep Greek + English)
company_type        <- type (company/partnership/business name/overseas) + (Ltd/Plc from name)
status              <- status (operational/struck-off/dissolved/...)
incorporation_date  <- registration_date
registered_address  <- registered_address
municipality        <- from address (town/district)
region              <- district (Nicosia/Limassol/Larnaca/Paphos/Famagusta)
activity_code       <- not_available (no public NACE/activity code in the open register)
officers[]          <- officers (directors/secretary) [PII]
financials[]        <- HE32 audited statements (paid PDF; parse) | commercial provider
country             <- "Cyprus"
source_url/name/at, raw_record
```
See `normalized/companies.sample.jsonl` (schematic — no per-company open record was downloadable here).
