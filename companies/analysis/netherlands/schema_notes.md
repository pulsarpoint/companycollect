# Netherlands — Schema Notes

The KvK publishes two open (CC-BY 4.0) datasets — basic company data and structured annual accounts — but both
are **anonymised in bulk** (no KvK number). Identified data keys on the **KvK-nummer** (via the HVDS API by KvK
number, or the paid Handelsregister API).

## Identifiers
- **KvK-nummer** — 8-digit Chamber-of-Commerce number; the company id / join key. Required by the HVDS + paid
  APIs; **not in the open bulk** (anonymised).
- **RSIN** — 9-digit legal-entity / tax number (rechtspersonen); the VAT base.
- **Vestigingsnummer** — 12-digit establishment number.
- **btw-nummer** (VAT) — `NL` + 9 digits (= RSIN for legal entities) + `B` + 2-digit suffix (VIES/Belastingdienst).
- **SBI** — Standaard Bedrijfsindeling activity code (NACE-aligned).

## Basis bedrijfsgegevens (open bulk CSV; `;`-delimited, UTF-8) — observed fields
```
Datum aanvang        - registration / start date (YYYYMMDD)
Actief               - active (J/N)
Insolventie          - insolvency indicator (blank if none)
Rechtsvorm           - legal form (BV, NV, EZ/eenmanszaak, VOF, Stichting, ...)
Postcode regio       - 2-digit postcode region (no full address)
SBI activiteiten     - comma-separated SBI activity codes
Hoofdactiviteiten    - main SBI activity
Lidstaat             - member state (NL)
```
1,891,639 records. **Anonymised** (no KvK number, name, address, directors). HVDS API returns the same by a
supplied KvK number (free with key).

## Jaarrekeningen (open bulk XML; XBRL-derived) — observed fields
```
<opendata> with opendataField key/value:
FinancialYear, DocumentAdoptionDate, SbiBusinessCode
BalanceSheet:
  BalanceSheetBeforeAfterAppropriationResults (Na/Voor)
  Assets, AssetsCurrent(+Other), AssetsNoncurrent(+Other)
  Equity, EquityAndLiabilities
  Liabilities, LiabilitiesMaturityLessThanOneYear, LiabilitiesMaturityExceedingOneYear
  Provisions, CalledUpShareCapital
```
One XML file per deposited report; split across ZIPs 0..5+. **Anonymised** (no KvK number/name). Currency EUR.
Income-statement detail limited (most BV file micro/small abridged accounts = balance sheet only). HVDS API
returns a company's jaarrekening by KvK number.

## KvK Handelsregister API (paid) — identified fields
```
kvkNummer, rsin, vestigingsnummer, naam/handelsnamen, rechtsvorm, adressen (registered + visiting),
sbiActiviteiten, statutaire naam, functionarissen (officers; Basisprofiel)
```

## Mapping to internal company model
```
company_id          <- KvK-nummer (8 digits)  [identified: HVDS API by number / paid API; NOT in open bulk]
registration_number <- KvK-nummer
tax_id              <- RSIN (9 digits)
vat_id              <- NL + RSIN + B + 2 (VIES; legal entities)
legal_name          <- naam (PAID KvK API / provider; stripped from open data)
company_type        <- Rechtsvorm (open) / rechtsvorm (API)
status              <- Actief (open) ; + insolvency
incorporation_date  <- Datum aanvang (open)
dissolution_date    <- (in open dataset variant / API)
registered_address  <- adressen (PAID API; open has only postcode region)
municipality/region <- Postcode regio (open) / address (API)
activity_code       <- SBI activiteiten (open + API)
officers[]          <- functionarissen (PAID API) [PII]
financials[]        <- jaarrekeningen (Assets/Equity/Liabilities/Provisions/share capital; XBRL) [EUR; open bulk anonymised | HVDS API by KvK number]
beneficial_owners[] <- UBO-register (restricted; AML) [PII]
country             <- "Netherlands"
source_url/name/at, raw_record
```
See `companies/data/netherlands/normalized/companies.sample.jsonl` (real anonymised basis row + real jaarrekening
figures; company_id null because the bulk is anonymised).
