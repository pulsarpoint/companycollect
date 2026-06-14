# Netherlands — Company Open Data Investigation

## Conclusion

The Netherlands is a **split-access** country. The **KvK (Kamer van Koophandel)** publishes two genuinely **open
(CC-BY 4.0)** datasets — basic company data and **structured annual accounts (jaarrekeningen)** — as EU
High-Value DataSets, but **both are anonymised in bulk** (no KvK number, name, address or directors). **Identified**
company data (names, addresses, officers) requires the **paid KvK Handelsregister API**; a company's basic data +
jaarrekening can be retrieved **by KvK number** via the **free HVDS open-data API** (with an API key). Everything
keys on the **KvK-nummer** (8 digits). **UBO** is restricted (post-CJEU).

## What was verified (live, with real downloads)

- **data.overheid.nl** (CKAN) — searching `KvK` returns the open datasets: `kvk-handelsregister-open-dataset-
  basis-bedrijfsgegevens` and `kvk-handelsregister-open-dataset-jaarrekeningen`, both **CC-BY 4.0**.
- **Basis bedrijfsgegevens** `kvk-open-dataset-basis-bedrijfsgegevens.zip` → HTTP 200, **12.7 MB zip / 95 MB CSV /
  1,891,639 records**. Columns (`;`-delimited): `Datum aanvang` (registration date, YYYYMMDD), `Actief` (J/N),
  `Insolventie`, `Rechtsvorm` (legal form, e.g. BV), `Postcode regio` (2-digit), `SBI activiteiten` (comma
  list), `Hoofdactiviteiten` (main SBI), `Lidstaat` (NL). **No KvK number / name / address** → anonymised.
- **Jaarrekeningen** `kvk-open-data-set-jaarrekeningen0.zip` → HTTP 200, **200 MB**, containing many individual
  `OpendataJaarrekening_{year}_{hash}.xml` files (one report each). Schema = `<opendata>` with key/value
  `opendataField`: `FinancialYear`, `DocumentAdoptionDate`, `SbiBusinessCode`, and `BalanceSheet` →
  `Assets` (Current/Noncurrent/Other), `Equity`, `EquityAndLiabilities`, `Liabilities`
  (MaturityLessThan/ExceedingOneYear), `Provisions`, `CalledUpShareCapital`,
  `BalanceSheetBeforeAfterAppropriationResults`. Real example: FinancialYear 2025, Assets 428763, Equity 275698,
  Liabilities 153065. **No KvK number / name** → anonymised. Split into multiple ZIPs (0..5+).
- **HVDS open-data API** `opendata.kvk.nl/api/v1/hvds/{basisbedrijfsgegevens|jaarrekeningen}/kvknummer/{kvknummer}`
  → live but **rate-limited (HTTP 429)** without an API key; the docs (developers.kvk.nl) confirm a **free API
  key** is required. Returns a company's data **by KvK number** (identified, but the open tier strips names/
  addresses/officers/UBO).
- **KvK Handelsregister API** (Zoeken/Basisprofiel/Vestigingsprofiel/Naamgeving) — the **paid** route to
  identified data (names, addresses, officers).

## Identifiers

- **KvK-nummer** — 8-digit Chamber-of-Commerce number; the company id / join key (required by the HVDS + paid
  APIs). **Not present in the open bulk** (anonymised).
- **RSIN** — 9-digit legal-entity/tax number (for rechtspersonen); the VAT base.
- **Vestigingsnummer** — 12-digit establishment number.
- **btw-nummer** (VAT) — `NL` + 9 digits (= RSIN for legal entities) + `B` + 2-digit suffix; via VIES/Belastingdienst.
- **SBI** — Standaard Bedrijfsindeling activity codes (NACE-aligned).

## Financial data model

- The **jaarrekeningen open dataset** is genuinely **structured** (XBRL-derived XML): balance-sheet figures
  (assets, equity, liabilities by maturity, provisions, called-up share capital) + financial year + adoption
  date + SBI. Income-statement detail is limited in the open micro/small filings. **CC-BY 4.0**, currency EUR.
- **But anonymised in bulk** — a jaarrekening cannot be linked to a named company from the bulk. The **HVDS
  jaarrekeningen API** returns a company's accounts **by KvK number** (free with a key) — the identified
  financial route. Most NL companies (BV) file micro/small abridged accounts (balance sheet only).

## Recommended ingestion

- **Anonymised bulk** (basis + jaarrekeningen) → statistics, financial benchmarks, SBI/legal-form analysis.
- **Identified per-company** → the **free HVDS API** (by KvK number, API key) for basic + financial data; the
  **paid KvK Handelsregister API** (Basisprofiel) or a **commercial provider** (Company.info, Graydon/CreditSafe)
  for names, addresses, officers + identified financials at scale.
- VAT via **VIES**; UBO only via authorised (AML) access.

## Risks / open questions

- **Anonymised open bulk**: no join key — cannot identify companies from the bulk; identification needs the API.
- **API key / rate limits**: HVDS API needs a free key and is rate-limited (429 observed).
- **Paid for identity**: names/addresses/officers via the paid KvK API or a vendor.
- **UBO** restricted (post-CJEU; AML-obliged entities; expanded via API from April 2026).
- **License**: open datasets are **CC-BY 4.0** (attribute KvK); the paid API has its own terms.
- **Volume**: jaarrekeningen ZIPs are ~200 MB each (multiple); basis CSV 95 MB — stream/chunk.
