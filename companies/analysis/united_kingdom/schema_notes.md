# United Kingdom — Schema Notes

## Identifiers

- **Company number** — 8 characters; digits (England/Wales) or 2-letter prefix +
  6 digits (`SC` Scotland, `NI` Northern Ireland, `OC`/`SO` LLPs, …). Company id +
  universal join key (register ↔ accounts ↔ PSC ↔ API).
- **No tax id** in Companies House. VAT numbers are held by HMRC (separate).
- **SIC** — UK SIC 2007 activity code (basic data has up to 4, with text).

## Free Company Data Product (BasicCompanyData CSV, 55 columns)

Key columns:

| Column | Meaning |
|---|---|
| CompanyName | Registered name |
| CompanyNumber | 8-char company id (join key) |
| RegAddress.AddressLine1/2, PostTown, County, Country, PostCode, CareOf, POBox | Registered office address |
| CompanyCategory | Legal form (Private Limited Company, PLC, LLP, …) |
| CompanyStatus | Active / Dissolved / Liquidation / … |
| CountryOfOrigin | Country of origin |
| IncorporationDate / DissolutionDate | DD/MM/YYYY |
| Accounts.AccountRefDay/Month | Accounting reference date |
| Accounts.NextDueDate / LastMadeUpDate | Accounts filing dates |
| Accounts.AccountCategory | Account type (DORMANT, MICRO ENTITY, SMALL, FULL, …) |
| Returns.* / ConfStmt* | Annual return / confirmation statement dates |
| Mortgages.NumMortCharges/Outstanding/PartSatisfied/Satisfied | Charge counts |
| SICCode.SicText_1..4 | SIC code + text (e.g. "99999 - Dormant Company") |
| LimitedPartnerships.NumGenPartners/NumLimPartners | LP partner counts |
| URI | data.gov.uk company URI |
| PreviousName_1..10.CompanyName/CONDATE | Former names + change dates |

~5.9M live companies; part1 of 7 = 849,999 rows. Dates **DD/MM/YYYY**.

## Accounts Bulk Data (iXBRL)

- ZIP of `Prod223_<run>_<companynumber>_<madeupto>.html` (iXBRL) + some `.xml`
  (XBRL). One daily zip ≈ 9,717 filings.
- iXBRL = HTML with embedded XBRL facts (FRC/UK GAAP taxonomy). Key tags:
  `core:TurnoverRevenue`, `core:ProfitLoss`, `core:FixedAssets`,
  `core:CashBankOnHand`, `core:NetCurrentAssetsLiabilities`,
  `core:NetAssetsLiabilities`, `core:Equity`,
  `bus:UKCompaniesHouseRegisteredNumber`, `bus:EntityCurrentLegalOrRegisteredName`.
  Values in **GBP**; `ix:nonFraction` numeric facts (comma thousands).
- Join on **company number** (filename + `bus:UKCompaniesHouseRegisteredNumber`).
- Coverage: **electronically-filed** accounts only (~60–75%).

## PSC snapshot

- JSON bulk; per-company PSC entries: name, kind, `natures_of_control`,
  `notified_on`, nationality, partial address, month/year of birth. **Personal
  data**.

## REST API (free key)

- `company/{number}` (profile), `company/{number}/officers`,
  `.../persons-with-significant-control`, `.../filing-history`, `.../charges`,
  document API. Officers/PSC = personal data.

## Mapping to internal model

| Internal | UK source |
|---|---|
| company_id | CompanyNumber |
| registration_number | CompanyNumber |
| tax_id | not_available (CH has none) |
| vat_id | not_available (HMRC, separate) |
| legal_name | CompanyName |
| company_type / legal_form | CompanyCategory |
| status | CompanyStatus |
| incorporation_date | IncorporationDate |
| dissolution_date | DissolutionDate |
| registered_address | RegAddress.* |
| activity_code | SICCode.SicText_1..4 (SIC 2007) |
| financials | Accounts Bulk Data iXBRL (FRC taxonomy), join on number |
| officers | REST API officers (PII; redact) |
| owners | PSC snapshot / API (PII; redact) |
| previous_names | PreviousName_1..10 |

## Gotchas

- Dates **DD/MM/YYYY** in the CSV; ISO in the API.
- iXBRL is HTML — parse `ix:nonFraction`/`ix:nonNumeric`; multiple contexts
  (current/prior period) — pick the right period via the context ref.
- Accounts cover only e-filed (~60–75%); officers/PSC are personal data.
