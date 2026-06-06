# PRH Open Data — YTJ API v3 Field Catalog

## Source Summary

- Country: Finland
- Source type: official_registry_api
- Organization: Finnish Patent and Registration Office (PRH) + Finnish Tax Administration (Business Information System / YTJ)
- URL: https://avoindata.prh.fi/opendata-ytj-api/v3/companies
- License: CC-BY-4.0 (attribution to PRH/YTJ required; redistribution allowed)
- Access: public, no authentication, no API key
- Freshness: daily
- Record shape: `{ totalResults, companies[] }`; each company nests arrays (`names`, `companyForms`, `registeredEntries`, `addresses`) and multilingual `descriptions[]` (`languageCode` 1=fi, 2=sv, 3=en)
- Primary keys: `businessId.value` (Y-tunnus)
- Join keys: `businessId.value`, `euId.value` (BRIS EUID)

Coverage figures below come from a **100-record live sample of 819,096 total**.
The sample is ordered by ascending Business ID, so it skews toward old/ceased
entities — real coverage of `addresses`/`website` is higher for active companies
than the sample percentages.

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| businessId.value | value | Finnish Business ID (Y-tunnus); national identifier & tax number | string | identifier | 0100130-4 | Primary + main join key; VAT = `FI`+8 digits |
| businessId.registrationDate | registrationDate | Date the Business ID was assigned | date | date | 1978-03-15 | Y-tunnus scheme began 1978-03-15 |
| businessId.source | source | Source/authority code for the ID | string | metadata | 3 | Undocumented code (low confidence) |
| euId.value | value | EU unique identifier (BRIS EUID) | string | identifier | FIFPRO.0100130-4 | Secondary join key; 18/100 |
| euId.source | source | Source code for EUID | string | metadata | 1 | Provenance |
| names[].name | name | Registered company/trade name | string | legal_name | Dynava Oy | Current = type 1 + null endDate |
| names[].type | type | 1=primary, 2=parallel, 3=auxiliary (aputoiminimi), 4=aux detail | string | legal_name | 1, 3 | type 3 = alternate brands |
| names[].registrationDate | registrationDate | Name registered date | date | date | 2022-01-21 | |
| names[].endDate | endDate | Name end date (null=current) | date | date | 2022-01-21 | Selects current name |
| names[].version | version | Name record version | integer | metadata | 2 | |
| names[].source | source | Name source code | string | metadata | 1 | Undocumented |
| mainBusinessLine.type | type | Industry (TOL/NACE) code | string | activity | 82200 | 92/100; absent for some |
| mainBusinessLine.descriptions[] | descriptions | Multilingual industry labels | array | activity | Activities of call centres | Pick languageCode 3 (en) |
| mainBusinessLine.typeCodeSet | typeCodeSet | TOL classification vintage | string | activity | TOIMI4, TOIMI2 | Mixed vintages in register |
| mainBusinessLine.registrationDate | registrationDate | Industry recorded date | date | date | 2026-01-01 | |
| companyForms[].type | type | Legal form code | string | legal_form | 16 | Current = null endDate |
| companyForms[].descriptions[] | descriptions | Multilingual legal-form labels | array | legal_form | Limited company / Osakeyhtiö | Oy, Asunto Oy, Ky, Ay, ry |
| companyForms[].endDate | endDate | Form end date (null=current) | date | date | 2005-12-19 | |
| companySituations[] | companySituations | Bankruptcy/liquidation/restructuring | array | status | (empty in sample) | Risk signal; sub-schema unobserved |
| registeredEntries[].type | type | Entry status (1=Registered,4=Ceased,80=VAT-liable,55=employer,41=prepayment) | string | status | 1, 80 | Read with `register` |
| registeredEntries[].descriptions[] | descriptions | Multilingual entry labels | array | status | VAT-liable for business activity | |
| registeredEntries[].register | register | 1=YTJ,4=Trade Register,5=Employer,6=VAT,7=Prepayment | string | status | 6 | Tax-registration flags |
| registeredEntries[].authority | authority | 1=Tax Admin, 2=PRH (inferred) | string | metadata | 1, 2 | |
| registeredEntries[].registrationDate/endDate | registrationDate, endDate | Entry validity window | date | date | 1994-06-01 | null endDate = active |
| website.url | url | Registered company website | string | metadata | www.dynava.fi | 6/100; only contact-type field |
| addresses[] | addresses | type 1=visiting, 2=postal; street/postCode/postOffices[] | array | address | Valimotie 17-19, 00380 HELSINKI | municipalityCode = geo key |
| tradeRegisterStatus | tradeRegisterStatus | **Real status**: 1=active, 4=ceased, 3=intermediate | string | status | 1, 4 | USE for active/ceased |
| status | status | Constant '2'; meaning unclear | string | status | 2 | DO NOT use as liveness |
| registrationDate | registrationDate | Trade Register registration (incorporation) | date | date | 1973-08-10 | Best incorporation date |
| endDate | endDate | Cessation/removal date (null=active) | date | date | 2005-12-19 | |
| lastModified | lastModified | Record last-updated timestamp | datetime | metadata | 2026-04-20T12:09:25 | Delta-crawl key |

## Interpretation Notes

### Status is the big trap
- `status` is a **constant `"2"`** across every record in the sample — for both
  active (Dynava, `tradeRegisterStatus=1`) and ceased (Artjärven, `tradeRegisterStatus=4`)
  companies. It does **not** indicate whether a company is alive.
- **`tradeRegisterStatus` is the real active/ceased flag** (1=active, 4=ceased), and
  it correlates perfectly with the top-level `endDate` (status 4 ⇔ endDate present).
- Code `3` appeared once (active, no endDate) — likely an intermediate/removal-pending
  state; meaning not confirmed.

### registeredEntries is the richest section
It encodes much more than "registered/ceased". Each entry names a **register**:
- `1` = Business Information System (YTJ)
- `4` = Trade Register (PRH)
- `5` = Employer Register (Tax) — entity is a registered employer
- `6` = VAT Register (Tax) — types `80` VAT-liable, `82` VAT on property rights, `83` agriculture/forestry, `V80` liability group member
- `7` = Prepayment Register (ennakkoperintärekisteri, Tax)

An active entry (null `endDate`) in register 6 means the company is currently VAT
registered; register 5 means a registered employer. These are valuable firmographic
flags derivable without any extra source. Register/authority code meanings are
**inferred from the entry descriptions in the sample** — confirm against the official
PRH code list before hard-coding.

### Names and forms are historical arrays
`names[]` and `companyForms[]` carry full history. The current value is the element
with a null `endDate` (and for names, `type=1`). `names[].type=3` are auxiliary trade
names (aputoiminimi) — alternate brands the entity trades under (e.g. Dynava's "Eniro",
"Sentraali", "Direktia").

### Addresses
`type=1` visiting, `type=2` postal. `postOffices[]` repeats the city per language
(HELSINKI / HELSINGFORS) sharing one `municipalityCode` (091 = Helsinki). Use
`municipalityCode` (Statistics Finland municipality code) as the geography join key.
Many ceased entities carry no address.

### Language and codes
Descriptions are trilingual (fi/sv/en). Numeric `source` codes are undocumented inline
and kept verbatim as provenance. Industry codes mix TOL vintages (`typeCodeSet`);
normalize before cross-company comparison.

### Not available in this source
Sole traders (toiminimi), email, phone, officers/board members, beneficial owners,
share capital, and financial statement figures are **not** in this companies endpoint.
Financials are a separate PRH digital-financial-statements API (see source inventory).
