# Latvia — Schema Notes

One authoritative open source (UR Register of Enterprises) with ~35 CC0 datasets, all keyed on **regcode**.
Register, **structured financial statements**, beneficial owners, officers/members are all open.

## Identifiers
- **regcode** (reģistrācijas numurs) — 11-digit registration number; the universal join key. In the financials
  it is `legal_entity_registration_number`.
- **VAT** (PVN reģistrācijas numurs) = `LV` + the 11-digit regcode; not in the register CSV → VIES/VID.
- **SEPA** id (e.g. `LV95ZZZ40103550818`) — in the register.
- **ATVK** (`atvk`) — administrative-territory classification code.
- **statement_id** / **file_id** — link the financial statement parts.

## Register (register.csv) — observed fields (`;`-delimited, UTF-8)
```
regcode                 - registration number (company id)
sepa                    - SEPA creditor identifier
name                    - full legal name
name_before_quotes / name_in_quotes / name_after_quotes / without_quotes - parsed name components
regtype / regtype_text  - register type code/text (e.g. K = Komercreģistrs / Commercial register)
type / type_text        - legal form (SIA=Sabiedrība ar ierobežotu atbildību/Ltd, AS=plc, IK=sole trader, ...)
registered              - registration date (YYYY-MM-DD)
terminated              - termination/dissolution date (if any)
closed                  - closed flag (L = liquidated, etc.)
address                 - registered address (free text)
index                   - postcode
addressid               - address registry id
region / city           - region/city codes
atvk                    - administrative-territory code
reregistration_term     - re-registration term
```
485,134 entities.

## Financial statements (gada pārskatu finanšu dati) — STRUCTURED, four CSVs
### 1. financial_statements.csv (report metadata) — 1,970,094 rows
```
id, file_id, legal_entity_registration_number (=regcode), source_schema, source_type,
year, year_started_on, year_ended_on, employees, rounded_to_nearest, currency (EUR/LVL), created_at
```
### 2. balance_sheets.csv (Bilances)
```
statement_id, file_id, cash, marketable_securities, accounts_receivable, inventories, total_current_assets,
investments, fixed_assets, intangible_assets, total_non_current_assets, total_assets,
future_housing_repairs_payments, current_liabilities, non_current_liabilities, provisions, equity, total_equities
```
### 3. income_statements.csv (Peļņas vai zaudējumu aprēķini)
```
statement_id, file_id, net_turnover, by_nature_* (inventory_change/material/labour/depreciation...),
by_function_* (cost_of_goods_sold/gross_profit/selling/administrative...), other_operating_*,
equity_investment_earnings, interest_expenses, income_before_income_taxes, provision_for_income_taxes,
income_after_income_taxes, net_income
```
### 4. cash_flow_statements.csv (Naudas plūsmas pārskati)
```
statement_id, file_id, ... (operating/investing/financing cash flows)
```
Join: `balance_sheets.statement_id = financial_statements.id` (and file_id); then
`financial_statements.legal_entity_registration_number -> register.regcode`. Currency EUR (pre-2014 LVL).

## Other open datasets (CC0; PII where persons)
```
amatpersonas (officers), dalībnieki (members/shareholders), equity-capitals (pamatkapitāls + investments),
patiesie-labuma-guveji (beneficial owners), maksatnespejas-procesi (insolvency), liquidations, reorganizatons,
tiesibu-subjektu-vesturiskie-nosaukumi (historical names), sanctions
```

## Mapping to internal company model
```
company_id          <- regcode
registration_number <- regcode
tax_id              <- (none separate; VAT below)
vat_id              <- LV + regcode (VIES/VID; not in register)
legal_name          <- name
company_type        <- type_text (+ type code: SIA/AS/IK)
status              <- derived (registered unless terminated/closed)
incorporation_date  <- registered
dissolution_date    <- terminated
registered_address  <- address (+ index postcode, atvk)
municipality/region <- city/region/atvk
activity_code       <- not_in_register_csv (NACE via other UR/CSP datasets if needed)
financials[]        <- financial_statements + balance_sheets + income_statements + cash_flow (join statement_id; EUR) [incl. employees]
officers[]          <- amatpersonas [PII]
members[]           <- dalībnieki [PII]
beneficial_owners[] <- patiesie labuma guvēji [PII]
country             <- "Latvia"
source_url/name/at, raw_record
```
See `companies/data/latvia/normalized/companies.sample.jsonl` (real record: SIA, regcode 40103550818, with latest financials).
