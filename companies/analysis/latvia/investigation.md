# Latvia — Company Open Data Investigation

## Conclusion

Latvia is a **best-in-class fully-open** country (comparable to Estonia) — and under **CC0-1.0 (public
domain)**, so no attribution is even required and commercial use is allowed. The **Register of Enterprises
(Uzņēmumu reģistrs / UR)** publishes the company register, **structured financial statements**, **beneficial
owners**, officers, members/shareholders, equity capital and lifecycle events as open CSV (also XLSX / JSON /
Parquet / SQLite / PostgreSQL dump) on **data.gov.lv**. Everything joins on the **regcode** (11-digit
registration number).

## What was verified (live, with real downloads)

- **CKAN** `data.gov.lv/dati/lv/api/3/action/` — UR org slug `ur`; `package_search` returned **35 datasets**,
  including `uz` (register), `gada-parskatu-finansu-dati` (financials), `patiesie-labuma-guveji` (beneficial
  owners), `equity-capitals`, `maksatnespejas-procesi` (insolvency), `liquidations`, `reorganizatons`,
  `tiesibu-subjektu-vesturiskie-nosaukumi` (historical names). All **CC0-1.0**.
- **Register** `register.csv` → HTTP 200, **128 MB**, **485,134 entities**. Columns: `regcode`, `sepa`, `name`
  (+ parsed name parts), `regtype`/`regtype_text` (e.g. K = Komercreģistrs), `type`/`type_text` (SIA =
  Sabiedrība ar ierobežotu atbildību / Ltd; AS = plc; IK = sole trader), `registered`, `terminated`, `closed`,
  `address`, `index` (postcode), `addressid`, `region`, `city`, `atvk` (territory code), `reregistration_term`.
- **Financial statements** `financial_statements.csv` → HTTP 200, **204 MB**, **1,970,094 annual reports**.
  Columns: `id`, `file_id`, `legal_entity_registration_number` (= regcode, join key), `source_schema`,
  `source_type`, `year`, `year_started_on`, `year_ended_on`, **`employees`**, `rounded_to_nearest`, `currency`
  (EUR; historical LVL), `created_at`.
  - **balance_sheets.csv** (sampled): `statement_id`, `file_id`, cash, marketable_securities,
    accounts_receivable, inventories, total_current_assets, investments, fixed_assets, intangible_assets,
    total_non_current_assets, **total_assets**, current_liabilities, non_current_liabilities, provisions,
    **equity**, total_equities.
  - **income_statements.csv** (sampled): `statement_id`, `file_id`, **net_turnover**, by_nature_* / by_function_*
    expense breakdowns, gross_profit, interest_expenses, income_before_income_taxes, income_after_income_taxes,
    **net_income**.
  - **cash_flow_statements.csv** also present.
- **Beneficial owners** `beneficial_owners.csv` (open CC0) — reachable; not fully downloaded.

## Identifiers

- **regcode** (reģistrācijas numurs) — 11-digit registration number; the universal join key (also the
  financial-statements `legal_entity_registration_number`).
- **VAT** (PVN reģistrācijas numurs) = `LV` + the 11-digit regcode; not in the register CSV → VIES/VID.
- **SEPA** id (e.g. `LV95ZZZ40103550818`) — present in the register.
- **ATVK** (`atvk`) — administrative-territory classification code.
- **type** — legal form code (SIA, AS, IK, …).

## Financial data model

Four joined CSVs, all CC0:

```
financial_statements (id, file_id, legal_entity_registration_number=regcode, year, period, employees, currency)
   ├─ balance_sheets    (statement_id, file_id, ... total_assets, equity, liabilities ...)
   ├─ income_statements (statement_id, file_id, net_turnover ... net_income)
   └─ cash_flow_statements (statement_id, file_id, ...)
```

Join: `balance_sheets.statement_id` = `financial_statements.id` (and `file_id`); then
`financial_statements.legal_entity_registration_number` → `register.regcode`. Currency EUR (historical LVL pre-
2014). Genuine **structured** open financial data (not PDF), with **employee counts**.

## Recommended ingestion

Bulk-first: load the register (keyed on regcode) + the four financial-statement CSVs (join via statement_id /
file_id and regcode) + beneficial owners + officers/members/equity. Use the CKAN API for updates. CC0 — no
attribution required; treat owner/officer personal data under GDPR. VAT = LV+regcode (VIES/VID).

## Risks / open questions

- **GDPR**: beneficial owners, officers and members (natural persons) are personal data — lawful basis +
  retention; no direct-marketing reuse. (CC0 governs IP, not data protection.)
- **Volume**: financials CSVs are large (200+ MB) — stream/chunk.
- **Currency**: EUR now; pre-2014 reports may be LVL.
- **statement_id vs file_id**: both link the statement parts; confirm the canonical join on first ingest.
- **VAT** is not in the register CSV (derive `LV`+regcode; validate via VIES).
