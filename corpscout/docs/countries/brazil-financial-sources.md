# Brazil Financial Sources Analysis

## Summary

Brazil has multiple financial-data sources, but they do not all solve the same
problem.

Use `brazil_comp_rfb` for broad national company coverage and only
`share_capital_amount_original`. Use CVM DFP/ITR for real structured financial
statements, but only for public/open companies regulated by CVM. Treat Central
de Balancos as a promising but harder second phase for private-company
documents, because public documents exist but a stable documented bulk API was
not found. Use Banco Central sources only for financial institutions. Use
Portal da Transparencia only as a government-revenue/procurement signal, not as
company accounts.

## Source Priority

| Priority | Source | Coverage | Structured? | Auth/payment | Recommendation |
|---|---|---|---|---|---|
| 1 | CVM DFP | Public/open companies, annual statements | Yes, CSV ZIP | Public, no auth | Implement first for annual financials. |
| 2 | CVM ITR | Public/open companies, quarterly statements | Yes, CSV ZIP | Public, no auth | Implement with DFP or immediately after. |
| 3 | CVM Cias Abertas Cadastro | Public/open company reference and join keys | Yes, CSV | Public, no auth | Required support table for DFP/ITR joins. |
| 4 | Receita Federal CNPJ | All CNPJ entities/establishments | Yes, CSV ZIP | Public, no auth | Already implemented; financial coverage is share capital only. |
| 5 | Central de Balancos SPED | Participant companies, including private-company publications | Mixed PDF/XBRL | Public UI; no confirmed bulk API | Investigate after CVM. |
| 6 | Banco Central financial institution data | BCB-supervised financial institutions | Mixed official files/pages | Public | Sector-specific enrichment only. |
| 7 | Portal da Transparencia | Federal contracts, procurement, spending, payments | API and CSV | Public API with token | Revenue/procurement signal, not financial statements. |
| 8 | Commercial credit bureaus | Credit risk and payment behavior | API/product-dependent | Paid | Use only with explicit commercial decision. |
| 9 | SPED ECD/ECF tax/accounting filings | Broad private-company filings | Not public in bulk | Restricted | Not pullable as open data. |

## 1. CVM DFP Annual Financial Statements

### What It Is

CVM DFP is the annual standardized financial statement filing for Brazilian
public/open companies. CVM describes DFP as a periodic electronic document filed
through Empresas.NET under CVM Resolution 80/22. The open dataset includes the
latest five years and historical files since 2010.

Official pages:

- Dataset page: https://dados.cvm.gov.br/dataset/cia_aberta-doc-dfp
- Direct data directory: https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/DFP/DADOS/
- Historical note: https://dados.cvm.gov.br/dataset/cia_aberta-doc-dfp

### Implementation Status

First DFP assets implemented under the `brazil_fin_cvm` Dagster group:
`brazil_fin_cvm_dfp_raw_archives_s3`, `brazil_fin_cvm_dfp_raw_duckdb`,
`brazil_fin_cvm_dfp_statement_rows_usd_duckdb`, and
`brazil_fin_cvm_dfp_raw_clickhouse`.

It is partitioned by year from `2010` through `2026`, downloads the raw
`dfp_cia_aberta_<year>.zip` archive into `source-brazil-cvm`, and skips the
download when `brazil_cvm/dfp/raw_archives/year=<year>/archive.zip` already
exists. Parsing and metric extraction are separate follow-up assets.

### What Can Be Pulled

Each yearly ZIP is named:

```text
dfp_cia_aberta_{YYYY}.zip
```

Example:

```text
https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/DFP/DADOS/dfp_cia_aberta_2026.zip
```

The ZIP contains one document index file plus statement-family CSV files:

| File pattern | Meaning | Grain |
|---|---|---|
| `dfp_cia_aberta_{YYYY}.csv` | Submitted DFP document index and download links | one row per company/document/version |
| `dfp_cia_aberta_BPA_con_{YYYY}.csv` | Consolidated balance sheet assets | account rows |
| `dfp_cia_aberta_BPA_ind_{YYYY}.csv` | Individual balance sheet assets | account rows |
| `dfp_cia_aberta_BPP_con_{YYYY}.csv` | Consolidated liabilities and equity | account rows |
| `dfp_cia_aberta_BPP_ind_{YYYY}.csv` | Individual liabilities and equity | account rows |
| `dfp_cia_aberta_DRE_con_{YYYY}.csv` | Consolidated income statement | account rows |
| `dfp_cia_aberta_DRE_ind_{YYYY}.csv` | Individual income statement | account rows |
| `dfp_cia_aberta_DFC_MD_con_{YYYY}.csv` | Consolidated cash flow, direct method | account rows |
| `dfp_cia_aberta_DFC_MD_ind_{YYYY}.csv` | Individual cash flow, direct method | account rows |
| `dfp_cia_aberta_DFC_MI_con_{YYYY}.csv` | Consolidated cash flow, indirect method | account rows |
| `dfp_cia_aberta_DFC_MI_ind_{YYYY}.csv` | Individual cash flow, indirect method | account rows |
| `dfp_cia_aberta_DMPL_con_{YYYY}.csv` | Consolidated changes in equity | account rows |
| `dfp_cia_aberta_DMPL_ind_{YYYY}.csv` | Individual changes in equity | account rows |
| `dfp_cia_aberta_DRA_con_{YYYY}.csv` | Consolidated comprehensive income | account rows |
| `dfp_cia_aberta_DRA_ind_{YYYY}.csv` | Individual comprehensive income | account rows |
| `dfp_cia_aberta_DVA_con_{YYYY}.csv` | Consolidated value added statement | account rows |
| `dfp_cia_aberta_DVA_ind_{YYYY}.csv` | Individual value added statement | account rows |
| `dfp_cia_aberta_composicao_capital_{YYYY}.csv` | Capital composition | document/company rows |
| `dfp_cia_aberta_parecer_{YYYY}.csv` | Auditor opinions/reports | document/company rows |

The statement CSVs have columns such as:

```text
CNPJ_CIA
DT_REFER
VERSAO
DENOM_CIA
CD_CVM
GRUPO_DFP
MOEDA
ESCALA_MOEDA
ORDEM_EXERC
DT_INI_EXERC
DT_FIM_EXERC
CD_CONTA
DS_CONTA
VL_CONTA
ST_CONTA_FIXA
```

Useful metrics can be derived from account codes and descriptions:

- revenue, usually from DRE account `3.01`
- gross profit, operating profit, financial result, tax, net income
- total assets from BPA
- current/non-current assets from BPA
- liabilities and equity from BPP
- cash-flow metrics from DFC direct/indirect
- value-added metrics from DVA
- equity changes from DMPL

### How To Pull

DFP can be pulled without auth:

```bash
curl -L -O https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/DFP/DADOS/dfp_cia_aberta_2026.zip
unzip -l dfp_cia_aberta_2026.zip
```

Implementation strategy:

1. Discover available years from the direct directory listing.
2. Download each `dfp_cia_aberta_{year}.zip`.
3. Preserve the raw ZIP with URL, retrieval timestamp, byte size, and SHA-256.
4. Load CSV files with semicolon delimiter and source encoding handling.
5. Store raw statement rows in a long table.
6. Derive normalized metric rows from fixed account codes.
7. Keep both consolidated (`con`) and individual (`ind`) rows; do not collapse
   them without an explicit metric rule.
8. Use `CNPJ_CIA` and `CD_CVM` as join keys.
9. Use `VERSAO` and document metadata to handle re-presentations.

Recommended tables:

```text
br_cvm_dfp_documents
br_cvm_dfp_statement_rows
br_cvm_dfp_metrics
```

Recommended partitioning:

- Yearly partitions by filing package year.
- A weekly refresh sensor for current five-year packages, because CVM updates
  files weekly for re-presentations.
- Historical packages since 2010 can be backfilled once, then periodically
  checksum-checked.

### Limitations

- Coverage is public/open companies, not all CNPJ companies.
- Account semantics differ by sector, especially banks/insurers.
- Values use `MOEDA` and `ESCALA_MOEDA`; metric extraction must preserve scale.
- Re-presentations mean latest version selection must be explicit.

## 2. CVM ITR Quarterly Financial Statements

### What It Is

CVM ITR is the quarterly information filing for public/open companies. CVM
describes it as a periodic electronic document under CVM Resolution 80/22. The
open dataset includes the latest five years and historical files since 2011.

Official pages:

- Dataset page: https://dados.cvm.gov.br/dataset/cia_aberta-doc-itr
- Direct data directory: https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/ITR/DADOS/

### What Can Be Pulled

Each yearly ZIP is named:

```text
itr_cia_aberta_{YYYY}.zip
```

Example:

```text
https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/ITR/DADOS/itr_cia_aberta_2026.zip
```

The file families mirror DFP:

```text
itr_cia_aberta_{YYYY}.csv
itr_cia_aberta_BPA_con_{YYYY}.csv
itr_cia_aberta_BPA_ind_{YYYY}.csv
itr_cia_aberta_BPP_con_{YYYY}.csv
itr_cia_aberta_BPP_ind_{YYYY}.csv
itr_cia_aberta_DRE_con_{YYYY}.csv
itr_cia_aberta_DRE_ind_{YYYY}.csv
itr_cia_aberta_DFC_MD_con_{YYYY}.csv
itr_cia_aberta_DFC_MD_ind_{YYYY}.csv
itr_cia_aberta_DFC_MI_con_{YYYY}.csv
itr_cia_aberta_DFC_MI_ind_{YYYY}.csv
itr_cia_aberta_DMPL_con_{YYYY}.csv
itr_cia_aberta_DMPL_ind_{YYYY}.csv
itr_cia_aberta_DRA_con_{YYYY}.csv
itr_cia_aberta_DRA_ind_{YYYY}.csv
itr_cia_aberta_DVA_con_{YYYY}.csv
itr_cia_aberta_DVA_ind_{YYYY}.csv
itr_cia_aberta_composicao_capital_{YYYY}.csv
itr_cia_aberta_parecer_{YYYY}.csv
```

The same core fields appear: `CNPJ_CIA`, `CD_CVM`, `DT_REFER`,
`DT_INI_EXERC`, `DT_FIM_EXERC`, `CD_CONTA`, `DS_CONTA`, `VL_CONTA`,
`ORDEM_EXERC`, `MOEDA`, `ESCALA_MOEDA`, and `VERSAO`.

### How To Pull

ITR can be pulled without auth:

```bash
curl -L -O https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/ITR/DADOS/itr_cia_aberta_2026.zip
unzip -l itr_cia_aberta_2026.zip
```

Implementation strategy is the same as DFP, but metrics are quarterly. Keep the
period fields intact:

- `DT_REFER`: document reference date
- `DT_INI_EXERC`: period start
- `DT_FIM_EXERC`: period end
- `ORDEM_EXERC`: comparative period marker

Recommended tables:

```text
br_cvm_itr_documents
br_cvm_itr_statement_rows
br_cvm_itr_metrics
```

Recommended partitioning:

- Yearly ZIP partitions for raw downloads.
- Metric rows partitioned or sorted by `DT_FIM_EXERC`.
- Weekly refresh for active packages because CVM updates for re-presentations.

### Limitations

- Coverage is public/open companies only.
- Quarterly values may be cumulative or period-specific depending on statement
  and account. Metric extraction must use period fields, not only `DT_REFER`.
- Sector-specific account trees need metric rules beyond a single universal DRE.

## 3. CVM Cias Abertas Cadastro

### What It Is

This is the CVM reference dataset for public/open companies. It is not a
financial statement dataset, but it is the best support source for joining CVM
financial statements to `br_companies`.

Official pages:

- Dataset page: https://dados.cvm.gov.br/dataset/cia_aberta-cad
- Direct CSV: https://dados.cvm.gov.br/dados/CIA_ABERTA/CAD/DADOS/cad_cia_aberta.csv

### What Can Be Pulled

The CSV includes company identity, CVM code, CNPJ, registration/status fields,
sector, address, investor-relations contact, responsible person, and auditor
information. Key fields include:

```text
CNPJ_CIA
DENOM_SOCIAL
DENOM_COMERC
DT_REG
DT_CONST
DT_CANCEL
SIT
CD_CVM
SETOR_ATIV
CATEG_REG
SIT_EMISSOR
EMAIL
CNPJ_AUDITOR
AUDITOR
```

### How To Pull

```bash
curl -L -O https://dados.cvm.gov.br/dados/CIA_ABERTA/CAD/DADOS/cad_cia_aberta.csv
```

Implementation strategy:

1. Download daily or before every CVM financial refresh into object storage.
2. Store raw CSV versions by content hash:
   `brazil_cvm/cad/raw_csv/sha256=<sha256>/cad_cia_aberta.csv`.
3. Store latest raw metadata at:
   `brazil_cvm/cad/raw_csv/latest/metadata.json`.
4. Load DuckDB from the stored raw CSV, not directly from the CVM URL.
5. Normalize `CNPJ_CIA` by stripping punctuation.
6. Join to `br_companies.cnpj_basico` by the first eight CNPJ digits and to
   `br_establishments.cnpj` by the full 14 digits.
7. Preserve `CD_CVM` as the stable CVM join key for DFP/ITR.
8. Store status, sector, category, and auditor for context.

Recommended table:

```text
br_cvm_companies
```

### Limitations

- Only CVM-regulated public/open companies.
- CNPJ may identify the issuer legal entity, while financial statements may
  include consolidated groups.

## 4. Receita Federal CNPJ

### What It Is

The RFB CNPJ public bulk source is the broad national company registry already
used by `brazil_comp_rfb`. It is excellent for identity, status, industry, address,
contact, and tax-regime data, but it is not a financial statement source.

Official/source pages:

- CNPJ metadata: https://www.gov.br/receitafederal/dados/cnpj-metadados.pdf
- Current implementation mirror: https://dados-abertos-rf-cnpj.casadosdados.com.br/

### What Can Be Pulled Financially

Only one financial-like field is currently useful:

```text
Empresas.capital_social -> share_capital_amount_original
```

This is registered share capital, not revenue, profit, assets, or balance-sheet
equity. It has no reporting period and should not be treated as a performance
metric.

### How To Pull

Already implemented through `brazil_comp_rfb` monthly full snapshots. For financial
classification, label it:

```text
financial_coverage = share_capital_only
```

### Limitations

- No revenue, profit, assets, liabilities, cash flow, statements, or employee
  counts.
- `Socios` is not a financial statement source and remains privacy-sensitive.

## 5. Central de Balancos SPED

### What It Is

Central de Balancos is a public SPED/Serpro platform intended to gather
accounting statements and published documents from participating entities. It is
important because it may contain private-company statements that CVM does not
cover.

Official pages:

- Public portal: https://www.gov.br/centraldebalancos/
- Serpro portal: https://centraldebalancos.estaleiro.serpro.gov.br/centraldebalancos/
- SPED/RFB page: https://sped.rfb.gov.br/item/show/4147

### What Can Be Pulled

Publicly visible examples show PDF document downloads under URLs like:

```text
https://centraldebalancos.estaleiro.serpro.gov.br/centralbalancos/servicesapi/api/Demonstracao/pdf/{id}
```

Public/secondary documentation describes the public area as a place where
documents can be consulted and downloaded, with possible PDF and XBRL
availability. Document types can include:

- balance sheet (`BP`)
- income statement (`DRE`)
- changes in equity (`DMPL`)
- retained earnings/loss statement (`DLPA`)
- cash-flow statement (`DFC`)
- value-added statement (`DVA`)
- comprehensive income (`DRA`)
- complete accounting statements (`DCC`)
- audit reports, management reports, notes, corporate acts, and meeting minutes

### How To Pull

Current recommendation: investigate before production ingestion.

Potential approach:

1. Use the public UI to identify search parameters by CNPJ, company name,
   document type, and reporting period.
2. Inspect network calls for documented or stable public endpoints.
3. If stable endpoints exist, build a bounded downloader by CNPJ or by explicit
   search result page. Do not crawl unbounded ID ranges.
4. Store PDFs/XBRL as raw documents.
5. Prefer XBRL where available; otherwise treat PDFs as OCR/table-extraction
   candidates.
6. Build source coverage metadata, because participation and document completeness
   will vary.

Recommended raw storage:

```text
source-brazil-central-balancos/raw_documents/cnpj=<cnpj>/year=<year>/
```

Recommended parsed tables, only after schema review:

```text
br_central_balancos_documents
br_central_balancos_xbrl_facts
br_central_balancos_pdf_extraction_candidates
```

### Limitations

- No confirmed documented bulk API yet.
- Coverage is not equivalent to all Brazilian companies.
- PDFs may be unstructured and expensive to parse reliably.
- XBRL availability appears document/company-dependent.
- Needs careful rate limiting and terms review before automation.

## 6. Banco Central Sources For Financial Institutions

### What It Is

Banco Central publishes sector-specific financial data for supervised financial
institutions. This is useful for banks, finance companies, credit institutions,
and other supervised entities, but not for ordinary companies.

Official pages:

- Balancetes e Balancos Patrimoniais:
  https://www.bcb.gov.br/estabilidadefinanceira/balancetesbalancospatrimoniais
- Central de Demonstracoes Financeiras do SFN:
  https://www.bcb.gov.br/estabilidadefinanceira/cdsfn

### What Can Be Pulled

Depending on the BCB source and file access pattern:

- monthly or quarterly trial balances / balance-sheet account balances
- supervised institution identifiers
- financial system account hierarchy up to the level published by BCB
- PDF/official financial statements from the CDSFN portal

### How To Pull

Current recommendation: keep this as a sector-specific enrichment after CVM.

Implementation steps:

1. Identify direct file URLs from the BCB pages or their backing APIs.
2. Map BCB institution identifiers and CNPJ to `br_companies`.
3. Store raw account-balance files separately from CVM because the account
   taxonomy is financial-institution-specific.
4. Produce bank-specific metrics, not generic company metrics.

Recommended tables:

```text
br_bcb_institutions
br_bcb_balance_rows
br_bcb_financial_institution_metrics
```

### Limitations

- Financial-institution coverage only.
- Different account taxonomy from CVM industrial/commercial issuers.
- Some BCB pages require JavaScript; direct file discovery must be part of the
  implementation task.

## 7. Portal da Transparencia

### What It Is

Portal da Transparencia is not a company financial-statement source. It is a
federal government transparency source for contracts, procurement, spending,
entities, sanctions, and related public-administration records.

Official pages:

- API: https://portaldatransparencia.gov.br/api-de-dados
- Bulk downloads: https://portaldatransparencia.gov.br/download-de-dados

### What Can Be Pulled

Relevant company-adjacent data may include:

- federal contracts
- procurement/licitacoes
- public expenses
- federal executive branch electronic invoices
- CEIS/CNEP sanctions
- convênios and other government transfers/relationships

### How To Pull

Two paths exist:

1. API, with token registration.
   The API is REST, requires token registration by email, and has rate limits.
   The portal documents 90 requests/minute during daytime and 300
   requests/minute overnight.

2. Bulk CSV downloads.
   The portal recommends bulk spreadsheet downloads for large-volume use.

Implementation strategy:

1. Use bulk CSV where possible.
2. Normalize supplier/payee CNPJ.
3. Join to `br_companies` by full CNPJ or `cnpj_basico`.
4. Store as revenue/procurement signals, not as accounting statements.

Recommended tables:

```text
br_transparencia_contracts
br_transparencia_payments
br_transparencia_supplier_signals
```

### Limitations

- Does not provide company-wide revenue or profit.
- Only captures government-facing activity.
- API requires token registration and rate-limit handling.

## 8. Commercial Credit Bureaus And Enrichment Providers

### What They Are

Brazil has commercial providers such as Serasa Experian that sell company credit
and risk data. These sources may answer questions that public sources do not,
such as credit risk, payment behavior, debt/protest signals, negative events, or
derived financial health.

Example:

- Serasa Experian: https://www.serasaexperian.com.br/

### What Can Be Pulled

Exact fields depend on contract and product. Treat these as paid enrichment
sources, not open-data feeds.

Potential field categories:

- credit/risk scores
- payment behavior
- debts/protests/negative events
- company registration enrichment
- ownership or group risk context

### How To Pull

Only after a commercial decision:

1. Get product documentation and contract terms.
2. Confirm permitted storage, redistribution, retention, and usage.
3. Build an authenticated API client with strict secrets handling.
4. Keep commercial data in separate tables with access controls.

### Limitations

- Paid.
- Contract terms may restrict storage and redistribution.
- Not suitable for open-source-like country ingestion by default.

## 9. SPED ECD/ECF And Tax Filings

### What They Are

SPED ECD/ECF and tax filings may contain rich accounting/tax data, but broad
company filings are not public bulk open data. Access normally belongs to the
taxpayer, authorized representatives, or government systems.

### What Can Be Pulled

For Corpscout open-data ingestion: nothing broad and lawful by default.

### How To Pull

Do not implement without explicit lawful access and a privacy/legal design.

### Limitations

- Not public open data.
- Likely contains sensitive tax/accounting information.
- Requires authorization and strict compliance controls.

## Recommended Implementation Plan

### Phase 1: CVM Structured Financials

Implement:

```text
brazil_fin_cvm_companies
brazil_fin_cvm_dfp_documents
brazil_fin_cvm_dfp_statement_rows
brazil_fin_cvm_dfp_metrics
brazil_fin_cvm_itr_documents
brazil_fin_cvm_itr_statement_rows
brazil_fin_cvm_itr_metrics
```

Metric extraction should start with:

| Metric | Source statement | Typical source account |
|---|---|---|
| revenue | DRE | `3.01` |
| net_income | DRE | account description/code review required |
| total_assets | BPA | root/total asset account |
| total_liabilities | BPP | root/total liability account |
| equity | BPP | equity account subtree |
| operating_cash_flow | DFC-MI or DFC-MD | account mapping required |
| value_added | DVA | account mapping required |

### Phase 2: CVM Quality And Sector Rules

- Build account-code dictionaries by statement family.
- Separate consolidated and individual metrics.
- Add sector-specific rules for banks/financial institutions.
- Track latest accepted version per `CNPJ_CIA`, `CD_CVM`, `DT_REFER`,
  statement family, consolidation type, and account code.

### Phase 3: Private-Company Document Investigation

Investigate Central de Balancos:

- search-by-CNPJ feasibility
- PDF/XBRL availability
- public endpoint stability
- terms/rate limits
- sample extraction quality

### Phase 4: Sector And Signal Enrichment

Add BCB for financial institutions and Portal da Transparencia for government
revenue/procurement signals only after the core CVM financials are stable.

## Open Questions

- Are Central de Balancos search endpoints documented or stable enough for
  production use?
- What exact account-code mapping should define canonical CVM metrics across
  sectors?
- Should CVM re-presentations overwrite previous metrics, or should all versions
  remain queryable with an `is_latest` flag?
- Should `brazil_fin_cvm` materialize both raw statement rows and wide metrics, or
  only long metric rows?
- How should banking/insurance issuers be handled when CVM and BCB account
  taxonomies diverge?
