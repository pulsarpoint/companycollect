# Brazil Improvements

## Current Gap

The implemented Brazil company-registry pipeline, `brazil_comp_rfb`, uses the
source slug `brazil_rfb` and gives strong registry coverage but weak financial
coverage. That is expected for this source: Receita Federal CNPJ open data is a
company-registration dataset. It contains legal identity, status, address, CNAE,
contact fields, Simples/MEI flags, partner-file data that we have deferred, and
registered share capital. It does not publish balance sheets, income statements,
revenue, profit, assets, liabilities, cash-flow statements, or employee counts.

Source evidence:

- Detailed source-by-source pull analysis:
  [brazil-financial-sources.md](brazil-financial-sources.md)
- Receita Federal CNPJ layout lists `CAPITAL SOCIAL DA EMPRESA` under
  `EMPRESAS`, plus registration, status, CNAE, address, phone, email, Simples,
  and partner fields. It does not list full accounting statements:
  https://www.gov.br/receitafederal/dados/cnpj-metadados.pdf
- Gov.br Consulta CNPJ describes the official online API as a cadastral lookup
  for legal entities. It returns fields such as CNPJ number, matrix/branch,
  business name, trade name, status, legal nature, opening date, CNAE, address,
  phone, email, share capital, and QSA:
  https://www.gov.br/conecta/catalogo/apis/consulta-cnpj
- SERPRO Consulta CNPJ similarly describes an HTTP REST service for basic
  cadastral information, not a financial-statement source:
  https://apicenter.estaleiro.serpro.gov.br/documentacao/consulta-cnpj/

## Why We Do Not Have Extensive Financial Data Yet

The problem is not that all Brazilian financial data is unavailable online. The
problem is source coverage and source grain:

1. Receita Federal CNPJ is broad but cadastral.
   It covers the national CNPJ registry at large scale, but only exposes
   registered share capital as a financial-like field.

2. CVM has open financial statement data, but only for regulated public/open
   companies.
   CVM DFP provides annual standardized financial statements for listed/open
   companies, including balance sheet, cash flow, equity changes, income
   statement, comprehensive income, and value-added statement. CVM says the
   latest five years are available in the active dataset, with historical files
   since 2010, ZIP resources, ODbL license, and weekly update for
   re-presentations:
   https://dados.cvm.gov.br/dataset/cia_aberta-doc-dfp

3. CVM ITR has quarterly financial data for the same public-company universe.
   The dataset contains the same major financial statement families for the last
   five years, with historical files since 2011 and weekly updates for
   re-presentations:
   https://dados.cvm.gov.br/dataset/cia_aberta-doc-itr

4. Private-company full accounts are not available from the RFB CNPJ public bulk
   source. If we need broad private-company credit risk, payment behavior,
   delinquency, or bureau-style scores, that appears to be a commercial-data
   problem rather than an open-data pipeline. Serasa Experian markets credit
   analysis, CNPJ consultation, portfolio management, and risk products, but this
   is a paid/commercial route, not a public bulk source:
   https://www.serasaexperian.com.br/

## What Should Be Improved

| Area | Improvement | Reason |
|---|---|---|
| Financial data | Extend the separate `brazil_fin_cvm` source with ITR. | Gives real annual and quarterly statements for public companies. |
| Financial coverage labeling | Mark RFB financial coverage as `share_capital_only`. | Prevents users from assuming revenue/profit data exists for all Brazilian companies. |
| Company join keys | Build a robust CVM-to-RFB join using CNPJ where available and CVM company identifiers where needed. | CVM and RFB have different source grains and identifiers. |
| NACE/company industries | Finish company-level `br_industries` materialization from native CNAE plus `br_cnae_to_nace`. | Current docs/design mention it, but the current asset list does not export a company industry table. |
| CNAE-to-NACE mapping | Expand `br_cnae_to_nace.csv` beyond the seed fixture. | Current mapping coverage is not production-complete. |
| Contact quality | Add metrics for malformed emails, accepted domains, rejected provider domains, and companies with contact rows. | Brazil has native contacts, but quality and derivation rules need visibility. |
| Website/domain data | Treat `br_websites` as email-derived domains, not verified websites. Add downstream verification if website presence matters. | RFB does not publish website URLs. |
| Partner data | Design a privacy-aware `Socios` ingestion path before using partner rows. | The file includes natural-person partner names, masked CPF, representative CPF, and age bands. |
| Translation | Add static English mappings for legal nature and status reason when complete fixtures are available. | Current export retains legal nature as Portuguese text. |
| Scheduling | Add a concrete monthly schedule/monitor once the source publication pattern is confirmed. | The current job is partitioned but not calendar-scheduled. |
| Source resilience | Keep the Casa dos Dados mirror, but add periodic checks against the official Receita source if it becomes directly browsable again. | The implementation currently depends on a mirror because the official historical path was inaccessible from the environment. |

## Recommended Financial Strategy

Keep `brazil_comp_rfb` as the broad national registry pipeline and use the
separate `brazil_fin_cvm` financial source:

- `brazil_comp_rfb`: all CNPJ entities/establishments, monthly full snapshot,
  share-capital-only financial field.
- `brazil_fin_cvm_dfp`: annual financial statements for public companies,
  yearly ZIP files, weekly refresh for re-presentations.
- `brazil_fin_cvm_itr`: quarterly financial statements for public companies,
  yearly ZIP files, weekly refresh for re-presentations.

Do not try to force CVM financials into the RFB ingestion stage. CVM has a
different cadence, schema, and coverage. It should produce separate financial
statement and financial metric tables, then join to `br_companies` where a CNPJ
match is available.
