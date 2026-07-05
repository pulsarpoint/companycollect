# Brazil Financial Sources

## Purpose

This document describes the intended source model for
`dagster_v3.defs.brazil_financial`.

`brazil_financial` is the country financial domain. `cvm` is currently the
first source package inside that domain. DFP, ITR, FRE, and FCA are not separate
source providers; they are separate CVM document datasets with different
business meaning.

Recommended naming:

```text
dagster_v3.defs.brazil_financial.cvm

brazil_fin_cvm_dfp_*
brazil_fin_cvm_itr_*
brazil_fin_cvm_fre_*
brazil_fin_cvm_fca_*
```

Keep DFP, ITR, FRE, and FCA under `brazil_fin_cvm` because they come from the
same regulator, use the same issuer identifiers, and are published in the same
yearly ZIP-file style.

## Source Hierarchy

```text
Brazil financial domain
  CVM source provider
    DFP - annual accounting statements
    ITR - quarterly/interim accounting statements
    FRE - reference form, financial/context enrichment
    FCA - cadastral form, company metadata and contacts
    Cias Abertas Cadastro - support reference table for issuer identity
  Future sources
    Central de Balancos - private-company document investigation
    Banco Central - financial-institution sector data
    Portal da Transparencia - government contract/revenue signals
```

## Common CVM Rules

The CVM document datasets are public open ZIP resources. They do not need
authentication. The official dataset pages state weekly update periodicity, but
that does not mean there is a weekly accounting-period feed. It means CVM may
republish yearly ZIP packages when companies submit or re-submit filings.

Use this refresh strategy:

| Dataset age | DFP | ITR | FRE | FCA |
|---|---|---|---|---|
| Current year | weekly | weekly | weekly after implemented | weekly after implemented |
| Previous 5 years | weekly or monthly, depending on cost | weekly or monthly, depending on cost | monthly | monthly |
| Older history | quarterly or manual checksum refresh | quarterly or manual checksum refresh | manual/backfill only | manual/backfill only |

Raw download strategy:

1. Treat each CVM yearly ZIP as the natural raw partition.
2. Store each raw ZIP in object storage with stable object keys.
3. Skip HTTP download if the object key already exists.
4. Store metadata next to the archive: source URL, byte size, SHA-256,
   source last-modified header when available, and sync timestamp.
5. Parse CSV files into DuckDB.
6. Export raw normalized tables to ClickHouse.
7. Build canonical financial metrics as a separate layer after raw DFP and ITR
   are stable.

## DFP - Annual Financial Statements

Official source:

- Dataset: https://dados.cvm.gov.br/dataset/cia_aberta-doc-dfp
- Direct files: https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/DFP/DADOS/

What it is:

DFP is the annual standardized financial statement filing for CVM-regulated
public/open companies. It is the primary source for annual accounting history.

What we pull:

| Family | Meaning | Use |
|---|---|---|
| DFP document index | filing/document metadata and source document links | document versioning and joins |
| BPA | balance sheet assets | total assets, current assets, cash candidates |
| BPP | liabilities and equity | liabilities, equity, debt candidates |
| DRE | income statement | revenue, operating result, net income |
| DFC-MD | cash flow, direct method | operating/investing/financing cash flow when present |
| DFC-MI | cash flow, indirect method | operating/investing/financing cash flow when present |
| DMPL | changes in equity | equity movement context |
| DRA | comprehensive income | comprehensive income context |
| DVA | value added statement | value-added enrichment |
| composicao_capital | capital composition | share/capital context |
| parecer | auditor reports/opinions | audit/reporting context, not first metrics |

Current implementation:

```text
brazil_fin_cvm_dfp_raw_archives_s3
brazil_fin_cvm_dfp_raw_duckdb
brazil_fin_cvm_dfp_statement_rows_usd_duckdb
brazil_fin_cvm_dfp_raw_clickhouse
```

Current raw storage and schema choices:

```text
bucket: source-brazil-cvm
raw key: brazil_cvm/dfp/raw_archives/year=<year>/archive.zip
DuckDB file: data/brazil_cvm_source.duckdb
DuckDB schema: brazil_cvm
ClickHouse tables: br_cvm_dfp_*
```

How DFP relates to other sources:

- DFP is annual. It should feed annual metrics.
- ITR is the quarterly companion to DFP. It should feed quarterly metrics and
  latest financial trends before the annual DFP exists.
- FRE can enrich DFP metrics with debt, dividends, capital structure, and
  management-comment context.
- FCA/CVM cadastro can enrich DFP issuer identity, investor-relations contacts,
  securities, and auditor metadata.

## ITR - Quarterly/Interim Financial Statements

Official source:

- Dataset: https://dados.cvm.gov.br/dataset/cia_aberta-doc-itr
- Direct files: https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/ITR/DADOS/

What it is:

ITR is the quarterly information filing for CVM-regulated public/open
companies. It has the same statement-family idea as DFP, but covers interim
periods instead of the annual filing.

What we should pull:

| Family | Meaning | Use |
|---|---|---|
| ITR document index | filing/document metadata and source document links | document versioning and joins |
| BPA | quarterly balance sheet assets | latest assets/cash candidates |
| BPP | quarterly liabilities and equity | latest liabilities/equity/debt candidates |
| DRE | quarterly income statement | revenue and income trend |
| DFC-MD | cash flow, direct method | quarterly cash-flow trend when present |
| DFC-MI | cash flow, indirect method | quarterly cash-flow trend when present |
| DMPL | changes in equity | interim equity movement context |
| DRA | comprehensive income | interim comprehensive income context |
| DVA | value added statement | interim value-added enrichment |
| composicao_capital | capital composition | share/capital context |
| parecer | review/auditor reports | reporting context, not first metrics |

Implementation idea:

Use the DFP implementation pattern, parameterized enough to avoid copying parser
logic where the file structure is genuinely identical:

```text
brazil_fin_cvm_itr_raw_archives_s3
brazil_fin_cvm_itr_raw_duckdb
brazil_fin_cvm_itr_statement_rows_usd_duckdb
brazil_fin_cvm_itr_raw_clickhouse
```

Expected raw storage:

```text
bucket: source-brazil-cvm
raw key: brazil_cvm/itr/raw_archives/year=<year>/archive.zip
DuckDB file: data/brazil_cvm_source.duckdb
DuckDB schema: brazil_cvm
ClickHouse tables: br_cvm_itr_*
```

How ITR relates to DFP:

- Do not pull ITR for historical annual values if DFP already answers the annual
  question better.
- Pull ITR for quarterly trend and freshness.
- Use DFP for annual metrics.
- Use ITR for quarterly metrics and latest available periods.
- Keep both datasets because they answer different time-grain questions.

Important metric caveat:

ITR values may be cumulative or period-specific depending on statement family,
period fields, and account. The normalized metrics layer must use
`DT_INI_EXERC`, `DT_FIM_EXERC`, `DT_REFER`, `ORDEM_EXERC`, filing version, and
statement family; it must not infer quarter values from `DT_REFER` alone.

## Normalized Metrics Layer

Purpose:

The raw DFP/ITR statement rows are long accounting rows. Query users should not
need to know CVM account codes for common facts such as revenue or total assets.
The normalized metrics layer should map raw account rows into canonical metric
names.

Target metric examples:

| Metric | Main source | Statement family | Notes |
|---|---|---|---|
| revenue | DFP + ITR | DRE | start with fixed account mappings, then sector review |
| net income | DFP + ITR | DRE | account mapping must handle parent/child variants |
| total assets | DFP + ITR | BPA | usually balance-sheet total assets |
| total liabilities | DFP + ITR | BPP | mapping must distinguish liabilities from equity |
| equity | DFP + ITR | BPP | equity subtree |
| operating cash flow | DFP + ITR | DFC-MD or DFC-MI | prefer reliable statement/account mapping |
| cash and equivalents | DFP + ITR | BPA | mapping requires account-code/description review |
| debt | DFP + ITR + FRE | BPP/FRE | only if reliably mappable |

Implemented output view:

```text
br_cvm_financial_metrics
```

This is a ClickHouse view, not a Dagster/DuckDB materialized asset. It is
defined by migration `000095_corpscout_br_cvm_financial_metrics` and reads the
exported raw statement rows from:

```text
br_cvm_dfp_statement_rows
br_cvm_itr_statement_rows
```

Implemented grain:

```text
source_dataset
cnpj
cnpj_basico
cvm_code
reference_date
period_start_date
period_end_date
period_type
consolidation_type
metric_name
currency
amount_original
amount_usd
source_statement_code
source_account_code
source_filing_version
is_latest_version
```

The view keeps source lineage in string columns on
`br_cvm_financial_metrics`:

- `source_statement_record_ids`
- `source_account_codes`
- `source_account_descriptions_original`
- `source_archive_keys`
- `source_file_names`

Initial mapped metrics:

| Metric | Main account mapping | Notes |
|---|---|---|
| revenue | DRE `3.01` | revenue from goods/services |
| net income | DRE `3.11` | consolidated/period profit or loss |
| total assets | BPA `1` | total assets |
| total liabilities | BPP `2.01` + `2.02` | current + non-current liabilities, excludes equity |
| equity | BPP `2.03` | equity |
| operating cash flow | DFC-MI/DFC-MD `6.01` | indirect method preferred when both exist |
| cash and equivalents | BPA `1.01.01` | cash and cash equivalents |

Debt is intentionally not mapped in v1. It needs a separate review because
Brazilian statement rows do not always expose a single reliable debt account
across issuer types. FRE debt/obligations data may be a better enrichment source
for this metric.

Versioning rule:

Keep raw rows for all versions. In the metrics layer, expose a clear
`is_latest_version` flag or latest-only view. Do not silently overwrite raw
restatements without retaining enough lineage to explain which filing produced
the metric.

## FRE - Reference Form

Official source:

- Dataset: https://dados.cvm.gov.br/dataset/cia_aberta-doc-fre
- Direct files: https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/FRE/DADOS/

What it is:

FRE is the reference form. It is not a replacement for DFP/ITR accounting
statements. It is an enrichment source for company context, financial summary,
dividends, debt/obligations, capital structure, securities, related-party
transactions, administrators, compensation, and management context.

What we should pull first:

| FRE file area | Use |
|---|---|
| `fre_cia_aberta_informacao_financeira` | financial summary context |
| `fre_cia_aberta_distribuicao_dividendos` | dividend history/enrichment |
| `fre_cia_aberta_endividamento` | debt context |
| `fre_cia_aberta_obrigacao` | obligations/debt detail |
| `fre_cia_capital_social*` | capital structure |
| `fre_cia_aberta_distribuicao_capital*` | capital distribution |
| `fre_cia_aberta_transacao_parte_relacionada` | related-party context |
| administrator/board files | governance enrichment |
| auditor files | audit context |

Implementation idea:

Add FRE after DFP, ITR, and the first metrics layer. FRE has many CSV families,
so the first implementation should not load every file into a generic table
without a read plan. Start with a narrow set of financial-enrichment files.

Expected assets:

```text
brazil_fin_cvm_fre_raw_archives_s3
brazil_fin_cvm_fre_raw_duckdb
brazil_fin_cvm_fre_financial_enrichment_duckdb
brazil_fin_cvm_fre_financial_enrichment_clickhouse
```

How FRE relates to DFP/ITR:

- Use DFP/ITR for primary accounting metrics.
- Use FRE to enrich or validate debt, dividends, capital structure, governance,
  and management context.
- Do not use FRE as the main revenue/profit/assets source unless a specific FRE
  field is explicitly mapped and documented.

## FCA - Cadastral Form

Official source:

- Dataset: https://dados.cvm.gov.br/dataset/cia_aberta-doc-fca
- Direct files: https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/FCA/DADOS/

What it is:

FCA is the cadastral form. It is not a financial statement source. It is useful
for company metadata around public/open issuers.

What we should pull:

| FCA file area | Use |
|---|---|
| `fca_cia_aberta_geral` | issuer metadata |
| `fca_cia_aberta_endereco` | addresses |
| `fca_cia_aberta_valor_mobiliario` | securities metadata |
| `fca_cia_aberta_auditor` | auditor metadata |
| `fca_cia_aberta_dri` | investor-relations contacts |
| `fca_cia_aberta_departamento_acionistas` | shareholder department contacts |
| `fca_cia_aberta_canal_divulgacao` | disclosure channels |

Implementation idea:

Add FCA after FRE unless company-contact enrichment becomes urgent. FCA should
be treated as metadata/enrichment and should join to DFP/ITR/FRE through
`CNPJ_CIA`, `CD_CVM`, and period/version fields where present.

Expected assets:

```text
brazil_fin_cvm_fca_raw_archives_s3
brazil_fin_cvm_fca_raw_duckdb
brazil_fin_cvm_fca_company_metadata_duckdb
brazil_fin_cvm_fca_company_metadata_clickhouse
```

How FCA relates to DFP/ITR/FRE:

- DFP/ITR answer financial statements.
- FRE adds broader reference-form context.
- FCA adds current/historical company metadata, contacts, securities, auditor,
  and shareholder-service fields.
- FCA should not replace the RFB company registry for broad company coverage;
  it only covers CVM public/open companies.

## Cias Abertas Cadastro Support Table

Official source:

- Dataset: https://dados.cvm.gov.br/dataset/cia_aberta-cad
- Direct CSV: https://dados.cvm.gov.br/dados/CIA_ABERTA/CAD/DADOS/cad_cia_aberta.csv

Why it matters:

This is a support/reference table for public/open companies. It is useful before
or alongside ITR because it provides issuer identity and status fields without
having to infer everything from filing rows.

What to pull:

```text
CNPJ_CIA
DENOM_SOCIAL
DENOM_COMERC
CD_CVM
SIT
SETOR_ATIV
CATEG_REG
SIT_EMISSOR
EMAIL
auditor fields where present
address fields where present
```

Recommended asset:

```text
brazil_fin_cvm_companies_duckdb
brazil_fin_cvm_companies_clickhouse
```

How it relates:

- Join to DFP/ITR/FRE/FCA by `CD_CVM` and normalized `CNPJ_CIA`.
- Join to RFB broad company data by full CNPJ and `cnpj_basico`.
- Use it to distinguish current active public companies from historical/canceled
  issuers.

## Coverage Boundaries

CVM sources cover public/open companies regulated by CVM. They do not provide
financial statements for every Brazilian CNPJ.

For broad national company coverage, use `brazil_companies.rfb`. RFB gives
identity, status, activity, address, contact, tax-regime, and share capital, but
not revenue, profit, assets, liabilities, or cash flow.

For private-company financial statements, keep Central de Balancos as a later
investigation. It should not block the CVM roadmap.

## Recommended Build Order

1. Stabilize DFP raw downloads, parsing, USD conversion, and ClickHouse export.
2. Add CVM company support table from `cad_cia_aberta.csv`.
3. Add ITR raw downloads, parsing, USD conversion, and ClickHouse export.
4. Build normalized metrics over DFP + ITR.
5. Add FRE financial-enrichment files.
6. Add FCA metadata/contact files.
7. Investigate Central de Balancos for non-public-company financial documents.
