# Brazil

## Summary

Brazil is processed from Receita Federal CNPJ open data. The implemented
Corpscout source is `brazil_rfb`, backed by monthly full bulk snapshots. It
currently produces legal entities, establishments, native contact rows, and
email-derived domain rows. It does not use an incremental source feed.

Related notes:

- [Brazil financial sources](brazil-financial-sources.md)
- [Brazil improvements](brazil-improvements.md)
- [Brazil todo](brazil-todo.md)

## Source And Strategy

| Question | Answer |
|---|---|
| Primary source | Receita Federal do Brasil / SERPRO public CNPJ registry data. |
| Source URL | Official historical path: `https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/`. Current implementation default mirror: `https://dados-abertos-rf-cnpj.casadosdados.com.br/arquivos/`. |
| Access | Public ZIP CSV bulk files, no authentication. |
| Implemented source slug | `brazil_rfb`. |
| Ingestion strategy | Full bulk download per monthly snapshot. No incremental/delta feed is used. |
| Partitioning | Yes. Dagster monthly partitions start at `2024-01-01`; partition `YYYY-MM-01` resolves to snapshot month `YYYY-MM`. |
| Scheduler activation | Run monthly after the source publishes a complete snapshot directory for the month. The code does not define a calendar schedule; it defines the partitioned job `brazil_rfb_resolve_job`. |
| Snapshot isolation | Stage files are written under `data/brazil_rfb/<YYYY-MM>/`, so reruns and backfills stay snapshot-scoped. |
| Serving-table behavior | ClickHouse exports replace current serving tables for the selected partition. |
| Raw file families used | `Empresas`, `Estabelecimentos`, `Simples`, and reference tables: `Cnaes`, `Naturezas`, `Municipios`, `Paises`, `Qualificacoes`, `Motivos`. |
| Deferred file families | `Socios` is intentionally deferred because it contains natural-person partner data and needs a privacy-specific ingestion design. |

## Data Products

| Output | Grain | Notes |
|---|---|---|
| `corpscout.br_companies` | One row per legal entity, keyed by `cnpj_basico`. | Joins `Empresas` to the selected headquarters/fallback establishment. |
| `corpscout.br_establishments` | One row per full 14-digit CNPJ establishment. | Keeps branch-level status, address, contact fields, and CNAE fields. |
| `corpscout.br_company_contact_info` | One row per normalized establishment contact. | Unpivots native email, phone 1, phone 2, and fax. |
| `corpscout.br_websites` | One row per accepted `(cnpj_basico, root_domain)`. | Derived from email domains only; no official website URL is present in RFB CNPJ. |
| `corpscout.br_cnae_to_nace` | Reference mapping edge from CNAE to NACE. | Curated seed fixture exists. Full company-industry materialization is planned in the design doc but is not wired in the current `brazil_rfb` asset list. |

## Company Data Coverage

| Question | Brazil answer |
|---|---|
| Do we have native company industry/category? | Yes. RFB provides native CNAE 2.0 codes on establishments: `cnae_fiscal_principal` and `cnae_fiscal_secundaria`. |
| Do we have NACE category for the company? | Not natively. A curated CNAE-to-NACE mapping table exists, but NACE is derived and the current company export does not directly include a company-level NACE table. |
| Do we have native contact info? | Yes. Establishments include email, two phone slots, and fax. |
| Do we generate contact info from company name? | No. Contacts are not generated from names. |
| Do we derive contact-like data? | Yes. Root domains in `br_websites` are derived from native email addresses when the root domain maps to exactly one `cnpj_basico` and is not a common email provider. |
| Do we have official website URLs? | No. `website_url`, `website_normalized_url`, and `website_host` are empty in `br_websites`; the usable domain signal is email-derived. |
| Do we translate company description fields? | No LLM translation is used. Company names, trade names, municipalities, streets, and addresses remain in the source language. |
| What translations are performed? | Static mappings for implemented finite code lists: status, company size, and contact type. CNAE/NACE English descriptions are available where the curated mapping fixture is used. Legal nature is retained as the source Portuguese description in the current export. |
| Do we get financial data? | Minimal registry financial data only. RFB provides `capital_social`, stored as `share_capital_amount_original`. |
| How extensive are the financial data? | Very limited. There are no balance sheets, income statements, revenue, profit, assets, liabilities, or filing periods from `brazil_rfb`. CVM listed-company financials are documented as a later separate phase, not part of this implemented source. |
| Do we have number of employees? | No. RFB CNPJ does not provide employee counts in this pipeline. |
| Do we have ownership/partners? | Not in the current implementation. The `Socios` file is deferred because it includes masked CPF and natural-person partner information. |

## Important Source Fields

| Area | Source fields | Normalized result |
|---|---|---|
| Entity identity | `cnpj_basico` | Legal-entity key in `br_companies`. |
| Establishment identity | `cnpj_basico`, `cnpj_ordem`, `cnpj_dv` | Full CNPJ in `br_establishments.cnpj`. |
| Legal name | `Empresas.razao_social` | `br_companies.legal_name`. |
| Trade name | `Estabelecimentos.nome_fantasia` | `trade_name` on company/establishment rows. |
| Status | `situacao_cadastral`, `data_situacao_cadastral`, `motivo_situacao_cadastral` | Status code, English status label, status date, and reason code. |
| Address | street type/name/number, complement, district, CEP, UF, municipality code | Normalized address columns on company and establishment rows. |
| Industry | `cnae_fiscal_principal`, `cnae_fiscal_secundaria`, `Cnaes.description_pt` | Native CNAE values on establishments; NACE possible through derived mapping. |
| Tax regime | `Simples.opcao_simples`, `Simples.opcao_mei` | `is_simples`, `is_mei`. |
| Contact | `correio_eletronico`, `ddd_1`, `telefone_1`, `ddd_2`, `telefone_2`, `ddd_fax`, `fax` | Rows in `br_company_contact_info`. |
| Financial | `Empresas.capital_social` | `share_capital_amount_original` in BRL-like source decimal format parsed to decimal. |

## Operational Notes

- The source files are semicolon-delimited CSV inside ZIP archives with fixed
  published column order and no header.
- Source text is Latin-1/CP1252-like; the loader normalizes extracted CSV files
  to UTF-8 for DuckDB.
- The pipeline is file-backed and resumable. Existing non-empty stage DuckDB
  files are reused for reruns of the same partition.
- Empty raw-family inputs are not allowed to replace existing staging tables.
- `brazil_rfb_resolve_job` selects the full `brazil_rfb` asset group for one
  monthly partition.
