# Brazil Todo

## Next Task

Implement the DFP parser asset that reads ZIPs stored by
`brazil_cvm_dfp_raw_archives_s3` and loads the CVM CSV family rows into DuckDB.

Minimum scope:

1. Read each raw archive from `source-brazil-cvm`.
2. Identify join keys to `br_companies`, especially CNPJ and CVM company code.
3. Load the DFP document index and statement-family rows as raw DuckDB tables.
4. Define normalized metric rows for revenue, net income, assets, liabilities,
   equity, cash flow, and reporting period.
5. Document coverage limits clearly: CVM covers public/open companies, not all
   CNPJ entities.

## Short Backlog

- Finish `br_industries` materialization and ClickHouse export.
- Expand CNAE-to-NACE mapping coverage.
- Add materialization metrics for contact quality and email-derived domains.
- Add a privacy design for `Socios` before ingesting partner data.
- Add static English fixtures for legal nature and status reason.
- Add a monthly scheduler or monitor for new RFB snapshot publication.
- Add a source-health check for official Receita URL versus Casa dos Dados
  mirror availability.
- Add documentation labels for financial coverage:
  `share_capital_only`, `public_company_statements`, and `commercial_only`.
