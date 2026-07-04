# Brazil Todo

## Next Task

Finish the normalized CVM metrics layer on top of the already parsed DFP rows
from `brazil_fin_cvm_dfp_raw_duckdb` and USD conversion asset
`brazil_fin_cvm_dfp_statement_rows_usd_duckdb`.

Minimum scope:

1. Identify join keys to `br_companies`, especially CNPJ and CVM company code.
2. Define normalized metric rows for revenue, net income, assets, liabilities,
   equity, cash flow, and reporting period.
3. Export the normalized metrics to ClickHouse.
4. Document coverage limits clearly: CVM covers public/open companies, not all
   CNPJ entities.

## Short Backlog

- Finish `br_industries` materialization and ClickHouse export.
- Expand CNAE-to-NACE mapping coverage.
- Add CVM ITR under the `brazil_fin_cvm` package for quarterly financials.
- Add materialization metrics for contact quality and email-derived domains.
- Add a privacy design for `Socios` before ingesting partner data.
- Add static English fixtures for legal nature and status reason.
- Add a monthly scheduler or monitor for new RFB snapshot publication.
- Add a source-health check for official Receita URL versus Casa dos Dados
  mirror availability.
- Add documentation labels for financial coverage:
  `share_capital_only`, `public_company_statements`, and `commercial_only`.
