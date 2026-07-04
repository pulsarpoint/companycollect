# Brazil Todo

## Next Task

Create a `brazil_cvm` financial-data investigation and implementation plan.
Use [brazil-financial-sources.md](brazil-financial-sources.md) as the source
analysis input.

Minimum scope:

1. Inspect CVM DFP and ITR ZIP schemas and dictionaries.
2. Identify join keys to `br_companies`, especially CNPJ and CVM company code.
3. Define annual and quarterly financial statement tables.
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
