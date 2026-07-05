# Brazil Todo

## Recently Done

- Added first PGFN Dívida Ativa source package:
  `dagster_v3.defs.brazil_companies.pgfn`.
- Added quarterly assets:
  `brazil_comp_pgfn_raw_archives_s3`,
  `brazil_comp_pgfn_company_debts_duckdb`, and
  `brazil_comp_pgfn_company_debts_clickhouse`.
- Added ClickHouse table `corpscout.br_pgfn_company_debts`.

## Next Task

Add current-risk summary views on top of `br_pgfn_company_debts`.

Minimum scope:

1. Select the latest `snapshot_year`/`snapshot_quarter`.
2. Aggregate per `cnpj` and `cnpj_basico`.
3. Produce flags and amounts: has active public debt, total consolidated debt,
   debt count, judicial-collection count, and counts by source system/situation.
4. Join to `br_companies`/`br_establishments` for company-level enrichment.

## Short Backlog

- Finish normalized CVM metrics validation and quality checks.
- Finish `br_industries` materialization and ClickHouse export.
- Expand CNAE-to-NACE mapping coverage.
- Add materialization metrics for contact quality and email-derived domains.
- Add a privacy design for `Socios` before ingesting partner data.
- Add static English fixtures for legal nature and status reason.
- Add a monthly scheduler or monitor for new RFB snapshot publication.
- Add a quarterly scheduler or monitor for new PGFN publication.
- Add a source-health check for official Receita URL versus Casa dos Dados
  mirror availability.
- Add documentation labels for financial coverage:
  `share_capital_only`, `public_company_statements`, and `commercial_only`.
