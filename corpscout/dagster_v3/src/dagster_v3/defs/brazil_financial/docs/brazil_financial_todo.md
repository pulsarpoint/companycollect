# Brazil Financial TODO

## Done

- Created `dagster_v3.defs.brazil_financial.cvm` as the CVM source package.
- Renamed DFP assets to the `brazil_fin_cvm_dfp_*` namespace.
- Kept stable DFP raw object keys:
  `brazil_cvm/dfp/raw_archives/year=<year>/archive.zip`.
- Implemented DFP yearly raw archive download asset:
  `brazil_fin_cvm_dfp_raw_archives_s3`.
- Implemented DFP raw ZIP parsing into DuckDB:
  `brazil_fin_cvm_dfp_raw_duckdb`.
- Implemented DFP statement-row USD conversion in DuckDB:
  `brazil_fin_cvm_dfp_statement_rows_usd_duckdb`.
- Implemented DFP raw ClickHouse export:
  `brazil_fin_cvm_dfp_raw_clickhouse`.
- Created ClickHouse DFP raw tables:
  `br_cvm_dfp_documents`,
  `br_cvm_dfp_statement_rows`,
  `br_cvm_dfp_capital_composition`,
  `br_cvm_dfp_auditor_reports`.
- Implemented CVM company support table assets:
  `brazil_fin_cvm_companies_duckdb` and
  `brazil_fin_cvm_companies_clickhouse`.
- Created ClickHouse CVM company support table:
  `br_cvm_companies`.
- Implemented ITR yearly raw archive download asset:
  `brazil_fin_cvm_itr_raw_archives_s3`.
- Implemented ITR raw ZIP parsing into DuckDB:
  `brazil_fin_cvm_itr_raw_duckdb`.
- Implemented ITR statement-row USD conversion in DuckDB:
  `brazil_fin_cvm_itr_statement_rows_usd_duckdb`.
- Implemented ITR raw ClickHouse export:
  `brazil_fin_cvm_itr_raw_clickhouse`.
- Created ClickHouse ITR raw tables:
  `br_cvm_itr_documents`,
  `br_cvm_itr_statement_rows`,
  `br_cvm_itr_capital_composition`,
  `br_cvm_itr_auditor_reports`.
- Documented the DFP DuckDB parser design in
  `cvm/docs/brazil_cvm_dfp_duckdb-design.md`.

## Next

1. Refactor shared CVM filing parser code where it removes real duplication.
   - Share only the stable DFP/ITR mechanics:
     - yearly ZIP URL construction;
     - archive object-key construction;
     - statement-family file mapping;
     - DuckDB load helpers;
     - common statement row normalization.
   - Keep document-specific constants explicit.

2. Build normalized metrics layer over DFP + ITR.
   - Initial metrics:
     - revenue;
     - net income;
     - total assets;
     - total liabilities;
     - equity;
     - operating cash flow;
     - cash and equivalents;
     - debt only if reliably mappable.
   - Required behavior:
     - preserve source dataset (`DFP` or `ITR`);
     - preserve consolidation type;
     - preserve original account code and description;
     - expose `is_latest_version`;
     - keep source lineage back to raw statement rows.

3. Add latest-version logic.
   - Raw rows should keep all filing versions.
   - Metrics should either expose `is_latest_version` or create a latest-only
     view/table.
   - Version selection must account for `CNPJ_CIA`, `CD_CVM`, reference date,
     statement family, consolidation type, account code, and filing version.

4. Add FRE financial enrichment after ITR and first metrics.
   - Source: `https://dados.cvm.gov.br/dataset/cia_aberta-doc-fre`
   - Start with:
     - financial summary;
     - dividends;
     - debt/obligations;
     - capital structure;
     - related-party transactions.
   - Do not treat FRE as the primary accounting statement source.

5. Add FCA metadata/contact enrichment after FRE.
   - Source: `https://dados.cvm.gov.br/dataset/cia_aberta-doc-fca`
   - Start with:
     - issuer metadata;
     - addresses;
     - securities metadata;
     - auditor;
     - investor-relations contacts;
     - shareholder department contacts;
     - disclosure channels.

6. Define schedules/sensors.
   - Current year DFP: weekly.
   - Current year ITR: weekly.
   - Last five years DFP/ITR: weekly or monthly based on cost.
   - Older years: quarterly or manual checksum refresh.
   - FRE/FCA: monthly after implementation, unless active restatement tracking
     requires weekly.

7. Investigate Central de Balancos.
   - Goal:
     - determine whether a stable public endpoint can pull private-company
       financial documents by CNPJ or bounded search.
   - Do not implement broad crawling until endpoint stability, terms, and
     rate limits are understood.

## Open Questions

- Should DFP and ITR statement rows be combined only at the metrics layer, or do
  we also need a raw union view?
- Should the first normalized metrics table store one row per metric or a wide
  row per company-period?
- Which account-code mappings are reliable across non-financial issuers, banks,
  insurers, and other regulated sectors?
- Should current-year ZIP refresh force re-download even when object storage
  already has an archive, or should freshness be implemented with a separate
  checksum/HEAD metadata asset?
