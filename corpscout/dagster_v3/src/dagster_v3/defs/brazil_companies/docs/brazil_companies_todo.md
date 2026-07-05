# Brazil Companies TODO

## Done

- Created `dagster_v3.defs.brazil_companies` as the Brazil company-domain
  package.
- Moved RFB company registry assets into
  `dagster_v3.defs.brazil_companies.rfb`.
- Kept stable RFB local data paths:
  `data/brazil_rfb` and `data/brazil_rfb_downloads`.
- Kept stable RFB source identity:
  `brazil_rfb`.
- Implemented RFB DuckDB and ClickHouse assets under the
  `brazil_comp_rfb_*` namespace.
- Implemented RFB partition cleanup asset for removing old local snapshot
  folders.
- Implemented CNAE-to-NACE reference mapping under
  `dagster_v3.defs.brazil_companies.cnae`.
- Added PGFN company debt/risk source package:
  `dagster_v3.defs.brazil_companies.pgfn`.
- Implemented PGFN raw archive download asset:
  `brazil_comp_pgfn_raw_archives_s3`.
- Implemented PGFN company debt parsing into DuckDB:
  `brazil_comp_pgfn_company_debts_duckdb`.
- Implemented PGFN company debt ClickHouse export:
  `brazil_comp_pgfn_company_debts_clickhouse`.
- Created ClickHouse PGFN table:
  `br_pgfn_company_debts`.

## Next

1. Materialize PGFN against a real quarterly partition.
   - Validate row counts by source system:
     - `nao_previdenciario`;
     - `fgts`;
     - `previdenciario`.
   - Validate company-only filtering by CNPJ.
   - Check skipped row counts for CPF/person records.

2. Add PGFN summary views after raw data is verified.
   - Current active debt flag by CNPJ.
   - Total active debt amount by CNPJ.
   - Debt count by situation and source system.
   - Judicial-collection flag.
   - Latest quarter per CNPJ.

3. Join PGFN enrichment to RFB companies.
   - Join on full CNPJ where establishment-level context is needed.
   - Join on CNPJ basico where company-root aggregation is needed.
   - Keep PGFN as enrichment, not canonical company identity.

4. Define PGFN retention and cleanup rules.
   - Keep raw ZIP objects in object storage.
   - Remove local extracted/staging files after successful ClickHouse export.
   - Retain enough quarterly partitions to support debt trend analysis.

5. Improve Brazil companies domain docs.
   - Move or summarize RFB design into this domain-level docs folder if source
     docs become hard to discover.
   - Add source-specific data dictionaries for high-value columns.

## Open Questions

- Should PGFN summary output be ClickHouse views only, or materialized tables
  for faster company profile joins?
- Should PGFN retain all historical debt inscriptions or expose only latest
  active debt to downstream company profile consumers?
- Should RFB monthly full snapshots remain partitioned assets, or should the
  canonical tables be treated as replace-current snapshots with separate
  cleanup/history rules?
