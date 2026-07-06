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
- Added CGU/Portal da Transparencia sanctions source package:
  `dagster_v3.defs.brazil_companies.cgu`.
- Implemented CGU raw archive download asset:
  `brazil_comp_cgu_raw_archives_s3`.
- Implemented CGU DuckDB raw company/compliance tables:
  `brazil_comp_cgu_ceis_company_sanctions_duckdb`,
  `brazil_comp_cgu_cnep_company_sanctions_duckdb`,
  `brazil_comp_cgu_cepim_blocked_entities_duckdb`,
  `brazil_comp_cgu_leniency_agreements_duckdb`,
  `brazil_comp_cgu_leniency_agreement_effects_duckdb`.
- Implemented matching CGU ClickHouse export assets.
- Created ClickHouse CGU tables:
  `br_cgu_ceis_company_sanctions`,
  `br_cgu_cnep_company_sanctions`,
  `br_cgu_cepim_blocked_entities`,
  `br_cgu_leniency_agreements`,
  `br_cgu_leniency_agreement_effects`.

## Next

1. Materialize CGU against the latest Portal snapshots.
   - Validate row counts by dataset:
     - CEIS;
     - CNEP;
     - CEPIM;
     - Acordos de Leniência.
   - Validate company-only filtering for CEIS/CNEP.
   - Check skipped non-company/person rows.
   - Confirm leniency agreement effects can join to agreements by
     `agreement_id`.

2. Materialize PGFN against a real quarterly partition.
   - Validate row counts by source system:
     - `nao_previdenciario`;
     - `fgts`;
     - `previdenciario`.
   - Validate company-only filtering by CNPJ.
   - Check skipped row counts for CPF/person records.

3. Add PGFN summary views after raw data is verified.
   - Current active debt flag by CNPJ.
   - Total active debt amount by CNPJ.
   - Debt count by situation and source system.
   - Judicial-collection flag.
   - Latest quarter per CNPJ.

4. Join PGFN and CGU enrichment to RFB companies.
   - Join on full CNPJ where establishment-level context is needed.
   - Join on CNPJ basico where company-root aggregation is needed.
   - Keep PGFN and CGU as enrichment, not canonical company identity.

5. Define PGFN and CGU retention and cleanup rules.
   - Keep raw ZIP objects in object storage.
   - Remove local extracted/staging files after successful ClickHouse export.
   - Retain enough quarterly partitions to support debt trend analysis.
   - Retain enough CGU snapshots to support sanctions trend/change analysis.

6. Improve Brazil companies domain docs.
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
- Should CGU remain a latest-discovery unpartitioned Dagster asset with
  source-date-partitioned object keys, or should we add dynamic Dagster
  partitions once schedules are defined?
