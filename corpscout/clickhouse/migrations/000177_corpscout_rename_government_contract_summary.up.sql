CREATE DATABASE IF NOT EXISTS corpscout;

-- Renamed for consistency with its sibling. The two tables describe the same
-- domain at two grains -- one row per company, one row per contract -- but were
-- named in two vocabularies: company_public_procurement_summary next to
-- company_government_contract_evidence.
--
-- RENAME preserves the data. Migration 000165 keeps the original CREATE as
-- history rather than being edited, per the forward-only ledger rule.
RENAME TABLE corpscout.company_public_procurement_summary
          TO corpscout.company_government_contract_summary;
