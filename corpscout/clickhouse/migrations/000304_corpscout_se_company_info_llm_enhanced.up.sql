CREATE DATABASE IF NOT EXISTS corpscout;

-- The owner's 2026-08-23 decision on description provenance: keep every source and where
-- we got it (description_sources, description_source_record_uids and the artifact rows
-- behind them), keep the model's suggestions (se_company_info_enrichment_observation plus
-- suggestion_id), and replace the single description_source label with one boolean --
-- llm_enhanced: did the published text come out of the model, or was it copied from an
-- input? No method enum, no 'reviewed' label: reviewer involvement is already visible in
-- correction_ids, which is the honest signal (a reject_suggestion leaves no trace in the
-- description columns at all).
--
-- What the flag says, per case:
--   copied from one input (SCB / Wikidata / ESEF), the deterministic pick included -> false
--   the model's merged summary                                                     -> true
--   an approved model suggestion (approve_suggestion)                              -> true
--   a reviewer override (override_field)                                           -> false
--   after reject_suggestion (deterministic pick republished)                        -> false
--   no text at all (description IS NULL)                                            -> false
--
-- DEFAULT false rather than Nullable: every row answers the question. Rows written before
-- this migration answer it with "no" until they are resolved again -- se_company_info is
-- keyed by company_id and every resolution appends a new version, so one resolve_all pass
-- (SECompanyInfoConfig.resolve_all) with the model OFF re-flags every published company
-- without spending a single model call. The multi-source rows reuse their stored
-- suggestions rather than asking again.
--
-- Two statements, add before drop: llm_enhanced is positioned AFTER description_language,
-- the column description_source used to follow, and the drop removes description_source
-- straight afterwards.
--
-- No MATERIALIZED expression reads either column (evidence_set_hash covers the artifacts'
-- hashes only), so description_source can be dropped on its own.

ALTER TABLE corpscout.se_company_info
    ADD COLUMN IF NOT EXISTS llm_enhanced Bool DEFAULT false AFTER description_language;

ALTER TABLE corpscout.se_company_info
    DROP COLUMN IF EXISTS description_source;
