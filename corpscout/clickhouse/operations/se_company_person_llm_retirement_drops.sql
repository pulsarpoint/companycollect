-- Retire the two unused person enrichment tracking tables (owner request 2026-09-13).
-- Both were empty and had no runtime readers, writers, or dependent views.
-- Run manually on installations that applied the original migration 000406.
-- Their DDL was removed from 000406 under the development-phase retirement policy.
-- Executed and verified on the shared ClickHouse server on 2026-09-13.

DROP TABLE IF EXISTS corpscout.llm_queue_se_company_person;
DROP TABLE IF EXISTS corpscout.llm_response_se_company_person;
