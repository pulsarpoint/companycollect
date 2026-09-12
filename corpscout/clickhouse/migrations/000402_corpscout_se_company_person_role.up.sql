CREATE DATABASE IF NOT EXISTS corpscout;

-- ROLES AS ROWS (spec 2026-09-09 section 11, person slice 5). The person row keeps its
-- index-parallel role block (role_codes, role_years, role_sources, current_roles), which
-- answers "who are the people of company X" and not "what did person Y do at company X
-- over time". This view answers the second question in plain SQL: one row per published
-- ACTIVE person and per role-carrying normalized observation the fold built them from.
--
-- DERIVED, NOT WRITTEN. Nothing inserts into it. The fold, the normalizer, the reviewer
-- rules and the precedence are untouched by this migration, and the arrays on the person
-- row stay exactly as the fold writes them. The view is recomputed whole on every
-- refresh, so it cannot drift from the two tables it reads -- it can only LAG them, by
-- at most one hour.
--
-- THE REFRESHABLE FORM THIS REPO USES (000326, 000335, 000391, 000392): the engine is
-- declared INSIDE the view, so corpscout.se_company_person_role IS the MergeTree readers
-- query and there is no separate target table to keep in step. The down file's DROP VIEW
-- takes that inner table with it.
--
-- REFRESH AT :20. se_companies_serving refreshes at :45 and takes 13 to 15 minutes,
-- se_address_geocodes_current at :00 -- :20 is the gap between them.
--
-- CREATED EMPTY, FIRST BUILD BY HAND. A refreshable view's first build over 1.1M persons
-- and 5.6M normalized rows is minutes of work, and the migrate client's read_timeout is
-- 300 seconds. EMPTY skips the initial refresh, so this CREATE returns in milliseconds
-- and the ledger can never be left dirty by a dropped client. The controller then runs
-- SYSTEM REFRESH VIEW corpscout.se_company_person_role and polls system.view_refreshes.
-- Until that lands the view answers with zero rows, which the backoffice panel handles
-- by falling back to the person row's own arrays.
--
-- THE SORT KEY HOLDS NO NULLABLE COLUMN (allow_nullable_key is off). role_code and
-- role_year are Nullable on the normalized row, so the SELECT unwraps both:
-- assumeNotNull is exact under the WHERE, and a role with no FISCAL year lands under
-- year 0 -- Wikidata delivers a span in role_from/role_to and never a fiscal year, so
-- every Wikidata role lands here. role_year 0 means "no fiscal year", not "no year at
-- all": the fold's own role_years array expands that span into the real years held.
--
-- THE NAME IS REUSED. corpscout.se_company_person_role was the 2026-08-19 model's role
-- table, dropped by hand in person slice 0 on 2026-09-09 -- exactly what 000398 did with
-- se_company_person. The operations script that dropped it is SPENT and must never run
-- again.
--
-- ALSO BOUNDS THE REFRESH ITSELF (F1): the SELECT carries the same trailing SETTINGS
-- every serving refresh has carried since 000347/000391 -- grace_hash spill joins,
-- external group-by/sort, and a 12 GiB max_memory_usage -- so this view's hourly
-- refresh cannot repeat the unbounded-refresh memory spike 000391 fixed on the serving
-- view. The EMPTY AS SELECT ... SETTINGS ... form parses on 26.5.
--
-- THE SELECT BELOW IS NOT HAND-WRITTEN AND MUST NOT BE HAND-EDITED -- exact rendering of
-- person/tables.py::build_se_company_person_role_sql(), drift-pinned by dagster_v3
-- tests/test_se_company_person_role_view.py.

CREATE MATERIALIZED VIEW corpscout.se_company_person_role
REFRESH EVERY 1 HOUR OFFSET 20 MINUTE
ENGINE = MergeTree
ORDER BY (company_id, person_key, role_year, role_code, source, slot)
EMPTY
AS SELECT
  p.company_id AS company_id,
  p.person_key AS person_key,
  p.display_name AS display_name,
  p.birth_year AS birth_year,
  assumeNotNull(n.role_code) AS role_code,
  ifNull(n.role_year, 0) AS role_year,
  n.role_from AS role_from,
  n.role_to AS role_to,
  n.source AS source,
  n.slot AS slot,
  n.normalized_id AS normalized_id,
  has(p.current_roles, assumeNotNull(n.role_code)) AS is_current,
  p.folded_at AS folded_at
FROM corpscout.se_company_person AS p FINAL
ARRAY JOIN p.normalized_ids AS member_id
INNER JOIN corpscout.se_company_person_normalized AS n FINAL
  ON n.company_id = p.company_id AND n.normalized_id = member_id
WHERE p.active = 1 AND n.role_code IS NOT NULL
SETTINGS join_algorithm = 'grace_hash,hash',
    grace_hash_join_initial_buckets = 16,
    max_bytes_before_external_group_by = 8589934592,
    max_bytes_before_external_sort = 8589934592,
    max_memory_usage = 12884901888;
