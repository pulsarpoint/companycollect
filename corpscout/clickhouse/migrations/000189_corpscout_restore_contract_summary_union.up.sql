CREATE DATABASE IF NOT EXISTS corpscout;

-- Put the cross-country summary back, as a thin union of the per-country ones.
--
-- companies_all still joins this name, and reworking that is deliberately out
-- of scope: the shape of a unified companies table is better decided once more
-- countries have proper per-country tables to generalise from. Dropping the
-- name in 000188 would have broken the daily build for a rework not being done.
--
-- It is now only a passthrough. The per-country summaries hold the logic, so
-- this adds no rules of its own and nothing has to be kept in step. Countries
-- with no procurement source are absent rather than present-and-empty, which is
-- what companies_all's LEFT JOIN already expects.
CREATE VIEW IF NOT EXISTS corpscout.company_government_contract_summary AS
SELECT * FROM corpscout.se_government_contract_summary
UNION ALL
SELECT * FROM corpscout.fi_government_contract_summary
UNION ALL
SELECT * FROM corpscout.no_government_contract_summary;
