CREATE DATABASE IF NOT EXISTS corpscout;

-- Superseded by the corpscout.company_listings view over instrument_venues,
-- instrument_issuer and company_identifier (migrations 000172 to 000175).
--
-- Precondition verified live before this migration was committed: the table
-- held 0 rows. The Sweden-only listing asset was migrated but never produced a
-- successful materialization, so nothing is lost. The replacement view carried
-- 114,288 rows for country_code = 'SE' at the same moment.
--
-- Migration 000170 stays on disk. Deleting it would give a fresh environment a
-- different migration history than production, which recorded it as applied,
-- and would break a downgrade past 000171. A forward drop converges both on the
-- same end state.
DROP TABLE IF EXISTS corpscout.se_company_listings;
