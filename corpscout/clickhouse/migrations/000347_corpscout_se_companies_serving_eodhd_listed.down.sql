CREATE DATABASE IF NOT EXISTS corpscout;

-- Swaps the retired pre-eodhd view back under the serving name, restarts its refresh loop
-- (the up-file stopped it), and discards the eodhd-flag render. Only meaningful while
-- _retired still exists -- after the follow-up drop, roll forward instead.
RENAME TABLE
    corpscout.se_companies_serving TO corpscout.se_companies_serving_eodhd_discard,
    corpscout.se_companies_serving_retired TO corpscout.se_companies_serving;

SYSTEM START VIEW corpscout.se_companies_serving;

DROP VIEW IF EXISTS corpscout.se_companies_serving_eodhd_discard;
