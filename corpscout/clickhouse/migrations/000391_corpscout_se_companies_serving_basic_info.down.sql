CREATE DATABASE IF NOT EXISTS corpscout;

-- Swap the pre-slice-4 render back under the serving name, restart its refresh, and
-- discard the basic-info render.
RENAME TABLE
    corpscout.se_companies_serving TO corpscout.se_companies_serving_basic_info_discard,
    corpscout.se_companies_serving_retired TO corpscout.se_companies_serving;

SYSTEM START VIEW corpscout.se_companies_serving;

DROP VIEW IF EXISTS corpscout.se_companies_serving_basic_info_discard;
