CREATE DATABASE IF NOT EXISTS corpscout;

-- Swaps the retired overlay-reading view -- 000391's basic-info render, parked under
-- _retired by the up-file's RENAME -- back under the serving name, restarts its refresh
-- loop (the up-file stopped it), and discards the address-entity render. Only meaningful
-- while _retired still exists -- after a follow-up drop, roll forward instead.
RENAME TABLE
    corpscout.se_companies_serving TO corpscout.se_companies_serving_address_entity_discard,
    corpscout.se_companies_serving_retired TO corpscout.se_companies_serving;

SYSTEM START VIEW corpscout.se_companies_serving;

DROP VIEW IF EXISTS corpscout.se_companies_serving_address_entity_discard;
