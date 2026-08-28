CREATE DATABASE IF NOT EXISTS corpscout;

-- Swaps the retired pre-translation view back under the serving name and discards the
-- translation-widened one. Only meaningful while _retired still exists (the follow-up drop
-- migration removes it) -- after that, roll forward instead.
RENAME TABLE
    corpscout.se_companies_serving TO corpscout.se_companies_serving_translated_discard,
    corpscout.se_companies_serving_retired TO corpscout.se_companies_serving;

DROP VIEW IF EXISTS corpscout.se_companies_serving_translated_discard;
