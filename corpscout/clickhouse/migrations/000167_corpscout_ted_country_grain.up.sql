CREATE DATABASE IF NOT EXISTS corpscout;

DROP TABLE IF EXISTS corpscout._tmp_ted_notices_country_grain;
DROP TABLE IF EXISTS corpscout._tmp_ted_notice_winners_country_grain;

CREATE TABLE corpscout._tmp_ted_notices_country_grain
AS corpscout.ted_notices
ENGINE = ReplacingMergeTree
ORDER BY (country_iso2, publication_number);

CREATE TABLE corpscout._tmp_ted_notice_winners_country_grain
AS corpscout.ted_notice_winners
ENGINE = ReplacingMergeTree
ORDER BY (
    country_iso2,
    winner_national_id,
    publication_number,
    lot_id,
    tender_id,
    winner_ordinal
);

INSERT INTO corpscout._tmp_ted_notices_country_grain
SELECT * FROM corpscout.ted_notices;

INSERT INTO corpscout._tmp_ted_notice_winners_country_grain
SELECT * FROM corpscout.ted_notice_winners;

EXCHANGE TABLES
    corpscout._tmp_ted_notices_country_grain
    AND corpscout.ted_notices;

EXCHANGE TABLES
    corpscout._tmp_ted_notice_winners_country_grain
    AND corpscout.ted_notice_winners;

DROP TABLE corpscout._tmp_ted_notices_country_grain;
DROP TABLE corpscout._tmp_ted_notice_winners_country_grain;
