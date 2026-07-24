DROP TABLE IF EXISTS corpscout._tmp_ted_notices_legacy_grain;
DROP TABLE IF EXISTS corpscout._tmp_ted_notice_winners_legacy_grain;

CREATE TABLE corpscout._tmp_ted_notices_legacy_grain
AS corpscout.ted_notices
ENGINE = ReplacingMergeTree
ORDER BY (publication_number);

CREATE TABLE corpscout._tmp_ted_notice_winners_legacy_grain
AS corpscout.ted_notice_winners
ENGINE = ReplacingMergeTree
ORDER BY (
    winner_national_id,
    publication_number,
    lot_id,
    tender_id,
    winner_ordinal
);

INSERT INTO corpscout._tmp_ted_notices_legacy_grain
SELECT * FROM corpscout.ted_notices;

INSERT INTO corpscout._tmp_ted_notice_winners_legacy_grain
SELECT * FROM corpscout.ted_notice_winners;

EXCHANGE TABLES
    corpscout._tmp_ted_notices_legacy_grain
    AND corpscout.ted_notices;

EXCHANGE TABLES
    corpscout._tmp_ted_notice_winners_legacy_grain
    AND corpscout.ted_notice_winners;

DROP TABLE corpscout._tmp_ted_notices_legacy_grain;
DROP TABLE corpscout._tmp_ted_notice_winners_legacy_grain;
