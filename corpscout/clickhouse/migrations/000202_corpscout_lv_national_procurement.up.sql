CREATE DATABASE IF NOT EXISTS corpscout;

CREATE TABLE IF NOT EXISTS corpscout.lv_iub_notices
(
    country_code LowCardinality(String),
    source_slug LowCardinality(String),
    source_run_id String,
    notice_id String,
    cloned_from String,
    previous_identifier String,
    procedure_id String,
    form_type LowCardinality(String),
    notice_type LowCardinality(String),
    publication_date Date,
    buyer_name String,
    buyer_regcode String,
    title String,
    cpv_code String,
    legal_basis LowCardinality(String),
    directive_governed LowCardinality(String),
    source_url String,
    source_object_key String,
    source_retrieved_at DateTime64(3, 'UTC'),
    resolved_at DateTime64(3, 'UTC'),
    partition_key String
)
ENGINE = ReplacingMergeTree(resolved_at)
PARTITION BY partition_key
ORDER BY notice_id;

CREATE TABLE IF NOT EXISTS corpscout.lv_iub_notice_lots
(
    country_code LowCardinality(String),
    source_slug LowCardinality(String),
    source_run_id String,
    notice_id String,
    lot_id String,
    lot_sequence Int32,
    lot_title String,
    decision_date Nullable(Date),
    winner_selection_status LowCardinality(String),
    estimated_value_amount_eur Nullable(Decimal(38, 2)),
    lowest_tender_amount_eur Nullable(Decimal(38, 2)),
    highest_tender_amount_eur Nullable(Decimal(38, 2)),
    received_tenders Nullable(Int32),
    publication_date Date,
    source_object_key String,
    resolved_at DateTime64(3, 'UTC'),
    partition_key String
)
ENGINE = ReplacingMergeTree(resolved_at)
PARTITION BY partition_key
ORDER BY (notice_id, lot_id);

CREATE TABLE IF NOT EXISTS corpscout.lv_iub_notice_winners
(
    company_id String,
    company_match_status LowCardinality(String),
    country_code LowCardinality(String),
    source_slug LowCardinality(String),
    source_run_id String,
    source_record_id String,
    notice_id String,
    procedure_id String,
    lot_id String,
    contract_id String,
    winner_ordinal Int32,
    party_ordinal Int32,
    winner_name String,
    winner_id_raw String,
    winner_regcode String,
    winner_country LowCardinality(String),
    is_natural_person UInt8,
    tender_value_amount_eur Nullable(Decimal(38, 2)),
    tender_value_amount_usd Nullable(Decimal(38, 2)),
    tender_value_attributable UInt8,
    contract_conclusion_date Nullable(Date),
    contract_title String,
    contract_url String,
    publication_date Date,
    buyer_name String,
    buyer_regcode String,
    notice_title String,
    cpv_code String,
    legal_basis LowCardinality(String),
    directive_governed LowCardinality(String),
    source_url String,
    source_object_key String,
    source_retrieved_at DateTime64(3, 'UTC'),
    resolved_at DateTime64(3, 'UTC'),
    partition_key String,
    match_eligibility LowCardinality(String)
)
ENGINE = ReplacingMergeTree(resolved_at)
PARTITION BY partition_key
ORDER BY source_record_id;

CREATE TABLE IF NOT EXISTS corpscout.lv_iub_contract_executions
(
    country_code LowCardinality(String),
    source_slug LowCardinality(String),
    source_run_id String,
    source_record_id String,
    notice_id String,
    procedure_id String,
    contract_id String,
    winner_ordinal Int32,
    party_ordinal Int32,
    winner_name String,
    winner_id_raw String,
    winner_regcode String,
    winner_country LowCardinality(String),
    is_natural_person UInt8,
    tender_value_amount_eur Nullable(Decimal(38, 2)),
    contract_conclusion_date Nullable(Date),
    actual_end_date Nullable(Date),
    contract_title String,
    publication_date Date,
    buyer_name String,
    buyer_regcode String,
    notice_title String,
    source_url String,
    source_object_key String,
    source_retrieved_at DateTime64(3, 'UTC'),
    resolved_at DateTime64(3, 'UTC'),
    partition_key String
)
ENGINE = ReplacingMergeTree(resolved_at)
PARTITION BY partition_key
ORDER BY source_record_id;

DROP VIEW IF EXISTS corpscout.lv_iub_contract_executions_current;
DROP VIEW IF EXISTS corpscout.lv_iub_notice_winners_current;
DROP VIEW IF EXISTS corpscout.lv_iub_notice_lots_current;
DROP VIEW IF EXISTS corpscout.lv_iub_notices_current;

CREATE VIEW corpscout.lv_iub_notices_current AS
SELECT *
FROM corpscout.lv_iub_notices
WHERE notice_id NOT IN
(
    SELECT cloned_from
    FROM corpscout.lv_iub_notices
    WHERE cloned_from != ''
);

CREATE VIEW corpscout.lv_iub_notice_lots_current AS
SELECT l.*
FROM corpscout.lv_iub_notice_lots AS l
INNER JOIN corpscout.lv_iub_notices_current AS n
    ON n.notice_id = l.notice_id;

CREATE VIEW corpscout.lv_iub_notice_winners_current AS
SELECT w.*
FROM corpscout.lv_iub_notice_winners AS w
INNER JOIN corpscout.lv_iub_notices_current AS n
    ON n.notice_id = w.notice_id;

CREATE VIEW corpscout.lv_iub_contract_executions_current AS
SELECT e.*
FROM corpscout.lv_iub_contract_executions AS e
INNER JOIN corpscout.lv_iub_notices_current AS n
    ON n.notice_id = e.notice_id;

DROP VIEW IF EXISTS corpscout.lv_government_contracts;

CREATE VIEW corpscout.lv_government_contracts AS
SELECT
    CAST('LV', 'String') AS country_code,
    CAST(i.company_id, 'String') AS company_id,
    CAST(concat('iub:', i.source_record_id), 'String') AS contract_id,
    CAST('latvia_iub_procurement', 'String') AS source_slug,
    CAST(i.notice_id, 'String') AS source_notice_id,
    CAST(i.lot_id, 'String') AS source_lot_id,
    CAST(i.winner_ordinal, 'Int32') AS source_winner_ordinal,
    CAST(i.winner_name, 'String') AS winner_name,
    CAST(i.source_url, 'String') AS source_url,
    CAST(i.publication_date, 'Nullable(Date)') AS publication_date,
    CAST(i.buyer_name, 'String') AS buyer_name,
    CAST(i.buyer_regcode, 'String') AS buyer_id,
    CAST(if(i.contract_title != '', i.contract_title, i.notice_title), 'String') AS title,
    CAST('', 'String') AS agreement_type,
    CAST(i.cpv_code, 'String') AS cpv_code,
    CAST(i.directive_governed, 'String') AS directive_governed,
    CAST(
        if(i.tender_value_attributable = 1, i.tender_value_amount_eur, NULL),
        'Nullable(Decimal(38, 2))'
    ) AS value_amount_original,
    CAST(
        if(
            (i.tender_value_attributable = 1)
            AND (i.tender_value_amount_eur IS NOT NULL),
            'EUR',
            ''
        ),
        'String'
    ) AS value_currency,
    CAST(
        if(i.tender_value_attributable = 1, i.tender_value_amount_usd, NULL),
        'Nullable(Decimal(38, 2))'
    ) AS value_amount_usd,
    CAST(NULL, 'Nullable(Decimal(38, 2))') AS notice_value_amount_original,
    CAST('', 'String') AS notice_value_currency,
    CAST(NULL, 'Nullable(Decimal(38, 2))') AS notice_value_amount_usd,
    CAST('tenderValue', 'String') AS value_source_field,
    CAST('', 'String') AS notice_value_source_field,
    CAST(i.resolved_at, 'DateTime64(3, \'UTC\')') AS source_updated_at,
    CAST(if(
        (i.publication_date IS NULL)
        OR (i.buyer_name = '')
        OR ((i.contract_title = '') AND (i.notice_title = '')),
        '',
        lower(hex(MD5(concat(
            lowerUTF8(replaceRegexpAll(trimBoth(i.buyer_name), '\\s+', ' ')),
            '|',
            toString(i.publication_date),
            '|',
            lowerUTF8(replaceRegexpAll(
                trimBoth(if(i.contract_title != '', i.contract_title, i.notice_title)),
                '\\s+',
                ' '
            ))
        ))))
    ), 'String') AS contract_key
FROM corpscout.lv_iub_notice_winners_current AS i
WHERE (i.company_match_status = 'exact')
  AND (i.company_id != '')
  AND (i.directive_governed = 'no')

UNION ALL

SELECT
    CAST('LV', 'String') AS country_code,
    CAST(c.regcode, 'String') AS company_id,
    CAST(concat('ted:', w.publication_number, ':', w.lot_id), 'String') AS contract_id,
    CAST('ted_procurement', 'String') AS source_slug,
    CAST(w.publication_number, 'String') AS source_notice_id,
    CAST(w.lot_id, 'String') AS source_lot_id,
    CAST(w.winner_ordinal, 'Int32') AS source_winner_ordinal,
    CAST(any(w.winner_name), 'String') AS winner_name,
    CAST(concat('https://ted.europa.eu/en/notice/', w.publication_number, '/xml'), 'String') AS source_url,
    w.publication_date AS publication_date,
    CAST(any(n.buyer_name), 'String') AS buyer_name,
    CAST(any(n.buyer_national_id), 'String') AS buyer_id,
    CAST(any(n.notice_title), 'String') AS title,
    CAST('', 'String') AS agreement_type,
    CAST('', 'String') AS cpv_code,
    CAST('yes', 'String') AS directive_governed,
    CAST(any(w.awarded_amount_original), 'Nullable(Decimal(38, 2))') AS value_amount_original,
    CAST(any(w.awarded_currency), 'String') AS value_currency,
    CAST(any(w.awarded_amount_usd), 'Nullable(Decimal(38, 2))') AS value_amount_usd,
    CAST(any(n.total_value_amount_original), 'Nullable(Decimal(38, 2))') AS notice_value_amount_original,
    CAST(any(n.total_value_currency), 'String') AS notice_value_currency,
    CAST(any(n.total_value_amount_usd), 'Nullable(Decimal(38, 2))') AS notice_value_amount_usd,
    CAST('awarded_amount', 'String') AS value_source_field,
    CAST('total_value_amount', 'String') AS notice_value_source_field,
    CAST(max(greatest(w.resolved_at, n.resolved_at)), 'DateTime64(3, \'UTC\')') AS source_updated_at,
    CAST(if((w.publication_date IS NULL) OR (any(n.buyer_name) = '') OR (any(n.notice_title) = ''), '',
        lower(hex(MD5(concat(lowerUTF8(replaceRegexpAll(trimBoth(any(n.buyer_name)), '\\s+', ' ')), '|',
        toString(w.publication_date), '|',
        lowerUTF8(replaceRegexpAll(trimBoth(any(n.notice_title)), '\\s+', ' '))))))), 'String') AS contract_key
FROM corpscout.ted_notice_winners AS w
INNER JOIN corpscout.ted_notices AS n
    ON (n.country_iso2 = w.country_iso2) AND (n.publication_number = w.publication_number)
INNER JOIN corpscout.lv_companies AS c ON c.regcode = w.winner_national_id
WHERE (w.country_iso2 = 'LV')
  AND (upper(w.winner_country) IN ('LV', 'LVA'))
  AND (length(w.winner_national_id) = 11)
  AND (length(c.regcode) = 11)
GROUP BY c.regcode, w.publication_number, w.lot_id, w.tender_id, w.winner_ordinal, w.publication_date;

DROP VIEW IF EXISTS corpscout.lv_government_contract_summary;

CREATE VIEW corpscout.lv_government_contract_summary AS
WITH cross_source_keys AS
(
    SELECT company_id, contract_key
    FROM corpscout.lv_government_contracts
    WHERE contract_key != ''
    GROUP BY company_id, contract_key
    HAVING uniqExact(source_slug) > 1
)
SELECT
    country_code,
    company_id,
    toUInt32(uniqExact(if(
        (company_id, contract_key) IN (
            SELECT company_id, contract_key FROM cross_source_keys
        ),
        concat('cross:', contract_key),
        contract_id
    ))) AS public_award_count,
    max(publication_date) AS public_award_last_date,
    arraySort(groupUniqArray(source_slug)) AS source_slugs,
    sum(value_amount_usd) AS public_award_value_usd,
    countIf(value_amount_usd IS NOT NULL) AS public_award_valued_count,
    max(source_updated_at) AS source_updated_at
FROM corpscout.lv_government_contracts
GROUP BY country_code, company_id;

DROP VIEW IF EXISTS corpscout.company_government_contract_summary;

CREATE VIEW corpscout.company_government_contract_summary AS
SELECT * FROM corpscout.se_government_contract_summary
UNION ALL
SELECT * FROM corpscout.fi_government_contract_summary
UNION ALL
SELECT * FROM corpscout.no_government_contract_summary
UNION ALL
SELECT * FROM corpscout.br_government_contract_summary
UNION ALL
SELECT * FROM corpscout.fr_government_contract_summary
UNION ALL
SELECT * FROM corpscout.sk_government_contract_summary
UNION ALL
SELECT * FROM corpscout.lv_government_contract_summary;
