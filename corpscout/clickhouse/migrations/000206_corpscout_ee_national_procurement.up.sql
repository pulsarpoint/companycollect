CREATE DATABASE IF NOT EXISTS corpscout;

CREATE TABLE IF NOT EXISTS corpscout.ee_rhr_procurement_notices
(
    country_code LowCardinality(String),
    source_slug LowCardinality(String),
    source_run_id String,
    notice_version_id String,
    notice_id String,
    version_id String,
    changed_notice_version_id String,
    procedure_id String,
    notice_type LowCardinality(String),
    notice_subtype LowCardinality(String),
    publication_date Date,
    buyer_name String,
    buyer_id_raw String,
    buyer_reg_code String,
    title String,
    cpv_code String,
    ted_publication_number String,
    ted_publication_date Nullable(Date),
    directive_governed LowCardinality(String),
    total_value_amount_original Nullable(Decimal(38, 2)),
    total_value_currency LowCardinality(String),
    estimated_value_amount_original Nullable(Decimal(38, 2)),
    estimated_value_currency LowCardinality(String),
    framework_maximum_amount_original Nullable(Decimal(38, 2)),
    framework_maximum_currency LowCardinality(String),
    framework_total_maximum_amount_original Nullable(Decimal(38, 2)),
    framework_total_maximum_currency LowCardinality(String),
    framework_total_approximate_amount_original Nullable(Decimal(38, 2)),
    framework_total_approximate_currency LowCardinality(String),
    source_url String,
    source_object_key String,
    source_retrieved_at DateTime64(3, 'UTC'),
    resolved_at DateTime64(3, 'UTC'),
    partition_key String
)
ENGINE = ReplacingMergeTree(resolved_at)
PARTITION BY partition_key
ORDER BY notice_version_id;

CREATE TABLE IF NOT EXISTS corpscout.ee_rhr_procurement_lots
(
    country_code LowCardinality(String),
    source_slug LowCardinality(String),
    source_run_id String,
    notice_version_id String,
    lot_id String,
    lot_title String,
    estimated_value_amount_original Nullable(Decimal(38, 2)),
    estimated_value_currency LowCardinality(String),
    framework_maximum_amount_original Nullable(Decimal(38, 2)),
    framework_maximum_currency LowCardinality(String),
    framework_value_maximum_amount_original Nullable(Decimal(38, 2)),
    framework_value_maximum_currency LowCardinality(String),
    framework_value_reestimated_amount_original Nullable(Decimal(38, 2)),
    framework_value_reestimated_currency LowCardinality(String),
    lower_tender_amount_original Nullable(Decimal(38, 2)),
    lower_tender_currency LowCardinality(String),
    higher_tender_amount_original Nullable(Decimal(38, 2)),
    higher_tender_currency LowCardinality(String),
    settled_contract_count Int32,
    settled_contracts_json String,
    publication_date Date,
    source_object_key String,
    resolved_at DateTime64(3, 'UTC'),
    partition_key String
)
ENGINE = ReplacingMergeTree(resolved_at)
PARTITION BY partition_key
ORDER BY (notice_version_id, lot_id);

CREATE TABLE IF NOT EXISTS corpscout.ee_rhr_procurement_winners
(
    company_id String,
    company_match_status LowCardinality(String),
    country_code LowCardinality(String),
    source_slug LowCardinality(String),
    source_run_id String,
    source_record_id String,
    notice_version_id String,
    notice_id String,
    procedure_id String,
    lot_id String,
    tender_id String,
    winner_ordinal Int32,
    winner_name String,
    winner_id_raw String,
    winner_reg_code String,
    winner_country LowCardinality(String),
    awarded_amount_original Nullable(Decimal(38, 2)),
    awarded_amount_eur Nullable(Decimal(38, 2)),
    awarded_amount_usd Nullable(Decimal(38, 2)),
    awarded_currency LowCardinality(String),
    subcontracting_amount_original Nullable(Decimal(38, 2)),
    subcontracting_amount_eur Nullable(Decimal(38, 2)),
    subcontracting_amount_usd Nullable(Decimal(38, 2)),
    subcontracting_currency LowCardinality(String),
    awarded_value_attributable UInt8,
    publication_date Date,
    source_object_key String,
    resolved_at DateTime64(3, 'UTC'),
    partition_key String,
    match_eligibility LowCardinality(String)
)
ENGINE = ReplacingMergeTree(resolved_at)
PARTITION BY partition_key
ORDER BY source_record_id;

DROP VIEW IF EXISTS corpscout.ee_rhr_procurement_winners_current;
DROP VIEW IF EXISTS corpscout.ee_rhr_procurement_lots_current;
DROP VIEW IF EXISTS corpscout.ee_rhr_procurement_notices_current;

CREATE VIEW corpscout.ee_rhr_procurement_notices_current AS
SELECT *
FROM corpscout.ee_rhr_procurement_notices
WHERE notice_version_id NOT IN
(
    SELECT changed_notice_version_id
    FROM corpscout.ee_rhr_procurement_notices
    WHERE changed_notice_version_id != ''
);

CREATE VIEW corpscout.ee_rhr_procurement_lots_current AS
SELECT l.*
FROM corpscout.ee_rhr_procurement_lots AS l
INNER JOIN corpscout.ee_rhr_procurement_notices_current AS n
    ON n.notice_version_id = l.notice_version_id;

CREATE VIEW corpscout.ee_rhr_procurement_winners_current AS
SELECT w.*
FROM corpscout.ee_rhr_procurement_winners AS w
INNER JOIN corpscout.ee_rhr_procurement_notices_current AS n
    ON n.notice_version_id = w.notice_version_id;

DROP VIEW IF EXISTS corpscout.ee_government_contracts;

CREATE VIEW corpscout.ee_government_contracts AS
SELECT
    CAST('EE', 'String') AS country_code,
    CAST(w.company_id, 'String') AS company_id,
    CAST(concat('rhr:', w.source_record_id), 'String') AS contract_id,
    CAST('estonia_rhr_procurement', 'String') AS source_slug,
    CAST(w.notice_id, 'String') AS source_notice_id,
    CAST(w.lot_id, 'String') AS source_lot_id,
    CAST(w.winner_ordinal, 'Int32') AS source_winner_ordinal,
    CAST(w.winner_name, 'String') AS winner_name,
    CAST(n.source_url, 'String') AS source_url,
    CAST(n.publication_date, 'Nullable(Date)') AS publication_date,
    CAST(n.buyer_name, 'String') AS buyer_name,
    CAST(if(n.buyer_reg_code != '', n.buyer_reg_code, n.buyer_id_raw), 'String') AS buyer_id,
    CAST(if(l.lot_title != '', l.lot_title, n.title), 'String') AS title,
    CAST('', 'String') AS agreement_type,
    CAST(n.cpv_code, 'String') AS cpv_code,
    CAST(n.directive_governed, 'String') AS directive_governed,
    CAST(
        if(w.awarded_value_attributable = 1, w.awarded_amount_original, NULL),
        'Nullable(Decimal(38, 2))'
    ) AS value_amount_original,
    CAST(
        if(
            (w.awarded_value_attributable = 1)
            AND (w.awarded_amount_original IS NOT NULL),
            w.awarded_currency,
            ''
        ),
        'String'
    ) AS value_currency,
    CAST(
        if(w.awarded_value_attributable = 1, w.awarded_amount_usd, NULL),
        'Nullable(Decimal(38, 2))'
    ) AS value_amount_usd,
    CAST(n.total_value_amount_original, 'Nullable(Decimal(38, 2))') AS notice_value_amount_original,
    CAST(n.total_value_currency, 'String') AS notice_value_currency,
    CAST(NULL, 'Nullable(Decimal(38, 2))') AS notice_value_amount_usd,
    CAST('BT-720', 'String') AS value_source_field,
    CAST('BT-161', 'String') AS notice_value_source_field,
    CAST(greatest(w.resolved_at, n.resolved_at, l.resolved_at), 'DateTime64(3, \'UTC\')') AS source_updated_at,
    CAST(if(
        (n.publication_date IS NULL)
        OR (n.buyer_name = '')
        OR ((l.lot_title = '') AND (n.title = '')),
        '',
        lower(hex(MD5(concat(
            lowerUTF8(replaceRegexpAll(trimBoth(n.buyer_name), '\\s+', ' ')),
            '|',
            toString(n.publication_date),
            '|',
            lowerUTF8(replaceRegexpAll(
                trimBoth(if(l.lot_title != '', l.lot_title, n.title)),
                '\\s+',
                ' '
            ))
        ))))
    ), 'String') AS contract_key
FROM corpscout.ee_rhr_procurement_winners_current AS w
INNER JOIN corpscout.ee_rhr_procurement_notices_current AS n
    ON n.notice_version_id = w.notice_version_id
INNER JOIN corpscout.ee_rhr_procurement_lots_current AS l
    ON (l.notice_version_id = w.notice_version_id) AND (l.lot_id = w.lot_id)
WHERE (w.company_match_status = 'exact')
  AND (w.company_id != '')

UNION ALL

SELECT
    CAST('EE', 'String') AS country_code,
    CAST(c.reg_code, 'String') AS company_id,
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
INNER JOIN corpscout.ee_companies AS c ON c.reg_code = w.winner_national_id
WHERE (w.country_iso2 = 'EE')
  AND (upper(w.winner_country) IN ('EE', 'EST'))
  AND (length(w.winner_national_id) = 8)
  AND (length(c.reg_code) = 8)
  AND w.publication_number NOT IN
  (
      SELECT ted_publication_number
      FROM corpscout.ee_rhr_procurement_notices_current
      WHERE ted_publication_number != ''
  )
GROUP BY c.reg_code, w.publication_number, w.lot_id, w.tender_id, w.winner_ordinal, w.publication_date;

DROP VIEW IF EXISTS corpscout.ee_government_contract_summary;

CREATE VIEW corpscout.ee_government_contract_summary AS
SELECT
    country_code,
    company_id,
    toUInt32(uniqExact(contract_id)) AS public_award_count,
    max(publication_date) AS public_award_last_date,
    arraySort(groupUniqArray(source_slug)) AS source_slugs,
    sum(value_amount_usd) AS public_award_value_usd,
    countIf(value_amount_usd IS NOT NULL) AS public_award_valued_count,
    max(source_updated_at) AS source_updated_at
FROM corpscout.ee_government_contracts
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
SELECT * FROM corpscout.lv_government_contract_summary
UNION ALL
SELECT * FROM corpscout.ee_government_contract_summary;
