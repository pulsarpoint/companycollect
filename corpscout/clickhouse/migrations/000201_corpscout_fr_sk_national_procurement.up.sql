CREATE DATABASE IF NOT EXISTS corpscout;

CREATE TABLE IF NOT EXISTS corpscout.fr_decp_contract_holders
(
    company_id String,
    company_match_status LowCardinality(String),
    source_slug LowCardinality(String),
    source_run_id String,
    source_record_id String,
    contract_id String,
    holder_ordinal Int32,
    holder_id_raw String,
    holder_id_type String,
    holder_siren String,
    buyer_id_raw String,
    buyer_siren String,
    notification_date Nullable(Date),
    publication_date Nullable(Date),
    title String,
    nature LowCardinality(String),
    procedure String,
    cpv_code String,
    duration_months Nullable(Int32),
    contract_amount_eur Nullable(Decimal(38, 2)),
    contract_amount_usd Nullable(Decimal(38, 2)),
    contract_amount_attributable UInt8,
    price_form String,
    offers_received Nullable(Int32),
    framework_id String,
    modification_id String,
    modification_amount_eur Nullable(Decimal(38, 2)),
    modification_notification_date Nullable(Date),
    subcontract_id String,
    subcontract_amount_eur Nullable(Decimal(38, 2)),
    subcontractor_id_raw String,
    source_system LowCardinality(String),
    source_url String,
    source_object_key String,
    source_retrieved_at DateTime64(3, 'UTC'),
    resolved_at DateTime64(3, 'UTC'),
    match_eligibility LowCardinality(String)
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY source_record_id;

CREATE TABLE IF NOT EXISTS corpscout.sk_uvo_procurement_notices
(
    company_id String,
    company_match_status LowCardinality(String),
    country_code LowCardinality(String),
    source_slug LowCardinality(String),
    source_run_id String,
    source_record_id String,
    uvo_notice_id String,
    bulletin_number String,
    bulletin_code String,
    publication_date Date,
    procedure_id String,
    notice_version_id String,
    buyer_name String,
    buyer_ico String,
    title String,
    cpv_code String,
    lot_id String,
    lot_title String,
    winner_ordinal Int32,
    winner_name String,
    winner_id_raw String,
    winner_ico String,
    winner_country LowCardinality(String),
    contract_conclusion_date Nullable(Date),
    awarded_amount_eur Nullable(Decimal(38, 2)),
    awarded_amount_usd Nullable(Decimal(38, 2)),
    awarded_currency LowCardinality(String),
    lowest_tender_amount_eur Nullable(Decimal(38, 2)),
    highest_tender_amount_eur Nullable(Decimal(38, 2)),
    notice_value_amount_eur Nullable(Decimal(38, 2)),
    received_tenders Nullable(Int32),
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

DROP VIEW IF EXISTS corpscout.fr_government_contracts;

CREATE VIEW corpscout.fr_government_contracts AS
SELECT
    CAST('FR', 'String') AS country_code,
    CAST(d.company_id, 'String') AS company_id,
    CAST(concat('decp:', d.source_record_id), 'String') AS contract_id,
    CAST('france_decp_procurement', 'String') AS source_slug,
    CAST(d.contract_id, 'String') AS source_notice_id,
    CAST('', 'String') AS source_lot_id,
    CAST(d.holder_ordinal, 'Int32') AS source_winner_ordinal,
    CAST('', 'String') AS winner_name,
    CAST(d.source_url, 'String') AS source_url,
    coalesce(d.notification_date, d.publication_date) AS publication_date,
    CAST('', 'String') AS buyer_name,
    CAST(if(d.buyer_siren != '', d.buyer_siren, d.buyer_id_raw), 'String') AS buyer_id,
    CAST(d.title, 'String') AS title,
    CAST(d.nature, 'String') AS agreement_type,
    CAST(d.cpv_code, 'String') AS cpv_code,
    CAST('', 'String') AS directive_governed,
    CAST(NULL, 'Nullable(Decimal(38, 2))') AS value_amount_original,
    CAST('', 'String') AS value_currency,
    CAST(NULL, 'Nullable(Decimal(38, 2))') AS value_amount_usd,
    CAST(d.contract_amount_eur, 'Nullable(Decimal(38, 2))') AS notice_value_amount_original,
    CAST('EUR', 'String') AS notice_value_currency,
    CAST(d.contract_amount_usd, 'Nullable(Decimal(38, 2))') AS notice_value_amount_usd,
    CAST('', 'String') AS value_source_field,
    CAST('montant', 'String') AS notice_value_source_field,
    CAST(d.resolved_at, 'DateTime64(3, \'UTC\')') AS source_updated_at,
    CAST(if(
        (coalesce(d.notification_date, d.publication_date) IS NULL)
        OR (d.buyer_siren = '')
        OR (d.title = ''),
        '',
        lower(hex(MD5(concat(
            d.buyer_siren,
            '|',
            toString(coalesce(d.notification_date, d.publication_date)),
            '|',
            lowerUTF8(replaceRegexpAll(trimBoth(d.title), '\\s+', ' '))
        ))))
    ), 'String') AS contract_key
FROM corpscout.fr_decp_contract_holders AS d
WHERE (d.company_match_status = 'exact')
  AND (d.company_id != '')

UNION ALL

SELECT
    CAST('FR', 'String') AS country_code,
    CAST(c.siren, 'String') AS company_id,
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
INNER JOIN corpscout.fr_companies AS c ON c.siren = w.winner_national_id
WHERE (w.country_iso2 = 'FR')
  AND (upper(w.winner_country) IN ('FR', 'FRA'))
  AND (length(w.winner_national_id) = 9)
  AND (length(c.siren) = 9)
GROUP BY c.siren, w.publication_number, w.lot_id, w.tender_id, w.winner_ordinal, w.publication_date;

DROP VIEW IF EXISTS corpscout.sk_government_contracts;

CREATE VIEW corpscout.sk_government_contracts AS
SELECT
    CAST('SK', 'String') AS country_code,
    CAST(u.company_id, 'String') AS company_id,
    CAST(concat('uvo:', u.source_record_id), 'String') AS contract_id,
    CAST('slovakia_uvo_procurement', 'String') AS source_slug,
    CAST(u.uvo_notice_id, 'String') AS source_notice_id,
    CAST(u.lot_id, 'String') AS source_lot_id,
    CAST(u.winner_ordinal, 'Int32') AS source_winner_ordinal,
    CAST(u.winner_name, 'String') AS winner_name,
    CAST(u.source_url, 'String') AS source_url,
    CAST(u.publication_date, 'Nullable(Date)') AS publication_date,
    CAST(u.buyer_name, 'String') AS buyer_name,
    CAST(u.buyer_ico, 'String') AS buyer_id,
    CAST(if(u.lot_title != '', u.lot_title, u.title), 'String') AS title,
    CAST('', 'String') AS agreement_type,
    CAST(u.cpv_code, 'String') AS cpv_code,
    CAST(u.directive_governed, 'String') AS directive_governed,
    CAST(u.awarded_amount_eur, 'Nullable(Decimal(38, 2))') AS value_amount_original,
    CAST(u.awarded_currency, 'String') AS value_currency,
    CAST(u.awarded_amount_usd, 'Nullable(Decimal(38, 2))') AS value_amount_usd,
    CAST(u.notice_value_amount_eur, 'Nullable(Decimal(38, 2))') AS notice_value_amount_original,
    CAST(if(u.notice_value_amount_eur IS NULL, '', 'EUR'), 'String') AS notice_value_currency,
    CAST(NULL, 'Nullable(Decimal(38, 2))') AS notice_value_amount_usd,
    CAST('BT-720', 'String') AS value_source_field,
    CAST('notice_value', 'String') AS notice_value_source_field,
    CAST(u.resolved_at, 'DateTime64(3, \'UTC\')') AS source_updated_at,
    CAST(if(
        (u.publication_date IS NULL)
        OR (u.buyer_name = '')
        OR ((u.lot_title = '') AND (u.title = '')),
        '',
        lower(hex(MD5(concat(
            lowerUTF8(replaceRegexpAll(trimBoth(u.buyer_name), '\\s+', ' ')),
            '|',
            toString(u.publication_date),
            '|',
            lowerUTF8(replaceRegexpAll(
                trimBoth(if(u.lot_title != '', u.lot_title, u.title)),
                '\\s+',
                ' '
            ))
        ))))
    ), 'String') AS contract_key
FROM corpscout.sk_uvo_procurement_notices AS u
WHERE (u.company_match_status = 'exact')
  AND (u.company_id != '')
  AND (u.directive_governed = 'no')

UNION ALL

SELECT
    CAST('SK', 'String') AS country_code,
    CAST(c.ico, 'String') AS company_id,
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
INNER JOIN corpscout.sk_companies AS c ON c.ico = w.winner_national_id
WHERE (w.country_iso2 = 'SK')
  AND (upper(w.winner_country) IN ('SK', 'SVK'))
  AND (length(w.winner_national_id) = 8)
  AND (length(c.ico) = 8)
GROUP BY c.ico, w.publication_number, w.lot_id, w.tender_id, w.winner_ordinal, w.publication_date;

DROP VIEW IF EXISTS corpscout.fr_government_contract_summary;

CREATE VIEW corpscout.fr_government_contract_summary AS
WITH cross_source_keys AS
(
    SELECT company_id, contract_key
    FROM corpscout.fr_government_contracts
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
FROM corpscout.fr_government_contracts
GROUP BY country_code, company_id;

DROP VIEW IF EXISTS corpscout.sk_government_contract_summary;

CREATE VIEW corpscout.sk_government_contract_summary AS
WITH cross_source_keys AS
(
    SELECT company_id, contract_key
    FROM corpscout.sk_government_contracts
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
FROM corpscout.sk_government_contracts
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
SELECT * FROM corpscout.sk_government_contract_summary;
