CREATE DATABASE IF NOT EXISTS corpscout;

-- Government contracts are views over the source tables, not a materialized
-- copy of them.
--
-- The earlier design flattened every source into twelve columns shared by all of
-- them, so any column only one source had was dropped on the floor. The biggest
-- casualty was contract value: TED carries an awarded amount on 15,305 winner
-- rows and Hilma a procurement value on 5,396 notices, and none of it reached
-- the signal layer, because the shared shape had nowhere to put it. Also lost:
-- Hilma's CPV codes, NUTS region, procedure type and trilingual titles, TED's
-- lot and tender ids and buyer national id.
--
-- Detail now stays in the source tables, which already hold it, and each country
-- gets a view merging its own sources. Nothing is copied, so nothing goes stale
-- and no partition is replaced after a source refresh.
--
-- Views are per country rather than one table partitioned by country because the
-- countries genuinely differ: Sweden reads UHM and TED, Finland reads Hilma and
-- TED, Norway has no ingested national register and reads TED alone.
--
-- On value, two different things are kept apart. TED publishes an amount per
-- winner, attributable to that company. Hilma publishes procurement_value at
-- NOTICE level: one figure across every lot and winner -- notice 2026-046809
-- carries a single value across 86 lots, and notice 2026-045162 has 22 winner
-- rows that would each claim its full 330M EUR. So value_amount_* means
-- "attributable to this winner" and is NULL where the source publishes none,
-- while notice_value_* carries the procurement total and repeats across the
-- notice's winners. The second must never be summed per company. Naming them
-- apart is what makes that legible at the call site rather than a footnote.
--
-- Replaces company_government_contract_evidence and the materialized
-- company_government_contract_summary, dropped here for databases that still
-- carry them.
DROP TABLE IF EXISTS corpscout.company_government_contract_evidence;

DROP TABLE IF EXISTS corpscout.company_government_contract_summary;

DROP VIEW IF EXISTS corpscout.se_government_contracts;

-- Every branch CASTs to the exact canonical type. The cross-country merge below
-- reads these views as one table, which requires their columns to line up, and
-- an implicit LowCardinality or Decimal difference between two branches would
-- only surface at query time. Plain String is used throughout: a view stores
-- nothing, so LowCardinality would buy nothing here.
CREATE VIEW corpscout.se_government_contracts AS
SELECT
    CAST('SE' AS String) AS country_code,
    CAST(u.company_id AS String) AS company_id,
    CAST(concat(
        'uhm:',
        lower(hex(MD5(concat(
            u.company_id, '|', u.source_procurement_id, '|', u.source_lot_id,
            '|', ifNull(toString(u.publication_date), ''), '|',
            lowerUTF8(replaceRegexpAll(trim(u.title), '\\s+', ' '))
        ))))
    ) AS String) AS contract_id,
    CAST('sweden_uhm_procurement' AS String) AS source_slug,
    CAST(u.source_procurement_id AS String) AS source_notice_id,
    CAST(u.source_lot_id AS String) AS source_lot_id,
    CAST(0 AS Int32) AS source_winner_ordinal,
    -- UHM publishes no address for an individual award, so the document a row
    -- traces back to is the bulk CSV resource it was parsed out of.
    CAST(any(u.source_url) AS String) AS source_url,
    u.publication_date AS publication_date,
    CAST(u.buyer_name AS String) AS buyer_name,
    CAST(u.buyer_id_normalized AS String) AS buyer_id,
    CAST(u.title AS String) AS title,
    CAST(any(u.agreement_type) AS String) AS agreement_type,
    CAST(any(u.cpv_code) AS String) AS cpv_code,
    -- UHM has no value field at all. All 44 source columns were checked, so
    -- NULL here is the source's own silence, not an extraction gap.
    CAST(NULL AS Nullable(Decimal(38, 2))) AS value_amount_original,
    CAST('' AS String) AS value_currency,
    CAST(NULL AS Nullable(Decimal(38, 2))) AS value_amount_usd,
    CAST(NULL AS Nullable(Decimal(38, 2))) AS notice_value_amount_original,
    CAST('' AS String) AS notice_value_currency,
    CAST(NULL AS Nullable(Decimal(38, 2))) AS notice_value_amount_usd,
    CAST(max(u.source_retrieved_at) AS DateTime64(3, 'UTC')) AS source_updated_at,
    CAST(if(
        u.publication_date IS NULL OR u.buyer_name = '' OR u.title = '',
        '',
        lower(hex(MD5(concat(
            u.company_id, '|',
            lowerUTF8(replaceRegexpAll(trim(u.buyer_name), '\\s+', ' ')), '|',
            toString(u.publication_date), '|',
            lowerUTF8(replaceRegexpAll(trim(u.title), '\\s+', ' '))
        ))))
    ) AS String) AS dedup_key
FROM corpscout.se_uhm_procurement_awards AS u
WHERE u.company_match_status = 'exact'
  AND u.company_id != ''
GROUP BY
    u.company_id,
    u.source_procurement_id,
    u.source_lot_id,
    u.publication_date,
    u.buyer_name,
    u.buyer_id_normalized,
    u.title
UNION ALL
SELECT
    CAST('SE' AS String) AS country_code,
    CAST(c.company_id AS String) AS company_id,
    CAST(concat(
        'ted:', w.publication_number, ':', w.lot_id, ':',
        w.tender_id, ':', toString(w.winner_ordinal)
    ) AS String) AS contract_id,
    CAST('ted_procurement' AS String) AS source_slug,
    CAST(w.publication_number AS String) AS source_notice_id,
    CAST(w.lot_id AS String) AS source_lot_id,
    CAST(w.winner_ordinal AS Int32) AS source_winner_ordinal,
    -- Verified live: this endpoint returns the notice XML (HTTP 200).
    CAST(concat('https://ted.europa.eu/en/notice/', w.publication_number, '/xml')
        AS String) AS source_url,
    w.publication_date AS publication_date,
    CAST(any(n.buyer_name) AS String) AS buyer_name,
    CAST(any(n.buyer_national_id) AS String) AS buyer_id,
    CAST(any(n.notice_title) AS String) AS title,
    CAST('' AS String) AS agreement_type,
    CAST('' AS String) AS cpv_code,
    -- The winner's own awarded amount, never the notice total, which covers
    -- every winner on the notice and would overstate a single company's share.
    CAST(any(w.awarded_amount_original) AS Nullable(Decimal(38, 2)))
        AS value_amount_original,
    CAST(any(w.awarded_currency) AS String) AS value_currency,
    CAST(any(w.awarded_amount_usd) AS Nullable(Decimal(38, 2))) AS value_amount_usd,
    -- The whole notice's total, repeated across its winners. Never sum this.
    CAST(any(n.total_value_amount_original) AS Nullable(Decimal(38, 2)))
        AS notice_value_amount_original,
    CAST(any(n.total_value_currency) AS String) AS notice_value_currency,
    CAST(any(n.total_value_amount_usd) AS Nullable(Decimal(38, 2)))
        AS notice_value_amount_usd,
    CAST(max(greatest(w.resolved_at, n.resolved_at)) AS DateTime64(3, 'UTC'))
        AS source_updated_at,
    CAST(if(
        w.publication_date IS NULL
            OR any(n.buyer_name) = ''
            OR any(n.notice_title) = '',
        '',
        lower(hex(MD5(concat(
            c.company_id, '|',
            lowerUTF8(replaceRegexpAll(trim(any(n.buyer_name)), '\\s+', ' ')), '|',
            toString(w.publication_date), '|',
            lowerUTF8(replaceRegexpAll(trim(any(n.notice_title)), '\\s+', ' '))
        ))))
    ) AS String) AS dedup_key
FROM corpscout.ted_notice_winners AS w
INNER JOIN corpscout.ted_notices AS n
    ON n.country_iso2 = w.country_iso2
   AND n.publication_number = w.publication_number
INNER JOIN corpscout.se_companies AS c
    ON c.company_id = w.winner_national_id
WHERE w.country_iso2 = 'SE'
  AND upper(w.winner_country) IN ('SE', 'SWE')
  AND length(w.winner_national_id) = 10
  AND length(c.company_id) = 10
GROUP BY
    c.company_id,
    w.publication_number,
    w.lot_id,
    w.tender_id,
    w.winner_ordinal,
    w.publication_date;

DROP VIEW IF EXISTS corpscout.fi_government_contracts;

CREATE VIEW corpscout.fi_government_contracts AS
SELECT
    CAST('FI' AS String) AS country_code,
    CAST(c.business_id AS String) AS company_id,
    CAST(concat(
        'hilma:', w.notice_number, ':', w.lot_id, ':',
        toString(w.winner_ordinal)
    ) AS String) AS contract_id,
    CAST('finland_hilma_procurement' AS String) AS source_slug,
    CAST(w.notice_number AS String) AS source_notice_id,
    CAST(w.lot_id AS String) AS source_lot_id,
    CAST(w.winner_ordinal AS Int32) AS source_winner_ordinal,
    -- hankintailmoitukset.fi is a single-page app and answers 200 for any path,
    -- so this pattern could not be confirmed the way TED's was. The notice
    -- number is the portal's own key, but treat a dead link as unsurprising.
    CAST(concat(
        'https://www.hankintailmoitukset.fi/fi/public/procurement/',
        w.notice_number, '/overview'
    ) AS String) AS source_url,
    CAST(toDate(w.published_at) AS Nullable(Date)) AS publication_date,
    -- Names are multilingual. Finnish preferred, English as fallback, matching
    -- how the Finnish detail page already reads them.
    CAST(any(coalesce(nullIf(n.buyer_name_fi, ''), n.buyer_name_en)) AS String)
        AS buyer_name,
    CAST(any(n.buyer_business_id) AS String) AS buyer_id,
    CAST(any(coalesce(
        nullIf(n.lot_name_fi, ''),
        nullIf(n.notice_name_fi, ''),
        nullIf(n.lot_name_en, ''),
        n.notice_name_en
    )) AS String) AS title,
    CAST(any(coalesce(n.procedure_type, '')) AS String) AS agreement_type,
    CAST(any(coalesce(nullIf(n.lot_cpv_code, ''), n.notice_cpv_code)) AS String)
        AS cpv_code,
    -- Hilma publishes no amount per winner, so nothing is attributable to this
    -- company. NULL is the source's silence, not an extraction gap.
    CAST(NULL AS Nullable(Decimal(38, 2))) AS value_amount_original,
    CAST('' AS String) AS value_currency,
    CAST(NULL AS Nullable(Decimal(38, 2))) AS value_amount_usd,
    -- Notice-level and repeated across every lot and winner. Never sum this.
    -- The estimated-value fields are deliberately not used as a fallback: an
    -- estimate in the same column as a realized value would silently mix them.
    CAST(any(n.procurement_value_amount_original) AS Nullable(Decimal(38, 2)))
        AS notice_value_amount_original,
    CAST(any(n.procurement_value_currency) AS String) AS notice_value_currency,
    CAST(any(n.procurement_value_amount_usd) AS Nullable(Decimal(38, 2)))
        AS notice_value_amount_usd,
    CAST(max(greatest(w.resolved_at, n.resolved_at)) AS DateTime64(3, 'UTC'))
        AS source_updated_at,
    CAST(if(
        w.published_at IS NULL
            OR any(coalesce(nullIf(n.buyer_name_fi, ''), n.buyer_name_en)) = ''
            OR any(coalesce(
                nullIf(n.lot_name_fi, ''), nullIf(n.notice_name_fi, ''),
                nullIf(n.lot_name_en, ''), n.notice_name_en
            )) = '',
        '',
        lower(hex(MD5(concat(
            c.business_id, '|',
            lowerUTF8(replaceRegexpAll(
                trim(any(coalesce(nullIf(n.buyer_name_fi, ''), n.buyer_name_en))),
                '\\s+', ' '
            )), '|',
            toString(toDate(w.published_at)), '|',
            lowerUTF8(replaceRegexpAll(trim(any(coalesce(
                nullIf(n.lot_name_fi, ''), nullIf(n.notice_name_fi, ''),
                nullIf(n.lot_name_en, ''), n.notice_name_en
            ))), '\\s+', ' '))
        ))))
    ) AS String) AS dedup_key
FROM corpscout.fi_hilma_notice_winners AS w
INNER JOIN corpscout.fi_hilma_notices AS n
    ON n.notice_number = w.notice_number
   AND n.lot_id = w.lot_id
INNER JOIN corpscout.fi_companies AS c
    ON c.business_id = w.winner_business_id
WHERE w.is_award = 1
  AND w.winner_business_id != ''
GROUP BY
    c.business_id,
    w.notice_number,
    w.lot_id,
    w.winner_ordinal,
    w.published_at
UNION ALL
SELECT
    CAST('FI' AS String) AS country_code,
    CAST(c.business_id AS String) AS company_id,
    CAST(concat(
        'ted:', w.publication_number, ':', w.lot_id, ':',
        w.tender_id, ':', toString(w.winner_ordinal)
    ) AS String) AS contract_id,
    CAST('ted_procurement' AS String) AS source_slug,
    CAST(w.publication_number AS String) AS source_notice_id,
    CAST(w.lot_id AS String) AS source_lot_id,
    CAST(w.winner_ordinal AS Int32) AS source_winner_ordinal,
    CAST(concat('https://ted.europa.eu/en/notice/', w.publication_number, '/xml')
        AS String) AS source_url,
    w.publication_date AS publication_date,
    CAST(any(n.buyer_name) AS String) AS buyer_name,
    CAST(any(n.buyer_national_id) AS String) AS buyer_id,
    CAST(any(n.notice_title) AS String) AS title,
    CAST('' AS String) AS agreement_type,
    CAST('' AS String) AS cpv_code,
    CAST(any(w.awarded_amount_original) AS Nullable(Decimal(38, 2)))
        AS value_amount_original,
    CAST(any(w.awarded_currency) AS String) AS value_currency,
    CAST(any(w.awarded_amount_usd) AS Nullable(Decimal(38, 2))) AS value_amount_usd,
    -- The whole notice's total, repeated across its winners. Never sum this.
    CAST(any(n.total_value_amount_original) AS Nullable(Decimal(38, 2)))
        AS notice_value_amount_original,
    CAST(any(n.total_value_currency) AS String) AS notice_value_currency,
    CAST(any(n.total_value_amount_usd) AS Nullable(Decimal(38, 2)))
        AS notice_value_amount_usd,
    CAST(max(greatest(w.resolved_at, n.resolved_at)) AS DateTime64(3, 'UTC'))
        AS source_updated_at,
    CAST(if(
        w.publication_date IS NULL
            OR any(n.buyer_name) = ''
            OR any(n.notice_title) = '',
        '',
        lower(hex(MD5(concat(
            c.business_id, '|',
            lowerUTF8(replaceRegexpAll(trim(any(n.buyer_name)), '\\s+', ' ')), '|',
            toString(w.publication_date), '|',
            lowerUTF8(replaceRegexpAll(trim(any(n.notice_title)), '\\s+', ' '))
        ))))
    ) AS String) AS dedup_key
FROM corpscout.ted_notice_winners AS w
INNER JOIN corpscout.ted_notices AS n
    ON n.country_iso2 = w.country_iso2
   AND n.publication_number = w.publication_number
INNER JOIN corpscout.fi_companies AS c
    ON c.business_id = w.winner_national_id
WHERE w.country_iso2 = 'FI'
  AND upper(w.winner_country) IN ('FI', 'FIN')
  AND length(w.winner_national_id) = 9
  AND length(c.business_id) = 9
GROUP BY
    c.business_id,
    w.publication_number,
    w.lot_id,
    w.tender_id,
    w.winner_ordinal,
    w.publication_date;

DROP VIEW IF EXISTS corpscout.no_government_contracts;

-- Norway has no ingested national register, so this view has a single branch.
-- That is the shape the design is meant to allow: a country is the list of
-- sources it actually has, not a fixed pair.
CREATE VIEW corpscout.no_government_contracts AS
SELECT
    CAST('NO' AS String) AS country_code,
    CAST(c.org_number AS String) AS company_id,
    CAST(concat(
        'ted:', w.publication_number, ':', w.lot_id, ':',
        w.tender_id, ':', toString(w.winner_ordinal)
    ) AS String) AS contract_id,
    CAST('ted_procurement' AS String) AS source_slug,
    CAST(w.publication_number AS String) AS source_notice_id,
    CAST(w.lot_id AS String) AS source_lot_id,
    CAST(w.winner_ordinal AS Int32) AS source_winner_ordinal,
    CAST(concat('https://ted.europa.eu/en/notice/', w.publication_number, '/xml')
        AS String) AS source_url,
    w.publication_date AS publication_date,
    CAST(any(n.buyer_name) AS String) AS buyer_name,
    CAST(any(n.buyer_national_id) AS String) AS buyer_id,
    CAST(any(n.notice_title) AS String) AS title,
    CAST('' AS String) AS agreement_type,
    CAST('' AS String) AS cpv_code,
    CAST(any(w.awarded_amount_original) AS Nullable(Decimal(38, 2)))
        AS value_amount_original,
    CAST(any(w.awarded_currency) AS String) AS value_currency,
    CAST(any(w.awarded_amount_usd) AS Nullable(Decimal(38, 2))) AS value_amount_usd,
    -- The whole notice's total, repeated across its winners. Never sum this.
    CAST(any(n.total_value_amount_original) AS Nullable(Decimal(38, 2)))
        AS notice_value_amount_original,
    CAST(any(n.total_value_currency) AS String) AS notice_value_currency,
    CAST(any(n.total_value_amount_usd) AS Nullable(Decimal(38, 2)))
        AS notice_value_amount_usd,
    CAST(max(greatest(w.resolved_at, n.resolved_at)) AS DateTime64(3, 'UTC'))
        AS source_updated_at,
    CAST(if(
        w.publication_date IS NULL
            OR any(n.buyer_name) = ''
            OR any(n.notice_title) = '',
        '',
        lower(hex(MD5(concat(
            c.org_number, '|',
            lowerUTF8(replaceRegexpAll(trim(any(n.buyer_name)), '\\s+', ' ')), '|',
            toString(w.publication_date), '|',
            lowerUTF8(replaceRegexpAll(trim(any(n.notice_title)), '\\s+', ' '))
        ))))
    ) AS String) AS dedup_key
FROM corpscout.ted_notice_winners AS w
INNER JOIN corpscout.ted_notices AS n
    ON n.country_iso2 = w.country_iso2
   AND n.publication_number = w.publication_number
INNER JOIN corpscout.no_companies AS c
    ON c.org_number = w.winner_national_id
WHERE w.country_iso2 = 'NO'
  AND upper(w.winner_country) IN ('NO', 'NOR')
  AND length(w.winner_national_id) = 9
  AND length(c.org_number) = 9
GROUP BY
    c.org_number,
    w.publication_number,
    w.lot_id,
    w.tender_id,
    w.winner_ordinal,
    w.publication_date;

DROP VIEW IF EXISTS corpscout.company_government_contracts;

-- The cross-country merge, and the reason the per-country objects are views with
-- a naming convention rather than one table. merge() resolves the regexp at
-- query time and reads views, so adding a country is one CREATE VIEW named
-- <cc>_government_contracts and nothing here changes. A UNION ALL would need
-- editing -- and a migration -- for every country added.
CREATE VIEW corpscout.company_government_contracts AS
SELECT *
FROM merge(corpscout, '^[a-z]{2}_government_contracts$');

DROP VIEW IF EXISTS corpscout.company_government_contract_summary;

-- One row per company, counting contracts rather than source rows: a contract
-- published in both a national register and TED is two rows above, on purpose,
-- because each has its own document to link to, but it is one contract here.
--
-- Collapsing is deliberately limited to keys seen in more than one source and
-- matching one-for-one across them. Within a single source the same buyer, date
-- and title can legitimately describe separate lots, so collapsing on the key
-- alone would undercount.
--
-- Only the winner-attributable value is summed. notice_value_* is deliberately
-- absent: repeated across a notice's winners, its sum is meaningless.
CREATE VIEW corpscout.company_government_contract_summary AS
WITH cross_source_keys AS
(
    SELECT dedup_key
    FROM corpscout.company_government_contracts
    WHERE dedup_key != ''
    GROUP BY dedup_key
    HAVING uniqExact(source_slug) > 1
       AND count() = uniqExact(source_slug)
)
SELECT
    country_code,
    company_id,
    toUInt32(uniqExact(if(
        dedup_key IN (SELECT dedup_key FROM cross_source_keys),
        concat('cross:', dedup_key),
        contract_id
    ))) AS public_award_count,
    max(publication_date) AS public_award_last_date,
    arraySort(groupUniqArray(source_slug)) AS source_slugs,
    sum(value_amount_usd) AS public_award_value_usd,
    countIf(value_amount_usd IS NOT NULL) AS public_award_valued_count,
    max(source_updated_at) AS source_updated_at
FROM corpscout.company_government_contracts
GROUP BY country_code, company_id;
