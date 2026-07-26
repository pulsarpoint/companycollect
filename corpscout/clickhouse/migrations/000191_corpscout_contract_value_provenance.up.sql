CREATE DATABASE IF NOT EXISTS corpscout;

-- Name the register field behind every figure.
--
-- A displayed number that cannot be traced to a column cannot be checked. The
-- UI could say only "Value", while the figure came from a different field in
-- every country: TED publishes awarded_amount per winner, Hilma publishes
-- procurement_value at notice level, Sweden's UHM publishes nothing at all.
-- Three different meanings rendered identically.
--
-- This is the same argument as source_url, one level down: that made evidence
-- traceable to a document, this makes a value traceable to the field it was
-- read from. Someone verifying a contract against its register now knows what
-- to look at.
--
-- The name is non-empty exactly when the value is non-NULL, so an empty field
-- name and a missing figure always agree.
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
    CAST(concat('uhm:', u.source_procurement_id, ':', u.source_lot_id) AS String)
        AS contract_id,
    CAST('sweden_uhm_procurement' AS String) AS source_slug,
    CAST(u.source_procurement_id AS String) AS source_notice_id,
    CAST(u.source_lot_id AS String) AS source_lot_id,
    CAST(0 AS Int32) AS source_winner_ordinal,
    CAST(u.supplier_name AS String) AS winner_name,
    -- UHM publishes no address for an individual award, so the document a row
    -- traces back to is the bulk CSV resource it was parsed out of.
    CAST(any(u.source_url) AS String) AS source_url,
    u.publication_date AS publication_date,
    CAST(u.buyer_name AS String) AS buyer_name,
    CAST(u.buyer_id_normalized AS String) AS buyer_id,
    CAST(u.title AS String) AS title,
    CAST(any(u.agreement_type) AS String) AS agreement_type,
    CAST(any(u.cpv_code) AS String) AS cpv_code,
    CAST(any(u.directive_governed) AS String) AS directive_governed,
    -- UHM has no value field at all. All 44 source columns were checked, so
    -- NULL here is the source's own silence, not an extraction gap.
    CAST(NULL AS Nullable(Decimal(38, 2))) AS value_amount_original,
    CAST('' AS String) AS value_currency,
    CAST(NULL AS Nullable(Decimal(38, 2))) AS value_amount_usd,
    CAST(NULL AS Nullable(Decimal(38, 2))) AS notice_value_amount_original,
    CAST('' AS String) AS notice_value_currency,
    CAST(NULL AS Nullable(Decimal(38, 2))) AS notice_value_amount_usd,
    CAST('' AS String) AS value_source_field,
    CAST('' AS String) AS notice_value_source_field,
    CAST(max(u.source_retrieved_at) AS DateTime64(3, 'UTC')) AS source_updated_at,
    CAST(if(
        u.publication_date IS NULL OR u.buyer_name = '' OR u.title = '',
        '',
        lower(hex(MD5(concat(
            lowerUTF8(replaceRegexpAll(trim(u.buyer_name), '\\s+', ' ')), '|',
            toString(u.publication_date), '|',
            lowerUTF8(replaceRegexpAll(trim(u.title), '\\s+', ' '))
        ))))
    ) AS String) AS contract_key
FROM corpscout.se_uhm_procurement_awards AS u
WHERE u.company_match_status = 'exact'
  AND u.company_id != ''
GROUP BY
    u.company_id,
    u.supplier_name,
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
    CAST(concat('ted:', w.publication_number, ':', w.lot_id) AS String)
        AS contract_id,
    CAST('ted_procurement' AS String) AS source_slug,
    CAST(w.publication_number AS String) AS source_notice_id,
    CAST(w.lot_id AS String) AS source_lot_id,
    CAST(w.winner_ordinal AS Int32) AS source_winner_ordinal,
    CAST(any(w.winner_name) AS String) AS winner_name,
    -- Verified live: this endpoint returns the notice XML (HTTP 200).
    CAST(concat('https://ted.europa.eu/en/notice/', w.publication_number, '/xml')
        AS String) AS source_url,
    w.publication_date AS publication_date,
    CAST(any(n.buyer_name) AS String) AS buyer_name,
    CAST(any(n.buyer_national_id) AS String) AS buyer_id,
    CAST(any(n.notice_title) AS String) AS title,
    CAST('' AS String) AS agreement_type,
    CAST('' AS String) AS cpv_code,
    -- A TED notice exists because the procurement is directive-governed, so
    -- the answer is yes by the fact of its being here.
    CAST('yes' AS String) AS directive_governed,
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
    CAST('awarded_amount' AS String) AS value_source_field,
    CAST('total_value_amount' AS String) AS notice_value_source_field,
    CAST(max(greatest(w.resolved_at, n.resolved_at)) AS DateTime64(3, 'UTC'))
        AS source_updated_at,
    CAST(if(
        w.publication_date IS NULL
            OR any(n.buyer_name) = ''
            OR any(n.notice_title) = '',
        '',
        lower(hex(MD5(concat(
            lowerUTF8(replaceRegexpAll(trim(any(n.buyer_name)), '\\s+', ' ')), '|',
            toString(w.publication_date), '|',
            lowerUTF8(replaceRegexpAll(trim(any(n.notice_title)), '\\s+', ' '))
        ))))
    ) AS String) AS contract_key
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
    CAST(concat('hilma:', w.notice_number, ':', w.lot_id) AS String)
        AS contract_id,
    CAST('finland_hilma_procurement' AS String) AS source_slug,
    CAST(w.notice_number AS String) AS source_notice_id,
    CAST(w.lot_id AS String) AS source_lot_id,
    CAST(w.winner_ordinal AS Int32) AS source_winner_ordinal,
    CAST(any(w.winner_name) AS String) AS winner_name,
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
    -- Hilma publishes no threshold flag, but a TED reference means the notice
    -- went to TED, which only directive-governed procurement does. Absence
    -- proves nothing, so it stays unknown rather than becoming a "no".
    CAST(if(any(n.ted_number) != '', 'yes', '') AS String) AS directive_governed,
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
    CAST('' AS String) AS value_source_field,
    CAST('procurement_value' AS String) AS notice_value_source_field,
    CAST(max(greatest(w.resolved_at, n.resolved_at)) AS DateTime64(3, 'UTC'))
        AS source_updated_at,
    -- Hilma publishes the TED number of the same procurement, so the two
    -- registers link on a reference they share. The previous key hashed buyer,
    -- date and title, and matched nothing at all: the registers render Finnish
    -- names differently, so a fuzzy match was never going to work. 10,879 of
    -- 12,544 Hilma notices carry the reference, 2,943 of them to TED notices
    -- currently held. Grain is the notice, since lot ids do not survive
    -- between the two registers.
    CAST(if(
        any(n.ted_number) = '',
        '',
        concat('ted:', replaceRegexpOne(any(n.ted_number), '^0+', ''))
    ) AS String) AS contract_key
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
    CAST(concat('ted:', w.publication_number, ':', w.lot_id) AS String)
        AS contract_id,
    CAST('ted_procurement' AS String) AS source_slug,
    CAST(w.publication_number AS String) AS source_notice_id,
    CAST(w.lot_id AS String) AS source_lot_id,
    CAST(w.winner_ordinal AS Int32) AS source_winner_ordinal,
    CAST(any(w.winner_name) AS String) AS winner_name,
    CAST(concat('https://ted.europa.eu/en/notice/', w.publication_number, '/xml')
        AS String) AS source_url,
    w.publication_date AS publication_date,
    CAST(any(n.buyer_name) AS String) AS buyer_name,
    CAST(any(n.buyer_national_id) AS String) AS buyer_id,
    CAST(any(n.notice_title) AS String) AS title,
    CAST('' AS String) AS agreement_type,
    CAST('' AS String) AS cpv_code,
    -- A TED notice exists because the procurement is directive-governed, so
    -- the answer is yes by the fact of its being here.
    CAST('yes' AS String) AS directive_governed,
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
    CAST('awarded_amount' AS String) AS value_source_field,
    CAST('total_value_amount' AS String) AS notice_value_source_field,
    CAST(max(greatest(w.resolved_at, n.resolved_at)) AS DateTime64(3, 'UTC'))
        AS source_updated_at,
    CAST(concat('ted:', w.publication_number) AS String) AS contract_key
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
    CAST(concat('ted:', w.publication_number, ':', w.lot_id) AS String)
        AS contract_id,
    CAST('ted_procurement' AS String) AS source_slug,
    CAST(w.publication_number AS String) AS source_notice_id,
    CAST(w.lot_id AS String) AS source_lot_id,
    CAST(w.winner_ordinal AS Int32) AS source_winner_ordinal,
    CAST(any(w.winner_name) AS String) AS winner_name,
    CAST(concat('https://ted.europa.eu/en/notice/', w.publication_number, '/xml')
        AS String) AS source_url,
    w.publication_date AS publication_date,
    CAST(any(n.buyer_name) AS String) AS buyer_name,
    CAST(any(n.buyer_national_id) AS String) AS buyer_id,
    CAST(any(n.notice_title) AS String) AS title,
    CAST('' AS String) AS agreement_type,
    CAST('' AS String) AS cpv_code,
    -- A TED notice exists because the procurement is directive-governed, so
    -- the answer is yes by the fact of its being here.
    CAST('yes' AS String) AS directive_governed,
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
    CAST('awarded_amount' AS String) AS value_source_field,
    CAST('total_value_amount' AS String) AS notice_value_source_field,
    CAST(max(greatest(w.resolved_at, n.resolved_at)) AS DateTime64(3, 'UTC'))
        AS source_updated_at,
    CAST(if(
        w.publication_date IS NULL
            OR any(n.buyer_name) = ''
            OR any(n.notice_title) = '',
        '',
        lower(hex(MD5(concat(
            lowerUTF8(replaceRegexpAll(trim(any(n.buyer_name)), '\\s+', ' ')), '|',
            toString(w.publication_date), '|',
            lowerUTF8(replaceRegexpAll(trim(any(n.notice_title)), '\\s+', ' '))
        ))))
    ) AS String) AS contract_key
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

-- One row per company. Contracts are counted, not source rows: the same
-- procurement published in both a national register and TED is two rows in the
-- view, on purpose, because each has its own document to link to, but it is one
-- contract here.
--
-- Collapsing is limited to keys the same company won under more than one
-- source. Two lots of one procurement stay two contracts, because they have
-- distinct ids within their source.
--
-- Only winner-attributable value is summed. notice_value_* is deliberately
-- absent: repeated across a notice's winners, its sum is meaningless.
CREATE VIEW corpscout.company_government_contract_summary AS
WITH cross_source_keys AS
(
    SELECT company_id, contract_key
    FROM corpscout.company_government_contracts
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
FROM corpscout.company_government_contracts
GROUP BY country_code, company_id;
