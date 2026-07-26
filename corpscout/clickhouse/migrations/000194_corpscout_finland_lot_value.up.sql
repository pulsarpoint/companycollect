CREATE DATABASE IF NOT EXISTS corpscout;

-- Give Finland a real per-winner contract value.
--
-- The previous definition read procurement_value, a notice-level total, and set
-- value_amount_* to NULL on the grounds that Hilma publishes nothing
-- attributable. That was wrong. lots_value_amount_original sits beside it at LOT
-- grain -- 60 distinct values across one 86-lot notice where procurement_value
-- had one -- with better coverage (5,770 notices against 5,396), and it is a
-- realized value, not an estimate: the estimated fields are named
-- *_estimated_* and this is not one.
--
-- Where a lot has exactly one winner that figure is that winner's amount, which
-- is 4,316 of Finland's lots against 1,164 with more. Where several share a lot
-- it moves to notice_value_* instead of being claimed in full by each of them,
-- which is the same error that once multiplied one 330M EUR procurement by its
-- 22 winners.
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
    -- lots_value is a realized value at LOT grain, not an estimate -- the
    -- estimated fields are named *_estimated_* and this is not one. Where a lot
    -- has exactly one winner it is that winner's amount, which is 4,316 of
    -- Finland's lots. Where it has several they share it, so it moves to
    -- notice_value_* rather than being claimed by each of them.
    CAST(if(lw.lot_winner_count = 1, any(n.lots_value_amount_original), NULL)
        AS Nullable(Decimal(38, 2))) AS value_amount_original,
    CAST(if(lw.lot_winner_count = 1, any(n.lots_value_currency), '') AS String)
        AS value_currency,
    CAST(if(lw.lot_winner_count = 1, any(n.lots_value_amount_usd), NULL)
        AS Nullable(Decimal(38, 2))) AS value_amount_usd,
    -- The shared figure: this lot's value when several winners split it, and
    -- the whole procurement's when the lot publishes none. Never sum either.
    CAST(if(
        lw.lot_winner_count > 1,
        any(n.lots_value_amount_original),
        any(n.procurement_value_amount_original)
    ) AS Nullable(Decimal(38, 2))) AS notice_value_amount_original,
    CAST(if(
        lw.lot_winner_count > 1,
        any(n.lots_value_currency),
        any(n.procurement_value_currency)
    ) AS String) AS notice_value_currency,
    CAST(if(
        lw.lot_winner_count > 1,
        any(n.lots_value_amount_usd),
        any(n.procurement_value_amount_usd)
    ) AS Nullable(Decimal(38, 2))) AS notice_value_amount_usd,
    CAST(if(lw.lot_winner_count = 1, 'lots_value', '') AS String)
        AS value_source_field,
    CAST(if(lw.lot_winner_count > 1, 'lots_value', 'procurement_value') AS String)
        AS notice_value_source_field,
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
-- How many companies share this lot. It decides whether the lot's value is one
-- winner's amount or a figure several of them split.
INNER JOIN
(
    SELECT notice_number, lot_id, uniqExact(winner_business_id) AS lot_winner_count
    FROM corpscout.fi_hilma_notice_winners
    WHERE is_award = 1 AND winner_business_id != ''
    GROUP BY notice_number, lot_id
) AS lw
    ON lw.notice_number = w.notice_number AND lw.lot_id = w.lot_id
WHERE w.is_award = 1
  AND w.winner_business_id != ''
GROUP BY
    c.business_id,
    w.notice_number,
    w.lot_id,
    w.winner_ordinal,
    w.published_at,
    lw.lot_winner_count
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
