CREATE DATABASE IF NOT EXISTS corpscout;

-- Link Finland's two registers on the reference they publish, not on a guess.
--
-- contract_key hashed buyer, date and title, which produced zero cross-source
-- matches for Finland: Hilma and TED render the same Finnish buyer and title
-- differently, so the hashes never agreed. The result looked like "these
-- registers do not overlap" when in fact they overlap heavily and the key was
-- simply unable to see it.
--
-- Hilma publishes ted_number, the TED notice for the same procurement, so the
-- match can be exact. Sweden keeps the hash fallback, because UHM
-- publishes no such cross-reference of its own.
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
