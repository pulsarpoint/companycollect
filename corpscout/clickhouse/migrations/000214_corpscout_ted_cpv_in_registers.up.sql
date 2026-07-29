CREATE DATABASE IF NOT EXISTS corpscout;

-- Surface TED's CPV in the per-country contract registers.
--
-- 000213 started storing the classification TED publishes. These seven views
-- were written before the column existed, so each one's TED branch hardcodes
-- CAST('', 'String') AS cpv_code -- meaning cpv_code stayed 0% for every
-- TED-fed country even once the data landed. This replaces that one literal per
-- view with the notice's real code. Nothing else in any view changes.
--
-- Notice grain, not lot grain. The register row is one winner of one lot and
-- carries source_lot_id, so a lot's own code would be the sharper answer -- and
-- for a multi-lot notice split across unrelated categories (13 lots with 13
-- distinct codes was observed) the procedure-level code does misclassify the
-- individual awards. Reaching it needs a join to ted_notice_lots on
-- (country_iso2, publication_number, lot_id) in all seven views: the country
-- must be in the key because one notice legitimately appears under two country
-- scopes, and omitting it would double every joined row. That is a larger,
-- fanout-sensitive change than replacing a literal, so it is left for its own
-- migration with its own verification. Notice grain takes cpv_code from 0% to
-- the 99.7% of procedures that publish one.
--
-- Each block below is its owning migration's text with that single line changed:
--   ee_government_contracts  <- 000206_corpscout_ee_national_procurement
--   fi_government_contracts  <- 000194_corpscout_finland_lot_value
--   fr_government_contracts  <- 000207_corpscout_fr_sk_national_procurement
--   lv_government_contracts  <- 000202_corpscout_lv_national_procurement
--   no_government_contracts  <- 000198_corpscout_no_doffin_notices
--   se_government_contracts  <- 000191_corpscout_contract_value_provenance
--   sk_government_contracts  <- 000207_corpscout_fr_sk_national_procurement

CREATE OR REPLACE VIEW corpscout.ee_government_contracts AS
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
    CAST(any(n.cpv_code), 'String') AS cpv_code,
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
GROUP BY c.reg_code, w.publication_number, w.lot_id, w.tender_id, w.winner_ordinal, w.publication_date
;

CREATE OR REPLACE VIEW corpscout.fi_government_contracts AS
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
    CAST(any(n.cpv_code), 'String') AS cpv_code,
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
    w.publication_date
;

CREATE OR REPLACE VIEW corpscout.fr_government_contracts AS
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
    CAST(any(n.cpv_code), 'String') AS cpv_code,
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
GROUP BY c.siren, w.publication_number, w.lot_id, w.tender_id, w.winner_ordinal, w.publication_date
;

CREATE OR REPLACE VIEW corpscout.lv_government_contracts AS
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
    CAST(any(n.cpv_code), 'String') AS cpv_code,
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
GROUP BY c.regcode, w.publication_number, w.lot_id, w.tender_id, w.winner_ordinal, w.publication_date
;

CREATE OR REPLACE VIEW corpscout.no_government_contracts AS
SELECT
    CAST('NO', 'String') AS country_code,
    CAST(c.org_number, 'String') AS company_id,
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
    CAST(any(n.cpv_code), 'String') AS cpv_code,
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
INNER JOIN corpscout.no_companies AS c ON c.org_number = w.winner_national_id
WHERE (w.country_iso2 = 'NO')
  AND (upper(w.winner_country) IN ('NO', 'NOR'))
  AND (length(w.winner_national_id) = 9)
  AND (length(c.org_number) = 9)
GROUP BY c.org_number, w.publication_number, w.lot_id, w.tender_id, w.winner_ordinal, w.publication_date

UNION ALL

SELECT
    CAST('NO', 'String') AS country_code,
    CAST(d.company_id, 'String') AS company_id,
    CAST(concat('doffin:', d.doffin_id, ':', d.lot_id, ':', toString(d.winner_ordinal)), 'String') AS contract_id,
    CAST('norway_doffin_procurement', 'String') AS source_slug,
    CAST(d.doffin_id, 'String') AS source_notice_id,
    CAST(d.lot_id, 'String') AS source_lot_id,
    CAST(d.winner_ordinal, 'Int32') AS source_winner_ordinal,
    CAST(d.winner_name, 'String') AS winner_name,
    CAST(d.source_url, 'String') AS source_url,
    d.publication_date AS publication_date,
    CAST(d.buyer_name, 'String') AS buyer_name,
    CAST(d.buyer_org_number, 'String') AS buyer_id,
    CAST(d.notice_title, 'String') AS title,
    CAST('', 'String') AS agreement_type,
    -- Doffin publishes several CPV codes per notice and the view's column is
    -- one, so it carries the first. The full array stays on no_doffin_notices.
    CAST(if(length(d.cpv_codes) > 0, d.cpv_codes[1], ''), 'String') AS cpv_code,
    -- Published as cbc:RegulatoryDomain rather than inferred.
    CAST(d.directive_governed, 'String') AS directive_governed,
    d.value_amount_original AS value_amount_original,
    CAST(d.value_currency, 'String') AS value_currency,
    d.value_amount_usd AS value_amount_usd,
    d.notice_value_amount_original AS notice_value_amount_original,
    CAST(d.notice_value_currency, 'String') AS notice_value_currency,
    d.notice_value_amount_usd AS notice_value_amount_usd,
    -- The eForms business term behind each number, so a displayed figure can be
    -- checked against the notice. The estimate is deliberately not surfaced
    -- here: this column pair is defined as realized figures.
    CAST('BT-720 PayableAmount', 'String') AS value_source_field,
    CAST('BT-161 TotalAmount', 'String') AS notice_value_source_field,
    d.resolved_at AS source_updated_at,
    -- Same buyer|date|title hash TED's branch builds, so a notice published in
    -- both registers can collapse. Both sides are eForms, unlike Sweden's
    -- UHM-vs-TED pairing where the hash matched nothing at all.
    CAST(if((d.publication_date IS NULL) OR (d.buyer_name = '') OR (d.notice_title = ''), '',
        lower(hex(MD5(concat(lowerUTF8(replaceRegexpAll(trimBoth(d.buyer_name), '\\s+', ' ')), '|',
        toString(d.publication_date), '|',
        lowerUTF8(replaceRegexpAll(trimBoth(d.notice_title), '\\s+', ' '))))))), 'String') AS contract_key
FROM corpscout.no_doffin_notices AS d FINAL
WHERE d.company_match_status = 'exact' AND d.company_id != ''
;

CREATE OR REPLACE VIEW corpscout.se_government_contracts AS
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
    CAST(any(n.cpv_code), 'String') AS cpv_code,
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
    w.publication_date
;

CREATE OR REPLACE VIEW corpscout.sk_government_contracts AS
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
    CAST(any(n.cpv_code), 'String') AS cpv_code,
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
GROUP BY c.ico, w.publication_number, w.lot_id, w.tender_id, w.winner_ordinal, w.publication_date
;
