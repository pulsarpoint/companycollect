CREATE DATABASE IF NOT EXISTS corpscout;

-- Every Norwegian award, including the ones whose supplier we could not match.
--
-- Same split as 000217 did for Brazil. `no_government_contracts` is COMPANY-keyed
-- and must stay that way for company pages and the cross-country union, so this
-- is a second view answering the other question: one row per award, matched or
-- not, plus the three columns a reader needs to interpret an unmatched supplier.
--
-- Norway's two branches hide their exclusions differently, and each needs its own
-- treatment:
--
-- Doffin branch: `WHERE company_match_status = 'exact' AND company_id != ''`
--   dropped, EXCEPT for `no_winner_named`. Those 54,847 rows are notices with no
--   award at all, so they are not contracts and do not belong in a contracts
--   register -- excluding them is the one part of that predicate that was about
--   the data rather than about our matcher. What is recovered is 2,635
--   unmatched_company and 1,461 foreign_winner.
--
-- TED branch: the exclusion was never a match predicate at all. Three separate
--   conditions each dropped rows:
--     upper(winner_country) IN ('NO','NOR')  -> domestic only, silently discarding
--                                               1,012 FOREIGN winners
--     length(winner_national_id) = 9         -> well-formed ids only
--     length(c.org_number) = 9               -> a condition on the JOINED table,
--                                               which is what makes the join
--                                               effectively INNER
--   The join becomes LEFT and the id-length test moves into it, where it belongs:
--   left in the WHERE it would filter the unmatched rows straight back out, which
--   is exactly the trap a generated version of this migration fell into.
--
-- winner_match_status is DERIVED for the TED branch, because TED winners never
-- had one -- they matched by national id or not at all. The vocabulary matches
-- what the national sources already emit so the UI needs one label map, not two.
-- Order matters: foreign is checked before the id tests, since a foreign
-- company's id is not expected to be a Norwegian org number in the first place.
--
-- Verified read-only against production before this was committed:
--   awards 55,493 vs company-keyed 49,642, so 5,851 recovered
--   TED branch totals 22,314 = every TED winner Norway has, no more and no less
--   Doffin branch totals 33,179 = 29,083 exact + 4,096 (58,943 non-exact less the
--     54,847 no_winner_named), both matching independent counts to the row
--   contract_id is non-unique here (55,493 rows over 41,422 ids), which is
--     PRE-EXISTING and by design: TED's contract_id is ted:publication:lot with
--     no winner ordinal, so a multi-winner lot is several rows. The company-keyed
--     view already runs at 2.66 rows per id for TED. Not fanout.
--
-- One deliberate difference from the company-keyed view: it admits 27 fewer
-- 'exact' TED rows, because it also required upper(winner_country) IN
-- ('NO','NOR'). Those 27 winners carry a national id that DOES match a Norwegian
-- company while TED states a foreign country (DNK 13, ARE 5, DEU 4, SWE 2, CAN,
-- GBR, FIN) -- Norwegian-registered entities with a foreign address, or a wrong
-- country field. The match genuinely succeeded, so they are kept and labelled
-- 'exact' rather than dropped, which is what happens today.

CREATE OR REPLACE VIEW corpscout.no_government_contract_awards AS
SELECT
    CAST('NO', 'String') AS country_code,
    CAST(c.org_number, 'String') AS company_id,
    CAST(concat('ted:', w.publication_number, ':', w.lot_id), 'String') AS contract_id,
    CAST('ted_procurement', 'String') AS source_slug,
    CAST(w.publication_number, 'String') AS source_notice_id,
    CAST(w.lot_id, 'String') AS source_lot_id,
    CAST(w.winner_ordinal, 'Int32') AS source_winner_ordinal,
    CAST(any(w.winner_name), 'String') AS winner_name,
    -- The id TED published, kept whether or not it resolved to a company.
    CAST(any(w.winner_national_id), 'String') AS winner_registered_id,
    CAST(multiIf(
        c.org_number != '', 'exact',
        (any(w.winner_country) != '')
            AND (upper(any(w.winner_country)) NOT IN ('NO', 'NOR')), 'foreign_winner',
        any(w.winner_national_id) = '', 'missing_supplier_id',
        length(any(w.winner_national_id)) != 9, 'invalid_identifier',
        'unmatched_company'), 'String') AS winner_match_status,
    CAST(any(w.winner_country), 'String') AS winner_country,
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
LEFT JOIN corpscout.no_companies AS c
    ON (c.org_number = w.winner_national_id)
    AND (length(w.winner_national_id) = 9)
WHERE w.country_iso2 = 'NO'
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
    -- Doffin's own normalized org number, present even when it matched nothing.
    CAST(d.winner_org_number, 'String') AS winner_registered_id,
    CAST(d.company_match_status, 'String') AS winner_match_status,
    CAST(d.winner_country, 'String') AS winner_country,
    CAST(d.source_url, 'String') AS source_url,
    d.publication_date AS publication_date,
    CAST(d.buyer_name, 'String') AS buyer_name,
    CAST(d.buyer_org_number, 'String') AS buyer_id,
    CAST(d.notice_title, 'String') AS title,
    CAST('', 'String') AS agreement_type,
    CAST(if(length(d.cpv_codes) > 0, d.cpv_codes[1], ''), 'String') AS cpv_code,
    CAST(d.directive_governed, 'String') AS directive_governed,
    d.value_amount_original AS value_amount_original,
    CAST(d.value_currency, 'String') AS value_currency,
    d.value_amount_usd AS value_amount_usd,
    d.notice_value_amount_original AS notice_value_amount_original,
    CAST(d.notice_value_currency, 'String') AS notice_value_currency,
    d.notice_value_amount_usd AS notice_value_amount_usd,
    CAST('BT-720 PayableAmount', 'String') AS value_source_field,
    CAST('BT-161 TotalAmount', 'String') AS notice_value_source_field,
    d.resolved_at AS source_updated_at,
    CAST(if((d.publication_date IS NULL) OR (d.buyer_name = '') OR (d.notice_title = ''), '',
        lower(hex(MD5(concat(lowerUTF8(replaceRegexpAll(trimBoth(d.buyer_name), '\\s+', ' ')), '|',
        toString(d.publication_date), '|',
        lowerUTF8(replaceRegexpAll(trimBoth(d.notice_title), '\\s+', ' '))))))), 'String') AS contract_key
FROM corpscout.no_doffin_notices AS d FINAL
-- Everything except notices with no award at all.
WHERE d.company_match_status != 'no_winner_named';
