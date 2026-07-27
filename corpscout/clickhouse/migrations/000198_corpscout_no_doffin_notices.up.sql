CREATE DATABASE IF NOT EXISTS corpscout;

-- Norway's national procurement register.
--
-- Until now Norway was TED-only, so every contract below the EU publication
-- threshold was absent and the coverage caveat said so in as many words. Doffin
-- is the register that fills it: 31,097 award notices against TED's 20,559
-- Norwegian rows.
--
-- Grain is one row per (notice, lot, winner), the same shape as TED and Hilma.
--
-- Three monetary figures, each with _original + _usd + its own currency, and
-- none of them merged. value is BT-720, the realized amount for THIS winner,
-- and it is the reason the notice XML is fetched at all -- the search endpoint
-- publishes only the estimate. notice_value is BT-161, the notice total, which
-- would double-count if summed across a notice's winners. estimated_value is
-- BT-27 and is not a contract value at all: one sampled notice estimates
-- 2,500,000 against a realized 1,485,571, so a column holding whichever was
-- present would make all three unreadable.
--
-- ORDER BY carries no Nullable column (allow_nullable_key is off).

CREATE TABLE IF NOT EXISTS corpscout.no_doffin_notices
(
    company_id String,
    company_match_status LowCardinality(String),
    country_code LowCardinality(String),
    source_slug LowCardinality(String),
    source_run_id String,
    source_record_id String,
    source_url String,
    doffin_id String,
    -- A stable procurement UUID shared by a competition notice and its award.
    contract_folder_id String,
    notice_type LowCardinality(String),
    notice_status LowCardinality(String),
    -- The only date the API can filter on, so partitions key on it.
    issue_date Nullable(Date),
    publication_date Nullable(Date),
    deadline_date Nullable(Date),
    buyer_name String,
    buyer_org_number String,
    notice_title String,
    notice_description String,
    cpv_codes Array(String),
    location_ids Array(String),
    lot_id String,
    lot_heading String,
    winner_ordinal Int32,
    winner_name String,
    -- Kept verbatim as well as normalised: a foreign winner's number is not
    -- nine digits and must not be coerced, but losing it would lose the only
    -- identifier that winner has.
    winner_org_number_raw String,
    winner_org_number String,
    winner_country LowCardinality(String),
    value_amount_original Nullable(Decimal(38, 2)),
    value_amount_usd Nullable(Decimal(38, 2)),
    value_currency LowCardinality(String),
    notice_value_amount_original Nullable(Decimal(38, 2)),
    notice_value_amount_usd Nullable(Decimal(38, 2)),
    notice_value_currency LowCardinality(String),
    estimated_value_amount_original Nullable(Decimal(38, 2)),
    estimated_value_amount_usd Nullable(Decimal(38, 2)),
    estimated_value_currency LowCardinality(String),
    fx_rate_to_usd Nullable(Decimal(38, 12)),
    fx_rate_date Nullable(Date),
    fx_source LowCardinality(String),
    -- Published by Doffin as cbc:RegulatoryDomain, not inferred from whether
    -- the notice also reached TED.
    directive_governed LowCardinality(String),
    received_tenders Int32,
    award_result LowCardinality(String),
    partition_key LowCardinality(String),
    source_retrieved_at DateTime64(3, 'UTC'),
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(resolved_at)
PARTITION BY partition_key
ORDER BY (company_id, doffin_id, lot_id, winner_ordinal);

-- --------------------------------------------------------------------------
-- no_government_contracts becomes a union, as Sweden's and Finland's already
-- are. Nothing downstream changes: the view already exists and the country's
-- contracts tab already reads it.
-- --------------------------------------------------------------------------
DROP VIEW IF EXISTS corpscout.no_government_contracts;

CREATE VIEW corpscout.no_government_contracts AS
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
WHERE d.company_match_status = 'exact' AND d.company_id != '';
