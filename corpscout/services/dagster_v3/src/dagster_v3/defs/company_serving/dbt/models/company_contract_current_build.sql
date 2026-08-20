{{ config(materialized='table', order_by=['country_code', 'company_id', 'contract_ref']) }}

WITH company_anchors AS (
    SELECT company_id
    FROM {{ source('corpscout', 'se_companies') }} FINAL
)

SELECT
    country_code,
    contracts.company_id,
    contract_ref,
    argMax(source_slug, resolved_at) AS source,
    argMax(source_notice_id, resolved_at) AS notice_ref,
    argMax(publication_date, resolved_at) AS contract_date,
    argMax(buyer_name, resolved_at) AS buyer_name,
    argMax(title, resolved_at) AS title,
    argMax(agreement_type, resolved_at) AS agreement_type,
    argMax(cpv_code, resolved_at) AS cpv_code,
    toUInt32(countDistinct(tuple(source_notice_id, source_lot_id, source_winner_ordinal))) AS supplier_count,
    toFloat64(argMax(value_amount_original, resolved_at)) AS amount_original,
    toFloat64(argMax(value_amount_usd, resolved_at)) AS amount_usd,
    argMax(value_currency, resolved_at) AS currency,
    toFloat64(argMax(notice_value_amount_original, resolved_at)) AS notice_amount_original,
    toFloat64(argMax(notice_value_amount_usd, resolved_at)) AS notice_amount_usd,
    argMax(notice_value_currency, resolved_at) AS notice_currency,
    argMax(source_url, resolved_at) AS source_url,
    now64(3, 'UTC') AS resolved_at
FROM {{ source('corpscout', 'company_contract_facts') }} AS contracts
INNER JOIN company_anchors AS anchors
    ON anchors.company_id = contracts.company_id
WHERE country_code = '{{ var("country_code") }}'
GROUP BY country_code, contracts.company_id, contract_ref
