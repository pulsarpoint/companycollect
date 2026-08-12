{{ config(materialized='table', order_by=['country_code', 'company_id']) }}

SELECT
    country_code,
    company_id,
    toUInt32(count()) AS contract_count,
    max(contract_date) AS last_contract_date,
    sumOrNull(ifNull(amount_usd, notice_amount_usd) / greatest(toFloat64(supplier_count), 1)) AS total_attributable_value_usd,
    toUInt32(countIf(amount_usd IS NOT NULL OR notice_amount_usd IS NOT NULL)) AS valued_contract_count,
    arraySort(groupUniqArray(source)) AS source_systems,
    now64(3, 'UTC') AS resolved_at
FROM {{ ref('company_contract_current_build') }}
GROUP BY country_code, company_id
