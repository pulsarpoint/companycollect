CREATE DATABASE IF NOT EXISTS corpscout;

DROP TABLE IF EXISTS corpscout.br_cvm_financial_metrics;
DROP VIEW IF EXISTS corpscout.br_cvm_financial_metrics;

CREATE VIEW IF NOT EXISTS corpscout.br_cvm_financial_metrics AS
WITH
    statement_rows AS
    (
        SELECT
            'DFP' AS source_dataset,
            dfp_year AS source_year,
            'annual' AS period_type,
            country_iso2,
            source_run_id,
            source_record_id,
            cnpj,
            cnpj_basico,
            company_name,
            cvm_code,
            reference_date,
            period_start_date,
            period_end_date,
            version,
            consolidation_type,
            currency,
            amount_original,
            amount_usd,
            fx_rate_to_usd,
            fx_rate_date,
            fx_source,
            statement_code,
            statement_name,
            account_code,
            account_description_original,
            source_archive_key,
            source_file_name,
            source_row_number,
            resolved_at
        FROM corpscout.br_cvm_dfp_statement_rows
        UNION ALL
        SELECT
            'ITR' AS source_dataset,
            itr_year AS source_year,
            'interim' AS period_type,
            country_iso2,
            source_run_id,
            source_record_id,
            cnpj,
            cnpj_basico,
            company_name,
            cvm_code,
            reference_date,
            period_start_date,
            period_end_date,
            version,
            consolidation_type,
            currency,
            amount_original,
            amount_usd,
            fx_rate_to_usd,
            fx_rate_date,
            fx_source,
            statement_code,
            statement_name,
            account_code,
            account_description_original,
            source_archive_key,
            source_file_name,
            source_row_number,
            resolved_at
        FROM corpscout.br_cvm_itr_statement_rows
    ),
    direct_candidates AS
    (
        SELECT
            country_iso2,
            'brazil_cvm_financial_metrics' AS source_slug,
            source_run_id AS source_run_id,
            source_dataset,
            source_year,
            cnpj,
            cnpj_basico,
            company_name,
            cvm_code,
            reference_date,
            period_start_date,
            period_end_date,
            period_type,
            version,
            consolidation_type,
            multiIf(
                statement_code = 'DRE' AND account_code = '3.01', 'revenue',
                statement_code = 'DRE' AND account_code = '3.11', 'net_income',
                statement_code = 'BPA' AND account_code = '1', 'total_assets',
                statement_code = 'BPP' AND account_code = '2.03', 'equity',
                statement_code = 'BPA' AND account_code = '1.01.01', 'cash_and_equivalents',
                statement_code IN ('DFC_MI', 'DFC_MD') AND account_code = '6.01', 'operating_cash_flow',
                ''
            ) AS metric_name,
            multiIf(
                statement_code = 'DRE' AND account_code = '3.01', 'Revenue',
                statement_code = 'DRE' AND account_code = '3.11', 'Net income',
                statement_code = 'BPA' AND account_code = '1', 'Total assets',
                statement_code = 'BPP' AND account_code = '2.03', 'Equity',
                statement_code = 'BPA' AND account_code = '1.01.01', 'Cash and equivalents',
                statement_code IN ('DFC_MI', 'DFC_MD') AND account_code = '6.01', 'Operating cash flow',
                ''
            ) AS metric_label,
            upper(currency) AS currency,
            CAST(amount_original, 'Nullable(Decimal(38, 6))') AS amount_original,
            CAST(amount_usd, 'Nullable(Decimal(38, 6))') AS amount_usd,
            CAST(fx_rate_to_usd, 'Nullable(Decimal(38, 12))') AS fx_rate_to_usd,
            fx_rate_date,
            fx_source,
            statement_code AS source_statement_code,
            statement_name AS source_statement_name,
            account_code AS source_account_codes,
            account_description_original AS source_account_descriptions_original,
            source_run_id AS source_statement_run_ids,
            source_record_id AS source_statement_record_ids,
            source_archive_key AS source_archive_keys,
            source_file_name AS source_file_names,
            toUInt64(1) AS source_statement_row_count,
            multiIf(statement_code = 'DFC_MI', 1, statement_code = 'DFC_MD', 2, 1) AS mapping_priority,
            source_row_number,
            resolved_at
        FROM statement_rows AS source_rows
        WHERE source_rows.amount_original IS NOT NULL
          AND source_rows.currency != ''
          AND (
              (statement_code = 'DRE' AND account_code IN ('3.01', '3.11'))
              OR (statement_code = 'BPA' AND account_code IN ('1', '1.01.01'))
              OR (statement_code = 'BPP' AND account_code = '2.03')
              OR (statement_code IN ('DFC_MI', 'DFC_MD') AND account_code = '6.01')
          )
    ),
    direct_ranked AS
    (
        SELECT
            *,
            row_number() OVER (
                PARTITION BY
                    source_dataset,
                    cnpj,
                    cvm_code,
                    reference_date,
                    period_start_date,
                    period_end_date,
                    version,
                    consolidation_type,
                    metric_name
                ORDER BY mapping_priority ASC, source_row_number ASC
            ) AS metric_rank
        FROM direct_candidates
    ),
    direct_metric_rows AS
    (
        SELECT
            country_iso2,
            source_slug,
            source_run_id,
            source_dataset,
            source_year,
            cnpj,
            cnpj_basico,
            company_name,
            cvm_code,
            reference_date,
            period_start_date,
            period_end_date,
            period_type,
            version,
            consolidation_type,
            metric_name,
            metric_label,
            currency,
            amount_original,
            amount_usd,
            fx_rate_to_usd,
            fx_rate_date,
            fx_source,
            source_statement_code,
            source_statement_name,
            source_account_codes,
            source_account_descriptions_original,
            source_statement_run_ids,
            source_statement_record_ids,
            source_archive_keys,
            source_file_names,
            source_statement_row_count,
            resolved_at
        FROM direct_ranked
        WHERE metric_rank = 1
    ),
    liability_metric_rows AS
    (
        SELECT
            any(liability_source_rows.country_iso2) AS country_iso2,
            'brazil_cvm_financial_metrics' AS source_slug,
            arrayStringConcat(arraySort(groupArray(liability_source_rows.source_run_id)), '|') AS source_run_id,
            liability_source_rows.source_dataset AS source_dataset,
            liability_source_rows.source_year AS source_year,
            liability_source_rows.cnpj AS cnpj,
            any(liability_source_rows.cnpj_basico) AS cnpj_basico,
            any(liability_source_rows.company_name) AS company_name,
            liability_source_rows.cvm_code AS cvm_code,
            liability_source_rows.reference_date AS reference_date,
            liability_source_rows.period_start_date AS period_start_date,
            liability_source_rows.period_end_date AS period_end_date,
            liability_source_rows.period_type AS period_type,
            liability_source_rows.version AS version,
            liability_source_rows.consolidation_type AS consolidation_type,
            'total_liabilities' AS metric_name,
            'Total liabilities' AS metric_label,
            upper(any(liability_source_rows.currency)) AS currency,
            CAST(sum(liability_source_rows.amount_original), 'Nullable(Decimal(38, 6))') AS amount_original,
            CAST(sum(liability_source_rows.amount_usd), 'Nullable(Decimal(38, 6))') AS amount_usd,
            CAST(max(liability_source_rows.fx_rate_to_usd), 'Nullable(Decimal(38, 12))') AS fx_rate_to_usd,
            max(liability_source_rows.fx_rate_date) AS fx_rate_date,
            any(liability_source_rows.fx_source) AS fx_source,
            'BPP' AS source_statement_code,
            any(liability_source_rows.statement_name) AS source_statement_name,
            arrayStringConcat(arrayMap(x -> x.1, arraySort(groupArray(tuple(liability_source_rows.account_code, liability_source_rows.account_description_original)))), '|') AS source_account_codes,
            arrayStringConcat(arrayMap(x -> x.2, arraySort(groupArray(tuple(liability_source_rows.account_code, liability_source_rows.account_description_original)))), '|') AS source_account_descriptions_original,
            arrayStringConcat(arrayMap(x -> x.2, arraySort(groupArray(tuple(liability_source_rows.account_code, liability_source_rows.source_run_id)))), '|') AS source_statement_run_ids,
            arrayStringConcat(arrayMap(x -> x.2, arraySort(groupArray(tuple(liability_source_rows.account_code, liability_source_rows.source_record_id)))), '|') AS source_statement_record_ids,
            arrayStringConcat(arrayMap(x -> x.2, arraySort(groupArray(tuple(liability_source_rows.account_code, liability_source_rows.source_archive_key)))), '|') AS source_archive_keys,
            arrayStringConcat(arrayMap(x -> x.2, arraySort(groupArray(tuple(liability_source_rows.account_code, liability_source_rows.source_file_name)))), '|') AS source_file_names,
            count() AS source_statement_row_count,
            max(liability_source_rows.resolved_at) AS resolved_at
        FROM statement_rows AS liability_source_rows
        WHERE liability_source_rows.statement_code = 'BPP'
          AND liability_source_rows.account_code IN ('2.01', '2.02')
          AND liability_source_rows.amount_original IS NOT NULL
          AND liability_source_rows.currency != ''
        GROUP BY
            liability_source_rows.source_dataset,
            liability_source_rows.source_year,
            liability_source_rows.cnpj,
            liability_source_rows.cvm_code,
            liability_source_rows.reference_date,
            liability_source_rows.period_start_date,
            liability_source_rows.period_end_date,
            liability_source_rows.period_type,
            liability_source_rows.version,
            liability_source_rows.consolidation_type
    ),
    candidate_metric_rows AS
    (
        SELECT * FROM direct_metric_rows
        UNION ALL
        SELECT * FROM liability_metric_rows
    ),
    latest_flagged AS
    (
        SELECT
            *,
            version = max(version) OVER (
                PARTITION BY
                    source_dataset,
                    cnpj,
                    cvm_code,
                    reference_date,
                    period_start_date,
                    period_end_date,
                    consolidation_type,
                    metric_name
            ) AS is_latest_version
        FROM candidate_metric_rows
    )
SELECT
    country_iso2 AS country_iso2,
    source_slug AS source_slug,
    source_run_id AS source_run_id,
    concat(
        source_dataset,
        ':',
        replaceRegexpAll(cnpj, '[^0-9]', ''),
        ':',
        toString(reference_date),
        ':',
        ifNull(toString(period_start_date), ''),
        ':',
        ifNull(toString(period_end_date), ''),
        ':',
        consolidation_type,
        ':',
        metric_name,
        ':',
        toString(version)
    ) AS source_record_id,
    source_dataset AS source_dataset,
    source_year AS source_year,
    cnpj AS cnpj,
    cnpj_basico AS cnpj_basico,
    company_name AS company_name,
    cvm_code AS cvm_code,
    reference_date AS reference_date,
    period_start_date AS period_start_date,
    period_end_date AS period_end_date,
    period_type AS period_type,
    version AS version,
    is_latest_version AS is_latest_version,
    consolidation_type AS consolidation_type,
    metric_name AS metric_name,
    metric_label AS metric_label,
    currency AS currency,
    amount_original AS amount_original,
    amount_usd AS amount_usd,
    fx_rate_to_usd AS fx_rate_to_usd,
    fx_rate_date AS fx_rate_date,
    fx_source AS fx_source,
    source_statement_code AS source_statement_code,
    source_statement_name AS source_statement_name,
    source_account_codes AS source_account_codes,
    source_account_descriptions_original AS source_account_descriptions_original,
    source_statement_run_ids AS source_statement_run_ids,
    source_statement_record_ids AS source_statement_record_ids,
    source_archive_keys AS source_archive_keys,
    source_file_names AS source_file_names,
    source_statement_row_count AS source_statement_row_count,
    'br-cvm-financial-metrics-v1' AS metric_mapping_version,
    resolved_at AS resolved_at
FROM latest_flagged;
