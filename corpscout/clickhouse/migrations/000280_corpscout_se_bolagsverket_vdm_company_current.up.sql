CREATE DATABASE IF NOT EXISTS corpscout;

CREATE OR REPLACE VIEW corpscout.se_bolagsverket_vdm_company_current AS
SELECT
    company_id,
    name_protection_sequence,
    latest.1 AS identity_type_code,
    latest.2 AS identity_type_label_original,
    latest.3 AS active_status_code,
    latest.4 AS is_active,
    latest.5 AS active_status_producer,
    latest.6 AS active_status_observed_at,
    latest.7 AS organisation_registered_on,
    latest.8 AS introduced_at_scb,
    latest.9 AS organisation_date_producer,
    latest.10 AS digital_report_document_count,
    latest.11 AS organisation_found,
    latest.12 AS source_run_id,
    latest.13 AS organisation_object_key,
    latest.14 AS organisation_sha256,
    latest.15 AS organisation_request_id,
    latest.16 AS document_list_object_key,
    latest.17 AS document_list_sha256,
    latest.18 AS document_list_request_id,
    latest.19 AS observed_at
FROM
(
    SELECT
        company_id,
        name_protection_sequence,
        argMax(
            tuple(
                identity_type_code,
                identity_type_label_original,
                active_status_code,
                is_active,
                active_status_producer,
                active_status_observed_at,
                organisation_registered_on,
                introduced_at_scb,
                organisation_date_producer,
                digital_report_document_count,
                organisation_found,
                source_run_id,
                organisation_object_key,
                organisation_sha256,
                organisation_request_id,
                document_list_object_key,
                document_list_sha256,
                document_list_request_id,
                observed_at
            ),
            tuple(observed_at, source_run_id)
        ) AS latest
    FROM corpscout.se_bolagsverket_vdm_company_observations
    GROUP BY company_id, name_protection_sequence
);
