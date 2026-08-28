ALTER TABLE corpscout.se_company_ratsit
    DROP CONSTRAINT se_company_ratsit_failure_type,
    ADD CONSTRAINT se_company_ratsit_failure_type CHECK
        (outcome = 'success' AND failure_type = '')
        OR (
            outcome = 'failure'
            AND failure_type IN ('navigation', 'http', 'parse')
        );

ALTER TABLE corpscout.se_company_ratsit
    DROP CONSTRAINT se_company_ratsit_diagnostic,
    ADD CONSTRAINT se_company_ratsit_diagnostic CHECK
        diagnostic_object_key = '' OR failure_type = 'parse';
