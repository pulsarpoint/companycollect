ALTER TABLE corpscout.se_company_ratsit
    DROP CONSTRAINT se_company_ratsit_failure_type,
    ADD CONSTRAINT se_company_ratsit_failure_type CHECK
        (outcome = 'success' AND failure_type = '')
        OR (outcome = 'not_found' AND failure_type = '')
        OR (
            outcome = 'failure'
            AND failure_type IN ('navigation', 'http', 'parse', 'not_found')
        );

ALTER TABLE corpscout.se_company_ratsit
    DROP CONSTRAINT se_company_ratsit_diagnostic,
    ADD CONSTRAINT se_company_ratsit_diagnostic CHECK
        diagnostic_object_key = ''
        OR failure_type IN ('parse', 'not_found')
        OR outcome = 'not_found';

ALTER TABLE corpscout.se_company_ratsit
    DROP CONSTRAINT se_company_ratsit_error,
    ADD CONSTRAINT se_company_ratsit_error CHECK
        (outcome = 'success' AND error_message = '')
        OR (outcome = 'not_found' AND error_message = '')
        OR (outcome = 'failure' AND error_message != '');

ALTER TABLE corpscout.se_company_ratsit
    UPDATE
        outcome = 'failure',
        failure_type = 'not_found',
        error_message = 'Ratsit company was not found'
    WHERE outcome = 'not_found'
    SETTINGS mutations_sync = 2;

ALTER TABLE corpscout.se_company_ratsit
    DROP CONSTRAINT se_company_ratsit_outcome,
    ADD CONSTRAINT se_company_ratsit_outcome CHECK
        outcome IN ('success', 'failure');

ALTER TABLE corpscout.se_company_ratsit
    DROP CONSTRAINT se_company_ratsit_failure_type,
    ADD CONSTRAINT se_company_ratsit_failure_type CHECK
        (outcome = 'success' AND failure_type = '')
        OR (
            outcome = 'failure'
            AND failure_type IN ('navigation', 'http', 'parse', 'not_found')
        );

ALTER TABLE corpscout.se_company_ratsit
    DROP CONSTRAINT se_company_ratsit_diagnostic,
    ADD CONSTRAINT se_company_ratsit_diagnostic CHECK
        diagnostic_object_key = ''
        OR failure_type IN ('parse', 'not_found');

ALTER TABLE corpscout.se_company_ratsit
    DROP CONSTRAINT se_company_ratsit_error,
    ADD CONSTRAINT se_company_ratsit_error CHECK
        (outcome = 'success' AND error_message = '')
        OR (outcome != 'success' AND error_message != '');
