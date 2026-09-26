DROP INDEX processing.brave_request_stats_pending;
DROP INDEX processing.brave_request_stats_result;
ALTER TABLE processing.llm_external_requests
    DROP COLUMN brave_result_id,
    DROP COLUMN captcha_stats,
    DROP COLUMN captcha_stats_updated_at,
    DROP COLUMN captcha_stats_complete;
