ALTER TABLE processing.llm_external_requests
    ADD COLUMN brave_result_id uuid,
    ADD COLUMN captcha_stats jsonb,
    ADD COLUMN captcha_stats_updated_at timestamptz,
    ADD COLUMN captcha_stats_complete boolean NOT NULL DEFAULT false;

CREATE INDEX brave_request_stats_result ON processing.llm_external_requests (brave_result_id)
    WHERE service = 'brave';
CREATE INDEX brave_request_stats_pending ON processing.llm_external_requests
    (captcha_stats_updated_at NULLS FIRST, created_at)
    WHERE service = 'brave' AND NOT captcha_stats_complete;

COMMENT ON COLUMN processing.llm_external_requests.captcha_stats IS
    'Per-browser-request CAPTCHA observations, cumulative provider-reported tokens, confirmation time and proxy route. NULL fields mean unknown, never inferred clearance.';
