CREATE DATABASE IF NOT EXISTS corpscout;

-- One observation per completed search, not one per agent step/retry.
-- Historical results without page verification remain unknown unless the search succeeded.
CREATE VIEW IF NOT EXISTS corpscout.company_brave_captcha_results AS
WITH JSONExtractArrayRaw(challenge_runs_json) AS runs,
     arrayElement(runs, -1) AS last_run
SELECT result_id, task_id, source_run_id, country_code, company_id, completed_at, route,
       ifNull(nullIf(JSONExtractString(last_run, 'model'), ''), 'unknown') AS model,
       length(runs) AS agent_attempts,
       multiIf(
           status = 'success', 'cleared',
           error_stage IN ('captcha', 'access'), 'failed',
           JSONExtractBool(last_run, 'access_cleared'), 'cleared',
           'unknown') AS outcome,
       if(length(runs) > 0 AND arrayAll(r -> JSONHas(r, 'elapsedSeconds'), runs),
          arraySum(arrayMap(r -> JSONExtractFloat(r, 'elapsedSeconds'), runs)), NULL) AS duration_seconds,
       multiIf(
           outcome != 'failed', '',
           JSONExtractString(last_run, 'error_code') != '', JSONExtractString(last_run, 'error_code'),
           error_type = 'AgentBudgetExhausted', 'agent_budget',
           error_type = 'AgentNotConfigured', 'agent_not_configured',
           error_stage = 'access', 'access_blocked',
           JSONExtractString(last_run, 'state') = 'timeout', 'agent_timeout',
           JSONExtractString(last_run, 'state') = 'needs_human', 'needs_human',
           'unknown') AS failure_reason
FROM corpscout.company_brave_search_results FINAL
WHERE length(runs) > 0 OR error_stage = 'captcha';

-- Shared policy and aggregation for Backoffice and the Dagster alert sensor.
CREATE VIEW IF NOT EXISTS corpscout.company_brave_captcha_health AS
WITH now64(3, 'UTC') AS checked_at,
     checked_at - INTERVAL 15 MINUTE AS window_start
SELECT task_id, source_run_id, model, route, checked_at,
       countIf(completed_at >= window_start) AS challenged,
       countIf(completed_at >= window_start AND outcome = 'cleared') AS cleared,
       countIf(completed_at >= window_start AND outcome = 'failed') AS failed,
       countIf(completed_at >= window_start AND outcome = 'unknown') AS unknown,
       cleared + failed AS known_outcomes,
       failed / nullIf(known_outcomes, 0) AS failure_rate,
       countIf(completed_at < window_start AND outcome IN ('cleared', 'failed')) AS previous_outcomes,
       countIf(completed_at < window_start AND outcome = 'failed') / nullIf(previous_outcomes, 0) AS previous_failure_rate,
       sumIf(agent_attempts, completed_at >= window_start) AS agent_attempts,
       countIf(completed_at >= window_start AND duration_seconds IS NOT NULL) AS timed_requests,
       sumIf(duration_seconds, completed_at >= window_start) AS total_seconds,
       avgOrNullIf(duration_seconds, completed_at >= window_start) AS average_seconds,
       quantileOrNullIf(0.95)(duration_seconds, completed_at >= window_start) AS p95_seconds,
       sumMap(map(failure_reason, toUInt64(completed_at >= window_start AND outcome = 'failed'))) AS failure_reasons,
       max(completed_at) AS latest_result_at,
       maxIf(completed_at, outcome = 'failed') AS latest_failure_at,
       multiIf(
           known_outcomes >= 5 AND failed >= 3 AND failure_rate >= 0.20, 'high_failures',
           known_outcomes >= 10 AND previous_outcomes >= 10 AND failed >= 3
               AND failure_rate - previous_failure_rate >= 0.15, 'rising_failures',
           '') AS warning
FROM corpscout.company_brave_captcha_results
WHERE completed_at >= checked_at - INTERVAL 30 MINUTE AND completed_at <= checked_at
GROUP BY task_id, source_run_id, model, route
HAVING challenged > 0;
