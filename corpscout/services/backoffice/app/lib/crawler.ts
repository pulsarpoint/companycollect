export const CRAWL_STATES = ["queued", "running", "blocked", "captcha", "awaiting_human", "completed", "failed", "cancelled"] as const;
export type CrawlState = typeof CRAWL_STATES[number];
export interface ChallengeAgentResult {
  model?: string;
  runId: string | null;
  sessionId?: string;
  state: string;
  reason: string | null;
  steps: {number: number; state: string; action?: {action: string; reason: string}}[];
  usage: {prompt_tokens: number; completion_tokens: number};
  elapsedSeconds: number;
  trigger?: "automatic";
  accessVerified?: boolean;
  pageUrl?: string;
}
export interface CrawlPublishReceipt {
  request_id: string;
  url: string;
  state: string;
}
export interface CrawlAttempt {
  request_id: string;
  attempt: number;
  url: string;
  domain: string;
  state: CrawlState;
  // "jetstream" appears only on history from the retired NATS input.
  source: "rest" | "jetstream" | "manual";
  submitted_at: string;
  updated_at: string;
  current_url: string | null;
  reason: string | null;
  blocked_reason: string | null;
  error: string | null;
  assistance_deadline: string | null;
  browser_available: boolean;
  verification_available: boolean;
  browser_session_id: string | null;
  challenge_agent_running?: boolean;
  challenge_agent_result?: ChallengeAgentResult | null;
  challenge_agent_results?: ChallengeAgentResult[];
  challenge_agent_max_runs?: number;
  challenge_agent_model?: string;
  challenge_agent_budget_exhausted?: boolean;
  interactive?: boolean;
  collected_pages?: number;
  crawl_status?: string | null;
  retry_of: string | null;
  retry_of_attempt: number | null;
  s3_state: "not_configured" | "pending" | "uploaded";
  s3_error: string | null;
  s3_event: {result?: {bucket: string; key: string}; page_count?: number} | null;
}
export interface CrawlSnapshot {
  attempts: CrawlAttempt[];
  total: number;
  limit: number;
  offset: number;
  revision: number;
  human_enabled: boolean;
  challenge_agent_enabled?: boolean;
}
export function isCrawlWaiting(attempt: CrawlAttempt) {
  return ["blocked", "captcha", "awaiting_human"].includes(attempt.state);
}
