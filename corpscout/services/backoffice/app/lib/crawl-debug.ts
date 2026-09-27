import type { CrawlAttempt } from "~/lib/crawler";

export interface CrawlDebugEvent {
  id: number;
  timestamp: string;
  elapsed_ms: number;
  duration_ms: number | null;
  operation: string | null;
  stage: string;
  level: string;
  message: string;
  has_details: boolean;
}

export interface CrawlDebugSnapshot {
  enabled: boolean;
  attempt: number;
  cursor: number;
  has_more: boolean;
  job: CrawlAttempt;
  events: CrawlDebugEvent[];
}

export function debugTime(ms: number): string {
  if (ms < 1000) return `${Math.round(ms)} ms`;
  if (ms < 60000) return `${(ms / 1000).toFixed(2)} s`;
  return `${Math.floor(ms / 60000)}m ${Math.floor(ms % 60000 / 1000)}s`;
}
