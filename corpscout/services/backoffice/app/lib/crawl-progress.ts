export interface CrawlRunProgress {
  runId: string;
  runUrl: string | null;
  status: string;
  startTime: number | null;
  endTime: number | null;
  selected: number | null;
  successful: number | null;
  unsuccessful: number | null;
  skipped: number | null;
}

export interface CrawlProgressSnapshot {
  runs: CrawlRunProgress[];
  warning: string | null;
}

export function crawlRunStatus(status: string): string {
  return ({SUCCESS: "Finished", FAILURE: "Failed", CANCELED: "Cancelled", CANCELING: "Cancelling", STARTED: "Running", STARTING: "Starting", QUEUED: "Queued", NOT_STARTED: "Queued", MANAGED: "Queued"} as Record<string, string>)[status] ?? status;
}
