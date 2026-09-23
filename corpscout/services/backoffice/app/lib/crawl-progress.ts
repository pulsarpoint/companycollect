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

/** One Brave-style crawl task: a frozen selection and every run of its execution. */
export interface CrawlTaskProgress {
  taskId: string;
  executionId: string;
  latestRunId: string;
  runUrl: string | null;
  status: string;
  runs: number;
  startTime: number | null;
  selected: number | null;
  successful: number | null;
  unsuccessful: number | null;
  skipped: number | null;
  resumable: boolean;
}

export interface CrawlProgressSnapshot {
  runs: CrawlRunProgress[];
  tasks: CrawlTaskProgress[];
  warning: string | null;
}

export function crawlRunStatus(status: string): string {
  return ({SUCCESS: "Finished", FAILURE: "Failed", CANCELED: "Cancelled", CANCELING: "Cancelling", STARTED: "Running", STARTING: "Starting", QUEUED: "Queued", NOT_STARTED: "Queued", MANAGED: "Queued"} as Record<string, string>)[status] ?? status;
}
