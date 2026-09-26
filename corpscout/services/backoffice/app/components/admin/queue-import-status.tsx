import { useEffect } from "react";
import { Link, useFetcher } from "react-router";
import { Alert, AlertDescription, AlertTitle } from "~/components/ui/alert";
type ImportProgress = {ok: true; runId: string; status: string; finished: boolean; taskId: string | null} | {ok: false; error: string};

export interface QueueImportReceipt {runId: string; status: string; runUrl: string | null}

export type ImportQueue = "webtech" | "crawler" | "ip-enrichment";
const LABELS: Record<ImportQueue, string> = {webtech: "Webtech", crawler: "Crawler", "ip-enrichment": "IP enrichment"};

export function useQueueSubmission(receipt: QueueImportReceipt | null, queue: ImportQueue = "webtech") {
  const progress = useFetcher<ImportProgress>();
  const state = progress.data?.ok && progress.data.runId === receipt?.runId ? progress.data : null;
  useEffect(() => {
    if (!receipt || state?.finished || progress.state !== "idle") return;
    const timer = setTimeout(() => { void progress.load(`/admin/${queue}/queue-submissions/${receipt.runId}`); }, 3000);
    return () => clearTimeout(timer);
  }, [queue, receipt?.runId, state?.finished, progress.state, progress.data, progress.load]);
  return {state, error: progress.data?.ok === false ? progress.data.error : null};
}

export function QueueImportStatus({receipt, state, fallbackSearch = "", crawlType, queue}: {
  receipt: QueueImportReceipt;
  state: ReturnType<typeof useQueueSubmission>["state"];
  fallbackSearch?: string;
  crawlType?: string;
  queue?: ImportQueue;
}) {
  const target: ImportQueue = queue ?? (crawlType ? "crawler" : "webtech");
  const label = LABELS[target];
  const completed = state?.status === "SUCCESS";
  const failed = state?.status === "FAILURE" || state?.status === "CANCELED";
  const params = new URLSearchParams();
  if (crawlType) params.set("crawlType", crawlType);
  if (state?.taskId) params.set("task", state.taskId);
  else if (fallbackSearch) params.set("search", fallbackSearch);
  return <Alert variant={failed ? "destructive" : "default"}>
    <AlertTitle>{completed ? `Added to the ${label} input queue` : failed ? "Queue import did not complete" : "Queue import submitted"}</AlertTitle>
    <AlertDescription>
      <p>{completed ? "The selected inputs are saved in the draft queue. No processing has been started." : failed ? "Retry this import using the same submission, or inspect the run for details." : `Dagster status: ${state?.status ?? receipt.status}. Waiting for the inputs to be saved.`}</p>
      <div className="flex flex-wrap gap-4">
        <Link className="underline" to={`/admin/queues/${target}${params.size ? `?${params}` : ""}`}>Open {label} queue</Link>
        {receipt.runUrl && <a className="underline" href={receipt.runUrl} target="_blank" rel="noreferrer">View import run</a>}
      </div>
    </AlertDescription>
  </Alert>;
}
