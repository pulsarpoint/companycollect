import { CheckCircle2Icon, TriangleAlertIcon } from "lucide-react";
import { useEffect, useRef, useState } from "react";
import { useFetcher, useRevalidator } from "react-router";
import { Alert, AlertDescription, AlertTitle } from "~/components/ui/alert";

/**
 * The Fold now poller, shared by the company tabs that launch a targeted fold
 * (Info, Address). It polls the Info tab's own run resource route --
 * `/admin/se/company/:companyId/info/run/:runId`, the one route that reads a
 * Dagster run's status -- so both tabs resolve the same path for the same run.
 */

/** Stop polling and tell the reviewer to check Dagster directly after this long. */
const POLL_TIMEOUT_MS = 600_000;
const POLL_INTERVAL_MS = 3000;

/** True only when it is safe to assume the tab is in front of someone --
 * skips polling a backgrounded tab. `document` guard first: this runs from an
 * effect, so it is always browser-side, but the check stays defensive. */
function tabIsVisible(): boolean {
  return typeof document !== "undefined" && document.visibilityState === "visible";
}

/** Polls the run resource route until the fold finishes, then reloads the page.
 * Stops (and tells the reviewer to check Dagster) after ten minutes, and never
 * polls a backgrounded tab. Disappears once the fold has finished and nothing
 * is pending; a failed or canceled run keeps the alert up with its status. */
export function FoldRunPoller({
  companyId,
  runId,
  url,
  foldPending,
}: {
  companyId: string;
  runId: string;
  url: string | null;
  foldPending: boolean;
}) {
  const fetcher = useFetcher<{ status: string; finished: boolean }>();
  const revalidator = useRevalidator();
  const finished = fetcher.data?.finished ?? false;
  const startedAtRef = useRef(Date.now());
  const [timedOut, setTimedOut] = useState(false);
  useEffect(() => {
    if (finished) {
      revalidator.revalidate();
      return;
    }
    const path = `/admin/se/company/${encodeURIComponent(companyId)}/info/run/${encodeURIComponent(runId)}`;
    if (tabIsVisible()) fetcher.load(path);
    const timer = setInterval(() => {
      if (Date.now() - startedAtRef.current > POLL_TIMEOUT_MS) {
        clearInterval(timer);
        setTimedOut(true);
        return;
      }
      if (tabIsVisible()) fetcher.load(path);
    }, POLL_INTERVAL_MS);
    return () => clearInterval(timer);
    // fetcher is stable per React Router's contract; re-run only on identity changes.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [companyId, runId, finished]);

  if (finished && !foldPending) return null;

  const dagsterLink = url ? (
    <>
      {" "}
      (<a className="underline" href={url} target="_blank" rel="noreferrer">open in Dagster</a>)
    </>
  ) : null;

  if (timedOut) {
    return (
      <Alert>
        <TriangleAlertIcon />
        <AlertTitle>Still running</AlertTitle>
        <AlertDescription>
          Fold not finished after 10 minutes; open it in Dagster.
          {dagsterLink}
        </AlertDescription>
      </Alert>
    );
  }

  return (
    <Alert>
      <CheckCircle2Icon />
      <AlertTitle>{finished ? `Fold ${fetcher.data?.status?.toLowerCase() ?? "finished"}` : "Folding"}</AlertTitle>
      <AlertDescription>
        Run <span className="font-mono">{runId}</span>
        {dagsterLink}
        {finished ? " -- reloading." : " -- the page reloads when it finishes."}
      </AlertDescription>
    </Alert>
  );
}
