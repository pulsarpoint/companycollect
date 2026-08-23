import { useEffect, useRef, useState } from "react";
import { useFetcher, useRevalidator } from "react-router";
import {
  CheckCircle2Icon,
  DatabaseIcon,
  LoaderCircleIcon,
  RotateCcwIcon,
  TriangleAlertIcon,
} from "lucide-react";
import {
  Alert,
  AlertDescription,
  AlertTitle,
} from "~/components/ui/alert";
import { Badge } from "~/components/ui/badge";
import { Button } from "~/components/ui/button";
import {
  Dialog,
  DialogClose,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "~/components/ui/dialog";
import {
  Progress,
  ProgressLabel,
  ProgressValue,
} from "~/components/ui/progress";
import type {
  SwedenPeopleDraftInitializationJob,
  SwedenPeopleDraftStatus,
} from "~/lib/sweden-people-draft.server";

type DraftInitializationActionData =
  | { ok: true; job: SwedenPeopleDraftInitializationJob }
  | { ok: false; error: string };

interface DraftInitializationStatusData {
  job: SwedenPeopleDraftInitializationJob | null;
}

const JOB_STATUS_PATH = "/admin/se/people/draft-1-job";
const numberFormat = new Intl.NumberFormat("en-US");

function newestJob(
  ...jobs: Array<SwedenPeopleDraftInitializationJob | null | undefined>
): SwedenPeopleDraftInitializationJob | null {
  return (
    jobs
      .filter((job): job is SwedenPeopleDraftInitializationJob => Boolean(job))
      .sort((left, right) => right.updatedAt.localeCompare(left.updatedAt))[0] ??
    null
  );
}

function InitializationJobStatus({
  job,
}: {
  job: SwedenPeopleDraftInitializationJob;
}) {
  if (job.status === "failed") {
    return (
      <Alert variant="destructive">
        <TriangleAlertIcon />
        <AlertTitle>Draft 1 initialization failed</AlertTitle>
        <AlertDescription>{job.errorMessage}</AlertDescription>
      </Alert>
    );
  }

  if (job.status === "completed") {
    return (
      <Alert>
        <CheckCircle2Icon />
        <AlertTitle>Draft 1 initialization completed</AlertTitle>
        <AlertDescription>
          Published {numberFormat.format(job.insertedRows)} immutable source
          observations.
        </AlertDescription>
      </Alert>
    );
  }

  return (
    <div className="flex flex-col gap-3 rounded-lg border p-4" role="status">
      <Progress value={job.progressPercent}>
        <ProgressLabel>{job.message}</ProgressLabel>
        <ProgressValue />
      </Progress>
      <div className="flex flex-wrap items-center gap-2">
        <Badge variant="secondary">
          {job.currentSource ?? "Preparing sources"}
        </Badge>
        <span className="text-xs text-muted-foreground">
          {numberFormat.format(job.processedRows)} of{" "}
          {numberFormat.format(job.totalRows)} processed ·{" "}
          {numberFormat.format(job.insertedRows)} inserted
        </span>
      </div>
    </div>
  );
}

export function PeopleDraftInitializer({
  status,
  initialJob,
}: {
  status: SwedenPeopleDraftStatus;
  initialJob: SwedenPeopleDraftInitializationJob | null;
}) {
  const actionFetcher = useFetcher<DraftInitializationActionData>();
  const progressFetcher = useFetcher<DraftInitializationStatusData>();
  const revalidator = useRevalidator();
  const [confirmationOpen, setConfirmationOpen] = useState(false);
  const refreshedTerminalJob = useRef("");
  const submittedJob = actionFetcher.data?.ok
    ? actionFetcher.data.job
    : null;
  const job = newestJob(
    progressFetcher.data?.job,
    submittedJob,
    initialJob,
  );
  const jobIsActive = job?.status === "queued" || job?.status === "running";
  const submitting = actionFetcher.state !== "idle";
  const hasPublishedRows = status.rowCount > 0;

  useEffect(() => {
    if (actionFetcher.state === "idle" && actionFetcher.data?.ok) {
      setConfirmationOpen(false);
    }
  }, [actionFetcher.data, actionFetcher.state]);

  useEffect(() => {
    if (!jobIsActive || progressFetcher.state !== "idle") return;
    const timer = window.setTimeout(() => {
      progressFetcher.load(JOB_STATUS_PATH);
    }, 750);
    return () => window.clearTimeout(timer);
  }, [
    job?.jobId,
    job?.status,
    job?.updatedAt,
    jobIsActive,
    progressFetcher.state,
  ]);

  useEffect(() => {
    const polledJob = progressFetcher.data?.job;
    if (
      !polledJob ||
      (polledJob.status !== "completed" && polledJob.status !== "failed")
    ) {
      return;
    }
    const terminalKey = `${polledJob.jobId}:${polledJob.status}`;
    if (refreshedTerminalJob.current === terminalKey) return;
    refreshedTerminalJob.current = terminalKey;
    revalidator.revalidate();
  }, [progressFetcher.data, revalidator]);

  return (
    <div className="flex w-full flex-col gap-3">
      {job ? <InitializationJobStatus job={job} /> : null}

      {actionFetcher.data && !actionFetcher.data.ok ? (
        <Alert variant="destructive">
          <TriangleAlertIcon />
          <AlertTitle>Initialization was not started</AlertTitle>
          <AlertDescription>{actionFetcher.data.error}</AlertDescription>
        </Alert>
      ) : null}

      {jobIsActive ? null : hasPublishedRows ? (
        <div className="flex flex-wrap items-center gap-2">
          <Badge variant="secondary">
            Draft ready · {numberFormat.format(status.rowCount)} rows
          </Badge>
          <Dialog open={confirmationOpen} onOpenChange={setConfirmationOpen}>
            <Button
              type="button"
              variant="outline"
              onClick={() => setConfirmationOpen(true)}
            >
              <RotateCcwIcon data-icon="inline-start" />
              Rebuild Draft 1
            </Button>
            <DialogContent>
              <DialogHeader>
                <DialogTitle>Rebuild Draft 1 from all sources?</DialogTitle>
                <DialogDescription>
                  A durable Temporal workflow will build a replacement from
                  Bolagsverket, ESEF, and Wikidata.
                </DialogDescription>
              </DialogHeader>

              <Alert variant="destructive">
                <TriangleAlertIcon />
                <AlertTitle>
                  {numberFormat.format(status.rowCount)} published rows will be
                  replaced
                </AlertTitle>
                <AlertDescription>
                  The current table remains available while the job runs and is
                  replaced only after the new import completes successfully.
                  Source tables are not changed.
                </AlertDescription>
              </Alert>

              <actionFetcher.Form method="post">
                <input
                  type="hidden"
                  name="intent"
                  value="start_people_draft_step_1_initialization"
                />
                <input
                  type="hidden"
                  name="confirm_replacement"
                  value="yes"
                />
                <DialogFooter>
                  <DialogClose
                    render={<Button type="button" variant="outline" />}
                  >
                    Cancel
                  </DialogClose>
                  <Button
                    type="submit"
                    variant="destructive"
                    disabled={submitting}
                  >
                    {submitting ? (
                      <LoaderCircleIcon
                        data-icon="inline-start"
                        className="animate-spin"
                      />
                    ) : null}
                    Start Temporal rebuild
                  </Button>
                </DialogFooter>
              </actionFetcher.Form>
            </DialogContent>
          </Dialog>
        </div>
      ) : (
        <actionFetcher.Form method="post">
          <input
            type="hidden"
            name="intent"
            value="start_people_draft_step_1_initialization"
          />
          <Button type="submit" disabled={submitting}>
            {submitting ? (
              <LoaderCircleIcon
                data-icon="inline-start"
                className="animate-spin"
              />
            ) : (
              <DatabaseIcon data-icon="inline-start" />
            )}
            {job?.status === "failed"
              ? "Retry Draft 1 initialization"
              : "Initialize Draft 1"}
          </Button>
        </actionFetcher.Form>
      )}
    </div>
  );
}
