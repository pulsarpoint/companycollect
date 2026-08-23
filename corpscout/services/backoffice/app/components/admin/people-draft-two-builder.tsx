import { useEffect, useRef, useState } from "react";
import { useFetcher, useRevalidator } from "react-router";
import {
  CheckCircle2Icon,
  CombineIcon,
  LoaderCircleIcon,
  RotateCcwIcon,
  TriangleAlertIcon,
} from "lucide-react";
import { Alert, AlertDescription, AlertTitle } from "~/components/ui/alert";
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
  SwedenPeopleDraftTwoJob,
  SwedenPeopleDraftTwoStatus,
} from "~/lib/sweden-people-draft-two.server";

type DraftTwoActionData =
  | { ok: true; target: "draft-2"; job: SwedenPeopleDraftTwoJob }
  | { ok: false; error: string };

interface DraftTwoStatusData {
  job: SwedenPeopleDraftTwoJob | null;
}

const JOB_STATUS_PATH = "/admin/se/people/draft-2-job";
const numberFormat = new Intl.NumberFormat("en-US");

function newestJob(
  ...jobs: Array<SwedenPeopleDraftTwoJob | null | undefined>
): SwedenPeopleDraftTwoJob | null {
  return (
    jobs
      .filter((job): job is SwedenPeopleDraftTwoJob => Boolean(job))
      .sort((left, right) => right.updatedAt.localeCompare(left.updatedAt))[0] ??
    null
  );
}

function UnmappedRoleWarning({ job }: { job: SwedenPeopleDraftTwoJob }) {
  if (job.skippedUnmappedRows === 0) return null;
  return (
    <Alert>
      <TriangleAlertIcon />
      <AlertTitle>
        {numberFormat.format(job.skippedUnmappedRows)} unmapped observations
        skipped
      </AlertTitle>
      <AlertDescription className="flex flex-col gap-2">
        <span>
          Their original values remain unchanged in Draft 1. They are not
          normalized or included in Draft 2.
        </span>
        {job.unmappedRoleExamples.length > 0 ? (
          <span className="flex flex-wrap gap-1">
            {job.unmappedRoleExamples.map((example) => (
              <Badge key={example} variant="outline">
                {example}
              </Badge>
            ))}
          </span>
        ) : null}
      </AlertDescription>
    </Alert>
  );
}

function JobStatus({ job }: { job: SwedenPeopleDraftTwoJob }) {
  if (job.status === "failed") {
    return (
      <Alert variant="destructive">
        <TriangleAlertIcon />
        <AlertTitle>Draft 2 build failed</AlertTitle>
        <AlertDescription>{job.errorMessage}</AlertDescription>
      </Alert>
    );
  }
  if (job.status === "completed") {
    return (
      <div className="flex flex-col gap-3">
        <Alert>
          <CheckCircle2Icon />
          <AlertTitle>Draft 2 build completed</AlertTitle>
          <AlertDescription>
            Created {numberFormat.format(job.outputRows)} merged person-position
            rows from {numberFormat.format(job.totalRows)} mapped source
            observations. {numberFormat.format(job.skippedRolelessRows)}
            roleless observations were retained in Draft 1 and skipped here.
          </AlertDescription>
        </Alert>
        <UnmappedRoleWarning job={job} />
      </div>
    );
  }
  return (
    <div className="flex flex-col gap-3">
      <div className="flex flex-col gap-3 rounded-lg border p-4" role="status">
        <Progress value={job.progressPercent}>
          <ProgressLabel>{job.message}</ProgressLabel>
          <ProgressValue />
        </Progress>
        <span className="text-xs text-muted-foreground">
          {numberFormat.format(job.processedRows)} of{" "}
          {numberFormat.format(job.totalRows)} mapped observations processed
        </span>
      </div>
      <UnmappedRoleWarning job={job} />
    </div>
  );
}

export function PeopleDraftTwoBuilder({
  status,
  initialJob,
}: {
  status: SwedenPeopleDraftTwoStatus;
  initialJob: SwedenPeopleDraftTwoJob | null;
}) {
  const actionFetcher = useFetcher<DraftTwoActionData>();
  const progressFetcher = useFetcher<DraftTwoStatusData>();
  const revalidator = useRevalidator();
  const [confirmationOpen, setConfirmationOpen] = useState(false);
  const refreshedTerminalJob = useRef("");
  const submittedJob =
    actionFetcher.data?.ok && actionFetcher.data.target === "draft-2"
      ? actionFetcher.data.job
      : null;
  const job = newestJob(progressFetcher.data?.job, submittedJob, initialJob);
  const jobIsActive = job?.status === "queued" || job?.status === "running";
  const submitting = actionFetcher.state !== "idle";
  const hasRows = status.rowCount > 0;

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
      {job ? <JobStatus job={job} /> : null}
      {actionFetcher.data && !actionFetcher.data.ok ? (
        <Alert variant="destructive">
          <TriangleAlertIcon />
          <AlertTitle>Draft 2 was not started</AlertTitle>
          <AlertDescription>{actionFetcher.data.error}</AlertDescription>
        </Alert>
      ) : null}

      {jobIsActive ? null : hasRows ? (
        <div className="flex flex-wrap items-center gap-2">
          <Badge variant="secondary">
            Draft 2 ready · {numberFormat.format(status.rowCount)} rows
          </Badge>
          <Dialog open={confirmationOpen} onOpenChange={setConfirmationOpen}>
            <Button
              type="button"
              variant="outline"
              onClick={() => setConfirmationOpen(true)}
            >
              <RotateCcwIcon data-icon="inline-start" />
              Rebuild Draft 2
            </Button>
            <DialogContent>
              <DialogHeader>
                <DialogTitle>Rebuild Draft 2?</DialogTitle>
                <DialogDescription>
                  A durable Temporal workflow will re-run role mapping and
                  person matching against the current immutable Draft 1
                  observations.
                </DialogDescription>
              </DialogHeader>
              <Alert variant="destructive">
                <TriangleAlertIcon />
                <AlertTitle>
                  {numberFormat.format(status.rowCount)} merged rows will be
                  replaced
                </AlertTitle>
                <AlertDescription>
                  The published Draft 2 table remains available until the
                  replacement finishes successfully.
                </AlertDescription>
              </Alert>
              <actionFetcher.Form method="post">
                <input
                  type="hidden"
                  name="intent"
                  value="start_people_draft_step_2_build"
                />
                <input type="hidden" name="confirm_replacement" value="yes" />
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
            value="start_people_draft_step_2_build"
          />
          <Button type="submit" disabled={submitting}>
            {submitting ? (
              <LoaderCircleIcon
                data-icon="inline-start"
                className="animate-spin"
              />
            ) : (
              <CombineIcon data-icon="inline-start" />
            )}
            {job?.status === "failed" ? "Retry Draft 2 build" : "Create Draft 2"}
          </Button>
        </actionFetcher.Form>
      )}
    </div>
  );
}
