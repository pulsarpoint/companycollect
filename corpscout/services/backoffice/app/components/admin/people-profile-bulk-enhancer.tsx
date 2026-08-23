import { useEffect, useRef, useState } from "react";
import { Link, useFetcher, useRevalidator } from "react-router";
import {
  CheckCircle2Icon,
  Clock3Icon,
  LoaderCircleIcon,
  Settings2Icon,
  SparklesIcon,
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
import type { LlmRequestAvailability } from "~/lib/llm-availability.server";
import type { SwedenPeopleProfileBulkJob } from "~/lib/sweden-person-profile-bulk.server";
import type { PeopleDraftFilter } from "./people-draft-tables";

type BulkEnhancementActionData =
  | {
      ok: true;
      target: "person-profile-bulk";
      job: SwedenPeopleProfileBulkJob;
      existing: boolean;
    }
  | {
      ok: false;
      target?: "person-profile-bulk";
      error: string;
    };

interface BulkEnhancementStatusData {
  job: SwedenPeopleProfileBulkJob | null;
}

const JOB_STATUS_PATH = "/admin/se/people/llm-bulk-job";
const numberFormat = new Intl.NumberFormat("en-US");

function newestJob(
  ...jobs: Array<SwedenPeopleProfileBulkJob | null | undefined>
): SwedenPeopleProfileBulkJob | null {
  return (
    jobs
      .filter((job): job is SwedenPeopleProfileBulkJob => Boolean(job))
      .sort((left, right) => right.updatedAt.localeCompare(left.updatedAt))[0] ??
    null
  );
}

function BulkJobStatus({ job }: { job: SwedenPeopleProfileBulkJob }) {
  if (job.status === "failed") {
    return (
      <Alert variant="destructive">
        <TriangleAlertIcon />
        <AlertTitle>Bulk LLM enhancement failed</AlertTitle>
        <AlertDescription>{job.errorMessage}</AlertDescription>
      </Alert>
    );
  }
  if (job.status === "completed") {
    return (
      <Alert>
        <CheckCircle2Icon />
        <AlertTitle>Bulk LLM enhancement completed</AlertTitle>
        <AlertDescription>
          Saved {numberFormat.format(job.enhancedCount)} new suggestions;
          skipped {numberFormat.format(job.skippedCurrentCount)} people whose
          existing response already matched the current evidence.{" "}
          {job.failedCount > 0
            ? numberFormat.format(job.failedCount) + " candidates failed."
            : "No candidates failed."}
        </AlertDescription>
      </Alert>
    );
  }
  return (
    <div className="flex flex-col gap-3 rounded-lg border p-4" role="status">
      <Progress value={job.progressPercent}>
        <ProgressLabel>
          {job.status === "queued"
            ? "Waiting for the Temporal worker"
            : "Enhancing person profiles"}
        </ProgressLabel>
        <ProgressValue />
      </Progress>
      <div className="flex flex-wrap items-center gap-2 text-xs text-muted-foreground">
        <Badge variant="secondary">
          <Clock3Icon data-icon="inline-start" />
          {job.status === "queued" ? "Queued" : "Running"}
        </Badge>
        <span>
          {numberFormat.format(job.processedCount)} of{" "}
          {numberFormat.format(job.totalCount)} processed
        </span>
        <span>·</span>
        <span>{numberFormat.format(job.enhancedCount)} saved</span>
        <span>·</span>
        <span>{numberFormat.format(job.skippedCurrentCount)} current</span>
        <span>·</span>
        <span>{numberFormat.format(job.failedCount)} failed</span>
      </div>
    </div>
  );
}

export function PeopleProfileBulkEnhancer({
  filter,
  selectedIds,
  allFiltered,
  selectedCount,
  initialJob,
  llmAvailability,
  onStarted,
}: {
  filter: PeopleDraftFilter;
  selectedIds: Set<string>;
  allFiltered: boolean;
  selectedCount: number;
  initialJob: SwedenPeopleProfileBulkJob | null;
  llmAvailability: LlmRequestAvailability;
  onStarted: () => void;
}) {
  const actionFetcher = useFetcher<BulkEnhancementActionData>();
  const progressFetcher = useFetcher<BulkEnhancementStatusData>();
  const revalidator = useRevalidator();
  const [dialogOpen, setDialogOpen] = useState(false);
  const refreshedTerminalJob = useRef("");
  const clearedStartedJob = useRef("");
  const submittedJob =
    actionFetcher.data?.ok &&
    actionFetcher.data.target === "person-profile-bulk"
      ? actionFetcher.data.job
      : null;
  const job = newestJob(progressFetcher.data?.job, submittedJob, initialJob);
  const jobIsActive = job?.status === "queued" || job?.status === "running";
  const submitting = actionFetcher.state !== "idle";

  useEffect(() => {
    if (actionFetcher.state !== "idle" || !actionFetcher.data?.ok) return;
    if (clearedStartedJob.current === actionFetcher.data.job.jobId) return;
    clearedStartedJob.current = actionFetcher.data.job.jobId;
    setDialogOpen(false);
    onStarted();
  }, [actionFetcher.data, actionFetcher.state, onStarted]);

  useEffect(() => {
    if (!jobIsActive || progressFetcher.state !== "idle") return;
    const timer = window.setTimeout(() => {
      progressFetcher.load(JOB_STATUS_PATH);
    }, 1_500);
    return () => window.clearTimeout(timer);
  }, [job?.jobId, job?.status, job?.updatedAt, jobIsActive, progressFetcher]);

  useEffect(() => {
    const polledJob = progressFetcher.data?.job;
    if (
      !polledJob ||
      (polledJob.status !== "completed" && polledJob.status !== "failed")
    ) {
      return;
    }
    const terminalKey = polledJob.jobId + ":" + polledJob.status;
    if (refreshedTerminalJob.current === terminalKey) return;
    refreshedTerminalJob.current = terminalKey;
    revalidator.revalidate();
  }, [progressFetcher.data, revalidator]);

  return (
    <div className="flex flex-col gap-3 border-b px-4 py-3">
      {job ? <BulkJobStatus job={job} /> : null}

      {actionFetcher.data && !actionFetcher.data.ok ? (
        <Alert variant="destructive">
          <TriangleAlertIcon />
          <AlertTitle>Bulk enhancement was not started</AlertTitle>
          <AlertDescription>{actionFetcher.data.error}</AlertDescription>
        </Alert>
      ) : null}

      <div className="flex flex-wrap items-center justify-between gap-3">
        <p className="text-sm text-muted-foreground">
          LLM enhancement is limited to selected people with evidence from at
          least two sources. Current saved responses are skipped.
        </p>
        <Dialog open={dialogOpen} onOpenChange={setDialogOpen}>
          <Button
            type="button"
            disabled={
              selectedCount === 0 || jobIsActive || !llmAvailability.ready
            }
            onClick={() => setDialogOpen(true)}
          >
            <SparklesIcon data-icon="inline-start" />
            Enhance selected with LLM
          </Button>
          <DialogContent>
            <DialogHeader>
              <DialogTitle>Start bulk LLM enhancement?</DialogTitle>
              <DialogDescription>
                Temporal will continue this job in the background after you
                leave this page or restart the backoffice web process.
              </DialogDescription>
            </DialogHeader>

            <div className="grid gap-3 rounded-lg border p-4 sm:grid-cols-2">
              <div className="flex flex-col gap-1">
                <span className="text-xs text-muted-foreground">Selection</span>
                <span className="font-medium">
                  {allFiltered
                    ? "All " +
                      numberFormat.format(selectedCount) +
                      " filtered rows"
                    : numberFormat.format(selectedCount) +
                      (selectedCount === 1 ? " selected row" : " selected rows")}
                </span>
              </div>
              <div className="flex flex-col gap-1">
                <span className="text-xs text-muted-foreground">LLM</span>
                <span className="font-medium">
                  {llmAvailability.profile
                    ? llmAvailability.profile.provider +
                      " · " +
                      llmAvailability.profile.model
                    : "Not configured"}
                </span>
              </div>
            </div>

            <Alert>
              <Clock3Icon />
              <AlertTitle>This can run for several hours</AlertTitle>
              <AlertDescription>
                Each eligible person is processed independently. Failed calls
                are retried by Temporal, and progress remains visible here.
              </AlertDescription>
            </Alert>

            <actionFetcher.Form method="post">
              <input
                type="hidden"
                name="intent"
                value="start_people_profile_bulk_enhancement"
              />
              <input
                type="hidden"
                name="selection_mode"
                value={allFiltered ? "all_filtered" : "explicit"}
              />
              <input type="hidden" name="company_id" value={filter.companyId} />
              <input
                type="hidden"
                name="require_saved_suggestion"
                value={filter.draftTwoHasLlmSuggestion ? "yes" : "no"}
              />
              {filter.draftTwoSources.map((source) => (
                <input
                  key={source}
                  type="hidden"
                  name="required_source"
                  value={source}
                />
              ))}
              {allFiltered
                ? null
                : [...selectedIds].map((draftTwoId) => (
                    <input
                      key={draftTwoId}
                      type="hidden"
                      name="draft_two_id"
                      value={draftTwoId}
                    />
                  ))}
              <DialogFooter>
                <DialogClose render={<Button type="button" variant="outline" />}>
                  Cancel
                </DialogClose>
                <Button type="submit" disabled={submitting}>
                  {submitting ? (
                    <LoaderCircleIcon
                      data-icon="inline-start"
                      className="animate-spin"
                    />
                  ) : (
                    <SparklesIcon data-icon="inline-start" />
                  )}
                  Start Temporal job
                </Button>
              </DialogFooter>
            </actionFetcher.Form>
          </DialogContent>
        </Dialog>
      </div>

      {!llmAvailability.ready && !jobIsActive ? (
        <Alert>
          <Settings2Icon />
          <AlertTitle>LLM configuration required</AlertTitle>
          <AlertDescription className="flex flex-wrap items-center justify-between gap-2">
            <span>{llmAvailability.warning}</span>
            <Button
              variant="outline"
              size="sm"
              nativeButton={false}
              render={<Link to="/admin/settings/llms" />}
            >
              Open LLM settings
            </Button>
          </AlertDescription>
        </Alert>
      ) : null}
    </div>
  );
}
