import { Link, useNavigate } from "react-router";
import {
  ArrowLeftIcon,
  ArrowRightIcon,
  CheckCircle2Icon,
  DatabaseIcon,
  FileSearchIcon,
  SparklesIcon,
} from "lucide-react";
import { PeopleDraftInitializer } from "~/components/admin/people-draft-initializer";
import {
  DraftOneRowsTable,
  DraftTwoRowsTable,
  PeopleDraftCompanyFilter,
  peopleDraftUrl,
  type PeopleDraftFilter,
  type PeopleDraftStep,
} from "~/components/admin/people-draft-tables";
import { PeopleDraftTwoBuilder } from "~/components/admin/people-draft-two-builder";
import { Alert, AlertDescription, AlertTitle } from "~/components/ui/alert";
import { Badge } from "~/components/ui/badge";
import { Button } from "~/components/ui/button";
import {
  Card,
  CardContent,
  CardDescription,
  CardFooter,
  CardHeader,
  CardTitle,
} from "~/components/ui/card";
import {
  Empty,
  EmptyDescription,
  EmptyHeader,
  EmptyMedia,
  EmptyTitle,
} from "~/components/ui/empty";
import {
  Progress,
  ProgressLabel,
} from "~/components/ui/progress";
import {
  ToggleGroup,
  ToggleGroupItem,
} from "~/components/ui/toggle-group";
import type {
  SwedenPeopleDraftInitializationJob,
  SwedenPeopleDraftStatus,
} from "~/lib/sweden-people-draft.server";
import type {
  SwedenPeopleDraftOneRow,
  SwedenPeopleDraftRowsPage,
  SwedenPeopleDraftTwoJob,
  SwedenPeopleDraftTwoRow,
  SwedenPeopleDraftTwoStatus,
} from "~/lib/sweden-people-draft-two.server";
import type { LlmRequestAvailability } from "~/lib/llm-availability.server";
import type { SwedenPeopleProfileBulkJob } from "~/lib/sweden-person-profile-bulk.server";

const WIZARD_STEPS = [
  {
    id: "draft-1",
    label: "Draft 1",
    shortDescription: "Collect source rows",
    icon: DatabaseIcon,
  },
  {
    id: "draft-2",
    label: "Draft 2",
    shortDescription: "Normalize people",
    icon: SparklesIcon,
  },
  {
    id: "final",
    label: "Final",
    shortDescription: "Review and publish",
    icon: CheckCircle2Icon,
  },
] as const;

type WizardStepId = PeopleDraftStep;

const numberFormat = new Intl.NumberFormat("en-US");
const unavailableLlm: LlmRequestAvailability = {
  ready: false,
  warning: "Configure and activate an LLM before starting bulk enhancement.",
  profile: null,
};

function DraftOneStep({
  status,
  initializationJob,
  rows,
  page,
  filter,
  onContinue,
}: {
  status: SwedenPeopleDraftStatus;
  initializationJob: SwedenPeopleDraftInitializationJob | null;
  rows: SwedenPeopleDraftOneRow[];
  page: SwedenPeopleDraftRowsPage<SwedenPeopleDraftOneRow>;
  filter: PeopleDraftFilter;
  onContinue: () => void;
}) {
  const hasPublishedRows = status.rowCount > 0;

  return (
    <Card>
      <CardHeader>
        <div className="flex items-center gap-2">
          <Badge variant="outline">Step 1</Badge>
          <Badge variant={hasPublishedRows ? "secondary" : "destructive"}>
            {hasPublishedRows ? "Initialized" : "Not initialized"}
          </Badge>
        </div>
        <CardTitle>Prepare immutable source observations</CardTitle>
        <CardDescription>
          Draft 1 stores source-owned person rows before matching or LLM
          normalization. Nothing in this table is a final person profile.
        </CardDescription>
      </CardHeader>

      <CardContent className="flex flex-col gap-5">
        {hasPublishedRows ? (
          <>
            <div className="grid overflow-hidden rounded-lg border sm:grid-cols-3">
              <div className="flex flex-col gap-1 p-4">
                <span className="text-xs text-muted-foreground">Table</span>
                <code className="text-sm font-medium">
                  people_draft_step_1
                </code>
              </div>
              <div className="flex flex-col gap-1 border-t p-4 sm:border-l sm:border-t-0">
                <span className="text-xs text-muted-foreground">
                  Observations
                </span>
                <span className="text-xl font-semibold tabular-nums">
                  {numberFormat.format(status.rowCount)}
                </span>
              </div>
              <div className="flex flex-col gap-1 border-t p-4 sm:border-l sm:border-t-0">
                <span className="text-xs text-muted-foreground">Storage</span>
                <span className="text-sm font-medium">Local DuckDB</span>
              </div>
            </div>

            <Alert>
              <CheckCircle2Icon />
              <AlertTitle>Source observations are available</AlertTitle>
              <AlertDescription>
                Draft 1 contains {numberFormat.format(status.rowCount)} rows
                ready for the normalization stage.
              </AlertDescription>
            </Alert>

            <div className="flex flex-wrap items-center justify-between gap-3">
              <PeopleDraftInitializer
                status={status}
                initialJob={initializationJob}
              />
              <Button
                variant="outline"
                nativeButton={false}
                render={<Link to="/admin/se/people/sources" />}
              >
                <FileSearchIcon data-icon="inline-start" />
                Inspect source tables
              </Button>
            </div>
            <DraftOneRowsTable
              rows={rows}
              page={page}
              filter={filter}
            />
          </>
        ) : (
          <Empty className="border">
            <EmptyHeader>
              <EmptyMedia variant="icon">
                <DatabaseIcon />
              </EmptyMedia>
              <EmptyTitle>Draft 1 has no imported observations</EmptyTitle>
              <EmptyDescription>
                {status.tableExists
                  ? "The DuckDB schema exists, but it is not initialized until source rows have been imported."
                  : "Start the background job to create the DuckDB table and import all source rows."}
              </EmptyDescription>
            </EmptyHeader>
            <PeopleDraftInitializer
              status={status}
              initialJob={initializationJob}
            />
          </Empty>
        )}
      </CardContent>

      <CardFooter className="justify-end">
        <Button onClick={onContinue} disabled={!hasPublishedRows}>
          Continue to Draft 2
          <ArrowRightIcon data-icon="inline-end" />
        </Button>
      </CardFooter>
    </Card>
  );
}

function DraftTwoStep({
  status,
  job,
  rows,
  page,
  filter,
  bulkJob,
  llmAvailability,
  onPrevious,
  onContinue,
}: {
  status: SwedenPeopleDraftTwoStatus;
  job: SwedenPeopleDraftTwoJob | null;
  rows: SwedenPeopleDraftTwoRow[];
  page: SwedenPeopleDraftRowsPage<SwedenPeopleDraftTwoRow>;
  filter: PeopleDraftFilter;
  bulkJob: SwedenPeopleProfileBulkJob | null;
  llmAvailability: LlmRequestAvailability;
  onPrevious: () => void;
  onContinue: () => void;
}) {
  const hasRows = status.rowCount > 0;
  return (
    <Card>
      <CardHeader>
        <div className="flex items-center gap-2">
          <Badge variant="outline">Step 2</Badge>
          <Badge variant={hasRows ? "secondary" : "destructive"}>
            {hasRows ? "Created" : "Not created"}
          </Badge>
        </div>
        <CardTitle>Build normalized person candidates</CardTitle>
        <CardDescription>
          Draft 2 groups yearly observations by company, matching name, and
          canonical position. Each row retains separate Bolagsverket, ESEF, and
          Wikidata evidence and descriptions.
        </CardDescription>
      </CardHeader>
      <CardContent className="flex flex-col gap-5">
        <PeopleDraftTwoBuilder status={status} initialJob={job} />
        {hasRows ? (
          <DraftTwoRowsTable
            rows={rows}
            page={page}
            filter={filter}
            bulkJob={bulkJob}
            llmAvailability={llmAvailability}
          />
        ) : null}
      </CardContent>
      <CardFooter className="justify-between gap-2">
        <Button variant="outline" onClick={onPrevious}>
          <ArrowLeftIcon data-icon="inline-start" />
          Back to Draft 1
        </Button>
        <Button onClick={onContinue} disabled={!hasRows}>
          Continue to Final
          <ArrowRightIcon data-icon="inline-end" />
        </Button>
      </CardFooter>
    </Card>
  );
}

function FinalStep({ onPrevious }: { onPrevious: () => void }) {
  return (
    <Card>
      <CardHeader>
        <div className="flex items-center gap-2">
          <Badge variant="outline">Step 3</Badge>
          <Badge variant="secondary">Planned</Badge>
        </div>
        <CardTitle>Review the final company people</CardTitle>
        <CardDescription>
          The final step will show normalized people, their year-scoped roles,
          and all source evidence before publishing them to the application.
        </CardDescription>
      </CardHeader>
      <CardContent>
        <Empty className="border">
          <EmptyHeader>
            <EmptyMedia variant="icon">
              <CheckCircle2Icon />
            </EmptyMedia>
            <EmptyTitle>The final review is not connected yet</EmptyTitle>
            <EmptyDescription>
              Publishing remains disabled until Draft 2 and the final tables
              are implemented.
            </EmptyDescription>
          </EmptyHeader>
        </Empty>
      </CardContent>
      <CardFooter>
        <Button variant="outline" onClick={onPrevious}>
          <ArrowLeftIcon data-icon="inline-start" />
          Back to Draft 2
        </Button>
      </CardFooter>
    </Card>
  );
}

export function PeopleCurationWorkspace({
  draftStatus,
  initializationJob,
  draftTwoStatus,
  draftTwoJob,
  draftOnePage,
  draftTwoPage,
  filter,
  bulkJob = null,
  llmAvailability = unavailableLlm,
}: {
  draftStatus: SwedenPeopleDraftStatus;
  initializationJob: SwedenPeopleDraftInitializationJob | null;
  draftTwoStatus: SwedenPeopleDraftTwoStatus;
  draftTwoJob: SwedenPeopleDraftTwoJob | null;
  draftOnePage: SwedenPeopleDraftRowsPage<SwedenPeopleDraftOneRow>;
  draftTwoPage: SwedenPeopleDraftRowsPage<SwedenPeopleDraftTwoRow>;
  filter: PeopleDraftFilter;
  bulkJob?: SwedenPeopleProfileBulkJob | null;
  llmAvailability?: LlmRequestAvailability;
}) {
  const navigate = useNavigate();
  const currentStepId = filter.currentStep;
  const currentStepIndex = WIZARD_STEPS.findIndex(
    (step) => step.id === currentStepId,
  );
  const currentStep = WIZARD_STEPS[currentStepIndex];
  const progress = ((currentStepIndex + 1) / WIZARD_STEPS.length) * 100;

  function goToStep(step: WizardStepId) {
    navigate(
      peopleDraftUrl({
        companyId: filter.companyId,
        draftOneView: filter.draftOneView,
        draftTwoView: filter.draftTwoView,
        draftTwoSources: filter.draftTwoSources,
        draftTwoHasLlmSuggestion: filter.draftTwoHasLlmSuggestion,
        draftOnePage: filter.draftOnePage,
        draftTwoPage: filter.draftTwoPage,
        currentStep: step,
      }),
    );
  }

  function selectStep(values: unknown) {
    if (!Array.isArray(values)) return;
    const selected = values[0];
    if (WIZARD_STEPS.some((step) => step.id === selected)) {
      goToStep(selected as WizardStepId);
    }
  }

  return (
    <div className="flex flex-1 flex-col gap-6 p-4 md:p-6">
      <header className="flex flex-col gap-1">
        <div className="flex items-center gap-2">
          <h1 className="text-2xl font-semibold tracking-tight">
            People processing
          </h1>
          <Badge variant="outline">Sweden</Badge>
        </div>
        <p className="max-w-2xl text-sm text-muted-foreground">
          Move source observations through draft normalization and final review.
        </p>
      </header>

      <PeopleDraftCompanyFilter filter={filter} />

      <section
        aria-label="People processing progress"
        className="flex flex-col gap-4"
      >
        <Progress value={progress}>
          <ProgressLabel>
            Step {currentStepIndex + 1} of {WIZARD_STEPS.length} ·{" "}
            {currentStep.label}
          </ProgressLabel>
        </Progress>

        <ToggleGroup
          value={[currentStepId]}
          onValueChange={selectStep}
          variant="outline"
          spacing={2}
          aria-label="People processing steps"
          className="grid w-full grid-cols-1 sm:grid-cols-3"
        >
          {WIZARD_STEPS.map((step, index) => {
            const StepIcon = step.icon;
            return (
              <ToggleGroupItem
                key={step.id}
                value={step.id}
                aria-label={`Open ${step.label}`}
                disabled={
                  (draftStatus.rowCount === 0 && step.id !== "draft-1") ||
                  (draftTwoStatus.rowCount === 0 && step.id === "final")
                }
                className="h-auto min-w-0 justify-start px-3 py-3"
              >
                <StepIcon data-icon="inline-start" />
                <span className="flex min-w-0 flex-col items-start">
                  <span>
                    {index + 1}. {step.label}
                  </span>
                  <span className="truncate text-xs font-normal text-muted-foreground">
                    {step.shortDescription}
                  </span>
                </span>
              </ToggleGroupItem>
            );
          })}
        </ToggleGroup>
      </section>

      <section
        aria-label={`${currentStep.label} content`}
        className="w-full"
      >
        {currentStepId === "draft-1" ? (
          <DraftOneStep
            status={draftStatus}
            initializationJob={initializationJob}
            rows={draftOnePage.rows}
            page={draftOnePage}
            filter={filter}
            onContinue={() => goToStep("draft-2")}
          />
        ) : null}
        {currentStepId === "draft-2" ? (
          <DraftTwoStep
            status={draftTwoStatus}
            job={draftTwoJob}
            rows={draftTwoPage.rows}
            page={draftTwoPage}
            filter={filter}
            bulkJob={bulkJob}
            llmAvailability={llmAvailability}
            onPrevious={() => goToStep("draft-1")}
            onContinue={() => goToStep("final")}
          />
        ) : null}
        {currentStepId === "final" ? (
          <FinalStep onPrevious={() => goToStep("draft-2")} />
        ) : null}
      </section>
    </div>
  );
}
