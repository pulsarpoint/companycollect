import { PlayIcon } from "lucide-react";
import { useFetcher } from "react-router";
import { Alert, AlertDescription, AlertTitle } from "~/components/ui/alert";
import { Badge } from "~/components/ui/badge";
import { Button } from "~/components/ui/button";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "~/components/ui/card";
import { Checkbox } from "~/components/ui/checkbox";
import { Input } from "~/components/ui/input";
import { Label } from "~/components/ui/label";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "~/components/ui/select";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "~/components/ui/table";
import {
  DEFAULT_COMPANY_BATCH_SIZE,
  DEFAULT_MAX_COMPANIES,
  DEFAULT_MERGE_LLM_PROFILE_NAME,
  DEFAULT_MERGE_TIMEOUT_SECONDS,
  DEFAULT_MIN_CONFIDENCE,
  DEFAULT_OBSERVATIONS_PER_REQUEST,
  DEFAULT_PERSON_LLM_PROFILE_NAME,
  DEFAULT_RESOLUTION_TIMEOUT_SECONDS,
  IDENTITY_EVALUATION_DEFAULT_WRITE_CANDIDATES,
  MERGE_LLM_PROFILES,
  PERSON_LLM_PROFILES,
} from "~/lib/se-company-person-pipeline";
import type { SeCompanyPersonPipelineStats } from "~/lib/se-company-person-pipeline.server";

/**
 * The People pipeline page (SE People Experiment Task 5) -- three
 * independently-launched Dagster jobs (spec §6.1: "the info-pilot / ESEF
 * pattern verbatim"), each its own confirm-then-launch form fed by its own
 * `useFetcher`. Three fetchers rather than one shared `actionData`: the
 * three jobs are unrelated, so a reviewer reviewing a merge-job confirmation
 * must not have it clobbered by clicking "Review" on the resolution form
 * next to it -- each fetcher's `.data` only ever reflects ITS OWN
 * submissions.
 */

const nf = new Intl.NumberFormat("en-US");

export interface PeoplePipelineRunRow {
  runId: string;
  status: string;
  startTime: number | null;
  url: string | null;
}

export interface PeoplePipelineView {
  kind: "view";
  identityRuns: PeoplePipelineRunRow[];
  cleanCopyRuns: PeoplePipelineRunRow[];
  llmSuggestionsRuns: PeoplePipelineRunRow[];
  promotionRuns: PeoplePipelineRunRow[];
  mergeRuns: PeoplePipelineRunRow[];
  dagsterError: string;
  stats: SeCompanyPersonPipelineStats | null;
  statsError: string;
  /** From the `?company_id=` query param -- the person review page's "No
   * merge suggestion yet." state links here so a reviewer can launch a
   * scoped merge run without copy-pasting the id by hand. Prefills the
   * merge section's company scope only (that is the launch a reviewer
   * coming from that link wants); "" when the page was opened directly. */
  prefilledCompanyId: string;
}

export type PeoplePipelineSection =
  | "identity"
  | "clean-copy"
  | "llm-suggestions"
  | "promotion"
  | "merge";

/** What the confirm step restates before anything is launched. */
export interface PeoplePipelineConfirmation {
  section: PeoplePipelineSection;
  title: string;
  lines: string[];
  /** Hidden fields replayed verbatim into the launch form. */
  fields: Record<string, string>;
}

export type PeoplePipelineResult =
  | { kind: "confirmation"; confirmation: PeoplePipelineConfirmation }
  | {
      kind: "launched";
      section: PeoplePipelineSection;
      runId: string;
      url: string | null;
      job: string;
    }
  | { kind: "error"; section: PeoplePipelineSection | null; error: string };

function Field({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div className="flex flex-col gap-1">
      <Label className="text-xs font-medium">{label}</Label>
      {children}
    </div>
  );
}

function RunsTable({ runs }: { runs: PeoplePipelineRunRow[] }) {
  if (runs.length === 0) {
    return <span className="text-sm text-muted-foreground">No runs yet.</span>;
  }
  return (
    <Table>
      <TableHeader>
        <TableRow>
          <TableHead>Run</TableHead>
          <TableHead>Status</TableHead>
          <TableHead>Started</TableHead>
        </TableRow>
      </TableHeader>
      <TableBody>
        {runs.map((run) => (
          <TableRow key={run.runId}>
            <TableCell className="font-mono text-xs">
              {run.url ? (
                <a className="underline" href={run.url} target="_blank" rel="noreferrer">
                  {run.runId.slice(0, 8)}
                </a>
              ) : (
                run.runId.slice(0, 8)
              )}
            </TableCell>
            <TableCell>
              <Badge variant="outline">{run.status}</Badge>
            </TableCell>
            <TableCell className="text-xs text-muted-foreground">
              {run.startTime ? new Date(run.startTime * 1000).toLocaleString() : "—"}
            </TableCell>
          </TableRow>
        ))}
      </TableBody>
    </Table>
  );
}

/** Every section's result banner + confirm-or-launch body is the same shape,
 * only the fields inside the "review" form differ -- factored once here. */
function SectionBody({
  section,
  runs,
  reviewForm,
}: {
  section: PeoplePipelineSection;
  runs: PeoplePipelineRunRow[];
  reviewForm: React.ReactNode;
}) {
  const fetcher = useFetcher<PeoplePipelineResult>();
  const data = fetcher.data;
  const busy = fetcher.state !== "idle";
  const confirmation =
    data?.kind === "confirmation" && data.confirmation.section === section
      ? data.confirmation
      : null;
  const launched = data?.kind === "launched" && data.section === section ? data : null;
  const error =
    data?.kind === "error" && (data.section === section || data.section === null)
      ? data.error
      : null;

  return (
    <CardContent className="flex flex-col gap-3">
      {launched ? (
        <Alert>
          <AlertTitle>Launched</AlertTitle>
          <AlertDescription>
            Run {launched.runId} on {launched.job}
            {launched.url ? (
              <>
                {" "}
                ·{" "}
                <a className="underline" href={launched.url} target="_blank" rel="noreferrer">
                  open in Dagster
                </a>
              </>
            ) : null}
          </AlertDescription>
        </Alert>
      ) : null}
      {error ? (
        <Alert variant="destructive">
          <AlertTitle>Not launched</AlertTitle>
          <AlertDescription>{error}</AlertDescription>
        </Alert>
      ) : null}

      {confirmation ? (
        <fetcher.Form method="post" className="flex flex-col gap-2">
          <input type="hidden" name="intent" value={`launch-${section}`} />
          {Object.entries(confirmation.fields).map(([key, value]) => (
            <input key={key} type="hidden" name={key} value={value} />
          ))}
          <div className="rounded-lg border p-3 text-sm">
            <p className="font-medium">{confirmation.title}</p>
            <ul className="mt-1 list-disc pl-4 text-muted-foreground">
              {confirmation.lines.map((line, index) => (
                <li key={index}>{line}</li>
              ))}
            </ul>
          </div>
          <Button type="submit" disabled={busy} aria-busy={busy}>
            <PlayIcon data-icon="inline-start" />
            Launch
          </Button>
        </fetcher.Form>
      ) : (
        <fetcher.Form method="post" className="grid gap-3 sm:grid-cols-2">
          <input type="hidden" name="intent" value={`confirm-${section}`} />
          {reviewForm}
          <Button
            type="submit"
            variant="outline"
            className="sm:col-span-2"
            disabled={busy}
            aria-busy={busy}
          >
            Review
          </Button>
        </fetcher.Form>
      )}

      <div>
        <p className="mb-1 text-sm font-medium">Recent runs</p>
        <RunsTable runs={runs} />
      </div>
    </CardContent>
  );
}

function IdentityEvaluationSection({ runs }: { runs: PeoplePipelineRunRow[] }) {
  return (
    <Card>
      <CardHeader>
        <CardTitle>Identity evaluation</CardTitle>
        <CardDescription>
          se_company_person_identity_evaluation_job -- computes K1/K2/K3
          person-identity counts over the three source views. When "write
          candidates" is on, it also TRUNCATES and rewrites
          se_company_person_collision_candidate from this run.
        </CardDescription>
      </CardHeader>
      <SectionBody
        section="identity"
        runs={runs}
        reviewForm={
          <>
            <div className="sm:col-span-2">
              <Field label="Company scope (comma-separated, blank = every SE company)">
                <Input name="company_ids" placeholder="5560125220,5565200028" />
              </Field>
            </div>
            <label className="flex items-center gap-2 text-sm sm:col-span-2">
              <Checkbox
                name="write_candidates"
                value="1"
                defaultChecked={IDENTITY_EVALUATION_DEFAULT_WRITE_CANDIDATES}
              />
              <span>Write collision candidates (truncates and rewrites the table)</span>
            </label>
          </>
        }
      />
    </Card>
  );
}

/** se_company_person_publish_job -- CLEAN COPY: deterministic single-source
 * companies straight to se_company_person, no LLM controls at all. */
function CleanCopySection({
  runs,
  publishedPersonCount,
}: {
  runs: PeoplePipelineRunRow[];
  publishedPersonCount: number | null;
}) {
  return (
    <Card>
      <CardHeader>
        <CardTitle>Clean copy (single-source, deterministic)</CardTitle>
        <CardDescription>
          se_company_person_publish_job -- publishes se_company_person for
          SINGLE-SOURCE companies only: a pure deterministic copy per
          company, no LLM involved and no LLM parameters accepted. A
          multi-source company in scope is skipped and counted, never
          partially processed -- use LLM suggestions + Promote suggestions
          below for those. Always writes when launched; there is no preview
          mode, and none is needed: this is deterministic and free.
          {publishedPersonCount !== null
            ? ` ${nf.format(publishedPersonCount)} people are currently published.`
            : ""}
        </CardDescription>
      </CardHeader>
      <SectionBody
        section="clean-copy"
        runs={runs}
        reviewForm={
          <>
            <div className="sm:col-span-2">
              <Field label="Company scope (comma-separated, blank = every company)">
                <Input name="company_ids" placeholder="5560125220,5565200028" />
              </Field>
            </div>
            <Field label="Max companies">
              <Input
                name="max_companies"
                defaultValue={String(DEFAULT_MAX_COMPANIES)}
                inputMode="numeric"
              />
            </Field>
            <Field label="Company batch size">
              <Input
                name="company_batch_size"
                defaultValue={String(DEFAULT_COMPANY_BATCH_SIZE)}
                inputMode="numeric"
              />
            </Field>
          </>
        }
      />
    </Card>
  );
}

function ProfileSelect({
  profiles,
  defaultValue,
}: {
  profiles: typeof MERGE_LLM_PROFILES;
  defaultValue: string;
}) {
  return (
    <Select
      items={profiles.map((profile) => ({
        label: `${profile.name} — ${profile.model}`,
        value: profile.name,
      }))}
      name="llm_profile"
      defaultValue={defaultValue}
    >
      <SelectTrigger className="w-full" size="sm">
        <SelectValue />
      </SelectTrigger>
      <SelectContent>
        {profiles.map((profile) => (
          <SelectItem key={profile.name} value={profile.name}>
            {`${profile.name} — ${profile.model}`}
          </SelectItem>
        ))}
      </SelectContent>
    </Select>
  );
}

/** se_company_person_llm_suggestions_job -- multi-source companies, writes
 * suggestions ONLY (se_company_person_enrichment_observation); never touches
 * se_company_person. execute off is a harmless preview, mirroring the merge
 * job below. */
function LlmSuggestionsSection({ runs }: { runs: PeoplePipelineRunRow[] }) {
  return (
    <Card>
      <CardHeader>
        <CardTitle>LLM resolution (multi-source)</CardTitle>
        <CardDescription>
          se_company_person_llm_suggestions_job -- resolves MULTI-SOURCE
          companies with the model and writes suggestions ONLY, to
          se_company_person_enrichment_observation. Nothing goes live in
          se_company_person until a Promote suggestions run. Execute off is
          a harmless preview: no model call, nothing written. May call
          DeepSeek for every multi-source company in scope with no cost
          estimate available here -- start narrow, or a deliberately larger
          max companies, not the default.
        </CardDescription>
      </CardHeader>
      <SectionBody
        section="llm-suggestions"
        runs={runs}
        reviewForm={
          <>
            <div className="sm:col-span-2">
              <Field label="Company scope (comma-separated, blank = every company)">
                <Input name="company_ids" placeholder="5560125220,5565200028" />
              </Field>
            </div>
            <Field label="LLM profile">
              <ProfileSelect
                profiles={PERSON_LLM_PROFILES}
                defaultValue={DEFAULT_PERSON_LLM_PROFILE_NAME}
              />
            </Field>
            <Field label="Max companies">
              <Input
                name="max_companies"
                defaultValue={String(DEFAULT_MAX_COMPANIES)}
                inputMode="numeric"
              />
            </Field>
            <Field label="Company batch size">
              <Input
                name="company_batch_size"
                defaultValue={String(DEFAULT_COMPANY_BATCH_SIZE)}
                inputMode="numeric"
              />
            </Field>
            <Field label="Max observations per LLM request">
              <Input
                name="maximum_observations_per_request"
                defaultValue={String(DEFAULT_OBSERVATIONS_PER_REQUEST)}
                inputMode="numeric"
              />
            </Field>
            <Field label="Timeout (seconds)">
              <Input
                name="timeout_seconds"
                defaultValue={String(DEFAULT_RESOLUTION_TIMEOUT_SECONDS)}
                inputMode="numeric"
              />
            </Field>
            <label className="flex items-center gap-2 text-sm sm:col-span-2">
              <Checkbox name="execute" value="1" />
              <span>
                Execute: call the model and write suggestions (unchecked =
                preview, writes nothing)
              </span>
            </label>
          </>
        }
      />
    </Card>
  );
}

/** se_company_person_promotion_job -- deterministic, model-free: copies an
 * eligible stored suggestion into se_company_person, gated only by
 * min_confidence. No execute gate, like clean copy: this is free. */
function PromotionSection({ runs }: { runs: PeoplePipelineRunRow[] }) {
  return (
    <Card>
      <CardHeader>
        <CardTitle>Promote LLM suggestions</CardTitle>
        <CardDescription>
          se_company_person_promotion_job -- copies a stored LLM suggestion
          into se_company_person for multi-source companies, applying the
          correction ledger on top exactly like clean copy does.
          min_confidence (default 0) gates which suggestions promote; a
          suggestion whose evidence has moved since it was written is
          skipped and counted, never guessed at. Deterministic and free --
          no model is ever called here.
        </CardDescription>
      </CardHeader>
      <SectionBody
        section="promotion"
        runs={runs}
        reviewForm={
          <>
            <div className="sm:col-span-2">
              <Field label="Company scope (comma-separated, blank = every company)">
                <Input name="company_ids" placeholder="5560125220,5565200028" />
              </Field>
            </div>
            <Field label="Max companies">
              <Input
                name="max_companies"
                defaultValue={String(DEFAULT_MAX_COMPANIES)}
                inputMode="numeric"
              />
            </Field>
            <Field label="Company batch size">
              <Input
                name="company_batch_size"
                defaultValue={String(DEFAULT_COMPANY_BATCH_SIZE)}
                inputMode="numeric"
              />
            </Field>
            <Field label="Min confidence (0-1, blank = 0: promote all)">
              <Input
                name="min_confidence"
                defaultValue={String(DEFAULT_MIN_CONFIDENCE)}
                inputMode="decimal"
              />
            </Field>
          </>
        }
      />
    </Card>
  );
}

function MergeSection({
  runs,
  stats,
  statsError,
  prefilledCompanyId,
}: {
  runs: PeoplePipelineRunRow[];
  stats: SeCompanyPersonPipelineStats | null;
  statsError: string;
  prefilledCompanyId: string;
}) {
  return (
    <Card>
      <CardHeader>
        <CardTitle>Merge suggestions</CardTitle>
        <CardDescription>
          se_company_person_merge_job -- asks an LLM to judge each open
          collision-candidate group (spec §6.1). Execute off is a harmless
          preview: no model call, nothing written. A suggestion is only ever
          approved or kept-separate by a human, on each person's review page.
          {stats
            ? ` ${nf.format(stats.collisionGroupCount)} collision groups, ${nf.format(stats.decidedGroupCount)} already decided.`
            : ""}
          {statsError ? ` (stats unavailable: ${statsError})` : ""}
        </CardDescription>
      </CardHeader>
      <SectionBody
        section="merge"
        runs={runs}
        reviewForm={
          <>
            <div className="sm:col-span-2">
              <Field label="Company scope (comma-separated, blank = every company)">
                <Input
                  name="company_ids"
                  placeholder="5560125220,5565200028"
                  defaultValue={prefilledCompanyId}
                />
              </Field>
            </div>
            <Field label="LLM profile">
              <ProfileSelect
                profiles={MERGE_LLM_PROFILES}
                defaultValue={DEFAULT_MERGE_LLM_PROFILE_NAME}
              />
            </Field>
            <Field label="Max groups (blank = no limit)">
              <Input name="max_groups" inputMode="numeric" />
            </Field>
            <Field label="Timeout (seconds)">
              <Input
                name="timeout_seconds"
                defaultValue={String(DEFAULT_MERGE_TIMEOUT_SECONDS)}
                inputMode="numeric"
              />
            </Field>
            <label className="flex items-center gap-2 text-sm sm:col-span-2">
              <Checkbox name="execute" value="1" />
              <span>
                Execute: call the model and write suggestions (unchecked =
                preview, writes nothing)
              </span>
            </label>
          </>
        }
      />
    </Card>
  );
}

export function SePeoplePipeline({ view }: { view: PeoplePipelineView }) {
  return (
    <div className="flex flex-1 flex-col gap-6 p-4 md:p-6">
      <header className="flex flex-col gap-2">
        <h1 className="text-2xl font-semibold tracking-tight">People pipeline</h1>
        <p className="text-sm text-muted-foreground">
          Backoffice-triggered runs for the Sweden company-person model (spec
          §6.1): identity evaluation, clean-copy and LLM-assisted
          resolution, and LLM-assisted merge suggestions. Never scheduled,
          never eager -- every launch here is tagged pilot=backoffice.
        </p>
        {view.dagsterError ? (
          <Alert variant="destructive">
            <AlertTitle>Dagster unavailable</AlertTitle>
            <AlertDescription>{view.dagsterError}</AlertDescription>
          </Alert>
        ) : null}
      </header>
      <IdentityEvaluationSection runs={view.identityRuns} />
      <CleanCopySection
        runs={view.cleanCopyRuns}
        publishedPersonCount={view.stats?.publishedPersonCount ?? null}
      />
      <LlmSuggestionsSection runs={view.llmSuggestionsRuns} />
      <PromotionSection runs={view.promotionRuns} />
      <MergeSection
        runs={view.mergeRuns}
        stats={view.stats}
        statsError={view.statsError}
        prefilledCompanyId={view.prefilledCompanyId}
      />
    </div>
  );
}
