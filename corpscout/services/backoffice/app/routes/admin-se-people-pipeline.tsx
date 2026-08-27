import type { Route } from "./+types/admin-se-people-pipeline";
import {
  type PeoplePipelineConfirmation,
  type PeoplePipelineResult,
  type PeoplePipelineRunRow,
  type PeoplePipelineSection,
  type PeoplePipelineView,
  SePeoplePipeline,
} from "~/components/admin/se-people-pipeline";
import {
  DagsterError,
  dagsterRunUrl,
  launchRun,
  listRuns,
  SE_COMPANY_PERSON_IDENTITY_EVALUATION_JOB,
  SE_COMPANY_PERSON_JOB,
  SE_COMPANY_PERSON_MERGE_JOB,
  type DagsterRun,
} from "~/lib/dagster.server";
import {
  buildIdentityEvaluationRunConfig,
  buildMergeRunConfig,
  buildResolutionRunConfig,
  loadSeCompanyPersonPipelineStats,
  type SeCompanyPersonPipelineStats,
} from "~/lib/se-company-person-pipeline.server";
import {
  DEFAULT_MAX_COMPANIES,
  DEFAULT_MERGE_LLM_PROFILE_NAME,
  DEFAULT_MERGE_TIMEOUT_SECONDS,
  clampCompanyBatchSize,
  clampMaxCompanies,
  clampMaxGroups,
  clampMergeTimeoutSeconds,
  clampObservationsPerRequest,
  clampResolutionTimeoutSeconds,
  dagsterApiKeyVariable,
  describeCompanyScope,
  formatCompanyIdScope,
  mergeLlmProfile,
  parseCompanyIdScope,
  PILOT_TAG_KEY,
  PILOT_TAG_VALUE,
} from "~/lib/se-company-person-pipeline";

/**
 * The People pipeline page (SE People Experiment Task 5): triggers Dagster's
 * three backoffice-only company-person jobs, mirroring the confirm-then-
 * launch pattern `/admin/se/companies/pipeline` established for company info
 * (spec §6.1 says so explicitly -- "the info-pilot / ESEF pattern verbatim").
 * Unlike that pipeline, this one is a real PAGE, not a sheet on a list: it
 * has no per-row selection to preserve and its stats are a cheap count, not
 * a FINAL read of a multi-million-row table, so there is no reason to defer
 * loading it behind a fetcher-opened sheet.
 *
 * Instigator queries are deliberately absent, not merely filtered: all three
 * jobs are "never scheduled, never eager" (identity_eval.py/normalization.py/
 * merge.py's own module docstrings), so there is no schedule or sensor name
 * to ask about -- the "filter instigator/asset queries to exactly these
 * jobs" lesson from the company-info pipeline, applied to a pipeline that
 * has no instigators to filter to. Run/asset queries ARE filtered, to
 * exactly these three job names.
 */

const RUN_LIMIT = 10;

function toRunRow(run: DagsterRun): PeoplePipelineRunRow {
  return {
    runId: run.runId,
    status: run.status,
    startTime: run.startTime,
    url: dagsterRunUrl(run.runId),
  };
}

async function dagsterView(): Promise<{
  identityRuns: PeoplePipelineRunRow[];
  resolutionRuns: PeoplePipelineRunRow[];
  mergeRuns: PeoplePipelineRunRow[];
  error: string;
}> {
  try {
    const [identityRuns, resolutionRuns, mergeRuns] = await Promise.all([
      listRuns({ job: SE_COMPANY_PERSON_IDENTITY_EVALUATION_JOB, limit: RUN_LIMIT }),
      listRuns({ job: SE_COMPANY_PERSON_JOB, limit: RUN_LIMIT }),
      listRuns({ job: SE_COMPANY_PERSON_MERGE_JOB, limit: RUN_LIMIT }),
    ]);
    return {
      identityRuns: identityRuns.map(toRunRow),
      resolutionRuns: resolutionRuns.map(toRunRow),
      mergeRuns: mergeRuns.map(toRunRow),
      error: "",
    };
  } catch (error) {
    if (error instanceof DagsterError) {
      return { identityRuns: [], resolutionRuns: [], mergeRuns: [], error: error.message };
    }
    throw error;
  }
}

async function statsView(): Promise<{
  stats: SeCompanyPersonPipelineStats | null;
  error: string;
}> {
  try {
    return { stats: await loadSeCompanyPersonPipelineStats(), error: "" };
  } catch (error) {
    return { stats: null, error: error instanceof Error ? error.message : String(error) };
  }
}

export async function loader({ request }: Route.LoaderArgs): Promise<PeoplePipelineView> {
  const [dagster, statsResult] = await Promise.all([dagsterView(), statsView()]);
  // Coordinator review item 4: the person review page's "No merge suggestion
  // yet." state links here with `?company_id=`, so a reviewer can launch a
  // scoped merge run without copy-pasting the id -- prefills the merge
  // section's company scope only.
  const rawCompanyId = new URL(request.url).searchParams.get("company_id") ?? "";
  return {
    kind: "view",
    identityRuns: dagster.identityRuns,
    resolutionRuns: dagster.resolutionRuns,
    mergeRuns: dagster.mergeRuns,
    dagsterError: dagster.error,
    stats: statsResult.stats,
    statsError: statsResult.error,
    prefilledCompanyId: rawCompanyId === "" ? "" : formatCompanyIdScope([rawCompanyId]),
  };
}

function formValue(form: FormData, name: string): string {
  const value = form.get(name);
  return typeof value === "string" ? value : "";
}

function formNumber(form: FormData, name: string, fallback: number): number {
  const parsed = Number.parseInt(formValue(form, name), 10);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function refused(
  section: PeoplePipelineSection | null,
  error: string,
): PeoplePipelineResult {
  return { kind: "error", section, error };
}

function confirmed(confirmation: PeoplePipelineConfirmation): PeoplePipelineResult {
  return { kind: "confirmation", confirmation };
}

export async function action({ request }: Route.ActionArgs): Promise<PeoplePipelineResult> {
  const form = await request.formData();
  const intent = formValue(form, "intent");

  const launch = async (
    section: PeoplePipelineSection,
    job: string,
    runConfig: Record<string, unknown>,
  ): Promise<PeoplePipelineResult> => {
    const run = await launchRun({
      job,
      runConfig,
      tags: { [PILOT_TAG_KEY]: PILOT_TAG_VALUE },
    });
    return {
      kind: "launched",
      section,
      runId: run.runId,
      url: dagsterRunUrl(run.runId),
      job,
    };
  };

  try {
    if (intent === "confirm-identity" || intent === "launch-identity") {
      const companyIds = parseCompanyIdScope(formValue(form, "company_ids"));
      const writeCandidates = formValue(form, "write_candidates") === "1";
      if (intent === "confirm-identity") {
        return confirmed({
          section: "identity",
          title: "Run identity evaluation",
          lines: [
            `Scope: ${describeCompanyScope(companyIds)}.`,
            writeCandidates
              ? "write_candidates is on: se_company_person_collision_candidate is truncated and rewritten from this run's comparison."
              : "write_candidates is off: computes K1/K2/K3 counts for this run's metadata only, writes nothing.",
          ],
          fields: {
            company_ids: formatCompanyIdScope(companyIds),
            write_candidates: writeCandidates ? "1" : "",
          },
        });
      }
      return await launch(
        "identity",
        SE_COMPANY_PERSON_IDENTITY_EVALUATION_JOB,
        buildIdentityEvaluationRunConfig({ companyIds, writeCandidates }),
      );
    }

    if (intent === "confirm-resolution" || intent === "launch-resolution") {
      const companyIds = parseCompanyIdScope(formValue(form, "company_ids"));
      const maxCompanies = clampMaxCompanies(formNumber(form, "max_companies", DEFAULT_MAX_COMPANIES));
      const companyBatchSize = clampCompanyBatchSize(formNumber(form, "company_batch_size", 5_000));
      const maximumObservationsPerRequest = clampObservationsPerRequest(
        formNumber(form, "maximum_observations_per_request", 50),
      );
      const timeoutSeconds = clampResolutionTimeoutSeconds(
        formNumber(form, "timeout_seconds", 180),
      );
      if (intent === "confirm-resolution") {
        return confirmed({
          section: "resolution",
          title: "Publish se_company_person / roles",
          lines: [
            `Scope: ${describeCompanyScope(companyIds)}.`,
            `Up to ${maxCompanies} companies per run, ${companyBatchSize} per batch, ` +
              `up to ${maximumObservationsPerRequest} observations per LLM request, ` +
              `${timeoutSeconds}s timeout.`,
            "This always writes when launched -- se_company_person_job has no preview mode.",
            // Coordinator review item 3: this IS a paid LLM path (DeepSeek,
            // normalization.py's multi-source resolution) with no cost
            // estimate available here -- say so plainly rather than let the
            // "always writes" line above read as merely a ClickHouse write.
            "May call DeepSeek for every multi-source company in scope; there is no preview/cost estimate.",
          ],
          fields: {
            company_ids: formatCompanyIdScope(companyIds),
            max_companies: String(maxCompanies),
            company_batch_size: String(companyBatchSize),
            maximum_observations_per_request: String(maximumObservationsPerRequest),
            timeout_seconds: String(timeoutSeconds),
          },
        });
      }
      return await launch(
        "resolution",
        SE_COMPANY_PERSON_JOB,
        buildResolutionRunConfig({
          companyIds,
          maxCompanies,
          companyBatchSize,
          maximumObservationsPerRequest,
          timeoutSeconds,
        }),
      );
    }

    if (intent === "confirm-merge" || intent === "launch-merge") {
      const companyIds = parseCompanyIdScope(formValue(form, "company_ids"));
      const execute = formValue(form, "execute") === "1";
      const llmProfileName = formValue(form, "llm_profile") || DEFAULT_MERGE_LLM_PROFILE_NAME;
      const profile = mergeLlmProfile(llmProfileName);
      if (!profile) {
        return refused("merge", `Unknown LLM profile "${llmProfileName}".`);
      }
      const maxGroups = clampMaxGroups(formValue(form, "max_groups"));
      const timeoutSeconds = clampMergeTimeoutSeconds(
        formNumber(form, "timeout_seconds", DEFAULT_MERGE_TIMEOUT_SECONDS),
      );
      if (intent === "confirm-merge") {
        const keyVariable = dagsterApiKeyVariable(profile.provider);
        const stats = await loadSeCompanyPersonPipelineStats().catch(() => null);
        return confirmed({
          section: "merge",
          title: execute ? "Call the model and write suggestions" : "Preview merge suggestions",
          lines: [
            `Scope: ${describeCompanyScope(companyIds)}.`,
            maxGroups === null
              ? "No limit on how many groups this run considers."
              : `Considers at most ${maxGroups} group${maxGroups === 1 ? "" : "s"}.`,
            execute
              ? `Calls ${profile.model} (${profile.provider}) at ${profile.baseUrl}; the key is read from ${keyVariable} on the Dagster host. Writes suggestions to se_company_person_enrichment_observation.`
              : "No model is called and nothing is written: a bare Materialize with execute off is a harmless preview.",
            stats
              ? `${stats.collisionGroupCount} collision groups exist, ${stats.decidedGroupCount} already decided.`
              : "Collision-candidate counts are unavailable right now; the run reads live state regardless.",
            "Approve/keep-separate decisions are made on each person's review page, never automatically.",
          ],
          fields: {
            company_ids: formatCompanyIdScope(companyIds),
            execute: execute ? "1" : "",
            llm_profile: profile.name,
            max_groups: maxGroups === null ? "" : String(maxGroups),
            timeout_seconds: String(timeoutSeconds),
          },
        });
      }
      return await launch(
        "merge",
        SE_COMPANY_PERSON_MERGE_JOB,
        buildMergeRunConfig({ execute, llmProfile: profile.name, companyIds, maxGroups, timeoutSeconds }),
      );
    }

    return refused(null, "Unknown pipeline action.");
  } catch (error) {
    if (error instanceof DagsterError) {
      const section: PeoplePipelineSection | null = intent.includes("identity")
        ? "identity"
        : intent.includes("resolution")
          ? "resolution"
          : intent.includes("merge")
            ? "merge"
            : null;
      return refused(section, error.message);
    }
    throw error;
  }
}

export default function AdminSePeoplePipeline({ loaderData }: Route.ComponentProps) {
  return <SePeoplePipeline view={loaderData} />;
}
