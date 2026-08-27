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
  SE_COMPANY_PERSON_LLM_SUGGESTIONS_JOB,
  SE_COMPANY_PERSON_MERGE_JOB,
  SE_COMPANY_PERSON_PROMOTION_JOB,
  SE_COMPANY_PERSON_PUBLISH_JOB,
  type DagsterRun,
} from "~/lib/dagster.server";
import {
  buildCleanCopyRunConfig,
  buildIdentityEvaluationRunConfig,
  buildLlmSuggestionsRunConfig,
  buildMergeRunConfig,
  buildPromotionRunConfig,
  loadSeCompanyPersonPipelineStats,
  type SeCompanyPersonPipelineStats,
} from "~/lib/se-company-person-pipeline.server";
import {
  DEFAULT_MAX_COMPANIES,
  DEFAULT_MERGE_LLM_PROFILE_NAME,
  DEFAULT_MERGE_TIMEOUT_SECONDS,
  DEFAULT_MIN_CONFIDENCE,
  DEFAULT_PERSON_LLM_PROFILE_NAME,
  clampCompanyBatchSize,
  clampMaxCompanies,
  clampMaxGroups,
  clampMergeTimeoutSeconds,
  clampMinConfidence,
  clampObservationsPerRequest,
  clampResolutionTimeoutSeconds,
  dagsterApiKeyVariable,
  describeCompanyScope,
  formatCompanyIdScope,
  mergeLlmProfile,
  parseCompanyIdScope,
  personLlmProfile,
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
 * exactly these job names.
 *
 * Resolution used to be one launch (se_company_person_job); it is now THREE --
 * clean copy, LLM suggestions, promote suggestions -- each its own single-asset
 * job (dagster_v3's LLM/promotion split), so there are five run lists here, not
 * three.
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
  cleanCopyRuns: PeoplePipelineRunRow[];
  llmSuggestionsRuns: PeoplePipelineRunRow[];
  promotionRuns: PeoplePipelineRunRow[];
  mergeRuns: PeoplePipelineRunRow[];
  error: string;
}> {
  try {
    const [identityRuns, cleanCopyRuns, llmSuggestionsRuns, promotionRuns, mergeRuns] =
      await Promise.all([
        listRuns({ job: SE_COMPANY_PERSON_IDENTITY_EVALUATION_JOB, limit: RUN_LIMIT }),
        listRuns({ job: SE_COMPANY_PERSON_PUBLISH_JOB, limit: RUN_LIMIT }),
        listRuns({ job: SE_COMPANY_PERSON_LLM_SUGGESTIONS_JOB, limit: RUN_LIMIT }),
        listRuns({ job: SE_COMPANY_PERSON_PROMOTION_JOB, limit: RUN_LIMIT }),
        listRuns({ job: SE_COMPANY_PERSON_MERGE_JOB, limit: RUN_LIMIT }),
      ]);
    return {
      identityRuns: identityRuns.map(toRunRow),
      cleanCopyRuns: cleanCopyRuns.map(toRunRow),
      llmSuggestionsRuns: llmSuggestionsRuns.map(toRunRow),
      promotionRuns: promotionRuns.map(toRunRow),
      mergeRuns: mergeRuns.map(toRunRow),
      error: "",
    };
  } catch (error) {
    if (error instanceof DagsterError) {
      return {
        identityRuns: [],
        cleanCopyRuns: [],
        llmSuggestionsRuns: [],
        promotionRuns: [],
        mergeRuns: [],
        error: error.message,
      };
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
    cleanCopyRuns: dagster.cleanCopyRuns,
    llmSuggestionsRuns: dagster.llmSuggestionsRuns,
    promotionRuns: dagster.promotionRuns,
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

    if (intent === "confirm-clean-copy" || intent === "launch-clean-copy") {
      const companyIds = parseCompanyIdScope(formValue(form, "company_ids"));
      const maxCompanies = clampMaxCompanies(formNumber(form, "max_companies", DEFAULT_MAX_COMPANIES));
      const companyBatchSize = clampCompanyBatchSize(formNumber(form, "company_batch_size", 5_000));
      if (intent === "confirm-clean-copy") {
        return confirmed({
          section: "clean-copy",
          title: "Publish se_company_person (clean copy)",
          lines: [
            `Scope: ${describeCompanyScope(companyIds)}.`,
            `Up to ${maxCompanies} companies per run, ${companyBatchSize} per batch.`,
            "Single-source companies only: a deterministic copy per company, no LLM involved and no LLM parameters accepted. A multi-source company in scope is skipped and counted, never partially processed.",
            "This always writes when launched -- clean copy has no preview mode; it is deterministic and free.",
          ],
          fields: {
            company_ids: formatCompanyIdScope(companyIds),
            max_companies: String(maxCompanies),
            company_batch_size: String(companyBatchSize),
          },
        });
      }
      return await launch(
        "clean-copy",
        SE_COMPANY_PERSON_PUBLISH_JOB,
        buildCleanCopyRunConfig({ companyIds, maxCompanies, companyBatchSize }),
      );
    }

    if (intent === "confirm-llm-suggestions" || intent === "launch-llm-suggestions") {
      const companyIds = parseCompanyIdScope(formValue(form, "company_ids"));
      const execute = formValue(form, "execute") === "1";
      const llmProfileName = formValue(form, "llm_profile") || DEFAULT_PERSON_LLM_PROFILE_NAME;
      const profile = personLlmProfile(llmProfileName);
      if (!profile) {
        return refused("llm-suggestions", `Unknown LLM profile "${llmProfileName}".`);
      }
      const maxCompanies = clampMaxCompanies(formNumber(form, "max_companies", DEFAULT_MAX_COMPANIES));
      const companyBatchSize = clampCompanyBatchSize(formNumber(form, "company_batch_size", 5_000));
      const maximumObservationsPerRequest = clampObservationsPerRequest(
        formNumber(form, "maximum_observations_per_request", 50),
      );
      const timeoutSeconds = clampResolutionTimeoutSeconds(
        formNumber(form, "timeout_seconds", 180),
      );
      if (intent === "confirm-llm-suggestions") {
        const keyVariable = dagsterApiKeyVariable(profile.provider);
        return confirmed({
          section: "llm-suggestions",
          title: execute ? "Call the model and write suggestions" : "Preview LLM suggestions",
          lines: [
            `Scope: ${describeCompanyScope(companyIds)}.`,
            `Up to ${maxCompanies} companies per run, ${companyBatchSize} per batch, ` +
              `up to ${maximumObservationsPerRequest} observations per LLM request, ` +
              `${timeoutSeconds}s timeout.`,
            "Multi-source companies only: this writes suggestions ONLY, to se_company_person_enrichment_observation -- nothing goes live in se_company_person until a Promote suggestions run.",
            execute
              ? `Calls ${profile.model} (${profile.provider}) at ${profile.baseUrl}; the key is read from ${keyVariable} on the Dagster host.`
              : "No model is called and nothing is written: a bare Materialize with execute off is a harmless preview.",
          ],
          fields: {
            company_ids: formatCompanyIdScope(companyIds),
            execute: execute ? "1" : "",
            llm_profile: profile.name,
            max_companies: String(maxCompanies),
            company_batch_size: String(companyBatchSize),
            maximum_observations_per_request: String(maximumObservationsPerRequest),
            timeout_seconds: String(timeoutSeconds),
          },
        });
      }
      return await launch(
        "llm-suggestions",
        SE_COMPANY_PERSON_LLM_SUGGESTIONS_JOB,
        buildLlmSuggestionsRunConfig({
          execute,
          llmProfile: profile.name,
          companyIds,
          maxCompanies,
          companyBatchSize,
          maximumObservationsPerRequest,
          timeoutSeconds,
        }),
      );
    }

    if (intent === "confirm-promotion" || intent === "launch-promotion") {
      const companyIds = parseCompanyIdScope(formValue(form, "company_ids"));
      const maxCompanies = clampMaxCompanies(formNumber(form, "max_companies", DEFAULT_MAX_COMPANIES));
      const companyBatchSize = clampCompanyBatchSize(formNumber(form, "company_batch_size", 5_000));
      const minConfidence = clampMinConfidence(
        Number.parseFloat(formValue(form, "min_confidence")) || DEFAULT_MIN_CONFIDENCE,
      );
      if (intent === "confirm-promotion") {
        return confirmed({
          section: "promotion",
          title: "Promote LLM suggestions",
          lines: [
            `Scope: ${describeCompanyScope(companyIds)}.`,
            `Up to ${maxCompanies} companies per run, ${companyBatchSize} per batch.`,
            minConfidence > 0
              ? `Only promotes a suggestion with confidence >= ${minConfidence}.`
              : "Promotes every live suggestion regardless of the model's own confidence (min_confidence is 0).",
            "Deterministic and free -- no model is ever called here. Copies an eligible stored suggestion into se_company_person, applying the correction ledger on top exactly like clean copy does. A suggestion whose evidence has moved since it was written is skipped and counted, never guessed at.",
          ],
          fields: {
            company_ids: formatCompanyIdScope(companyIds),
            max_companies: String(maxCompanies),
            company_batch_size: String(companyBatchSize),
            min_confidence: String(minConfidence),
          },
        });
      }
      return await launch(
        "promotion",
        SE_COMPANY_PERSON_PROMOTION_JOB,
        buildPromotionRunConfig({ companyIds, maxCompanies, companyBatchSize, minConfidence }),
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
        : intent.includes("clean-copy")
          ? "clean-copy"
          : intent.includes("llm-suggestions")
            ? "llm-suggestions"
            : intent.includes("promotion")
              ? "promotion"
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
