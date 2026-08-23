import type { Route } from "./+types/admin-se-company-info-pipeline";
import { SeCompanyInfoPipeline } from "~/components/admin/se-company-info-pipeline";
import {
  assetMaterializations,
  DagsterError,
  dagsterRunUrl,
  instigatorStates,
  launchRun,
  listRuns,
  SE_COMPANY_INFO_ASSET,
  SE_COMPANY_INFO_JOB,
  SE_COMPANY_INFO_REVIEW_JOB,
  type AssetMaterialization,
  type DagsterRun,
  type InstigatorStates,
} from "~/lib/dagster.server";
import { listLlmProfiles, type LlmProfile } from "~/lib/llm-settings.server";
import {
  buildArtifactRunConfig,
  buildInfoRunConfig,
  infoArtifactAsset,
  loadSeCompanyInfoPipelineStats,
} from "~/lib/se-company-info-pipeline.server";
import {
  clampConcurrency,
  dagsterApiKeyVariable,
  isInfoArtifact,
  PILOT_TAG_KEY,
  PILOT_TAG_VALUE,
  type PipelineConfirmation,
} from "~/lib/se-company-info-pipeline";

// Only `loader`, `action`, `meta` and the component live here -- any other export
// that touched `~/lib/*.server` would keep that module in the client bundle and
// break the production build (see CLAUDE.md).

const RUN_LIMIT = 12;
const nf = new Intl.NumberFormat("en-US");

function formValue(form: FormData, name: string): string {
  const value = form.get(name);
  return typeof value === "string" ? value : "";
}

function formNumber(form: FormData, name: string, fallback: number): number {
  const parsed = Number.parseInt(formValue(form, name), 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

/** A Dagster outage must not take the page down: the stats and the LLM profiles
 * come from ClickHouse and SQLite and are still worth showing. */
async function dagsterView(): Promise<{
  runs: DagsterRun[];
  materializations: AssetMaterialization[];
  instigators: InstigatorStates | null;
  error: string;
}> {
  try {
    const [jobRuns, reviewRuns, materializations, instigators] = await Promise.all([
      listRuns({ job: SE_COMPANY_INFO_JOB, limit: RUN_LIMIT }),
      listRuns({ job: SE_COMPANY_INFO_REVIEW_JOB, limit: RUN_LIMIT }),
      assetMaterializations({ asset: SE_COMPANY_INFO_ASSET, limit: RUN_LIMIT * 2 }),
      instigatorStates(),
    ]);
    const runs = [...jobRuns, ...reviewRuns]
      .sort((a, b) => (b.startTime ?? 0) - (a.startTime ?? 0))
      .slice(0, RUN_LIMIT);
    return { runs, materializations, instigators, error: "" };
  } catch (error) {
    if (error instanceof DagsterError) {
      return { runs: [], materializations: [], instigators: null, error: error.message };
    }
    throw error;
  }
}

function profileFor(profiles: LlmProfile[], profileId: string): LlmProfile | null {
  return profiles.find((profile) => profile.profileId === profileId) ?? null;
}

export async function loader() {
  const [stats, dagster] = await Promise.all([
    loadSeCompanyInfoPipelineStats(),
    dagsterView(),
  ]);
  const profiles = listLlmProfiles();
  return {
    stats,
    profiles: profiles.map((profile) => ({
      profileId: profile.profileId,
      name: profile.name,
      provider: profile.provider,
      model: profile.model,
      baseUrl: profile.baseUrl,
      isActive: profile.isActive,
      // The variable the profile page stores, beside the one the Dagster host
      // will actually read. A mismatch means the run fails on the host, so the
      // page says so before anything is launched.
      apiKeyEnvironmentVariable: profile.apiKeyEnvironmentVariable,
      dagsterApiKeyVariable: dagsterApiKeyVariable(profile.provider),
    })),
    runs: dagster.runs.map((run) => ({
      ...run,
      url: dagsterRunUrl(run.runId),
      numbers:
        dagster.materializations.find((entry) => entry.runId === run.runId)?.numbers ?? {},
    })),
    instigators: dagster.instigators,
    dagsterError: dagster.error,
  };
}

export async function action({ request }: Route.ActionArgs) {
  const form = await request.formData();
  const intent = formValue(form, "intent");
  const profiles = listLlmProfiles();

  const modelRun = (pendingModelOnly: boolean) => {
    const useModel = pendingModelOnly || formValue(form, "use_model") === "1";
    const maxCompanies = formNumber(form, "max_companies", 1_000);
    const concurrency = clampConcurrency(Number.parseInt(formValue(form, "concurrency"), 10));
    const profile = useModel ? profileFor(profiles, formValue(form, "profile_id")) : null;
    return { useModel, maxCompanies, concurrency, profile };
  };

  try {
    if (intent === "confirm-resolve" || intent === "confirm-model-pass") {
      const pendingModelOnly = intent === "confirm-model-pass";
      const { useModel, maxCompanies, concurrency, profile } = modelRun(pendingModelOnly);
      if (useModel && !profile) {
        return { error: "Choose an LLM profile before launching a run that calls the model.", confirmation: null, launched: null };
      }
      // The numbers are re-read here rather than taken from the page the operator
      // is looking at: a confirmation must restate what is true now, not what was
      // true when the tab was opened.
      const { selection } = await loadSeCompanyInfoPipelineStats();
      const selected = pendingModelOnly
        ? selection.pendingModelCount
        : useModel
          ? selection.changedCount
          : selection.changedWithoutModelCount;
      const bounded = Math.min(selected, maxCompanies);
      const calls = pendingModelOnly ? bounded : Math.min(selection.wouldCallModelCount, bounded);
      const confirmation: PipelineConfirmation = {
        intent: pendingModelOnly ? "launch-model-pass" : "launch-resolve",
        title: pendingModelOnly ? "Run the model pass" : "Re-resolve changed companies",
        lines: [
          `${nf.format(selected)} companies match right now; this run stops after ${nf.format(maxCompanies)}, so it will resolve ${nf.format(bounded)}.`,
          useModel
            ? `Up to ${nf.format(calls)} of them enter the model step, ${concurrency} call${concurrency === 1 ? "" : "s"} at a time. Answers already stored for the same request are reused and cost nothing.`
            : "The model is switched off: multi-source companies publish their deterministic pick and no call is made.",
          useModel && profile
            ? `Model ${profile.model} (${profile.provider}) at ${profile.baseUrl}; the key comes from ${dagsterApiKeyVariable(profile.provider)} on the Dagster host.`
            : "No model profile is sent.",
          "The run writes to corpscout.se_company_info and, when the model answers, to se_company_info_enrichment_observation.",
        ],
        fields: {
          use_model: useModel ? "1" : "",
          max_companies: String(maxCompanies),
          concurrency: String(concurrency),
          profile_id: profile?.profileId ?? "",
        },
      };
      return { error: "", confirmation, launched: null };
    }

    if (intent === "confirm-artifact") {
      const artifact = formValue(form, "artifact");
      if (!isInfoArtifact(artifact)) {
        return { error: "Choose an artifact to refresh.", confirmation: null, launched: null };
      }
      const confirmation: PipelineConfirmation = {
        intent: "launch-artifact",
        title: `Refresh the ${artifact} artifact`,
        lines: [
          `Runs ${infoArtifactAsset(artifact)} on ${SE_COMPANY_INFO_JOB} and nothing else.`,
          "New evidence appended here makes those companies changed, so the next resolve run picks them up.",
          "No model is called and se_company_info is not written by this run.",
        ],
        fields: { artifact },
      };
      return { error: "", confirmation, launched: null };
    }

    if (intent === "launch-resolve" || intent === "launch-model-pass") {
      const pendingModelOnly = intent === "launch-model-pass";
      const { useModel, maxCompanies, concurrency, profile } = modelRun(pendingModelOnly);
      if (useModel && !profile) {
        return { error: "Choose an LLM profile before launching a run that calls the model.", confirmation: null, launched: null };
      }
      const run = await launchRun({
        job: SE_COMPANY_INFO_REVIEW_JOB,
        runConfig: buildInfoRunConfig({
          maxCompanies,
          useModel,
          pendingModelOnly,
          llm: {
            // A model-off run still carries a profile so the run records which
            // model its stored suggestions were hashed against; it is never called.
            provider: profile?.provider ?? "deepseek",
            model: profile?.model ?? "deepseek-v4-flash",
            baseUrl: profile?.baseUrl ?? "https://api.deepseek.com",
            concurrency,
          },
        }),
        tags: { [PILOT_TAG_KEY]: PILOT_TAG_VALUE },
      });
      return {
        error: "",
        confirmation: null,
        launched: { runId: run.runId, url: dagsterRunUrl(run.runId), job: SE_COMPANY_INFO_REVIEW_JOB },
      };
    }

    if (intent === "launch-artifact") {
      const artifact = formValue(form, "artifact");
      if (!isInfoArtifact(artifact)) {
        return { error: "Choose an artifact to refresh.", confirmation: null, launched: null };
      }
      const run = await launchRun({
        job: SE_COMPANY_INFO_JOB,
        assetSelection: [infoArtifactAsset(artifact)],
        runConfig: buildArtifactRunConfig(),
        tags: { [PILOT_TAG_KEY]: PILOT_TAG_VALUE },
      });
      return {
        error: "",
        confirmation: null,
        launched: { runId: run.runId, url: dagsterRunUrl(run.runId), job: SE_COMPANY_INFO_JOB },
      };
    }

    return { error: "Unknown pipeline action.", confirmation: null, launched: null };
  } catch (error) {
    if (error instanceof DagsterError) {
      return { error: error.message, confirmation: null, launched: null };
    }
    throw error;
  }
}

export function meta() {
  return [{ title: "Company info pipeline | CompanyCollect" }];
}

export default function AdminSeCompanyInfoPipeline({
  loaderData,
  actionData,
}: Route.ComponentProps) {
  return (
    <SeCompanyInfoPipeline
      stats={loaderData.stats}
      profiles={loaderData.profiles}
      runs={loaderData.runs}
      instigators={loaderData.instigators}
      dagsterError={loaderData.dagsterError}
      confirmation={actionData?.confirmation ?? null}
      launched={actionData?.launched ?? null}
      error={actionData?.error ?? ""}
    />
  );
}
