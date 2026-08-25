import { redirect } from "react-router";
import type { Route } from "./+types/admin-se-companies-pipeline";
import type {
  PipelineResult,
  PipelineView,
} from "~/components/admin/se-company-info-pipeline";
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
  SE_COMPANY_INFO_SCHEDULE,
  SE_COMPANY_INFO_SENSOR,
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
  type LaunchIntent,
  type SeCompanyInfoPipelineStats,
} from "~/lib/se-company-info-pipeline.server";
import {
  clampConcurrency,
  clampMaxCompanies,
  dagsterApiKeyVariable,
  describeCompanyScope,
  formatCompanyIdScope,
  isInfoArtifact,
  parseCompanyIdScope,
  PILOT_TAG_KEY,
  PILOT_TAG_VALUE,
  SE_COMPANY_INFO_LIST_PATH,
  type PipelineConfirmation,
} from "~/lib/se-company-info-pipeline";

/**
 * The pipeline, as a RESOURCE route: a loader, an action and no component.
 *
 * It used to be a page of its own. It is now the Pipeline sheet on the
 * companies list, which loads this route with a `useFetcher` when it is opened
 * and posts its launches back to the same fetcher -- which is the whole point
 * of the move: the change-scan aggregations below cost a FINAL read of a 3.5M
 * row table, so they must run when a reviewer asks for them and never as part
 * of the list's own loader.
 *
 * Only `loader` and `action` live here -- any other export that touched
 * `~/lib/*.server` would keep that module in the client bundle and break the
 * production build (see CLAUDE.md).
 */

const RUN_LIMIT = 12;
const nf = new Intl.NumberFormat("en-US");

function refused(error: string): PipelineResult {
  return { kind: "result", ok: false, error, confirmation: null, launched: null };
}

function message(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function formValue(form: FormData, name: string): string {
  const value = form.get(name);
  return typeof value === "string" ? value : "";
}

function formNumber(form: FormData, name: string, fallback: number): number {
  const parsed = Number.parseInt(formValue(form, name), 10);
  return Number.isFinite(parsed) ? parsed : fallback;
}

/**
 * A GET a human typed, rather than the sheet's fetcher asking for data.
 *
 * React Router 8 fetches route data at `<path>.data` (single fetch:
 * `singleFetchUrl` appends the extension, and the server dispatches on exactly
 * this suffix), so the pathname is the framework's own signal. The `Accept`
 * header is read as well and BOTH have to say "document": a navigation always
 * asks for `text/html`, while the client's own `fetch` sets no Accept at all
 * (`createRequestInit` sends only the method and body). Anything ambiguous is
 * therefore served as data -- the failure that leaves the sheet working, rather
 * than redirecting it into a navigation.
 */
function isDocumentRequest(request: Request): boolean {
  const { pathname } = new URL(request.url);
  if (pathname.endsWith(".data")) return false;
  return (request.headers.get("Accept") ?? "").includes("text/html");
}

/** A Dagster outage must not take the sheet down: the stats and the LLM profiles
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
      // This pipeline's two, not the repository's 52 schedules and 15 sensors.
      instigatorStates({ names: [SE_COMPANY_INFO_SCHEDULE, SE_COMPANY_INFO_SENSOR] }),
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

/** ClickHouse is guarded exactly like Dagster is: the sheet's job is to say what
 * it knows and why it does not know the rest, never to 500. */
async function statsView(): Promise<{
  stats: SeCompanyInfoPipelineStats | null;
  error: string;
}> {
  try {
    return { stats: await loadSeCompanyInfoPipelineStats(), error: "" };
  } catch (error) {
    return { stats: null, error: message(error) };
  }
}

function profileFor(profiles: LlmProfile[], profileId: string): LlmProfile | null {
  return profiles.find((profile) => profile.profileId === profileId) ?? null;
}

export async function loader({ request }: Route.LoaderArgs): Promise<PipelineView> {
  // The old page's URL still exists in bookmarks and in the browser history.
  // It is not a page any more, so a person who asks for it lands where the
  // pipeline now lives -- without paying for the change scan on the way.
  if (isDocumentRequest(request)) throw redirect(SE_COMPANY_INFO_LIST_PATH);

  const [statsResult, dagster] = await Promise.all([statsView(), dagsterView()]);
  const profiles = listLlmProfiles();
  return {
    kind: "view",
    stats: statsResult.stats,
    statsError: statsResult.error,
    profiles: profiles.map((profile) => ({
      profileId: profile.profileId,
      name: profile.name,
      provider: profile.provider,
      model: profile.model,
      baseUrl: profile.baseUrl,
      isActive: profile.isActive,
      // The variable the profile page stores, beside the one the Dagster host
      // will actually read. A mismatch means the run fails on the host, so the
      // sheet says so before anything is launched; "" means the provider name
      // cannot produce a readable variable at all.
      apiKeyEnvironmentVariable: profile.apiKeyEnvironmentVariable,
      dagsterApiKeyVariable: dagsterApiKeyVariable(profile.provider) ?? "",
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

export async function action({ request }: Route.ActionArgs): Promise<PipelineResult> {
  const form = await request.formData();
  const intent = formValue(form, "intent");
  const profiles = listLlmProfiles();

  /** The four values a model run is parameterised by, read the same way in the
   * confirm branch and the launch branch -- the launch has to rebuild exactly
   * what was signed, so there is one reader, not two. */
  const modelRun = (pendingModelOnly: boolean) => {
    const useModel = pendingModelOnly || formValue(form, "use_model") === "1";
    const maxCompanies = clampMaxCompanies(formNumber(form, "max_companies", 1_000));
    const concurrency = clampConcurrency(formNumber(form, "concurrency", 1));
    const profile = useModel ? profileFor(profiles, formValue(form, "profile_id")) : null;
    // The companies picked on the list, if any: [] is the ordinary "every
    // changed company" run, and is what the sheet posts when nothing is ticked.
    const companyIds = parseCompanyIdScope(formValue(form, "company_ids"));
    return { useModel, maxCompanies, concurrency, profile, companyIds };
  };

  const modelIntent = (
    pendingModelOnly: boolean,
    run: ReturnType<typeof modelRun>,
  ): LaunchIntent => ({
    job: SE_COMPANY_INFO_REVIEW_JOB,
    runConfig: buildInfoRunConfig({
      maxCompanies: run.maxCompanies,
      useModel: run.useModel,
      pendingModelOnly,
      companyIds: run.companyIds,
      llm: {
        // A model-off run still carries a profile so the run records which model
        // its stored suggestions were hashed against; it is never called.
        provider: run.profile?.provider ?? "deepseek",
        model: run.profile?.model ?? "deepseek-v4-flash",
        baseUrl: run.profile?.baseUrl ?? "https://api.deepseek.com",
        concurrency: run.concurrency,
      },
    }),
  });

  const artifactIntent = (artifact: string): LaunchIntent => ({
    job: SE_COMPANY_INFO_JOB,
    assetSelection: [infoArtifactAsset(artifact as never)],
    runConfig: buildArtifactRunConfig(),
  });

  /**
   * The second step of the two: the operator has seen the numbers a `confirm-*`
   * re-read and posted the same run back. `launchIntent` is rebuilt from those
   * replayed fields rather than carried over, so what starts is what the confirm
   * branch would describe for exactly these fields.
   */
  const launch = async (launchIntent: LaunchIntent): Promise<PipelineResult> => {
    const run = await launchRun({
      job: launchIntent.job,
      ...(launchIntent.assetSelection ? { assetSelection: launchIntent.assetSelection } : {}),
      runConfig: launchIntent.runConfig,
      tags: { [PILOT_TAG_KEY]: PILOT_TAG_VALUE },
    });
    return {
      kind: "result",
      ok: true,
      error: "",
      confirmation: null,
      launched: { runId: run.runId, url: dagsterRunUrl(run.runId), job: launchIntent.job },
    };
  };

  try {
    if (intent === "confirm-resolve" || intent === "confirm-model-pass") {
      const pendingModelOnly = intent === "confirm-model-pass";
      const run = modelRun(pendingModelOnly);
      const { useModel, maxCompanies, concurrency, profile, companyIds } = run;
      if (useModel && !profile) {
        return refused("Choose an LLM profile before launching a run that calls the model.");
      }
      const keyVariable = profile ? dagsterApiKeyVariable(profile.provider) : null;
      if (useModel && profile && keyVariable === null) {
        return refused(
          `Provider "${profile.provider}" does not name an environment variable the Dagster ` +
            "host can read a key from. Fix the provider name in LLM settings.",
        );
      }
      // The numbers are re-read here rather than taken from the sheet the operator
      // is looking at: a confirmation must restate what is true now, not what was
      // true when it was opened. For a SCOPED run this read is a liveness gate
      // and nothing more -- it proves ClickHouse can still answer before a paid
      // run is offered; the scoped lines below are built from the picked ids,
      // never from these unscoped counts.
      const statsResult = await statsView();
      if (!statsResult.stats) {
        return refused(`The selection counts are unavailable, so nothing is confirmed: ${statsResult.error}`);
      }
      const { selection } = statsResult.stats;
      const selected = pendingModelOnly
        ? selection.pendingModelCount
        : useModel
          ? selection.changedCount
          : selection.changedWithoutModelCount;
      const bounded = Math.min(selected, maxCompanies);
      const calls = pendingModelOnly ? bounded : Math.min(selection.wouldCallModelCount, bounded);
      // A scoped run's size is NOT the scan's total: the counts describe every
      // changed company, and the scope narrows the scan to the picked ids while
      // still applying the same change predicate to them. Saying "1,240 match"
      // for a two-company pick would be a lie, so the scoped line says what the
      // scope is and what it does not promise.
      const scoped = companyIds.length > 0;
      const scopeLine = scoped
        ? `Scoped to ${describeCompanyScope(companyIds)}; of those, only the ones the change scan still selects are resolved. This run stops after ${nf.format(maxCompanies)}.`
        : `${nf.format(selected)} companies match right now; this run stops after ${nf.format(maxCompanies)}, so it will resolve ${nf.format(bounded)}.`;
      const modelLine = useModel
        ? scoped
          ? `Those of them with several description sources enter the model step, ${concurrency} call${concurrency === 1 ? "" : "s"} at a time. Answers already stored for the same request are reused and cost nothing.`
          : `Up to ${nf.format(calls)} of them enter the model step, ${concurrency} call${concurrency === 1 ? "" : "s"} at a time. Answers already stored for the same request are reused and cost nothing.`
        : "The model is switched off: multi-source companies publish their deterministic pick and no call is made.";
      const confirmation: PipelineConfirmation = {
        intent: pendingModelOnly ? "launch-model-pass" : "launch-resolve",
        title: pendingModelOnly ? "Run the model pass" : "Re-resolve changed companies",
        lines: [
          scopeLine,
          modelLine,
          useModel && profile
            ? `Model ${profile.model} (${profile.provider}) at ${profile.baseUrl}; the key comes from ${keyVariable} on the Dagster host.`
            : "No model is called.",
          "The run writes to corpscout.se_company_info and, when the model answers, to se_company_info_enrichment_observation.",
        ],
        fields: {
          use_model: useModel ? "1" : "",
          max_companies: String(maxCompanies),
          concurrency: String(concurrency),
          profile_id: profile?.profileId ?? "",
          // Replayed as ONE field, normalised the same way the launch will parse
          // it back: what starts is scoped to exactly the ids just described.
          company_ids: formatCompanyIdScope(companyIds),
        },
      };
      return { kind: "result", ok: true, error: "", confirmation, launched: null };
    }

    if (intent === "confirm-artifact") {
      const artifact = formValue(form, "artifact");
      if (!isInfoArtifact(artifact)) return refused("Choose an artifact to refresh.");
      const confirmation: PipelineConfirmation = {
        intent: "launch-artifact",
        title: `Refresh the ${artifact} artifact`,
        lines: [
          `Runs ${infoArtifactAsset(artifact)} on ${SE_COMPANY_INFO_JOB} and nothing else.`,
          "The whole source is re-read: an artifact refresh takes no company scope, so what is picked on the list makes no difference to it.",
          "New evidence appended here makes those companies changed, so the next resolve run picks them up.",
          "No model is called and se_company_info is not written by this run.",
        ],
        fields: { artifact },
      };
      return { kind: "result", ok: true, error: "", confirmation, launched: null };
    }

    if (intent === "launch-resolve" || intent === "launch-model-pass") {
      const pendingModelOnly = intent === "launch-model-pass";
      const run = modelRun(pendingModelOnly);
      if (run.useModel && !run.profile) {
        return refused("Choose an LLM profile before launching a run that calls the model.");
      }
      return await launch(modelIntent(pendingModelOnly, run));
    }

    if (intent === "launch-artifact") {
      const artifact = formValue(form, "artifact");
      if (!isInfoArtifact(artifact)) return refused("Choose an artifact to refresh.");
      return await launch(artifactIntent(artifact));
    }

    return refused("Unknown pipeline action.");
  } catch (error) {
    // A Dagster that refuses or cannot be reached is an answer in the sheet, not
    // a crash; nothing was started either way.
    if (error instanceof DagsterError) {
      return refused(error.message);
    }
    throw error;
  }
}
