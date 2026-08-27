/**
 * The People page's Tasks tab: latest-run stats for every people asset/job,
 * over Dagster's GraphQL API (`dagster.server.ts` -- extended with two job
 * constants for this tab, not forked; every query here is `listRuns` /
 * `assetMaterializations`, both already used by the Pipeline page).
 *
 * Guarded like `admin-se-people-pipeline.tsx`'s own `dagsterView()`: Dagster
 * unreachable degrades this tab to an error banner over an empty table, never
 * a thrown error that would take the whole page down.
 */
import {
  DagsterError,
  dagsterRunUrl,
  listRuns,
  assetMaterializations,
  SE_COMPANY_PERSON_ASSET,
  SE_COMPANY_PERSON_IDENTITY_EVALUATION_ASSET,
  SE_COMPANY_PERSON_IDENTITY_EVALUATION_JOB,
  SE_COMPANY_PERSON_LLM_SUGGESTIONS_ASSET,
  SE_COMPANY_PERSON_LLM_SUGGESTIONS_JOB,
  SE_COMPANY_PERSON_MERGE_ASSET,
  SE_COMPANY_PERSON_MERGE_JOB,
  SE_COMPANY_PERSON_PROMOTION_ASSET,
  SE_COMPANY_PERSON_PROMOTION_JOB,
  SE_COMPANY_PERSON_PUBLISH_JOB,
  SE_COMPANY_PERSON_REVIEW_JOB,
  SE_COMPANY_PERSON_ROLE_ASSET,
  SE_COMPANY_PERSON_ROLE_JOB,
  type DagsterRun,
} from "~/lib/dagster.server";

/**
 * One row per asset/job the spec names. `metricKeys` picks the 1-2 int
 * metadata entries (from each asset's own `dg.MaterializeResult(metadata=...)`,
 * `normalization.py`/`identity_eval.py`/`merge.py`/`roles.py`) worth showing
 * beside status/timing -- not every numeric key an asset happens to log, just
 * the ones that answer "did this do anything". `asset` is omitted for the
 * review job: it selects FOUR assets in one run
 * (`corrections.py`'s `REVIEW_ASSET_NAMES`), so no single materialization
 * speaks for the whole run -- status/timing only there, per the task's own
 * "if metadata extraction is disproportionate, status+timing suffices" out.
 */
interface SePeopleTaskSpec {
  key: string;
  label: string;
  job: string;
  asset?: string;
  metricKeys?: readonly string[];
}

const TASK_SPECS: readonly SePeopleTaskSpec[] = [
  {
    key: "clean-copy",
    label: "Clean copy (se_company_person_clickhouse)",
    job: SE_COMPANY_PERSON_PUBLISH_JOB,
    asset: SE_COMPANY_PERSON_ASSET,
    metricKeys: ["inserted_count", "total_person_count"],
  },
  {
    key: "llm-suggestions",
    label: "se_company_person_llm_suggestions",
    job: SE_COMPANY_PERSON_LLM_SUGGESTIONS_JOB,
    asset: SE_COMPANY_PERSON_LLM_SUGGESTIONS_ASSET,
    metricKeys: ["suggestion_inserted_count", "would_call_model"],
  },
  {
    key: "promotion",
    label: "se_company_person_promotion",
    job: SE_COMPANY_PERSON_PROMOTION_JOB,
    asset: SE_COMPANY_PERSON_PROMOTION_ASSET,
    metricKeys: ["inserted_count", "total_person_count"],
  },
  {
    key: "identity-evaluation",
    label: "se_company_person_identity_evaluation",
    job: SE_COMPANY_PERSON_IDENTITY_EVALUATION_JOB,
    asset: SE_COMPANY_PERSON_IDENTITY_EVALUATION_ASSET,
    metricKeys: ["collision_candidate_count", "excluded_blank_name_count"],
  },
  {
    key: "merge-suggestions",
    label: "se_company_person_merge_suggestions",
    job: SE_COMPANY_PERSON_MERGE_JOB,
    asset: SE_COMPANY_PERSON_MERGE_ASSET,
    metricKeys: ["suggestion_inserted_count", "decided_group_count"],
  },
  {
    key: "roles",
    label: "Roles (se_company_person_role_job)",
    job: SE_COMPANY_PERSON_ROLE_JOB,
    asset: SE_COMPANY_PERSON_ROLE_ASSET,
    metricKeys: ["inserted_role_count", "total_current_role_count"],
  },
  {
    key: "review",
    label: "Review (se_company_person_review_job)",
    job: SE_COMPANY_PERSON_REVIEW_JOB,
    // No `asset`: this job launches four assets per run (clean copy, role
    // draft, role, promotion) -- see the module doc comment above.
  },
];

export interface SePeopleTaskRow {
  key: string;
  label: string;
  job: string;
  runId: string | null;
  status: string | null;
  url: string | null;
  startTime: number | null;
  endTime: number | null;
  metrics: Record<string, number>;
}

async function loadTaskRow(spec: SePeopleTaskSpec): Promise<SePeopleTaskRow> {
  const runs = await listRuns({ job: spec.job, limit: 1 });
  const run: DagsterRun | undefined = runs[0];
  let metrics: Record<string, number> = {};
  if (spec.asset && run && run.status === "SUCCESS") {
    const materializations = await assetMaterializations({ asset: spec.asset, limit: 1 });
    const latest = materializations[0];
    if (latest) {
      metrics = Object.fromEntries(
        (spec.metricKeys ?? [])
          .filter((key) => latest.numbers[key] !== undefined)
          .map((key) => [key, latest.numbers[key]]),
      );
    }
  }
  return {
    key: spec.key,
    label: spec.label,
    job: spec.job,
    runId: run?.runId ?? null,
    status: run?.status ?? null,
    url: run ? dagsterRunUrl(run.runId) : null,
    startTime: run?.startTime ?? null,
    endTime: run?.endTime ?? null,
    metrics,
  };
}

export interface SePeopleTasksView {
  rows: SePeopleTaskRow[];
  error: string;
}

/** One row per `TASK_SPECS` entry, or an empty table plus an error message
 * when Dagster cannot be reached at all -- mirrors
 * `admin-se-people-pipeline.tsx`'s `dagsterView()` guarded-read shape. */
export async function loadSePeopleTasks(): Promise<SePeopleTasksView> {
  try {
    const rows = await Promise.all(TASK_SPECS.map(loadTaskRow));
    return { rows, error: "" };
  } catch (error) {
    if (error instanceof DagsterError) {
      return { rows: [], error: error.message };
    }
    throw error;
  }
}
