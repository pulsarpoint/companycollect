/**
 * Run-config builders and light stats for the People pipeline page -- the
 * server-only half of `se-company-person-pipeline.ts`'s split. Each builder
 * is a PORT of the matching dagster_v3 op config's field names/shape
 * (`company_people/identity_eval.py`'s SECompanyPersonIdentityEvaluationConfig,
 * `normalization.py`'s SECompanyPersonConfig +
 * `roles.py`'s SECompanyPersonRoleConfig, `merge.py`'s
 * SECompanyPersonMergeConfig) -- op keys and field names must match exactly,
 * or Dagster rejects the run config as invalid before anything runs.
 */
import { chQuery } from "~/lib/clickhouse.server";
import {
  SE_COMPANY_PERSON_ASSET,
  SE_COMPANY_PERSON_IDENTITY_EVALUATION_ASSET,
  SE_COMPANY_PERSON_MERGE_ASSET,
  SE_COMPANY_PERSON_ROLE_ASSET,
  SE_COMPANY_PERSON_ROLE_DRAFT_ASSET,
} from "~/lib/dagster.server";
import {
  clampCompanyBatchSize,
  clampMaxCompanies,
  clampMergeTimeoutSeconds,
  clampObservationsPerRequest,
  clampResolutionTimeoutSeconds,
  normalizeCompanyIdScope,
} from "~/lib/se-company-person-pipeline";

export interface IdentityEvaluationRunOptions {
  companyIds: readonly string[];
  writeCandidates: boolean;
}

/** `se_company_person_identity_evaluation_job` -- one asset, one op key. */
export function buildIdentityEvaluationRunConfig(
  options: IdentityEvaluationRunOptions,
): Record<string, unknown> {
  return {
    ops: {
      [SE_COMPANY_PERSON_IDENTITY_EVALUATION_ASSET]: {
        config: {
          company_ids: normalizeCompanyIdScope(options.companyIds),
          write_candidates: options.writeCandidates,
        },
      },
    },
  };
}

export interface ResolutionRunOptions {
  companyIds: readonly string[];
  maxCompanies: number;
  companyBatchSize: number;
  maximumObservationsPerRequest: number;
  timeoutSeconds: number;
}

/** `se_company_person_job` selects three ops
 * (se_company_person_role_draft_clickhouse, se_company_person_clickhouse,
 * se_company_person_role_clickhouse); every op takes a company_ids scope,
 * and only the middle one takes the numeric bounds. */
export function buildResolutionRunConfig(
  options: ResolutionRunOptions,
): Record<string, unknown> {
  const companyIds = normalizeCompanyIdScope(options.companyIds);
  return {
    ops: {
      [SE_COMPANY_PERSON_ROLE_DRAFT_ASSET]: { config: { company_ids: companyIds } },
      [SE_COMPANY_PERSON_ASSET]: {
        config: {
          company_ids: companyIds,
          max_companies: clampMaxCompanies(options.maxCompanies),
          company_batch_size: clampCompanyBatchSize(options.companyBatchSize),
          maximum_observations_per_request: clampObservationsPerRequest(
            options.maximumObservationsPerRequest,
          ),
          timeout_seconds: clampResolutionTimeoutSeconds(options.timeoutSeconds),
        },
      },
      [SE_COMPANY_PERSON_ROLE_ASSET]: { config: { company_ids: companyIds } },
    },
  };
}

export interface MergeRunOptions {
  execute: boolean;
  llmProfile: string;
  companyIds: readonly string[];
  /** null -- omit the key entirely, so Dagster falls back to its own
   * `max_groups: int | None = None` default (no limit). */
  maxGroups: number | null;
  timeoutSeconds: number;
}

/** `se_company_person_merge_job` -- one asset, one op key. `max_groups` is
 * omitted rather than sent as `null` when unset: Dagster's own config default
 * already means "no limit", so there is nothing this run needs to say. */
export function buildMergeRunConfig(options: MergeRunOptions): Record<string, unknown> {
  const config: Record<string, unknown> = {
    execute: options.execute,
    llm_profile: options.llmProfile,
    company_ids: normalizeCompanyIdScope(options.companyIds),
    timeout_seconds: clampMergeTimeoutSeconds(options.timeoutSeconds),
  };
  if (options.maxGroups !== null) {
    config.max_groups = options.maxGroups;
  }
  return { ops: { [SE_COMPANY_PERSON_MERGE_ASSET]: { config } } };
}

/* -------------------------------------------------------------------- */
/* Light stats for the confirm step -- cheap counts, not a full change   */
/* scan (unlike se-company-info-pipeline.server.ts's 3.5M-row read, the   */
/* people tables are small enough that this is affordable per page load, */
/* not just per deliberate opening).                                     */
/* -------------------------------------------------------------------- */

export interface SeCompanyPersonPipelineStats {
  publishedPersonCount: number;
  collisionGroupCount: number;
  decidedGroupCount: number;
}

const PUBLISHED_PERSON_COUNT_SQL = `SELECT toString(count()) AS person_count
FROM corpscout.se_company_person FINAL
WHERE merged_into_person_id IS NULL`;

const COLLISION_GROUP_COUNT_SQL = `SELECT toString(count()) AS group_count
FROM (
  SELECT DISTINCT company_id, candidate_group_id
  FROM corpscout.se_company_person_collision_candidate
)`;

/** Mirrors merge.py's build_decided_candidate_group_ids_sql, unscoped (every
 * company) -- the same decided/undo-excluded shape, counted rather than
 * listed. */
const DECIDED_GROUP_COUNT_SQL = `SELECT toString(count()) AS decided_count
FROM (
  SELECT DISTINCT company_id, JSONExtractString(payload, 'candidate_group_id') AS candidate_group_id
  FROM corpscout.se_company_person_correction
  WHERE correction_kind IN ('merge_persons', 'keep_separate')
    AND JSONExtractString(payload, 'candidate_group_id') != ''
    AND correction_id NOT IN (
      SELECT supersedes_correction_id
      FROM corpscout.se_company_person_correction
      WHERE supersedes_correction_id IS NOT NULL
    )
)`;

export async function loadSeCompanyPersonPipelineStats(): Promise<SeCompanyPersonPipelineStats> {
  const [[published], [groups], [decided]] = await Promise.all([
    chQuery<{ person_count: string }>(PUBLISHED_PERSON_COUNT_SQL),
    chQuery<{ group_count: string }>(COLLISION_GROUP_COUNT_SQL),
    chQuery<{ decided_count: string }>(DECIDED_GROUP_COUNT_SQL),
  ]);
  return {
    publishedPersonCount: Number(published?.person_count ?? 0),
    collisionGroupCount: Number(groups?.group_count ?? 0),
    decidedGroupCount: Number(decided?.decided_count ?? 0),
  };
}
