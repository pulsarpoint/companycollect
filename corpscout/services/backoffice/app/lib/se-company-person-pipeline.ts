/**
 * The client-safe half of the People pipeline page: config bounds, the
 * company-id scope encoding, the merge job's LLM profile choices and the
 * confirmation-panel shape. Mirrors `se-company-info-pipeline.ts`'s split
 * (see that file's own docstring) -- deliberately free of Dagster job/asset
 * names and anything that reaches ClickHouse, so those stay out of the
 * client bundle (see backoffice/CLAUDE.md's route-module rule).
 *
 * Small, deliberate duplication of se-company-info-pipeline.ts's scope/clamp
 * helpers rather than a cross-import: the two pipelines' bounds differ
 * (different Pydantic Field(ge=..., le=...) ranges on the Dagster side) and
 * this repo's own convention (see company_people/merge.py's module
 * docstring) is to replicate a small helper locally rather than couple two
 * otherwise-independent domains for a few lines of code.
 */

export const PILOT_TAG_KEY = "pilot";
export const PILOT_TAG_VALUE = "backoffice";

/**
 * Named LLM profiles the merge job's `llm_profile: str` config selects from
 * -- mirrors dagster_v3's `company_people/merge.py`
 * `MERGE_LLM_PROFILES`/`DEFAULT_MERGE_LLM_PROFILE_NAME` exactly (today: one
 * entry). This is NOT the backoffice's own LLM-settings profile table
 * (`~/lib/llm-settings.server`) -- the merge job resolves its profile by NAME
 * on the Dagster host, from a small Python dict, not from a UUID the
 * backoffice manages. Adding a second provider there is a pure data
 * addition (Task 4's report, concern 4); this list must be kept in sync by
 * hand when that happens.
 */
export interface MergeLlmProfileOption {
  name: string;
  provider: string;
  model: string;
  baseUrl: string;
}

export const MERGE_LLM_PROFILES: readonly MergeLlmProfileOption[] = [
  {
    name: "deepseek-default",
    provider: "deepseek",
    model: "deepseek-v4-flash",
    baseUrl: "https://api.deepseek.com",
  },
];

export const DEFAULT_MERGE_LLM_PROFILE_NAME = "deepseek-default";

export function mergeLlmProfile(name: string): MergeLlmProfileOption | null {
  return MERGE_LLM_PROFILES.find((profile) => profile.name === name) ?? null;
}

/**
 * Named LLM profiles the LLM-suggestions job's `llm_profile: str` config selects
 * from -- mirrors dagster_v3's `company_people/normalization.py`
 * `PERSON_LLM_PROFILES`/`DEFAULT_PERSON_LLM_PROFILE_NAME`. A SEPARATE hand-mirrored
 * list from MERGE_LLM_PROFILES above (today: identical values) -- the two
 * Dagster-side registries are themselves separate dicts that may diverge
 * independently (e.g. max_tokens), so this list is not reused for the merge job's
 * picker; keep both in sync by hand when either changes.
 */
export const PERSON_LLM_PROFILES: readonly MergeLlmProfileOption[] = [
  {
    name: "deepseek-default",
    provider: "deepseek",
    model: "deepseek-v4-flash",
    baseUrl: "https://api.deepseek.com",
  },
];

export const DEFAULT_PERSON_LLM_PROFILE_NAME = "deepseek-default";

export function personLlmProfile(name: string): MergeLlmProfileOption | null {
  return PERSON_LLM_PROFILES.find((profile) => profile.name === name) ?? null;
}

/** Same normalization rule as dagster_v3's `llm_api_key_variable`: purely
 * descriptive here (the key itself is read on the Dagster host, never here),
 * so the confirm step can say which variable a run will need. */
export function dagsterApiKeyVariable(provider: string): string {
  return `${provider.toUpperCase()}_API_KEY`;
}

/** Mirrors identity_eval.py's SECompanyPersonIdentityEvaluationConfig -- no
 * numeric bounds, just a company scope and a boolean. */
export const IDENTITY_EVALUATION_DEFAULT_WRITE_CANDIDATES = true;

/**
 * Mirrors normalization.py's SECompanyPersonConfig field bounds
 * (`max_companies: int = Field(default=1_000_000, ge=1, le=1_000_000)`) --
 * except the DEFAULT, which is deliberately NOT Dagster's own 1,000,000.
 * Resolution is a paid DeepSeek path for every multi-source company in
 * scope, with no preview/cost gate on the asset itself (controller ruling,
 * SE People Experiment Task 5 review round 1: this repo has a prior
 * incident of an accidental full-corpus LLM spend). Prefilling the UI's
 * bound instead of its own conservative default would make an
 * accidental full-corpus run the path of least resistance again; a
 * full-corpus run now requires deliberately typing a bigger number. The
 * upper BOUND stays 1,000,000 -- Dagster would reject anything past it
 * regardless of what this page prefills.
 */
export const MIN_MAX_COMPANIES = 1;
export const MAX_MAX_COMPANIES = 1_000_000;
export const DEFAULT_MAX_COMPANIES = 10_000;

export const MIN_COMPANY_BATCH_SIZE = 1;
export const MAX_COMPANY_BATCH_SIZE = 25_000;
export const DEFAULT_COMPANY_BATCH_SIZE = 5_000;

export const MIN_OBSERVATIONS_PER_REQUEST = 1;
export const MAX_OBSERVATIONS_PER_REQUEST = 500;
export const DEFAULT_OBSERVATIONS_PER_REQUEST = 50;

export const MIN_RESOLUTION_TIMEOUT_SECONDS = 1;
export const MAX_RESOLUTION_TIMEOUT_SECONDS = 600;
export const DEFAULT_RESOLUTION_TIMEOUT_SECONDS = 180;

/** Mirrors merge.py's SECompanyPersonMergeConfig field bounds. */
export const MIN_MAX_GROUPS = 1;
export const MIN_MERGE_TIMEOUT_SECONDS = 1;
export const MAX_MERGE_TIMEOUT_SECONDS = 600;
export const DEFAULT_MERGE_TIMEOUT_SECONDS = 60;

/** Mirrors normalization.py's SECompanyPersonPromotionConfig.min_confidence
 * (Field(default=0.0, ge=0, le=1)) -- 0.0 promotes every live suggestion
 * regardless of the model's own confidence. */
export const MIN_CONFIDENCE = 0;
export const MAX_CONFIDENCE = 1;
export const DEFAULT_MIN_CONFIDENCE = 0;

function clamp(value: number, low: number, high: number, fallback: number): number {
  if (!Number.isFinite(value)) return fallback;
  return Math.min(high, Math.max(low, Math.trunc(value)));
}

/** Unlike clamp() above, min_confidence is a FRACTION (0..1), not an integer count --
 * Math.trunc would floor 0.7 to 0. */
export function clampMinConfidence(value: number): number {
  if (!Number.isFinite(value)) return DEFAULT_MIN_CONFIDENCE;
  return Math.min(MAX_CONFIDENCE, Math.max(MIN_CONFIDENCE, value));
}

export function clampMaxCompanies(value: number): number {
  return clamp(value, MIN_MAX_COMPANIES, MAX_MAX_COMPANIES, DEFAULT_MAX_COMPANIES);
}

export function clampCompanyBatchSize(value: number): number {
  return clamp(
    value,
    MIN_COMPANY_BATCH_SIZE,
    MAX_COMPANY_BATCH_SIZE,
    DEFAULT_COMPANY_BATCH_SIZE,
  );
}

export function clampObservationsPerRequest(value: number): number {
  return clamp(
    value,
    MIN_OBSERVATIONS_PER_REQUEST,
    MAX_OBSERVATIONS_PER_REQUEST,
    DEFAULT_OBSERVATIONS_PER_REQUEST,
  );
}

export function clampResolutionTimeoutSeconds(value: number): number {
  return clamp(
    value,
    MIN_RESOLUTION_TIMEOUT_SECONDS,
    MAX_RESOLUTION_TIMEOUT_SECONDS,
    DEFAULT_RESOLUTION_TIMEOUT_SECONDS,
  );
}

export function clampMergeTimeoutSeconds(value: number): number {
  return clamp(
    value,
    MIN_MERGE_TIMEOUT_SECONDS,
    MAX_MERGE_TIMEOUT_SECONDS,
    DEFAULT_MERGE_TIMEOUT_SECONDS,
  );
}

/** null (not clamped to a floor) means "no limit" -- Dagster's own
 * `max_groups: int | None = Field(default=None, ge=1)` accepts the field
 * being entirely absent as "no limit"; a bad positive-but-too-small typed
 * value still gets floored to the minimum rather than silently becoming
 * "no limit". */
export function clampMaxGroups(raw: string): number | null {
  const trimmed = raw.trim();
  if (trimmed === "") return null;
  const parsed = Number.parseInt(trimmed, 10);
  if (!Number.isFinite(parsed)) return null;
  return Math.max(MIN_MAX_GROUPS, Math.trunc(parsed));
}

/* -------------------------------------------------------------------- */
/* The picked companies, as a form field -- same encoding as              */
/* se-company-info-pipeline.ts's scope, duplicated per this file's own    */
/* docstring.                                                             */
/* -------------------------------------------------------------------- */

export function normalizeCompanyIdScope(ids: readonly string[]): string[] {
  const seen = new Set<string>();
  const scope: string[] = [];
  for (const raw of ids) {
    const id = raw.trim();
    if (id === "" || seen.has(id)) continue;
    seen.add(id);
    scope.push(id);
  }
  return scope;
}

export function formatCompanyIdScope(ids: readonly string[]): string {
  return normalizeCompanyIdScope(ids).join(",");
}

export function parseCompanyIdScope(value: string): string[] {
  return normalizeCompanyIdScope(value.split(","));
}

export function describeCompanyScope(ids: readonly string[]): string {
  const scope = normalizeCompanyIdScope(ids);
  if (scope.length === 0) return "every company in scope";
  return `${new Intl.NumberFormat("en-US").format(scope.length)} selected ${
    scope.length === 1 ? "company" : "companies"
  }`;
}
