/**
 * The client-safe half of the Pipeline sheet: the artifact names the form
 * offers, the bounds the asset's config accepts, the select items the profile
 * picker renders, the scope the picked companies travel as, and the shape of the
 * confirmation panel the action hands back.
 *
 * Deliberately free of Dagster asset names, run-config shapes and anything that
 * reaches ClickHouse -- those live in `se-company-info-pipeline.server.ts`, and
 * keeping them out of here is what keeps the sheet (which renders on the
 * companies list) out of the server bundle's dependency graph (see CLAUDE.md)
 * and the word "clickhouse" out of the client build.
 */

/** The three artifacts, in the order Dagster's ARTIFACT_TABLES declares them. */
export const INFO_ARTIFACT_SOURCES = ["scb", "esef", "wikidata"] as const;

export type InfoArtifact = (typeof INFO_ARTIFACT_SOURCES)[number];

export function isInfoArtifact(value: string): value is InfoArtifact {
  return (INFO_ARTIFACT_SOURCES as readonly string[]).includes(value);
}

/** Every run this page starts is tagged, so a run launched by a reviewer is
 * distinguishable in the Dagster UI from the schedule's and the sensor's. */
export const PILOT_TAG_KEY = "pilot";
export const PILOT_TAG_VALUE = "backoffice";

/** Mirrors LlmProfileConfig.concurrency's bounds in info.py. */
export const MIN_CONCURRENCY = 1;
export const MAX_CONCURRENCY = 8;

/** Mirrors SECompanyInfoConfig.max_companies (ge=1, le=1_000_000). Out-of-range
 * values are a config error on the Dagster side, so the form never submits one
 * and the action clamps whatever arrives regardless of the input's `max`. */
export const MIN_COMPANIES = 1;
export const MAX_COMPANIES = 1_000_000;

function clamp(value: number, low: number, high: number): number {
  if (!Number.isFinite(value)) return low;
  return Math.min(high, Math.max(low, Math.trunc(value)));
}

export function clampConcurrency(value: number): number {
  return clamp(value, MIN_CONCURRENCY, MAX_CONCURRENCY);
}

export function clampMaxCompanies(value: number): number {
  return clamp(value, MIN_COMPANIES, MAX_COMPANIES);
}

/**
 * The environment variable the Dagster host reads this provider's API key from,
 * or null when there is none it could read.
 *
 * Dagster builds the name with a bare `provider.upper()`
 * (`llm_api_key_variable`), so normalising here can only ever DESCRIBE what the
 * host will look up -- it cannot fix it. A provider name that uppercases to
 * something which is not a valid environment identifier ("open-ai",
 * "3rd-party", "") therefore has no key variable at all, and the page says so
 * rather than showing a name nothing will ever read. The key itself is never
 * read here and never sent anywhere.
 */
export function dagsterApiKeyVariable(provider: string): string | null {
  const upper = provider.trim().toUpperCase();
  const normalized = upper.replace(/[^A-Z0-9]/g, "_");
  if (normalized !== upper) return null;
  if (!/^[A-Z_][A-Z0-9_]*$/.test(normalized)) return null;
  return `${normalized}_API_KEY`;
}

/** One stored LLM profile as the page sees it -- never the key, only the name
 * of the variable it lives in. */
export interface PipelineProfileOption {
  profileId: string;
  name: string;
  provider: string;
  model: string;
  baseUrl: string;
  isActive: boolean;
  /** The variable the LLM settings page stored. */
  apiKeyEnvironmentVariable: string;
  /** The variable the Dagster host will actually read; "" when the provider
   * name cannot produce one. */
  dagsterApiKeyVariable: string;
}

/**
 * Base UI's `Select` renders the trigger from its `items` list, not from the
 * chosen `<SelectItem>`'s children: without it the trigger shows the raw value,
 * which for a profile is a UUID.
 */
export function profileSelectItems(
  profiles: PipelineProfileOption[],
): { label: string; value: string }[] {
  return profiles.map((profile) => ({
    label: `${profile.name} — ${profile.model}`,
    value: profile.profileId,
  }));
}

export function artifactSelectItems(): { label: string; value: string }[] {
  return INFO_ARTIFACT_SOURCES.map((source) => ({ label: source, value: source }));
}

export type PipelineIntent =
  | "confirm-resolve"
  | "launch-resolve"
  | "confirm-model-pass"
  | "launch-model-pass"
  | "confirm-artifact"
  | "launch-artifact";

/** What the confirmation step restates before anything is launched. */
export interface PipelineConfirmation {
  intent: PipelineIntent;
  title: string;
  /** One line per number the operator is agreeing to. */
  lines: string[];
  /** Hidden fields replayed verbatim into the launch form, so the run that
   * starts is the one these numbers describe. */
  fields: Record<string, string>;
}

/* -------------------------------------------------------------------- */
/* Where the pipeline lives                                              */
/* -------------------------------------------------------------------- */

/**
 * The companies list, and the resource route the Pipeline sheet talks to.
 *
 * The pipeline is no longer a page: it is a Sheet on the list, fed by a
 * `useFetcher` that loads THIS path and posts its launches back to it. The path
 * is spelled once here so the sheet's `fetcher.load`, every form's `action` and
 * the loader's own document-request redirect cannot drift apart.
 */
export const SE_COMPANY_INFO_LIST_PATH = "/admin/se/companies";
export const SE_COMPANY_INFO_PIPELINE_PATH = "/admin/se/companies/pipeline";

/* -------------------------------------------------------------------- */
/* The picked companies, as a form field                                 */
/* -------------------------------------------------------------------- */

/**
 * The company ids a scoped run covers, normalised.
 *
 * The selection arrives from TanStack's row-selection map (`selectedRowIds`),
 * travels as ONE comma-separated hidden field through the confirm step, and is
 * replayed verbatim into the launch -- so the run that starts is scoped to
 * exactly the ids that were confirmed. Blanks are dropped and duplicates
 * collapse (a replayed field must not be able to grow the scope), and the order
 * the reviewer picked in is kept so the confirmation and the run config read
 * alike.
 */
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

/** The hidden field's value: "" when nothing is picked, which is what makes an
 * unscoped run the DEFAULT rather than something the form has to ask for. */
export function formatCompanyIdScope(ids: readonly string[]): string {
  return normalizeCompanyIdScope(ids).join(",");
}

export function parseCompanyIdScope(value: string): string[] {
  return normalizeCompanyIdScope(value.split(","));
}

/**
 * What a launch covers, in one phrase -- said out loud before every launch
 * button rather than implied by a tick somewhere up the page: a selection can
 * name companies that the current filter does not show at all.
 */
export function describeCompanyScope(ids: readonly string[]): string {
  const scope = normalizeCompanyIdScope(ids);
  if (scope.length === 0) return "all changed companies";
  return `${new Intl.NumberFormat("en-US").format(scope.length)} selected ${
    scope.length === 1 ? "company" : "companies"
  }`;
}
