/**
 * The client-safe half of the Pipeline page: the artifact names the form offers,
 * the concurrency range the asset accepts, and the shape of the confirmation
 * panel the action hands back.
 *
 * Deliberately free of Dagster asset names, run-config shapes and anything that
 * reaches ClickHouse -- those live in `se-company-info-pipeline.server.ts`, and
 * keeping them out of here is what keeps the route component out of the server
 * bundle's dependency graph (see CLAUDE.md) and the word "clickhouse" out of the
 * client build.
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

export function clampConcurrency(value: number): number {
  if (!Number.isFinite(value)) return MIN_CONCURRENCY;
  return Math.min(MAX_CONCURRENCY, Math.max(MIN_CONCURRENCY, Math.trunc(value)));
}

/**
 * The environment variable the Dagster host reads this provider's API key from.
 * Derived from the provider name exactly as `llm_api_key_variable` does, so the
 * page can warn when a stored LLM profile names a different variable -- the key
 * itself is never read here and never sent anywhere.
 */
export function dagsterApiKeyVariable(provider: string): string {
  return `${provider.toUpperCase()}_API_KEY`;
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
  /** Hidden fields replayed verbatim into the launch form. */
  fields: Record<string, string>;
}
