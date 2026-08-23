/**
 * The numbers that decide whether to materialize `se_company_info`.
 *
 * The selection query is a PORT of Dagster's `build_changed_companies_sql`
 * (dagster_v3/defs/se_company/info.py): the same three CTEs, the same per-source
 * `maxIf` freshness, the same reason expressions, the same `ifNull` around every
 * LEFT JOIN miss. Two deliberate differences: it has no `WHERE` selection and no
 * paging, because it counts every company at once rather than returning a page,
 * and it aggregates the reasons instead of projecting them per row.
 *
 * When the Dagster scan changes, this changes with it -- a Pipeline page that
 * promises "1,240 companies would be re-resolved" and then launches a run that
 * picks a different set is worse than showing nothing. The reason expressions are
 * therefore single constants here too, spelled once and reused by both the
 * per-reason counts and the combined "changed" predicates.
 *
 * Cost: one FINAL read of `se_company_info` (3.5M rows) joined to the three
 * artifacts' per-company maxima -- the same work the Dagster scan does on every
 * run. It is an admin page, loaded by hand, so that is affordable; it is not
 * something to put behind an auto-refresh.
 */
import { createHmac, timingSafeEqual } from "node:crypto";
import { chQuery } from "~/lib/clickhouse.server";
import {
  clampConcurrency,
  clampMaxCompanies,
  INFO_ARTIFACT_SOURCES,
  type InfoArtifact,
} from "~/lib/se-company-info-pipeline";

/** A LEFT JOIN miss reads as this instant, exactly as EPOCH_SQL does in info.py. */
const EPOCH = "toDateTime64('1970-01-01 00:00:00', 3, 'UTC')";
const PUBLISHED_AT = `ifNull(published.resolved_at, ${EPOCH})`;
const MULTI_SOURCE = "ifNull(published.description_source_count, 0) > 1";

/** Each artifact's table and the asset that fills it. Kept here rather than in
 * the client-safe module so no Dagster asset name reaches the browser bundle. */
export const INFO_ARTIFACTS = INFO_ARTIFACT_SOURCES.map((source) => ({
  source,
  table: `se_company_info_${source}`,
}));

export type InfoArtifactSource = InfoArtifact;

/** The final asset's op key in a run config, and the jobs that can run it. */
export const INFO_ASSET = "se_company_info_clickhouse";

export function infoArtifactAsset(source: InfoArtifact): string {
  return `se_company_info_${source}_clickhouse`;
}

/** Pinned to Dagster's DEFAULT_LLM_PROFILE. Sent explicitly rather than left to
 * the asset's own defaults, so the run's config records what it called. */
export const DEFAULT_PROMPT_VERSION = "se-company-info-description-v3";
export const DEFAULT_TEMPERATURE = 0;
export const DEFAULT_MAX_TOKENS = 6000;

export interface PipelineLlmProfile {
  provider: string;
  model: string;
  baseUrl: string;
  concurrency: number;
}

export interface InfoRunOptions {
  maxCompanies: number;
  /** false publishes the deterministic pick for multi-source companies and never
   * calls the model. `pendingModelOnly` requires this to be true. */
  useModel: boolean;
  pendingModelOnly?: boolean;
  llm: PipelineLlmProfile;
}

/**
 * The run config for one `se_company_info_clickhouse` run, in Dagster's own
 * snake_case. `execute: true` is written here and nowhere else -- without it the
 * asset runs a preview that writes nothing, which is what a bare Dagster UI
 * click gets. The `llm` block carries the profile and never a key: the Dagster
 * host resolves `dagsterApiKeyVariable(provider)` from its own environment.
 */
export function buildInfoRunConfig(options: InfoRunOptions): Record<string, unknown> {
  return {
    ops: {
      [INFO_ASSET]: {
        config: {
          execute: true,
          max_companies: clampMaxCompanies(options.maxCompanies),
          resolve_multi_source_with_llm: options.useModel,
          pending_model_only: options.pendingModelOnly ?? false,
          llm: {
            provider: options.llm.provider,
            model: options.llm.model,
            base_url: options.llm.baseUrl,
            temperature: DEFAULT_TEMPERATURE,
            max_tokens: DEFAULT_MAX_TOKENS,
            prompt_version: DEFAULT_PROMPT_VERSION,
            concurrency: clampConcurrency(options.llm.concurrency),
          },
        },
      },
    },
  };
}

/**
 * Refreshing one artifact runs only that asset on `se_company_info_job`, so the
 * run carries NO config: an `ops` entry for an asset outside the selection is a
 * config error, and the artifact assets take no config of their own.
 */
export function buildArtifactRunConfig(): Record<string, unknown> {
  return {};
}

/**
 * "Still owed a description": several sources offered one, no suggestion was ever
 * stored, and no reviewer decision has been applied. The `correction_ids` test is
 * what keeps a reviewed company out -- `reject_suggestion` leaves the row looking
 * exactly like a never-modelled one.
 */
const PENDING_MODEL = `${MULTI_SOURCE} AND published.suggestion_id IS NULL AND length(published.correction_ids) = 0`;

/** New evidence in one artifact: that source's newest row is newer than the
 * published resolution. `maxIf` over no rows is 1970, so a source the company has
 * no row in never reads as new evidence. */
function newEvidence(source: string): string {
  return `artifacts.${source}_observed_at > ${PUBLISHED_AT}`;
}

const LEDGER_PENDING = `ifNull(ledger.latest_correction_at, ${EPOCH}) > ${PUBLISHED_AT}`;
const NEVER_PUBLISHED = "ifNull(published.company_id, '') = ''";

/** Everything an ordinary run picks up before the model term: a company that was
 * never published, whose evidence moved, or whose ledger gained a row. */
const EVIDENCE_CHANGED = [
  "never_published",
  ...INFO_ARTIFACTS.map((artifact) => `new_evidence_${artifact.source}`),
  "ledger_pending",
].join(" OR ");

/** ... and what a MODEL-ON ordinary run picks up: the same, plus the companies
 * the model still owes a description. Mirrors info.py's `include_pending` term. */
const CHANGED = `${EVIDENCE_CHANGED} OR pending_model`;

export const INFO_SELECTION_CTE_SQL = `WITH artifacts AS (
  SELECT company_id,
    ${INFO_ARTIFACTS.map(
      (artifact) =>
        `maxIf(source_observed_at, source = '${artifact.source}') AS ${artifact.source}_observed_at`,
    ).join(",\n    ")}
  FROM (
    ${INFO_ARTIFACTS.map(
      (artifact) =>
        `SELECT '${artifact.source}' AS source, company_id, max(observed_at) AS source_observed_at
    FROM corpscout.${artifact.table} GROUP BY company_id`,
    ).join("\n    UNION ALL\n    ")}
  )
  GROUP BY company_id
),
ledger AS (
  SELECT company_id, max(created_at) AS latest_correction_at
  FROM corpscout.se_company_info_correction
  GROUP BY company_id
),
published AS (
  SELECT final.company_id AS company_id, final.resolved_at AS resolved_at,
    final.description_source_count AS description_source_count,
    final.suggestion_id AS suggestion_id,
    final.correction_ids AS correction_ids
  FROM corpscout.se_company_info AS final FINAL
),
selection AS (
  SELECT artifacts.company_id AS company_id,
    ${NEVER_PUBLISHED} AS never_published,
    ${INFO_ARTIFACTS.map(
      (artifact) => `${newEvidence(artifact.source)} AS new_evidence_${artifact.source}`,
    ).join(",\n    ")},
    ${LEDGER_PENDING} AS ledger_pending,
    (${PENDING_MODEL}) AS pending_model,
    ${MULTI_SOURCE} AS multi_source
  FROM artifacts
  LEFT JOIN published ON published.company_id = artifacts.company_id
  LEFT JOIN ledger ON ledger.company_id = artifacts.company_id
)`;

/** Counts are read back as strings: a UInt64 over 2^53 would lose precision as a
 * JSON number, and every other list page in this app reads counts the same way. */
export const INFO_SELECTION_COUNTS_SQL = `${INFO_SELECTION_CTE_SQL}
SELECT
  toString(count()) AS company_count,
  toString(countIf(${CHANGED})) AS changed_count,
  toString(countIf(${EVIDENCE_CHANGED})) AS changed_without_model_count,
  toString(countIf((${CHANGED}) AND multi_source)) AS would_call_model_count,
  toString(countIf(never_published)) AS never_published_count,
  ${INFO_ARTIFACTS.map(
    (artifact) =>
      `toString(countIf(new_evidence_${artifact.source})) AS new_evidence_${artifact.source}_count`,
  ).join(",\n  ")},
  toString(countIf(ledger_pending)) AS ledger_pending_count,
  toString(countIf(pending_model)) AS pending_model_count
FROM selection`;

export const INFO_ARTIFACT_FRESHNESS_SQL = INFO_ARTIFACTS.map(
  (artifact) => `SELECT '${artifact.source}' AS source,
  toString(max(observed_at)) AS latest_observed_at,
  toString(count()) AS row_count
FROM corpscout.${artifact.table}`,
).join("\nUNION ALL\n");

/** Per model, what one description call has actually cost. The averages are aliased
 * `avg_*` so the WHERE below binds to the source columns, not the aggregates. `prompt_tokens > 0`
 * drops rows stored without usage numbers, which would drag the average down. */
export const INFO_OBSERVATION_AVERAGES_SQL = `SELECT
  model_name AS model_name,
  toString(count()) AS call_count,
  toString(round(avg(prompt_tokens))) AS avg_prompt_tokens,
  toString(round(avg(completion_tokens))) AS avg_completion_tokens
FROM corpscout.se_company_info_enrichment_observation
WHERE prompt_tokens > 0
GROUP BY model_name
ORDER BY count() DESC`;

export interface SeCompanyInfoSelectionCounts {
  companyCount: number;
  /** What a model-on "Re-resolve changed companies" run would select. */
  changedCount: number;
  /** ... and the same run with the model switched off. */
  changedWithoutModelCount: number;
  /** Selected companies whose last published resolution had several description
   * sources, i.e. the ones that would enter the model step. */
  wouldCallModelCount: number;
  neverPublishedCount: number;
  newEvidenceCounts: Record<InfoArtifactSource, number>;
  ledgerPendingCount: number;
  /** What a "Run the model pass" (`pending_model_only`) run would select. */
  pendingModelCount: number;
}

export interface SeCompanyInfoArtifactFreshness {
  source: InfoArtifactSource;
  latestObservedAt: string;
  rowCount: number;
}

export interface SeCompanyInfoModelAverages {
  modelName: string;
  callCount: number;
  promptTokens: number;
  completionTokens: number;
}

export interface SeCompanyInfoPipelineStats {
  selection: SeCompanyInfoSelectionCounts;
  artifacts: SeCompanyInfoArtifactFreshness[];
  models: SeCompanyInfoModelAverages[];
}

/** Every column of every query above is `toString()`-ed, so one row type covers
 * all three reads -- and injecting the reader keeps the SQL testable without a
 * ClickHouse (production always passes `chQuery`, which is readonly=2). */
type PipelineQuery = (
  sql: string,
  params?: Record<string, unknown>,
) => Promise<Record<string, string>[]>;

function num(row: Record<string, string> | undefined, column: string): number {
  return Number(row?.[column] ?? 0);
}

/**
 * Every number the Pipeline page shows, in three reads.
 */
export async function loadSeCompanyInfoPipelineStats(
  options: { queryImpl?: PipelineQuery } = {},
): Promise<SeCompanyInfoPipelineStats> {
  const query: PipelineQuery =
    options.queryImpl ?? ((sql, params) => chQuery<Record<string, string>>(sql, params));
  const [counts, artifacts, models] = await Promise.all([
    query(INFO_SELECTION_COUNTS_SQL),
    query(INFO_ARTIFACT_FRESHNESS_SQL),
    query(INFO_OBSERVATION_AVERAGES_SQL),
  ]);
  const row = counts[0];
  return {
    selection: {
      companyCount: num(row, "company_count"),
      changedCount: num(row, "changed_count"),
      changedWithoutModelCount: num(row, "changed_without_model_count"),
      wouldCallModelCount: num(row, "would_call_model_count"),
      neverPublishedCount: num(row, "never_published_count"),
      newEvidenceCounts: {
        scb: num(row, "new_evidence_scb_count"),
        esef: num(row, "new_evidence_esef_count"),
        wikidata: num(row, "new_evidence_wikidata_count"),
      },
      ledgerPendingCount: num(row, "ledger_pending_count"),
      pendingModelCount: num(row, "pending_model_count"),
    },
    artifacts: artifacts.map((entry) => ({
      source: entry.source as InfoArtifactSource,
      latestObservedAt: entry.latest_observed_at,
      rowCount: Number(entry.row_count),
    })),
    models: models.map((entry) => ({
      modelName: entry.model_name,
      callCount: Number(entry.call_count),
      promptTokens: Number(entry.avg_prompt_tokens),
      completionTokens: Number(entry.avg_completion_tokens),
    })),
  };
}

/* -------------------------------------------------------------------- */
/* Binding a launch to the confirmation that described it                */
/* -------------------------------------------------------------------- */

/**
 * The backoffice has no authentication and these launches spend money, so a
 * `launch-*` POST is not trusted on its own: the `confirm-*` branch signs the
 * exact run it described, and `launch-*` refuses anything that does not carry a
 * live signature for the run it is about to start.
 *
 * The token carries no payload -- only an expiry and a MAC over
 * `expiry.canonical(intent)`. Verification recomputes the MAC from the intent
 * the launch branch actually built, so a replayed token whose form fields have
 * been edited (a bigger `max_companies`, a different model, another artifact)
 * signs a different intent and is rejected. It is a binding, not a session: it
 * does not authenticate anybody, it only proves that this exact run was the one
 * just described on screen.
 */
export const ACTION_TOKEN_TTL_SECONDS = 600;

export class ActionTokenError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "ActionTokenError";
  }
}

/** What a launch is: the job, the assets, and the config. All of it is signed. */
export interface LaunchIntent {
  job: string;
  assetSelection?: string[];
  runConfig: Record<string, unknown>;
}

function actionSecret(secret = process.env.BACKOFFICE_ACTION_SECRET): string {
  const value = (secret ?? "").trim();
  if (value === "") {
    throw new ActionTokenError(
      "BACKOFFICE_ACTION_SECRET is not set, so a launch cannot be bound to its " +
        "confirmation. Set it in the backoffice environment; runs stay refused until then.",
    );
  }
  return value;
}

/** Key order must not change the signature, so the JSON is written sorted. */
function canonical(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(canonical).join(",")}]`;
  if (value !== null && typeof value === "object") {
    const entries = Object.entries(value as Record<string, unknown>)
      .filter(([, item]) => item !== undefined)
      .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0));
    return `{${entries.map(([key, item]) => `${JSON.stringify(key)}:${canonical(item)}`).join(",")}}`;
  }
  return JSON.stringify(value ?? null);
}

function sign(expiresAt: number, intent: LaunchIntent, secret?: string): string {
  return createHmac("sha256", actionSecret(secret))
    .update(`${expiresAt}.${canonical(intent)}`)
    .digest("hex");
}

export interface ActionTokenOptions {
  now?: number;
  secret?: string;
}

/** Throws `ActionTokenError` when no secret is configured: a page that cannot
 * sign must not offer a launch button that would be refused anyway. */
export function mintLaunchToken(intent: LaunchIntent, options: ActionTokenOptions = {}): string {
  const nowSeconds = Math.floor((options.now ?? Date.now()) / 1000);
  const expiresAt = nowSeconds + ACTION_TOKEN_TTL_SECONDS;
  return `${expiresAt}.${sign(expiresAt, intent, options.secret)}`;
}

/** Never throws: a missing secret, a malformed token and a forged one are all
 * the same answer to the caller -- do not launch, and say why. */
export function verifyLaunchToken(
  token: string,
  intent: LaunchIntent,
  options: ActionTokenOptions = {},
): { ok: true } | { ok: false; error: string } {
  const [rawExpiry, mac] = token.split(".");
  if (!rawExpiry || !mac) {
    return { ok: false, error: "Confirm the run again: this launch carried no confirmation." };
  }
  const expiresAt = Number.parseInt(rawExpiry, 10);
  if (!Number.isFinite(expiresAt)) {
    return { ok: false, error: "Confirm the run again: this launch carried no confirmation." };
  }
  if (Math.floor((options.now ?? Date.now()) / 1000) > expiresAt) {
    return { ok: false, error: "That confirmation has expired. Review the numbers again." };
  }
  let expected: string;
  try {
    expected = sign(expiresAt, intent, options.secret);
  } catch (error) {
    return { ok: false, error: error instanceof Error ? error.message : String(error) };
  }
  const given = Buffer.from(mac, "hex");
  const want = Buffer.from(expected, "hex");
  if (given.length !== want.length || !timingSafeEqual(given, want)) {
    return {
      ok: false,
      error:
        "This launch does not match the run that was confirmed. Review it again — " +
        "nothing was started.",
    };
  }
  return { ok: true };
}
