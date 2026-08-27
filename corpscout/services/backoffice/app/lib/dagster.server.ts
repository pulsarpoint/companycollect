/**
 * The backoffice's window onto the Dagster instance: launch a run with explicit
 * config, list what has run, read an asset's last materializations, and report
 * whether the schedule and the sensor are actually on.
 *
 * Everything here is one POST to `DAGSTER_GRAPHQL_URL` (the Dagster webserver's
 * `/graphql`, reachable from this host). Nothing is cached: a Pipeline sheet load
 * is rare and a stale run status is worse than a slow one.
 *
 * Secrets never travel this way. The run config a caller passes carries the model
 * PROFILE -- provider, model, base_url and the sampling parameters -- and the
 * Dagster host resolves that provider's API key from its own environment. If a
 * key ever appears in a `runConfig` here it would be stored in the run's config,
 * shown in the Dagster UI and kept in the run database forever.
 */
import "dotenv/config";

import type {
  BackofficeAssetGroupQuery,
  BackofficeAssetGroupQueryVariables,
  BackofficeAssetMaterializationsQuery,
  BackofficeAssetMaterializationsQueryVariables,
  BackofficeInstigatorsQuery,
  BackofficeInstigatorsQueryVariables,
  BackofficeLaunchRunMutation,
  BackofficeLaunchRunMutationVariables,
  BackofficeRunQuery,
  BackofficeRunQueryVariables,
  BackofficeRunsQuery,
  BackofficeRunsQueryVariables,
  EvaluationErrorReason,
  RunStatus,
  StaleStatus,
} from "~/lib/dagster.generated";
import {
  BACKOFFICE_ASSET_GROUP_QUERY,
  BACKOFFICE_ASSET_MATERIALIZATIONS_QUERY,
  BACKOFFICE_INSTIGATORS_QUERY,
  BACKOFFICE_LAUNCH_RUN_MUTATION,
  BACKOFFICE_RUN_QUERY,
  BACKOFFICE_RUNS_QUERY,
} from "~/lib/dagster.operations";

/** The deployed code location and repository these jobs live in. */
export const REPOSITORY_LOCATION_NAME = "dagster_v3";
export const REPOSITORY_NAME = "__repository__";

export const SE_COMPANY_INFO_JOB = "se_company_info_job";
export const SE_COMPANY_INFO_REVIEW_JOB = "se_company_info_review_job";
export const SE_COMPANY_INFO_ASSET = "se_company_info_clickhouse";

/** The two instigators that drive THIS pipeline. The repository has 52 schedules
 * and 15 sensors; a page that renders all of them tells its reader nothing. */
export const SE_COMPANY_INFO_SCHEDULE = "se_company_info_weekly";
export const SE_COMPANY_INFO_SENSOR = "se_company_info_correction_sensor";

/**
 * SE People Experiment Task 5: the three backoffice-triggered people jobs
 * (spec §6.1 -- "the info-pilot / ESEF pattern verbatim"). None of the three
 * is ever scheduled or eager (dagster_v3's identity_eval.py/normalization.py/
 * merge.py module docstrings say so explicitly): there is no schedule or
 * sensor name to filter instigator queries to here, unlike SE_COMPANY_INFO's
 * pair above -- the people pipeline page simply never calls
 * `instigatorStates`, which is the "filter to exactly these jobs" lesson
 * applied to a pipeline that has none to filter to.
 */
export const SE_COMPANY_PERSON_IDENTITY_EVALUATION_JOB =
  "se_company_person_identity_evaluation_job";
export const SE_COMPANY_PERSON_IDENTITY_EVALUATION_ASSET =
  "se_company_person_identity_evaluation";
/** The combined role_draft+person+role chain -- unused by the Pipeline page's own
 * three resolution launches below (each is a single-asset job, "1:1"), kept for
 * whoever else wants the full cascade (e.g. a manual UI materialize). */
export const SE_COMPANY_PERSON_JOB = "se_company_person_job";
export const SE_COMPANY_PERSON_ROLE_DRAFT_ASSET = "se_company_person_role_draft_clickhouse";
export const SE_COMPANY_PERSON_ASSET = "se_company_person_clickhouse";
export const SE_COMPANY_PERSON_ROLE_ASSET = "se_company_person_role_clickhouse";
/**
 * Three-asset split (dagster_v3 company_people/normalization.py's module docstring):
 * se_company_person_clickhouse is CLEAN COPY ONLY (single-source companies, no LLM
 * config at all). se_company_person_llm_suggestions resolves multi-source companies
 * with the model but writes ONLY to se_company_person_enrichment_observation --
 * never the final table. se_company_person_promotion is the separate, model-free
 * asset that copies an eligible suggestion into se_company_person. Each gets its
 * own single-asset job so the Pipeline page's three resolution launches map 1:1.
 */
export const SE_COMPANY_PERSON_PUBLISH_JOB = "se_company_person_publish_job";
export const SE_COMPANY_PERSON_LLM_SUGGESTIONS_JOB = "se_company_person_llm_suggestions_job";
export const SE_COMPANY_PERSON_LLM_SUGGESTIONS_ASSET = "se_company_person_llm_suggestions";
export const SE_COMPANY_PERSON_PROMOTION_JOB = "se_company_person_promotion_job";
export const SE_COMPANY_PERSON_PROMOTION_ASSET = "se_company_person_promotion";
export const SE_COMPANY_PERSON_MERGE_JOB = "se_company_person_merge_job";
export const SE_COMPANY_PERSON_MERGE_ASSET = "se_company_person_merge_suggestions";

export class DagsterError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "DagsterError";
  }
}

/** No `DAGSTER_GRAPHQL_URL` configured: the page renders, the actions refuse. */
export class DagsterNotConfiguredError extends DagsterError {
  constructor() {
    super(
      "Dagster is not configured: set DAGSTER_GRAPHQL_URL (for example http://dagster:3000/graphql).",
    );
    this.name = "DagsterNotConfiguredError";
  }
}

/** The request never got a usable HTTP response (network, timeout, 5xx). */
export class DagsterRequestError extends DagsterError {
  constructor(message: string) {
    super(message);
    this.name = "DagsterRequestError";
  }
}

/** Dagster answered, and the answer was an error -- GraphQL or a typed union. */
export class DagsterGraphQLError extends DagsterError {
  constructor(message: string) {
    super(message);
    this.name = "DagsterGraphQLError";
  }
}

export interface DagsterRunConfigError {
  message: string;
  path: readonly string[];
  reason: EvaluationErrorReason;
}

/** Dagster accepted the launch request but rejected the job's run config. */
export class DagsterRunConfigValidationError extends DagsterGraphQLError {
  readonly errors: readonly DagsterRunConfigError[];

  constructor(job: string, errors: readonly DagsterRunConfigError[]) {
    const details = errors
      .map((error) => {
        const path = error.path.length === 0 ? "<root>" : error.path.join(".");
        return `${path}: ${error.message}`;
      })
      .join("; ");
    super(
      `Dagster rejected the run config for ${job}${details === "" ? "." : `: ${details}`}`,
    );
    this.name = "DagsterRunConfigValidationError";
    this.errors = errors;
  }
}

export interface DagsterOptions {
  fetchImpl?: typeof fetch;
  url?: string;
}

export interface DagsterRun {
  runId: string;
  status: RunStatus;
  jobName: string;
  /** Seconds since the epoch, as Dagster reports them; null while unstarted. */
  startTime: number | null;
  endTime: number | null;
  runConfig: Record<string, unknown>;
  /** Null means Dagster launched the complete job rather than an asset subset. */
  selectedAssets: string[] | null;
  tags: Record<string, string>;
}

export interface LaunchRunInput {
  job: string;
  /** Asset names to run; omitted runs the whole job. */
  assetSelection?: string[];
  runConfig: Record<string, unknown>;
  tags?: Record<string, string>;
}

/** One materialization of an asset, with only its numeric metadata kept. */
export interface AssetMaterialization {
  runId: string;
  timestamp: number | null;
  numbers: Record<string, number>;
}

export interface DagsterAsset {
  asset: string;
  description: string;
  groupName: string;
  kinds: string[];
  dependencies: string[];
  jobNames: string[];
  staleStatus: StaleStatus | null;
  partitioned: boolean;
  materialization: AssetMaterialization | null;
}

export interface InstigatorState {
  name: string;
  status: string;
  /** Schedules only. */
  cronSchedule?: string;
}

export interface InstigatorStates {
  schedules: InstigatorState[];
  sensors: InstigatorState[];
}

export function dagsterGraphqlUrl(url?: string): string {
  const configured = (url ?? process.env.DAGSTER_GRAPHQL_URL ?? "").trim();
  if (configured === "") throw new DagsterNotConfiguredError();
  return configured;
}

/**
 * The browser-facing Dagster UI. `DAGSTER_UI_URL` when set; otherwise derived
 * from the GraphQL endpoint by dropping its `/graphql` path -- which is right
 * whenever the two are the same host, and overridable when they are not (the
 * backoffice reaches Dagster over a container network the browser cannot).
 */
export function dagsterRunUrl(runId: string, url?: string): string | null {
  const explicit = (process.env.DAGSTER_UI_URL ?? "").trim();
  let base = explicit;
  if (base === "") {
    try {
      base = dagsterGraphqlUrl(url).replace(/\/graphql\/?$/, "");
    } catch {
      return null;
    }
  }
  return `${base.replace(/\/+$/, "")}/runs/${encodeURIComponent(runId)}`;
}

interface GraphQLResponse<T> {
  data?: T;
  errors?: { message?: string }[];
}

async function graphql<TData, TVariables>(
  query: string,
  variables: TVariables,
  options: DagsterOptions = {},
): Promise<TData> {
  const endpoint = dagsterGraphqlUrl(options.url);
  const doFetch = options.fetchImpl ?? fetch;
  let response: Response;
  try {
    response = await doFetch(endpoint, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ query, variables }),
    });
  } catch (error) {
    throw new DagsterRequestError(
      `Dagster at ${endpoint} did not answer: ${
        error instanceof Error ? error.message : String(error)
      }`,
    );
  }
  if (!response.ok) {
    throw new DagsterRequestError(
      `Dagster at ${endpoint} answered HTTP ${response.status}.`,
    );
  }
  const payload = (await response.json()) as GraphQLResponse<TData>;
  if (payload.errors && payload.errors.length > 0) {
    throw new DagsterGraphQLError(
      payload.errors
        .map((entry) => entry.message ?? "unknown error")
        .join("; "),
    );
  }
  if (!payload.data) {
    throw new DagsterGraphQLError("Dagster returned no data.");
  }
  return payload.data;
}

/**
 * A GraphQL union answered with an error member. Dagster spells failures as
 * types, not as GraphQL errors, so every call has to look at `__typename`
 * before trusting the payload -- an unhandled `PythonError` would otherwise
 * read as "no runs" or "nothing materialized".
 */
function unionError(
  node: { __typename: string; message?: string | null },
  what: string,
): DagsterGraphQLError {
  const kind = node.__typename;
  const detail = node?.message ? `: ${node.message}` : "";
  return new DagsterGraphQLError(`${what} failed (${kind})${detail}`);
}

function tagMap(
  tags: readonly { key: string; value: string }[],
): Record<string, string> {
  return Object.fromEntries(tags.map((tag) => [tag.key, tag.value]));
}

function assetName(path: readonly string[]): string {
  return path.join("/");
}

function runConfig(value: unknown): Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : {};
}

/**
 * Start one run of `job`, optionally narrowed to `assetSelection`, with exactly
 * the config given -- no defaults are filled in here. The caller owns the whole
 * `runConfig`, including the `execute` gate: a config without it produces a
 * preview run, which is the asset's deliberate default, not this function's.
 */
export async function launchRun(
  input: LaunchRunInput,
  options: DagsterOptions = {},
): Promise<{ runId: string; status: string }> {
  const variables: BackofficeLaunchRunMutationVariables = {
    executionParams: {
      selector: {
        repositoryLocationName: REPOSITORY_LOCATION_NAME,
        repositoryName: REPOSITORY_NAME,
        jobName: input.job,
        ...(input.assetSelection
          ? {
              assetSelection: input.assetSelection.map((name) => ({
                path: [name],
              })),
            }
          : {}),
      },
      runConfigData: input.runConfig,
      mode: "default",
      executionMetadata: {
        tags: Object.entries(input.tags ?? {}).map(([key, value]) => ({
          key,
          value,
        })),
      },
    },
  };
  const data = await graphql<
    BackofficeLaunchRunMutation,
    BackofficeLaunchRunMutationVariables
  >(BACKOFFICE_LAUNCH_RUN_MUTATION, variables, options);
  if (data.launchRun.__typename === "RunConfigValidationInvalid") {
    throw new DagsterRunConfigValidationError(
      input.job,
      data.launchRun.errors.map(({ message, path, reason }) => ({
        message,
        path,
        reason,
      })),
    );
  }
  if (data.launchRun.__typename !== "LaunchRunSuccess") {
    throw unionError(data.launchRun, `Launching ${input.job}`);
  }
  const run = data.launchRun.run;
  return { runId: run.runId, status: run.status };
}

export async function listRuns(
  input: { job: string; limit: number; statuses?: readonly RunStatus[] },
  options: DagsterOptions = {},
): Promise<DagsterRun[]> {
  const variables: BackofficeRunsQueryVariables = {
    filter: {
      pipelineName: input.job,
      ...(input.statuses ? { statuses: [...input.statuses] } : {}),
    },
    limit: input.limit,
  };
  const data = await graphql<BackofficeRunsQuery, BackofficeRunsQueryVariables>(
    BACKOFFICE_RUNS_QUERY,
    variables,
    options,
  );
  if (data.runsOrError.__typename !== "Runs") {
    throw unionError(data.runsOrError, `Listing runs of ${input.job}`);
  }
  return data.runsOrError.results.map((run) => ({
    runId: run.runId,
    status: run.status,
    jobName: run.jobName,
    startTime: run.startTime ?? null,
    endTime: run.endTime ?? null,
    runConfig: runConfig(run.runConfig),
    selectedAssets:
      run.assetSelection?.map((asset) => assetName(asset.path)) ?? null,
    tags: tagMap(run.tags),
  }));
}

export async function runStatus(
  runId: string,
  options: DagsterOptions = {},
): Promise<DagsterRun> {
  const variables: BackofficeRunQueryVariables = { runId };
  const data = await graphql<BackofficeRunQuery, BackofficeRunQueryVariables>(
    BACKOFFICE_RUN_QUERY,
    variables,
    options,
  );
  if (data.runOrError.__typename !== "Run") {
    throw unionError(data.runOrError, `Reading run ${runId}`);
  }
  const run = data.runOrError;
  return {
    runId: run.runId,
    status: run.status,
    jobName: run.jobName,
    startTime: run.startTime ?? null,
    endTime: run.endTime ?? null,
    runConfig: runConfig(run.runConfig),
    selectedAssets:
      run.assetSelection?.map((asset) => assetName(asset.path)) ?? null,
    tags: tagMap(run.tags),
  };
}

/** Load one complete Dagster asset group without scanning every repository asset. */
export async function assetGroup(
  groupName: string,
  options: DagsterOptions = {},
): Promise<DagsterAsset[]> {
  const variables: BackofficeAssetGroupQueryVariables = {
    group: {
      groupName,
      repositoryLocationName: REPOSITORY_LOCATION_NAME,
      repositoryName: REPOSITORY_NAME,
    },
  };
  const data = await graphql<
    BackofficeAssetGroupQuery,
    BackofficeAssetGroupQueryVariables
  >(BACKOFFICE_ASSET_GROUP_QUERY, variables, options);
  return data.assetNodes.map((asset) => {
    const materialization = asset.assetMaterializations[0];
    return {
      asset: assetName(asset.assetKey.path),
      description: asset.description ?? "",
      groupName: asset.groupName,
      kinds: [...asset.kinds],
      dependencies: asset.dependencyKeys.map((key) => assetName(key.path)),
      jobNames: [...asset.jobNames],
      staleStatus: asset.staleStatus,
      partitioned: asset.partitionDefinition !== null,
      materialization: materialization
        ? {
            runId: materialization.runId,
            timestamp: Number(materialization.timestamp),
            numbers: {},
          }
        : null,
    };
  });
}

/**
 * `assetMaterializations` carries the MaterializeResult metadata, which is where
 * a run says how many companies it selected, inserted and sent to the model.
 * Only integer entries are kept: those are the counts, and asking for every
 * metadata type would make one unexpected entry shape fail the whole query.
 */
export async function assetMaterializations(
  input: { asset: string; limit: number },
  options: DagsterOptions = {},
): Promise<AssetMaterialization[]> {
  const variables: BackofficeAssetMaterializationsQueryVariables = {
    assetKeys: [{ path: [input.asset] }],
    limit: input.limit,
  };
  const data = await graphql<
    BackofficeAssetMaterializationsQuery,
    BackofficeAssetMaterializationsQueryVariables
  >(BACKOFFICE_ASSET_MATERIALIZATIONS_QUERY, variables, options);
  return data.assetNodes.flatMap((node) =>
    node.assetMaterializations.map((materialization) => ({
      runId: materialization.runId,
      timestamp: Number(materialization.timestamp),
      numbers: Object.fromEntries(
        materialization.metadataEntries
          .filter(
            (
              entry,
            ): entry is Extract<
              typeof entry,
              { __typename: "IntMetadataEntry" }
            > & {
              intValue: number;
            } =>
              entry.__typename === "IntMetadataEntry" &&
              typeof entry.intValue === "number",
          )
          .map((entry) => [entry.label, entry.intValue]),
      ),
    })),
  );
}

/**
 * Whether the named schedules and sensors are RUNNING or STOPPED.
 *
 * `names` is applied here rather than in the query: Dagster's
 * `schedulesOrError`/`sensorsOrError` take a repository selector and nothing
 * else, so the repository's whole roster comes back and the caller says which of
 * it is theirs. An empty or omitted list means "everything", which is only ever
 * useful for a repository-wide view.
 */
export async function instigatorStates(
  input: { names?: readonly string[] } = {},
  options: DagsterOptions = {},
): Promise<InstigatorStates> {
  const variables: BackofficeInstigatorsQueryVariables = {
    repositorySelector: {
      repositoryLocationName: REPOSITORY_LOCATION_NAME,
      repositoryName: REPOSITORY_NAME,
    },
  };
  const data = await graphql<
    BackofficeInstigatorsQuery,
    BackofficeInstigatorsQueryVariables
  >(BACKOFFICE_INSTIGATORS_QUERY, variables, options);
  if (data.schedulesOrError.__typename !== "Schedules") {
    throw unionError(data.schedulesOrError, "Reading schedules");
  }
  if (data.sensorsOrError.__typename !== "Sensors") {
    throw unionError(data.sensorsOrError, "Reading sensors");
  }
  const wanted = new Set(input.names ?? []);
  const mine = (name: string) => wanted.size === 0 || wanted.has(name);
  return {
    schedules: data.schedulesOrError.results
      .filter((schedule) => mine(schedule.name))
      .map((schedule) => ({
        name: schedule.name,
        status: schedule.scheduleState.status,
        cronSchedule: schedule.cronSchedule,
      })),
    sensors: data.sensorsOrError.results
      .filter((sensor) => mine(sensor.name))
      .map((sensor) => ({
        name: sensor.name,
        status: sensor.sensorState.status,
      })),
  };
}
