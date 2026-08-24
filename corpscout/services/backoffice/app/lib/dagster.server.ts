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

export interface DagsterOptions {
  fetchImpl?: typeof fetch;
  url?: string;
}

export interface DagsterRun {
  runId: string;
  status: string;
  jobName: string;
  /** Seconds since the epoch, as Dagster reports them; null while unstarted. */
  startTime: number | null;
  endTime: number | null;
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

async function graphql<T>(
  query: string,
  variables: Record<string, unknown>,
  options: DagsterOptions = {},
): Promise<T> {
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
  const payload = (await response.json()) as GraphQLResponse<T>;
  if (payload.errors && payload.errors.length > 0) {
    throw new DagsterGraphQLError(
      payload.errors.map((entry) => entry.message ?? "unknown error").join("; "),
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
  node: { __typename?: string; message?: string } | null | undefined,
  expected: string,
  what: string,
): DagsterGraphQLError | null {
  if (node && node.__typename === expected) return null;
  const kind = node?.__typename ?? "no response";
  const detail = node?.message ? `: ${node.message}` : "";
  return new DagsterGraphQLError(`${what} failed (${kind})${detail}`);
}

function tagMap(tags: { key: string; value: string }[] | undefined): Record<string, string> {
  return Object.fromEntries((tags ?? []).map((tag) => [tag.key, tag.value]));
}

const LAUNCH_RUN_MUTATION = `mutation BackofficeLaunchRun($executionParams: ExecutionParams!) {
  launchRun(executionParams: $executionParams) {
    __typename
    ... on LaunchRunSuccess { run { runId status } }
    ... on PythonError { message }
    ... on RunConfigValidationInvalid { message }
    ... on PipelineNotFoundError { message }
    ... on InvalidSubsetError { message }
    ... on ConflictingExecutionParamsError { message }
    ... on PresetNotFoundError { message }
    ... on UnauthorizedError { message }
  }
}`;

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
  const data = await graphql<{
    launchRun: {
      __typename?: string;
      message?: string;
      run?: { runId: string; status: string };
    };
  }>(
    LAUNCH_RUN_MUTATION,
    {
      executionParams: {
        selector: {
          repositoryLocationName: REPOSITORY_LOCATION_NAME,
          repositoryName: REPOSITORY_NAME,
          jobName: input.job,
          ...(input.assetSelection
            ? { assetSelection: input.assetSelection.map((name) => ({ path: [name] })) }
            : {}),
        },
        runConfigData: input.runConfig,
        mode: "default",
        executionMetadata: {
          tags: Object.entries(input.tags ?? {}).map(([key, value]) => ({ key, value })),
        },
      },
    },
    options,
  );
  const error = unionError(data.launchRun, "LaunchRunSuccess", `Launching ${input.job}`);
  if (error) throw error;
  const run = data.launchRun.run;
  if (!run) throw new DagsterGraphQLError(`Launching ${input.job} returned no run.`);
  return { runId: run.runId, status: run.status };
}

const RUNS_QUERY = `query BackofficeRuns($filter: RunsFilter!, $limit: Int!) {
  runsOrError(filter: $filter, limit: $limit) {
    __typename
    ... on Runs {
      results { runId status jobName startTime endTime tags { key value } }
    }
    ... on PythonError { message }
    ... on InvalidPipelineRunsFilterError { message }
  }
}`;

export async function listRuns(
  input: { job: string; limit: number },
  options: DagsterOptions = {},
): Promise<DagsterRun[]> {
  const data = await graphql<{
    runsOrError: {
      __typename?: string;
      message?: string;
      results?: {
        runId: string;
        status: string;
        jobName: string;
        startTime: number | null;
        endTime: number | null;
        tags?: { key: string; value: string }[];
      }[];
    };
  }>(
    RUNS_QUERY,
    { filter: { pipelineName: input.job }, limit: input.limit },
    options,
  );
  const error = unionError(data.runsOrError, "Runs", `Listing runs of ${input.job}`);
  if (error) throw error;
  return (data.runsOrError.results ?? []).map((run) => ({
    runId: run.runId,
    status: run.status,
    jobName: run.jobName,
    startTime: run.startTime ?? null,
    endTime: run.endTime ?? null,
    tags: tagMap(run.tags),
  }));
}

const RUN_QUERY = `query BackofficeRun($runId: ID!) {
  runOrError(runId: $runId) {
    __typename
    ... on Run { runId status jobName startTime endTime tags { key value } }
    ... on RunNotFoundError { message }
    ... on PythonError { message }
  }
}`;

export async function runStatus(
  runId: string,
  options: DagsterOptions = {},
): Promise<DagsterRun> {
  const data = await graphql<{
    runOrError: {
      __typename?: string;
      message?: string;
      runId?: string;
      status?: string;
      jobName?: string;
      startTime?: number | null;
      endTime?: number | null;
      tags?: { key: string; value: string }[];
    };
  }>(RUN_QUERY, { runId }, options);
  const error = unionError(data.runOrError, "Run", `Reading run ${runId}`);
  if (error) throw error;
  const run = data.runOrError;
  return {
    runId: run.runId ?? runId,
    status: run.status ?? "",
    jobName: run.jobName ?? "",
    startTime: run.startTime ?? null,
    endTime: run.endTime ?? null,
    tags: tagMap(run.tags),
  };
}

/**
 * `assetMaterializations` carries the MaterializeResult metadata, which is where
 * a run says how many companies it selected, inserted and sent to the model.
 * Only integer entries are kept: those are the counts, and asking for every
 * metadata type would make one unexpected entry shape fail the whole query.
 */
const MATERIALIZATIONS_QUERY = `query BackofficeAssetMaterializations($assetKeys: [AssetKeyInput!], $limit: Int!) {
  assetNodes(assetKeys: $assetKeys) {
    id
    assetMaterializations(limit: $limit) {
      runId
      timestamp
      metadataEntries {
        label
        __typename
        ... on IntMetadataEntry { intValue }
      }
    }
  }
}`;

export async function assetMaterializations(
  input: { asset: string; limit: number },
  options: DagsterOptions = {},
): Promise<AssetMaterialization[]> {
  const data = await graphql<{
    assetNodes: {
      assetMaterializations?: {
        runId: string;
        timestamp: number | string | null;
        metadataEntries?: {
          label: string;
          __typename?: string;
          intValue?: number | null;
        }[];
      }[];
    }[];
  }>(
    MATERIALIZATIONS_QUERY,
    { assetKeys: [{ path: [input.asset] }], limit: input.limit },
    options,
  );
  return (data.assetNodes ?? []).flatMap((node) =>
    (node.assetMaterializations ?? []).map((materialization) => ({
      runId: materialization.runId,
      timestamp:
        materialization.timestamp === null || materialization.timestamp === undefined
          ? null
          : Number(materialization.timestamp),
      numbers: Object.fromEntries(
        (materialization.metadataEntries ?? [])
          .filter(
            (entry): entry is { label: string; intValue: number } =>
              typeof entry.intValue === "number",
          )
          .map((entry) => [entry.label, entry.intValue]),
      ),
    })),
  );
}

const INSTIGATORS_QUERY = `query BackofficeInstigators($repositorySelector: RepositorySelector!) {
  schedulesOrError(repositorySelector: $repositorySelector) {
    __typename
    ... on Schedules { results { name cronSchedule scheduleState { status } } }
    ... on PythonError { message }
  }
  sensorsOrError(repositorySelector: $repositorySelector) {
    __typename
    ... on Sensors { results { name sensorState { status } } }
    ... on PythonError { message }
  }
}`;

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
  const data = await graphql<{
    schedulesOrError: {
      __typename?: string;
      message?: string;
      results?: {
        name: string;
        cronSchedule: string;
        scheduleState?: { status?: string };
      }[];
    };
    sensorsOrError: {
      __typename?: string;
      message?: string;
      results?: { name: string; sensorState?: { status?: string } }[];
    };
  }>(
    INSTIGATORS_QUERY,
    {
      repositorySelector: {
        repositoryLocationName: REPOSITORY_LOCATION_NAME,
        repositoryName: REPOSITORY_NAME,
      },
    },
    options,
  );
  const scheduleError = unionError(data.schedulesOrError, "Schedules", "Reading schedules");
  if (scheduleError) throw scheduleError;
  const sensorError = unionError(data.sensorsOrError, "Sensors", "Reading sensors");
  if (sensorError) throw sensorError;
  const wanted = new Set(input.names ?? []);
  const mine = (name: string) => wanted.size === 0 || wanted.has(name);
  return {
    schedules: (data.schedulesOrError.results ?? [])
      .filter((schedule) => mine(schedule.name))
      .map((schedule) => ({
        name: schedule.name,
        status: schedule.scheduleState?.status ?? "UNKNOWN",
        cronSchedule: schedule.cronSchedule,
      })),
    sensors: (data.sensorsOrError.results ?? [])
      .filter((sensor) => mine(sensor.name))
      .map((sensor) => ({
        name: sensor.name,
        status: sensor.sensorState?.status ?? "UNKNOWN",
      })),
  };
}
