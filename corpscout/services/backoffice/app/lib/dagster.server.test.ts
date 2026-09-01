import { afterEach, describe, expect, it, vi } from "vitest";
import {
  assetGroup,
  assetMaterializations,
  DagsterGraphQLError,
  DagsterNotConfiguredError,
  DagsterRequestError,
  DagsterRunConfigValidationError,
  dagsterRunUrl,
  instigatorStates,
  launchRun,
  listRuns,
  REPOSITORY_LOCATION_NAME,
  REPOSITORY_NAME,
  runStatus,
  SE_COMPANY_INFO_SCHEDULE,
  SE_COMPANY_INFO_SENSOR,
} from "~/lib/dagster.server";

const URL_OPTION = "http://dagster:3000/graphql";

/** One canned GraphQL response per call, plus the request bodies that asked for
 * them -- the point of most of these tests is the request, not the answer. */
function fetchFake(payloads: unknown[]) {
  const calls: {
    url: string;
    body: { query: string; variables: Record<string, unknown> };
  }[] = [];
  const impl = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
    calls.push({
      url: String(input),
      body: JSON.parse(String(init?.body)),
    });
    return new Response(JSON.stringify(payloads.shift() ?? {}), {
      status: 200,
      headers: { "content-type": "application/json" },
    });
  });
  return { impl: impl as unknown as typeof fetch, calls };
}

afterEach(() => {
  vi.unstubAllEnvs();
});

describe("launchRun", () => {
  it("sends the selector, the run config and the tags, and returns the run", async () => {
    const { impl, calls } = fetchFake([
      {
        data: {
          launchRun: {
            __typename: "LaunchRunSuccess",
            run: { runId: "abc", status: "QUEUED" },
          },
        },
      },
    ]);

    await expect(
      launchRun(
        {
          job: "se_company_info_review_job",
          runConfig: {
            ops: { se_company_info_clickhouse: { config: { execute: true } } },
          },
          tags: { pilot: "backoffice" },
        },
        { fetchImpl: impl, url: URL_OPTION },
      ),
    ).resolves.toEqual({ runId: "abc", status: "QUEUED" });

    expect(calls[0].url).toBe(URL_OPTION);
    expect(calls[0].body.variables).toEqual({
      executionParams: {
        selector: {
          repositoryLocationName: REPOSITORY_LOCATION_NAME,
          repositoryName: REPOSITORY_NAME,
          jobName: "se_company_info_review_job",
        },
        runConfigData: {
          ops: { se_company_info_clickhouse: { config: { execute: true } } },
        },
        mode: "default",
        executionMetadata: { tags: [{ key: "pilot", value: "backoffice" }] },
      },
    });
    // No assetSelection at all when none was asked for -- an empty list would run
    // nothing rather than the whole job.
    expect(calls[0].body.query).toContain("mutation BackofficeLaunchRun");
  });

  it("narrows to an asset selection as AssetKeyInput paths", async () => {
    const { impl, calls } = fetchFake([
      {
        data: {
          launchRun: {
            __typename: "LaunchRunSuccess",
            run: { runId: "r1", status: "QUEUED" },
          },
        },
      },
    ]);

    await launchRun(
      {
        job: "se_company_info_job",
        assetSelection: ["se_company_info_scb_clickhouse"],
        runConfig: {},
      },
      { fetchImpl: impl, url: URL_OPTION },
    );

    const params = calls[0].body.variables.executionParams as {
      selector: { assetSelection: { path: string[] }[] };
    };
    expect(params.selector.assetSelection).toEqual([
      { path: ["se_company_info_scb_clickhouse"] },
    ]);
  });

  it("turns a typed union failure into an error instead of a silent no-op", async () => {
    // Dagster reports a bad run config as a TYPE, not as a GraphQL error: an
    // unhandled union member would otherwise read as "launched, no run id".
    const { impl } = fetchFake([
      {
        data: {
          launchRun: {
            __typename: "RunConfigValidationInvalid",
            pipelineName: "se_company_info_review_job",
            errors: [
              {
                __typename: "FieldNotDefinedConfigError",
                message: 'Received unexpected config entry "llm"',
                path: ["root", "ops", "se_company_info_clickhouse", "config"],
                reason: "FIELD_NOT_DEFINED",
              },
            ],
          },
        },
      },
    ]);

    const launch = launchRun(
      { job: "se_company_info_review_job", runConfig: {} },
      { fetchImpl: impl, url: URL_OPTION },
    );
    await expect(launch).rejects.toBeInstanceOf(
      DagsterRunConfigValidationError,
    );
    await expect(launch).rejects.toMatchObject({
      errors: [
        {
          message: 'Received unexpected config entry "llm"',
          path: ["root", "ops", "se_company_info_clickhouse", "config"],
          reason: "FIELD_NOT_DEFINED",
        },
      ],
    });
  });
});

describe("listRuns and runStatus", () => {
  it("filters by job name and flattens the tag list", async () => {
    const { impl, calls } = fetchFake([
      {
        data: {
          runsOrError: {
            __typename: "Runs",
            results: [
              {
                runId: "r1",
                status: "SUCCESS",
                jobName: "se_company_info_job",
                startTime: 1_770_000_000,
                endTime: 1_770_000_600,
                runConfig: { ops: { example: { config: { limit: 5 } } } },
                assetSelection: [{ path: ["se_company_info_clickhouse"] }],
                tags: [{ key: "pilot", value: "backoffice" }],
              },
            ],
          },
        },
      },
    ]);

    await expect(
      listRuns(
        { job: "se_company_info_job", limit: 5 },
        { fetchImpl: impl, url: URL_OPTION },
      ),
    ).resolves.toEqual([
      {
        runId: "r1",
        status: "SUCCESS",
        jobName: "se_company_info_job",
        startTime: 1_770_000_000,
        endTime: 1_770_000_600,
        runConfig: { ops: { example: { config: { limit: 5 } } } },
        selectedAssets: ["se_company_info_clickhouse"],
        tags: { pilot: "backoffice" },
      },
    ]);
    expect(calls[0].body.variables).toEqual({
      filter: { pipelineName: "se_company_info_job" },
      limit: 5,
    });
  });

  it("can ask Dagster only for unfinished runs", async () => {
    const { impl, calls } = fetchFake([
      {
        data: {
          runsOrError: {
            __typename: "Runs",
            results: [],
          },
        },
      },
    ]);

    await listRuns(
      {
        job: "esef_filings_backfill_job",
        limit: 1,
        statuses: ["QUEUED", "STARTED"],
      },
      { fetchImpl: impl, url: URL_OPTION },
    );

    expect(calls[0].body.variables).toEqual({
      filter: {
        pipelineName: "esef_filings_backfill_job",
        statuses: ["QUEUED", "STARTED"],
      },
      limit: 1,
    });
  });

  it("reads one run by id", async () => {
    const { impl, calls } = fetchFake([
      {
        data: {
          runOrError: {
            __typename: "Run",
            runId: "r2",
            status: "STARTED",
            jobName: "se_company_info_review_job",
            startTime: 1_770_000_000,
            endTime: null,
            tags: [],
          },
        },
      },
    ]);

    const run = await runStatus("r2", { fetchImpl: impl, url: URL_OPTION });
    expect(run.status).toBe("STARTED");
    expect(run.endTime).toBeNull();
    expect(calls[0].body.variables).toEqual({ runId: "r2" });
  });

  it("raises when the runs query answers with an error member", async () => {
    const { impl } = fetchFake([
      { data: { runsOrError: { __typename: "PythonError", message: "boom" } } },
    ]);
    await expect(
      listRuns(
        { job: "se_company_info_job", limit: 5 },
        { fetchImpl: impl, url: URL_OPTION },
      ),
    ).rejects.toThrow(/PythonError.*boom/);
  });
});

describe("assetGroup", () => {
  it("loads one exact repository group and maps operational asset state", async () => {
    const { impl, calls } = fetchFake([
      {
        data: {
          assetNodes: [
            {
              id: "asset-1",
              assetKey: { path: ["esef_filing_facts_duckdb"] },
              groupName: "esef",
              description: "Normalised XBRL facts",
              jobNames: ["esef_filings_backfill_job", "__ASSET_JOB"],
              kinds: ["duckdb", "python"],
              dependencyKeys: [{ path: ["esef_document_artifacts_s3"] }],
              staleStatus: "FRESH",
              partitionDefinition: { type: "TIME_WINDOW" },
              assetMaterializations: [
                { runId: "facts-run", timestamp: "1770000000000" },
              ],
            },
          ],
        },
      },
    ]);

    await expect(
      assetGroup("esef", { fetchImpl: impl, url: URL_OPTION }),
    ).resolves.toEqual([
      {
        asset: "esef_filing_facts_duckdb",
        description: "Normalised XBRL facts",
        groupName: "esef",
        kinds: ["duckdb", "python"],
        dependencies: ["esef_document_artifacts_s3"],
        jobNames: ["esef_filings_backfill_job", "__ASSET_JOB"],
        staleStatus: "FRESH",
        partitioned: true,
        materialization: {
          runId: "facts-run",
          timestamp: 1_770_000_000_000,
          numbers: {},
        },
      },
    ]);
    expect(calls[0].body.variables).toEqual({
      group: {
        groupName: "esef",
        repositoryLocationName: REPOSITORY_LOCATION_NAME,
        repositoryName: REPOSITORY_NAME,
      },
    });
  });
});

describe("assetMaterializations", () => {
  it("keeps the integer metadata a run reported and drops the rest", async () => {
    const { impl, calls } = fetchFake([
      {
        data: {
          assetNodes: [
            {
              assetMaterializations: [
                {
                  runId: "r1",
                  timestamp: "1770000000000",
                  metadataEntries: [
                    {
                      __typename: "IntMetadataEntry",
                      label: "selected_company_count",
                      intValue: 42,
                    },
                    {
                      __typename: "IntMetadataEntry",
                      label: "llm_request_count",
                      intValue: 7,
                    },
                    { __typename: "TextMetadataEntry", label: "table" },
                  ],
                },
              ],
            },
          ],
        },
      },
    ]);

    await expect(
      assetMaterializations(
        { asset: "se_company_info_clickhouse", limit: 10 },
        { fetchImpl: impl, url: URL_OPTION },
      ),
    ).resolves.toEqual([
      {
        runId: "r1",
        timestamp: 1_770_000_000_000,
        numbers: { selected_company_count: 42, llm_request_count: 7 },
      },
    ]);
    expect(calls[0].body.variables).toEqual({
      assetKeys: [{ path: ["se_company_info_clickhouse"] }],
      limit: 10,
    });
  });
});

describe("instigatorStates", () => {
  /** The real repository answers with 52 schedules and 15 sensors; the query
   * takes a repository selector and nothing else, so the filtering is ours. */
  const ROSTER = [
    {
      data: {
        schedulesOrError: {
          __typename: "Schedules",
          results: [
            {
              name: "norway_brreg_weekly",
              cronSchedule: "0 3 * * 2",
              scheduleState: { status: "RUNNING" },
            },
            {
              name: "se_company_info_weekly",
              cronSchedule: "50 6 * * 1",
              scheduleState: { status: "RUNNING" },
            },
            {
              name: "ted_procurement_daily",
              cronSchedule: "0 1 * * *",
              scheduleState: { status: "STOPPED" },
            },
          ],
        },
        sensorsOrError: {
          __typename: "Sensors",
          results: [
            {
              name: "se_company_person_correction_sensor",
              sensorState: { status: "RUNNING" },
            },
            {
              name: "se_company_info_field_value_sensor",
              sensorState: { status: "STOPPED" },
            },
          ],
        },
      },
    },
  ];

  it("reports only the instigators it was asked about", async () => {
    const { impl, calls } = fetchFake([...ROSTER]);

    await expect(
      instigatorStates(
        { names: [SE_COMPANY_INFO_SCHEDULE, SE_COMPANY_INFO_SENSOR] },
        { fetchImpl: impl, url: URL_OPTION },
      ),
    ).resolves.toEqual({
      schedules: [
        {
          name: "se_company_info_weekly",
          status: "RUNNING",
          cronSchedule: "50 6 * * 1",
        },
      ],
      sensors: [
        { name: "se_company_info_field_value_sensor", status: "STOPPED" },
      ],
    });
    expect(calls[0].body.variables).toEqual({
      repositorySelector: {
        repositoryLocationName: REPOSITORY_LOCATION_NAME,
        repositoryName: REPOSITORY_NAME,
      },
    });
  });

  it("returns the whole roster when no names are given", async () => {
    const { impl } = fetchFake([...ROSTER]);
    const states = await instigatorStates(
      {},
      { fetchImpl: impl, url: URL_OPTION },
    );
    expect(states.schedules).toHaveLength(3);
    expect(states.sensors).toHaveLength(2);
  });

  it("names an instigator that is not in the repository by simply omitting it", async () => {
    const { impl } = fetchFake([...ROSTER]);
    const states = await instigatorStates(
      { names: ["se_company_info_weekly", "renamed_away"] },
      { fetchImpl: impl, url: URL_OPTION },
    );
    expect(states.schedules.map((entry) => entry.name)).toEqual([
      "se_company_info_weekly",
    ]);
    expect(states.sensors).toEqual([]);
  });
});

describe("transport failures", () => {
  it("names the missing configuration rather than fetching undefined", async () => {
    vi.stubEnv("DAGSTER_GRAPHQL_URL", "");
    const { impl } = fetchFake([]);
    await expect(
      listRuns({ job: "se_company_info_job", limit: 1 }, { fetchImpl: impl }),
    ).rejects.toBeInstanceOf(DagsterNotConfiguredError);
    expect(impl).not.toHaveBeenCalled();
  });

  it("separates a non-answer from an error answer", async () => {
    const failing = vi.fn(async () => {
      throw new Error("ECONNREFUSED");
    }) as unknown as typeof fetch;
    await expect(
      listRuns(
        { job: "se_company_info_job", limit: 1 },
        { fetchImpl: failing, url: URL_OPTION },
      ),
    ).rejects.toBeInstanceOf(DagsterRequestError);

    const http500 = vi.fn(
      async () => new Response("nope", { status: 500 }),
    ) as unknown as typeof fetch;
    await expect(
      listRuns(
        { job: "se_company_info_job", limit: 1 },
        { fetchImpl: http500, url: URL_OPTION },
      ),
    ).rejects.toThrow(/HTTP 500/);

    const graphqlErrors = vi.fn(
      async () =>
        new Response(
          JSON.stringify({ errors: [{ message: "unknown field" }] }),
          { status: 200 },
        ),
    ) as unknown as typeof fetch;
    await expect(
      listRuns(
        { job: "se_company_info_job", limit: 1 },
        { fetchImpl: graphqlErrors, url: URL_OPTION },
      ),
    ).rejects.toBeInstanceOf(DagsterGraphQLError);
  });
});

describe("dagsterRunUrl", () => {
  it("prefers the explicit UI url and otherwise drops /graphql", () => {
    vi.stubEnv("DAGSTER_UI_URL", "");
    vi.stubEnv("DAGSTER_GRAPHQL_URL", "http://dagster:3000/graphql");
    expect(dagsterRunUrl("r1")).toBe("http://dagster:3000/runs/r1");
    vi.stubEnv("DAGSTER_UI_URL", "https://dagster.example/");
    expect(dagsterRunUrl("r1")).toBe("https://dagster.example/runs/r1");
    // Unconfigured is a missing link, never a broken one.
    vi.stubEnv("DAGSTER_UI_URL", "");
    vi.stubEnv("DAGSTER_GRAPHQL_URL", "");
    expect(dagsterRunUrl("r1")).toBeNull();
  });
});
