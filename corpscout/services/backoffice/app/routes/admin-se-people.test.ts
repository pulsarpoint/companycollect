/**
 * `/admin/se/people`'s action: the Simple Sync sheet's two intents.
 * `confirm-simple-sync` answers with the preview `se-people-simple-sync.server.ts`
 * computes; `launch-simple-sync` must call Dagster with EXACTLY the combined
 * role_draft+person+role cascade (`SE_COMPANY_PERSON_JOB` +
 * `buildSimpleSyncRunConfig`, `se-company-person-pipeline.server.ts`) --
 * deliberately NOT the Pipeline page's own single-asset clean-copy launch
 * (`SE_COMPANY_PERSON_PUBLISH_JOB` + `buildCleanCopyRunConfig`, person table
 * only, which this file does not touch). This pins the launched job name and
 * its three-op selection so a future job-shape change breaks loudly here
 * instead of silently skipping role assignments in production.
 */
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { SimpleSyncPreview } from "~/lib/se-people-simple-sync.server";

const launchRun = vi.fn(async () => ({ runId: "run-1", status: "QUEUED" }));
const loadSimpleSyncPreview = vi.fn(async (): Promise<SimpleSyncPreview> => PREVIEW);

const PREVIEW: SimpleSyncPreview = {
  companyCount: 42,
  personCount: 50,
  bySource: [
    { source: "bolagsverket", companyCount: 30, personCount: 35 },
    { source: "esef", companyCount: 10, personCount: 12 },
    { source: "wikidata", companyCount: 2, personCount: 3 },
  ],
  sample: [{ name: "Anna Svensson", companyId: "5560125220", source: "bolagsverket" }],
  sampleSize: 20,
};

vi.mock("~/lib/dagster.server", async (importOriginal) => ({
  ...(await importOriginal<typeof import("~/lib/dagster.server")>()),
  launchRun: (...args: unknown[]) => launchRun(...(args as [])),
}));

vi.mock("~/lib/se-people-simple-sync.server", async (importOriginal) => ({
  ...(await importOriginal<typeof import("~/lib/se-people-simple-sync.server")>()),
  loadSimpleSyncPreview: () => loadSimpleSyncPreview(),
}));

// The tasks/tab data path is exercised by se-people-tasks.server.test.ts and
// se-people-sources.server.ts's own dispatch -- not re-mocked here, since the
// loader is not what this file is asserting on.

const { action } = await import("~/routes/admin-se-people");

beforeEach(() => {
  launchRun.mockClear();
  loadSimpleSyncPreview.mockClear();
  loadSimpleSyncPreview.mockImplementation(async () => PREVIEW);
});

type ActionResult = Awaited<ReturnType<typeof action>>;

function post(fields: Record<string, string>): Promise<ActionResult> {
  const body = new FormData();
  for (const [key, value] of Object.entries(fields)) body.append(key, value);
  return action({
    request: new Request("http://backoffice/admin/se/people", { method: "POST", body }),
  } as unknown as Parameters<typeof action>[0]);
}

describe("confirm-simple-sync", () => {
  it("answers with the live preview, not a canned confirmation", async () => {
    const result = await post({ intent: "confirm-simple-sync" });
    expect(result).toEqual({ kind: "preview", preview: PREVIEW });
    expect(loadSimpleSyncPreview).toHaveBeenCalledTimes(1);
  });

  it("surfaces a Dagster/ClickHouse failure as an error result, not a thrown exception", async () => {
    loadSimpleSyncPreview.mockRejectedValueOnce(new Error("ClickHouse: TIMEOUT"));
    await expect(post({ intent: "confirm-simple-sync" })).rejects.toThrow("TIMEOUT");
  });
});

describe("launch-simple-sync", () => {
  it("launches se_company_person_job (role_draft+person+role) with an all-companies config and the pilot tag", async () => {
    const result = await post({ intent: "launch-simple-sync" });

    // `dagsterRunUrl` derives from DAGSTER_GRAPHQL_URL (backoffice/.env, loaded
    // via dagster.server.ts's own `dotenv/config`) when DAGSTER_UI_URL is unset.
    expect(result).toEqual({
      kind: "launched",
      runId: "run-1",
      url: "http://dagster:3000/runs/run-1",
      job: "se_company_person_job",
    });
    expect(launchRun).toHaveBeenCalledTimes(1);
    const [input] = launchRun.mock.calls[0] as unknown as [
      {
        job: string;
        runConfig: { ops: Record<string, { config: Record<string, unknown> }> };
        tags: Record<string, string>;
      },
    ];
    // Pinned so a future job-shape change (dagster_v3's se_company_person_job
    // definition) breaks this test loudly instead of silently dropping role
    // assignments from what Simple Sync publishes.
    expect(input.job).toBe("se_company_person_job");
    expect(input.tags).toEqual({ pilot: "backoffice" });
    const { ops } = input.runConfig;
    expect(Object.keys(ops).sort()).toEqual(
      [
        "se_company_person_clickhouse",
        "se_company_person_role_clickhouse",
        "se_company_person_role_draft_clickhouse",
      ].sort(),
    );

    const personConfig = ops.se_company_person_clickhouse.config;
    // Every company, exactly like the Pipeline page's own default-scope
    // clean-copy launch (se-company-person-pipeline.ts's normalizeCompanyIdScope([])).
    expect(personConfig.company_ids).toEqual([]);
    expect(personConfig).not.toHaveProperty("execute");
    expect(personConfig).not.toHaveProperty("llm_profile");

    // The two role ops (company_people/roles.py's SECompanyPersonRoleConfig)
    // take ONLY company_ids -- no max_companies/company_batch_size field
    // exists on that config class, unlike the person op's.
    expect(ops.se_company_person_role_draft_clickhouse.config).toEqual({
      company_ids: [],
    });
    expect(ops.se_company_person_role_clickhouse.config).toEqual({
      company_ids: [],
    });
  });
});

describe("an unknown intent", () => {
  it("is refused without calling Dagster", async () => {
    const result = await post({ intent: "delete-everything" });
    expect(result).toEqual({ kind: "error", error: "Unknown action." });
    expect(launchRun).not.toHaveBeenCalled();
  });
});
