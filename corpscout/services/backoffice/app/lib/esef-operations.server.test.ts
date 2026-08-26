import { beforeEach, describe, expect, it, vi } from "vitest";
import type { DagsterAsset, DagsterRun } from "~/lib/dagster.server";

const mocks = vi.hoisted(() => ({
  assetGroup: vi.fn(),
  assetMaterializations: vi.fn(),
  listRuns: vi.fn(),
}));

vi.mock("~/lib/dagster.server", () => mocks);

const {
  assertEsefLaunchAllowed,
  ESEF_ENRICHMENT_ASSET,
  ESEF_INPUT_ASSETS,
  loadEsefOperationsStatus,
} = await import("~/lib/esef-operations.server");

function run(
  jobName: string,
  status: "QUEUED" | "STARTED" | "SUCCESS",
  runId = `${jobName}-run`,
  selectedAssets: string[] | null = null,
): DagsterRun {
  return {
    runId,
    status,
    jobName,
    startTime: 1,
    endTime: status === "SUCCESS" ? 2 : null,
    runConfig: {},
    selectedAssets,
    tags: {},
  };
}

function asset(
  name: string,
  timestamp: number | null,
  jobNames: string[] = ["__ASSET_JOB"],
): DagsterAsset {
  return {
    asset: name,
    description: "",
    groupName: "esef",
    kinds: ["clickhouse"],
    dependencies: [],
    jobNames,
    staleStatus: timestamp === null ? "MISSING" : "FRESH",
    partitioned: false,
    materialization:
      timestamp === null
        ? null
        : { runId: `${name}-run`, timestamp, numbers: {} },
  };
}

function assetInventory(
  overrides: Partial<Record<string, number | null>> = {},
): DagsterAsset[] {
  return [
    ...ESEF_INPUT_ASSETS.map((name) =>
      asset(name, overrides[name] === undefined ? 200 : overrides[name]!, [
        "esef_filings_backfill_job",
        "esef_filings_refresh_job",
        "__ASSET_JOB",
      ]),
    ),
    asset(
      ESEF_ENRICHMENT_ASSET,
      overrides[ESEF_ENRICHMENT_ASSET] === undefined
        ? 100
        : overrides[ESEF_ENRICHMENT_ASSET]!,
      ["esef_document_company_information_job", "__ASSET_JOB"],
    ),
    asset("esef_filing_facts_duckdb", 200, [
      "esef_filings_backfill_job",
      "__ASSET_JOB",
    ]),
  ];
}

beforeEach(() => {
  mocks.assetGroup.mockReset();
  mocks.assetMaterializations.mockReset();
  mocks.listRuns.mockReset();
  mocks.assetGroup.mockResolvedValue(assetInventory());
  mocks.assetMaterializations.mockResolvedValue([
    {
      runId: "enrichment-run",
      timestamp: 100,
      numbers: {
        attempted_document_count: 10,
        processed_document_count: 9,
        failed_document_count: 1,
      },
    },
  ]);
  mocks.listRuns.mockImplementation(
    async ({ job, statuses }: { job: string; statuses?: string[] }) => {
      if (statuses) return [];
      return job === "esef_document_company_information_job"
        ? [run(job, "SUCCESS")]
        : [];
    },
  );
});

describe("loadEsefOperationsStatus", () => {
  it("marks enrichment out of sync when any direct input is newer", async () => {
    const status = await loadEsefOperationsStatus();

    expect(status.syncState).toBe("out_of_sync");
    expect(status.canLaunch).toBe(true);
    expect(status.latestEnrichmentRun?.status).toBe("SUCCESS");
    expect(
      status.assets
        .filter((item) => item.role === "input")
        .every((item) => item.newerThanOutput),
    ).toBe(true);
    await expect(assertEsefLaunchAllowed()).resolves.toBeUndefined();
  });

  it("blocks while an input job still has unfinished runs", async () => {
    mocks.listRuns.mockImplementation(
      async ({ job, statuses }: { job: string; statuses?: string[] }) => {
        if (job === "esef_filings_backfill_job" && statuses) {
          return [run(job, "STARTED", "backfill-157")];
        }
        return [];
      },
    );

    const status = await loadEsefOperationsStatus();

    expect(status.syncState).toBe("inputs_updating");
    expect(status.canLaunch).toBe(false);
    expect(status.blockingReasons[0]).toContain("backfill-157");
    await expect(assertEsefLaunchAllowed()).rejects.toThrow(
      "esef_filings_backfill_job",
    );
  });

  it("blocks a duplicate named enrichment run and a missing required input", async () => {
    mocks.assetGroup.mockResolvedValue(
      assetInventory({ [ESEF_INPUT_ASSETS[1]]: null }),
    );
    mocks.listRuns.mockImplementation(
      async ({ job, statuses }: { job: string; statuses?: string[] }) => {
        if (job === "esef_document_company_information_job" && statuses) {
          return [run(job, "QUEUED", "enrichment-2")];
        }
        return [];
      },
    );

    const status = await loadEsefOperationsStatus();

    expect(status.syncState).toBe("materializing");
    expect(status.canLaunch).toBe(false);
    expect(status.blockingReasons).toEqual(
      expect.arrayContaining([
        expect.stringContaining("enrichment-2"),
        expect.stringContaining(ESEF_INPUT_ASSETS[1]),
      ]),
    );
  });

  it("recognizes a manual Dagster asset launch of the enrichment asset", async () => {
    mocks.listRuns.mockImplementation(
      async ({ job, statuses }: { job: string; statuses?: string[] }) => {
        if (job === "__ASSET_JOB" && statuses) {
          return [
            run("__ASSET_JOB", "STARTED", "manual-enrichment", [
              ESEF_ENRICHMENT_ASSET,
            ]),
          ];
        }
        return [];
      },
    );

    const status = await loadEsefOperationsStatus();

    expect(status.syncState).toBe("materializing");
    expect(status.canLaunch).toBe(false);
    expect(status.blockingReasons[0]).toContain("manual-enrichment");
  });

  it("reports a completed batch with document failures as partial", async () => {
    mocks.assetGroup.mockResolvedValue(
      assetInventory({
        [ESEF_ENRICHMENT_ASSET]: 300,
        [ESEF_INPUT_ASSETS[0]]: 200,
        [ESEF_INPUT_ASSETS[1]]: 200,
        [ESEF_INPUT_ASSETS[2]]: 200,
      }),
    );

    const status = await loadEsefOperationsStatus();

    expect(status.syncState).toBe("partially_processed");
    expect(status.canLaunch).toBe(true);
    expect(status.latestBatch?.numbers).toMatchObject({
      attempted_document_count: 10,
      processed_document_count: 9,
      failed_document_count: 1,
    });
  });
});
