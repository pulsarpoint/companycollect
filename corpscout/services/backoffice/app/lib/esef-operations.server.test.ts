import { beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({
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
) {
  return {
    runId,
    status,
    jobName,
    startTime: 1,
    endTime: status === "SUCCESS" ? 2 : null,
    tags: {},
  };
}

function materialization(runId: string, timestamp: number) {
  return { runId, timestamp, numbers: {} };
}

beforeEach(() => {
  mocks.listRuns.mockReset();
  mocks.assetMaterializations.mockReset();
  mocks.listRuns.mockImplementation(
    async ({ job, statuses }: { job: string; statuses?: string[] }) => {
      if (statuses) return [];
      return job === "esef_document_company_information_job"
        ? [run(job, "SUCCESS")]
        : [];
    },
  );
  mocks.assetMaterializations.mockImplementation(
    async ({ asset }: { asset: string }) => [
      materialization(`${asset}-run`, asset === ESEF_ENRICHMENT_ASSET ? 100 : 200),
    ],
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
        .filter((asset) => asset.role === "input")
        .every((asset) => asset.newerThanOutput),
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

  it("blocks a duplicate enrichment run and missing required input", async () => {
    mocks.listRuns.mockImplementation(
      async ({ job, statuses }: { job: string; statuses?: string[] }) => {
        if (statuses) return [];
        return job === "esef_document_company_information_job"
          ? [run(job, "QUEUED", "enrichment-2")]
          : [];
      },
    );
    mocks.assetMaterializations.mockImplementation(
      async ({ asset }: { asset: string }) =>
        asset === ESEF_INPUT_ASSETS[1]
          ? []
          : [materialization(`${asset}-run`, 100)],
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
});
