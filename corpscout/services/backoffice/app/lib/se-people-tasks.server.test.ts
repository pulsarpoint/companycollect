/**
 * The Tasks tab's data: one row per people asset/job, over `listRuns` /
 * `assetMaterializations` (both already used by admin-se-people-pipeline.tsx).
 * Dagster itself is faked at the module boundary; the job/asset catalog and
 * the guarded-read fallback are real.
 */
import { beforeEach, describe, expect, it, vi } from "vitest";

const listRuns = vi.fn();
const assetMaterializations = vi.fn();

vi.mock("~/lib/dagster.server", async (importOriginal) => ({
  ...(await importOriginal<typeof import("~/lib/dagster.server")>()),
  listRuns: (...args: unknown[]) => listRuns(...args),
  assetMaterializations: (...args: unknown[]) => assetMaterializations(...args),
}));

const { loadSePeopleTasks } = await import("~/lib/se-people-tasks.server");
const { DagsterRequestError, SE_COMPANY_PERSON_ROLE_JOB, SE_COMPANY_PERSON_REVIEW_JOB } =
  await import("~/lib/dagster.server");

beforeEach(() => {
  listRuns.mockReset();
  assetMaterializations.mockReset();
});

describe("loadSePeopleTasks", () => {
  it("covers all eight assets/jobs, the Simple Sync cascade included", async () => {
    listRuns.mockResolvedValue([]);
    const { rows, error } = await loadSePeopleTasks();

    expect(error).toBe("");
    expect(rows.map((row) => row.key)).toEqual([
      "simple-sync",
      "clean-copy",
      "llm-suggestions",
      "promotion",
      "identity-evaluation",
      "merge-suggestions",
      "roles",
      "review",
    ]);
    expect(rows.find((row) => row.key === "roles")?.job).toBe(SE_COMPANY_PERSON_ROLE_JOB);
    expect(rows.find((row) => row.key === "review")?.job).toBe(SE_COMPANY_PERSON_REVIEW_JOB);
  });

  it("reads a successful run's latest materialization for its key metrics", async () => {
    listRuns.mockImplementation(async ({ job }: { job: string }) =>
      job === "se_company_person_publish_job"
        ? [{ runId: "run-1", status: "SUCCESS", startTime: 100, endTime: 130, jobName: job }]
        : [],
    );
    assetMaterializations.mockImplementation(async ({ asset }: { asset: string }) =>
      asset === "se_company_person_clickhouse"
        ? [{ runId: "run-1", timestamp: 130, numbers: { inserted_count: 12, total_person_count: 340 } }]
        : [],
    );

    const { rows } = await loadSePeopleTasks();
    const cleanCopy = rows.find((row) => row.key === "clean-copy");

    expect(cleanCopy?.status).toBe("SUCCESS");
    expect(cleanCopy?.startTime).toBe(100);
    expect(cleanCopy?.endTime).toBe(130);
    expect(cleanCopy?.metrics).toEqual({ inserted_count: 12, total_person_count: 340 });
  });

  it("never asks for materializations on a job with no run, or one that did not succeed", async () => {
    listRuns.mockImplementation(async ({ job }: { job: string }) =>
      job === "se_company_person_merge_job"
        ? [{ runId: "run-2", status: "FAILURE", startTime: 5, endTime: 9, jobName: job }]
        : [],
    );
    assetMaterializations.mockResolvedValue([]);

    await loadSePeopleTasks();

    expect(assetMaterializations).not.toHaveBeenCalled();
  });

  it("skips materialization metrics for the review job -- it selects four assets per run", async () => {
    listRuns.mockImplementation(async ({ job }: { job: string }) =>
      job === SE_COMPANY_PERSON_REVIEW_JOB
        ? [{ runId: "run-3", status: "SUCCESS", startTime: 1, endTime: 2, jobName: job }]
        : [],
    );
    assetMaterializations.mockResolvedValue([]);

    const { rows } = await loadSePeopleTasks();
    const review = rows.find((row) => row.key === "review");

    expect(review?.status).toBe("SUCCESS");
    expect(review?.metrics).toEqual({});
    expect(assetMaterializations).not.toHaveBeenCalled();
  });

  it("degrades to an empty table plus an error message when Dagster is unreachable", async () => {
    listRuns.mockRejectedValue(new DagsterRequestError("Dagster at http://x did not answer"));

    const { rows, error } = await loadSePeopleTasks();

    expect(rows).toEqual([]);
    expect(error).toContain("did not answer");
  });
});
