import { describe, expect, it, vi } from "vitest";
import { loadCompanyProcessing } from "~/lib/company-processing.server";
import { ACTIVE_COMPANY_RUN_STATUSES, COMPANY_ACTION_AREAS } from "~/lib/company-actions";
import { mergeProcessingSnapshot, processingDuration, processingStepLabel, type ProcessingSnapshot } from "~/lib/company-processing";

const run = (id: string, status: string, creationTime: number) => ({ runId: id, jobName: "test", status, creationTime, startTime: creationTime + 1, endTime: status === "SUCCESS" ? creationTime + 42 : null, tags: [{ key: "corpscout/requested_by", value: "operator" }] });
const rows = (results: unknown[] = []) => ({ __typename: "Runs", results });

describe("processing history", () => {
  it("loads all ten jobs, older active runs and independent last successes without run configuration", async () => {
    const fetchImpl = vi.fn(async (_url: RequestInfo | URL, init?: RequestInit) => {
      const { variables, query } = JSON.parse(String(init?.body));
      expect(variables.activeStatuses).toEqual(ACTIVE_COMPANY_RUN_STATUSES);
      expect(query).not.toContain("runConfig");
      expect(init?.signal).toBeInstanceOf(AbortSignal);
      const job = variables.job;
      return new Response(JSON.stringify({ data: {
        recent: rows([run(`${job}-recent`, "FAILURE", 500)]),
        active: rows([{ ...run(`${job}-old-active`, "STARTED", 100), stepStats: [
          { stepKey: "se_company_person_match", status: "IN_PROGRESS" }, { stepKey: "done", status: "SUCCESS" },
        ] }]),
        successful: rows([run(`${job}-success`, "SUCCESS", 50)]),
      } }));
    });
    const snapshot = await loadCompanyProcessing({ url: "http://dagster:3000/graphql", fetchImpl });
    expect(fetchImpl).toHaveBeenCalledTimes(10);
    expect(snapshot.runs).toHaveLength(20);
    expect(snapshot.errors).toEqual({});
    expect(snapshot.runs[0].createdAt).toBe(500);
    expect(snapshot.runs.at(-1)).toMatchObject({ status: "STARTED", operator: "operator", activeSteps: ["se_company_person_match"] });
    for (const area of COMPANY_ACTION_AREAS) for (const [operation, job] of Object.entries(area.jobs)) {
      expect(snapshot.lastSuccess[job]).toMatchObject({ area: area.value, operation, runId: `${job}-success`, status: "SUCCESS" });
    }
  });

  it("reports partial Dagster failures without dropping successful workflows or duplicating active rows", async () => {
    const snapshot = await loadCompanyProcessing({ url: "http://dagster:3000/graphql", fetchImpl: async (_url, init) => {
      const job = JSON.parse(String(init?.body)).variables.job;
      if (job.includes("person")) throw new Error("People connection failed");
      if (job.includes("address")) return new Response(JSON.stringify({ data: { recent: { __typename: "PythonError", message: "History unavailable" }, active: rows(), successful: rows() } }));
      const active = { ...run(job, "STARTED", 100), stepStats: [] };
      return new Response(JSON.stringify({ data: { recent: rows([active]), active: rows([active]), successful: rows() } }));
    } });
    expect(snapshot.errors.people).toContain("People connection failed");
    expect(snapshot.errors.addresses).toContain("History unavailable");
    expect(snapshot.errors.info).toBeUndefined();
    expect(snapshot.runs).toHaveLength(6);
  });

  it("retains last known active runs during an outage, then replaces them with the terminal status", () => {
    const active = { runId: "active", area: "people", operation: "process", status: "STARTED", createdAt: 100, startTime: 110, endTime: null, operator: "user", activeSteps: [], runUrl: null } as const;
    const previous: ProcessingSnapshot = { runs: [{ ...active, activeSteps: [] }], lastSuccess: {}, errors: {}, checkedAt: 120 };
    const outage = mergeProcessingSnapshot(previous, { runs: [], lastSuccess: {}, errors: { people: "offline" }, checkedAt: 130 });
    expect(outage.runs).toEqual(previous.runs);
    const recovered = mergeProcessingSnapshot(outage, { runs: [{ ...previous.runs[0], status: "SUCCESS", endTime: 140 }], lastSuccess: {}, errors: {}, checkedAt: 150 });
    expect(recovered.runs).toHaveLength(1);
    expect(recovered.runs[0].status).toBe("SUCCESS");
    expect(recovered.errors).toEqual({});
    expect(processingDuration(110, 140)).toBe("30s");
    expect(processingStepLabel("se_company_person_match")).toBe("Match people with the LLM");
  });
});
