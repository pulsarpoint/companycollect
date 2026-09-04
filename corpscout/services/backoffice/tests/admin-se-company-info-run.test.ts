import { describe, expect, it, vi } from "vitest";

const dagster = vi.hoisted(() => ({ runStatus: vi.fn() }));
vi.mock("~/lib/dagster.server", () => ({ runStatus: dagster.runStatus }));

import { loader } from "~/routes/admin-se-company-info-run";

describe("info run resource route", () => {
  it("reports whether the run reached a terminal state", async () => {
    dagster.runStatus.mockResolvedValueOnce({ runId: "run-9", status: "STARTED" });
    const running = await loader({ params: { companyId: "0113004022", runId: "run-9" } } as never);
    expect(running).toEqual({ runId: "run-9", status: "STARTED", finished: false });
    dagster.runStatus.mockResolvedValueOnce({ runId: "run-9", status: "SUCCESS" });
    const done = await loader({ params: { companyId: "0113004022", runId: "run-9" } } as never);
    expect(done).toEqual({ runId: "run-9", status: "SUCCESS", finished: true });
  });

  it("resolves to an UNKNOWN, not-finished status when Dagster fails transiently", async () => {
    dagster.runStatus.mockRejectedValueOnce(new Error("gql down"));
    const result = await loader({ params: { companyId: "0113004022", runId: "run-9" } } as never);
    expect(result).toEqual({ runId: "run-9", status: "UNKNOWN", finished: false });
  });
});
