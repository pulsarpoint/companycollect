import { beforeEach, describe, expect, it, vi } from "vitest";

const brave = vi.hoisted(() => ({ launchSeCompanyBraveAnalysis: vi.fn() }));
vi.mock("~/lib/se-company-brave.server", () => brave);
import { action } from "~/routes/admin-se-companies-info";

function submit(body: unknown) {
  return action({ request: new Request("http://localhost/admin/se/companies?page=5&status=active", {
    method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body),
  }) } as never);
}

describe("Brave action on the companies list", () => {
  beforeEach(() => { brave.launchSeCompanyBraveAnalysis.mockReset(); });

  it("uses the submitted selection rather than page or URL filters", async () => {
    const selection = { mode: "ids", companyIds: ["5560004615"] };
    brave.launchSeCompanyBraveAnalysis.mockResolvedValue({ ok: true, taskId: "task", runId: "run", runUrl: "http://dagster/runs/run" });
    expect(await submit({ action: "brave_analysis", selection })).toMatchObject({ data: { ok: true, runId: "run" } });
    expect(brave.launchSeCompanyBraveAnalysis).toHaveBeenCalledWith(selection, expect.any(String));
  });

  it("rejects unsupported actions without launching", async () => {
    expect(await submit({ action: "sync" })).toMatchObject({ data: { ok: false }, init: { status: 400 } });
    expect(brave.launchSeCompanyBraveAnalysis).not.toHaveBeenCalled();
  });

  it("returns launch errors for display while retaining the user's selection", async () => {
    brave.launchSeCompanyBraveAnalysis.mockRejectedValue(new Error("Dagster unavailable"));
    expect(await submit({ action: "brave_analysis", selection: {} })).toMatchObject({
      data: { ok: false, error: "Dagster unavailable" }, init: { status: 400 },
    });
  });
});
