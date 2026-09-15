import { beforeEach, describe, expect, it, vi } from "vitest";

const launches = vi.hoisted(() => ({ launchCompanyAction: vi.fn() }));
vi.mock("~/lib/company-actions.server", () => launches);
import { action } from "~/routes/admin-se-company-actions";

function submit(fields: Record<string, string>) {
  return action({ request: new Request("http://x/admin/se/company-actions?company=beijer", {
    method: "POST", body: new URLSearchParams(fields),
  }) } as never);
}

describe("company actions route", () => {
  beforeEach(() => { launches.launchCompanyAction.mockReset().mockResolvedValue({ ok: true, error: "", runId: "run" }); });

  it.each(["info", "finance", "addresses", "people", "domains"].flatMap((area) => ["sync", "process"].map((operation) => [area, operation])))
    ("launches %s / %s globally and drops company filters and selections", async (area, operation) => {
      await submit({ area, operation, profile_id: "model-1", prompt_id: "prompt-1", prompt_revision: "3", llm_max_companies: "5000", company_ids: "5560125220", selection: "hostile scope" });
      expect(launches.launchCompanyAction).toHaveBeenCalledWith({ area, operation, profileId: "model-1", promptId: "prompt-1", promptRevision: 3, changedOnly: true, llmMaxCompanies: 5000, verifyDomains: false, requestedBy: expect.any(String) });
    });

  it.each(["true", "false"])("forwards changed_only=%s as a boolean", async (changedOnly) => {
    await submit({ area: "people", operation: "process", changed_only: changedOnly });
    expect(launches.launchCompanyAction).toHaveBeenCalledWith(expect.objectContaining({ changedOnly: changedOnly === "true" }));
  });

  it("rejects an invalid changed_only value before launching", async () => {
    expect(await submit({ area: "people", operation: "process", changed_only: "yes" }))
      .toMatchObject({ data: { ok: false, error: "changed_only must be true or false." }, init: { status: 400 } });
    expect(launches.launchCompanyAction).not.toHaveBeenCalled();
  });

  it("returns launch errors to the dialog", async () => {
    launches.launchCompanyAction.mockRejectedValue(new Error("code location unavailable"));
    expect(await submit({ area: "finance", operation: "sync" }))
      .toMatchObject({ data: { ok: false, error: "code location unavailable" }, init: { status: 400 } });
  });

  it("returns an active-run conflict with a link instead of a successful submission", async () => {
    launches.launchCompanyAction.mockResolvedValue({ ok: false, error: "Already active", runId: "existing", runUrl: "http://dagster:3000/runs/existing" });
    expect(await submit({ area: "people", operation: "sync" })).toMatchObject({
      data: { ok: false, runId: "existing", runUrl: "http://dagster:3000/runs/existing" }, init: { status: 409 },
    });
  });
});
