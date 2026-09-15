import { renderToStaticMarkup } from "react-dom/server";
import { createMemoryRouter, RouterProvider } from "react-router";
import { describe, expect, it, vi } from "vitest";
import type { ProcessingRun, ProcessingSnapshot } from "~/lib/company-processing";

const server = vi.hoisted(() => ({ loadCompanyProcessing: vi.fn() }));
vi.mock("~/lib/company-processing.server", () => server);
vi.mock("~/lib/llm-settings.server", () => ({ listLlmProfiles: () => [{ profileId: "model", name: "Model", provider: "openrouter", model: "example/model", isActive: true, apiKeyEnvironmentVariable: "PRIVATE_ENV_NAME" }] }));
vi.mock("~/lib/people-prompts.server", () => ({ listPeoplePrompts: () => [] }));
vi.mock("~/lib/domain-prompts.server", () => ({ listDomainPrompts: () => [] }));
import AdminSeProcessing, { loader } from "~/routes/admin-se-processing";

const empty: ProcessingSnapshot = { runs: [], lastSuccess: {}, errors: {}, checkedAt: 1_789_388_200 };
const run: ProcessingRun = { runId: "run", area: "people", operation: "process", status: "STARTED", createdAt: 1_789_388_000, startTime: 1_789_388_050, endTime: null, operator: "reviewer", activeSteps: ["se_company_person_match"], runUrl: "http://dagster:3000/runs/run" };

function render(snapshot: ProcessingSnapshot) {
  const element = <AdminSeProcessing {...{ loaderData: { snapshot, profiles: [], prompts: [] } } as unknown as Parameters<typeof AdminSeProcessing>[0]} />;
  const router = createMemoryRouter([{ path: "/admin/se/processing", element }], { initialEntries: ["/admin/se/processing"] });
  return renderToStaticMarkup(<RouterProvider router={router} />);
}

describe("Processing page", () => {
  it("loads Dagster status and only the model fields needed by the dialogs", async () => {
    server.loadCompanyProcessing.mockResolvedValue(empty);
    const data = await loader();
    expect(data.snapshot).toBe(empty);
    expect(data.profiles[0]).toEqual({ profileId: "model", name: "Model", provider: "openrouter", model: "example/model", isActive: true });
  });

  it("shows five global workflows and two actions each, with configuration deferred to dialogs", () => {
    const html = render(empty);
    for (const area of ["Company info", "Finance", "Addresses", "People", "Domains"]) {
      expect(html).toContain(`aria-label="${area}: Sync inputs"`);
      expect(html).toContain(`aria-label="${area}: Full processing"`);
    }
    expect(html).toContain("all Swedish companies");
    expect(html).toContain("No completed runs yet");
    expect(html).not.toContain("Active runs");
    expect(html).not.toContain("company-action-llm");
  });

  it("shows current steps and disables both actions for an active workflow", () => {
    const html = render({ ...empty, runs: [run] });
    expect(html).toContain("Active runs");
    expect(html).toContain("Match people with the LLM");
    expect(html).toContain("http://dagster:3000/runs/run");
    expect(html).toContain("reviewer");
    for (const action of ["Sync inputs", "Full processing"]) {
      const button = html.match(new RegExp(`<button[^>]*aria-label="People: ${action}"[^>]*>`))?.[0];
      expect(button).toContain('disabled=""');
    }
    const finance = html.match(/<button[^>]*aria-label="Finance: Sync inputs"[^>]*>/)?.[0];
    expect(finance).not.toContain('disabled=""');
  });

  it("labels stale status and blocks launches when Dagster cannot be reached", () => {
    const html = render({ ...empty, runs: [run], errors: { people: "offline" } });
    expect(html).toContain("Dagster status unavailable for People");
    expect(html).toContain("Last known status");
    expect(html).toContain("History is partially unavailable");
    expect(html.match(/<button[^>]*aria-label="People: Full processing"[^>]*>/)?.[0]).toContain('disabled=""');
  });

  it("renders completed runs and preserves last success independently of recent failures", () => {
    const success = { ...run, status: "SUCCESS", endTime: run.startTime! + 40 };
    const html = render({ ...empty, runs: [{ ...run, status: "FAILURE", endTime: run.startTime! + 20 }], lastSuccess: { se_company_person_refresh_job: success } });
    expect(html).toContain("Failed");
    expect(html).toContain("20s");
    expect(html).toContain("Successful run run");
    expect(html).toContain("processing-workflow");
    expect(html).toContain("processing-status");
    expect(html).not.toContain("Active runs");
  });
});
