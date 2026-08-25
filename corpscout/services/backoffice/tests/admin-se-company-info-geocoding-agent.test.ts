/**
 * The Geocoding tab's own server exports, once the analysis agent is wired in:
 * what the loader adds to the page payload, and what the two action intents
 * do.
 *
 * The agent, the store and the ClickHouse list are faked at their module
 * boundaries -- a real run costs minutes and tokens. What is real here is the
 * branching that decides whether a click starts a run at all, and the promise
 * that the HTTP action returns without waiting for the agent.
 */
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { GeocodeAgentPanel, GeocodeAgentRun } from "~/agents/geocode-analysis-contract";

const RUN: GeocodeAgentRun = {
  id: "11111111-1111-4111-8111-111111111111",
  countryCode: "SE",
  params: { focus: "box addresses", maxIterations: 12, maxRowsPerQuery: 200 },
  status: "queued",
  model: "",
  threadId: "",
  iterations: 0,
  inputTokens: 0,
  outputTokens: 0,
  converged: false,
  reportMd: "",
  errorMessage: "",
  createdAt: "2026-08-25T10:00:00.000Z",
  startedAt: null,
  finishedAt: null,
};

const PANEL: GeocodeAgentPanel = {
  countryCode: "SE",
  available: true,
  unavailableReason: "",
  runs: [RUN],
  suggestions: [],
};

const agent = vi.hoisted(() => ({
  loadPanel: vi.fn(),
  startRun: vi.fn(),
  setStatus: vi.fn(),
  /** The real class is re-declared here so the route's `instanceof` branch is
   * exercised rather than mocked away. */
  ActiveError: class GeocodeAgentRunActiveError extends Error {},
}));

vi.mock("~/agents/geocode-analysis.server", () => ({
  loadGeocodeAgentPanel: agent.loadPanel,
  startGeocodeAnalysisRun: agent.startRun,
}));

vi.mock("~/lib/geocode-agent-store.server", () => ({
  GeocodeAgentRunActiveError: agent.ActiveError,
  setGeocodeAgentSuggestionStatus: agent.setStatus,
}));

vi.mock("~/lib/se-company-geocoding-list.server", () => ({
  listSeCompanyGeocodingPage: vi.fn(async () => ({ rows: [] })),
  loadSeCompanyGeocodingCounts: vi.fn(async () => ({
    total: 10,
    needsAttention: 4,
    geocoded: 6,
    ambiguous: 1,
    unmatched: 2,
    noOutcome: 1,
  })),
  countForFilter: vi.fn(() => 4),
}));

const { action, loader } = await import("~/routes/admin-se-company-info-geocoding");

function post(fields: Record<string, string>): Request {
  const body = new URLSearchParams(fields);
  return new Request("http://localhost/admin/se/company-info/geocoding", {
    method: "POST",
    body,
  });
}

beforeEach(() => {
  vi.clearAllMocks();
  agent.loadPanel.mockResolvedValue(PANEL);
  agent.startRun.mockResolvedValue(RUN);
});

describe("loader", () => {
  it("carries the agent panel alongside the list, for country SE", async () => {
    const data = await loader({
      request: new Request("http://localhost/admin/se/company-info/geocoding"),
      params: {},
      context: {} as never,
    } as never);
    expect(agent.loadPanel).toHaveBeenCalledWith("SE");
    expect((data as { agent: GeocodeAgentPanel }).agent).toEqual(PANEL);
    // The list half of the payload is untouched by this task.
    expect(data).toMatchObject({ total: 4, page: 1, filter: "needs_attention" });
  });
});

describe("action: start_geocode_analysis", () => {
  it("starts a run with the focus directive and answers with the queued row", async () => {
    const response = await action({
      request: post({
        intent: "start_geocode_analysis",
        country: "SE",
        focus: "box addresses",
      }),
      params: {},
      context: {} as never,
    } as never);
    expect(agent.startRun).toHaveBeenCalledWith({
      countryCode: "SE",
      focus: "box addresses",
    });
    expect(response).toEqual({ ok: true, intent: "start", run: RUN });
  });

  it("explains a second click while a run is live instead of queueing another", async () => {
    agent.startRun.mockRejectedValueOnce(new agent.ActiveError("already active"));
    const response = await action({
      request: post({ intent: "start_geocode_analysis" }),
      params: {},
      context: {} as never,
    } as never);
    expect(response).toEqual({
      ok: false,
      error:
        "An analysis run is already active for this country. Wait for it to finish.",
    });
  });

  it("reports a start failure as data, never as a thrown request", async () => {
    agent.startRun.mockRejectedValueOnce(new Error("BACKOFFICE_POSTGRES_URL is not set."));
    const response = await action({
      request: post({ intent: "start_geocode_analysis" }),
      params: {},
      context: {} as never,
    } as never);
    expect(response).toEqual({ ok: false, error: "BACKOFFICE_POSTGRES_URL is not set." });
  });
});

describe("action: set_geocode_suggestion_status", () => {
  it("records an accepted suggestion", async () => {
    agent.setStatus.mockResolvedValueOnce({ id: "s1", status: "accepted" });
    const response = await action({
      request: post({
        intent: "set_geocode_suggestion_status",
        suggestion_id: "s1",
        status: "accepted",
      }),
      params: {},
      context: {} as never,
    } as never);
    expect(agent.setStatus).toHaveBeenCalledWith("s1", "accepted", { decidedBy: "" });
    expect(response).toMatchObject({ ok: true, intent: "decide" });
  });

  it("refuses a status outside the lifecycle, and a missing id", async () => {
    for (const fields of [
      { suggestion_id: "s1", status: "implemented_and_shipped" },
      { suggestion_id: "", status: "accepted" },
    ]) {
      const response = await action({
        request: post({ intent: "set_geocode_suggestion_status", ...fields }),
        params: {},
        context: {} as never,
      } as never);
      expect(response).toEqual({ ok: false, error: "Unknown suggestion or status." });
    }
    expect(agent.setStatus).not.toHaveBeenCalled();
  });

  it("says so when the suggestion has gone", async () => {
    agent.setStatus.mockResolvedValueOnce(null);
    const response = await action({
      request: post({
        intent: "set_geocode_suggestion_status",
        suggestion_id: "s9",
        status: "rejected",
      }),
      params: {},
      context: {} as never,
    } as never);
    expect(response).toEqual({ ok: false, error: "That suggestion no longer exists." });
  });
});

describe("action: anything else", () => {
  it("is rejected by name", async () => {
    const response = await action({
      request: post({ intent: "drop_the_table" }),
      params: {},
      context: {} as never,
    } as never);
    expect(response).toEqual({ ok: false, error: "Unknown geocoding action." });
  });
});
