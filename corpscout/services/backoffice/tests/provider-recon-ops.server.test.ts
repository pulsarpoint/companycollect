import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  loadProviderReconDagster,
  ProviderReconServiceError,
  restoreProviderRanges,
  runProviderReconNow,
} from "~/lib/provider-recon-ops.server";

beforeEach(() => {
  vi.stubEnv("DAGSTER_GRAPHQL_URL", "http://dagster.test/graphql");
  vi.stubEnv("DAGSTER_UI_URL", "");
  vi.stubEnv("PROVIDER_RECON_API_URL", "http://recon.test:8095");
});
afterEach(() => vi.unstubAllEnvs());

/** Answers Dagster GraphQL by operation name. */
function dagster(answers: Record<string, unknown>) {
  const seen: string[] = [];
  const fetchImpl = vi.fn(async (_u: string | URL | Request, init?: RequestInit) => {
    const { query } = JSON.parse(String(init?.body));
    const op = /(?:query|mutation) (\w+)/.exec(query)?.[1] ?? "";
    seen.push(op);
    if (!(op in answers)) return new Response("no", { status: 500 });
    return Response.json({ data: answers[op] });
  }) as unknown as typeof fetch;
  return { seen, fetchImpl };
}

describe("loadProviderReconDagster", () => {
  it("reports Dagster errors without throwing", async () => {
    const { fetchImpl } = dagster({});
    const panel = await loadProviderReconDagster({ fetchImpl });
    expect(panel.error).toContain("500");
    expect(panel.assets).toEqual([]);
    expect(panel.schedule).toBeNull();
  });
});

describe("runProviderReconNow", () => {
  it("launches the whole provider_recon_job with an empty run config", async () => {
    const { fetchImpl, seen } = dagster({
      BackofficeLaunchRun: { launchRun: { __typename: "LaunchRunSuccess", run: { runId: "r1", status: "QUEUED" } } },
    });
    expect(await runProviderReconNow({ fetchImpl })).toBe("r1");
    expect(seen).toContain("BackofficeLaunchRun");
  });
});

describe("restoreProviderRanges", () => {
  const call = (status: number, body: unknown) =>
    restoreProviderRanges({ provider: "aws", collector: "aws_ip_ranges", removedSince: "2026-09-28" },
      vi.fn(async () => Response.json(body, { status })) as unknown as typeof fetch);

  it("returns the run id and count on success", async () => {
    await expect(call(200, { run_id: "20260928T060000Z-restore", restored: 3 })).resolves.toEqual({ runId: "20260928T060000Z-restore", restored: 3 });
  });

  it.each([
    [409, { error: "an operation is already running", run_id: "x" }, "busy"],
    [404, { error: "no published document" }, "no published document"],
    [422, { error: "aws aws_ip_ranges has no grace-expired removals on or after 2026-09-28: nothing to restore" }, "nothing to restore"],
    [400, { error: "invalid date" }, "invalid date"],
  ])("maps HTTP %i to a clear error", async (status, body, text) => {
    const error = await call(status, body).catch((e: unknown) => e);
    expect(error).toBeInstanceOf(ProviderReconServiceError);
    expect((error as Error).message).toContain(text);
  });

  it("reports an unreachable service", async () => {
    const down = vi.fn(async () => { throw new TypeError("fetch failed"); }) as unknown as typeof fetch;
    await expect(restoreProviderRanges({ provider: "aws", collector: "aws_ip_ranges", removedSince: "2026-09-28" }, down))
      .rejects.toThrow("provider-recon service unreachable");
  });
});
