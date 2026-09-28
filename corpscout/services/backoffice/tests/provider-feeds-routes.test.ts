import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { action, loader } from "~/routes/admin-provider-feeds";
import { action as providerAction } from "~/routes/admin-provider-feeds-provider";
import { loader as runLoader } from "~/routes/admin-provider-feeds-run";

beforeEach(() => {
  vi.stubEnv("CORPSCOUT_S3_ENDPOINT", "http://s3.test:9000");
  vi.stubEnv("CORPSCOUT_S3_ACCESS_KEY", "access");
  vi.stubEnv("CORPSCOUT_S3_SECRET_KEY", "secret");
  vi.stubEnv("DAGSTER_GRAPHQL_URL", "http://dagster.test/graphql");
  vi.stubEnv("PROVIDER_RECON_API_URL", "http://recon.test:8095");
});

afterEach(() => {
  vi.unstubAllEnvs();
  vi.unstubAllGlobals();
});

function unreachable() {
  const fetchMock = vi.fn(async () => {
    throw new TypeError("fetch failed");
  });
  vi.stubGlobal("fetch", fetchMock);
  return fetchMock;
}

describe("provider feeds routes", () => {
  it("index shows an error instead of crashing when the object store is unreachable", async () => {
    unreachable();
    const result = await loader();
    expect(result.error).toContain("fetch failed");
    expect(result.runs).toEqual([]);
  });

  it("saving rules reports an unreachable store as a 502, not a crash", async () => {
    unreachable();
    const body = new URLSearchParams({ missingPercent: "5", removedPercent: "5", addedPercent: "50" });
    const request = new Request("http://localhost/admin/provider-feeds", { method: "POST", body });
    const result = (await action({ request } as never)) as { data: { error: string }; init: { status: number } };
    expect(result.init.status).toBe(502);
    expect(result.data.error).toContain("fetch failed");
  });

  it("an invalid run id is a 404 without touching the object store", async () => {
    const fetchMock = unreachable();
    await expect(runLoader({ params: { runId: "../settings/indicator-rules" } } as never)).rejects.toMatchObject({ init: { status: 404 } });
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it("index loader reports Dagster unreachable in the panel, not as a crash", async () => {
    unreachable();
    const result = await loader();
    expect(result.dagster.error).toContain("fetch failed");
  });

  it.each(["run-now", "schedule-start", "schedule-stop"])("%s reports an unreachable Dagster as a 502", async (intent) => {
    unreachable();
    const request = new Request("http://localhost/admin/provider-feeds", { method: "POST", body: new URLSearchParams({ intent }) });
    const result = (await action({ request } as never)) as { data: { error: string }; init: { status: number } };
    expect(result.init.status).toBe(502);
    expect(result.data.error).toContain("fetch failed");
  });

  it("run-now returns the launched run", async () => {
    vi.stubGlobal("fetch", vi.fn(async () => Response.json({ data: { launchRun: { __typename: "LaunchRunSuccess", run: { runId: "abc", status: "QUEUED" } } } })));
    const request = new Request("http://localhost/admin/provider-feeds", { method: "POST", body: new URLSearchParams({ intent: "run-now" }) });
    const result = (await action({ request } as never)) as { data: { launched: { runId: string; url: string | null } } };
    expect(result.data.launched.runId).toBe("abc");
  });

  it("restore requires the confirmation tick and never calls the service without it", async () => {
    const fetchMock = unreachable();
    const body = new URLSearchParams({ collector: "aws_ip_ranges", removed_since: "2026-09-28" });
    const request = new Request("http://localhost/admin/provider-feeds/providers/aws", { method: "POST", body });
    const result = (await providerAction({ request, params: { slug: "aws" } } as never)) as { data: { error: string }; init: { status: number } };
    expect(result.init.status).toBe(400);
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it("restore shows the service's answer when there is nothing to restore", async () => {
    vi.stubGlobal("fetch", vi.fn(async () => Response.json({ error: "nothing to restore" }, { status: 422 })));
    const body = new URLSearchParams({ collector: "aws_ip_ranges", removed_since: "2099-01-01", confirm: "yes" });
    const request = new Request("http://localhost/admin/provider-feeds/providers/aws", { method: "POST", body });
    const result = (await providerAction({ request, params: { slug: "aws" } } as never)) as { data: { error: string }; init: { status: number } };
    expect(result.init.status).toBe(409);
    expect(result.data.error).toContain("nothing to restore");
  });
});

