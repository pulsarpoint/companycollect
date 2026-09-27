import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { action, loader } from "~/routes/admin-provider-feeds";
import { loader as runLoader } from "~/routes/admin-provider-feeds-run";

beforeEach(() => {
  vi.stubEnv("CORPSCOUT_S3_ENDPOINT", "http://s3.test:9000");
  vi.stubEnv("CORPSCOUT_S3_ACCESS_KEY", "access");
  vi.stubEnv("CORPSCOUT_S3_SECRET_KEY", "secret");
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
});
