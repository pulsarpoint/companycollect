import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { DEFAULT_INDICATOR_RULES } from "~/lib/provider-recon";
import {
  loadIndicatorRules,
  loadManifest,
  loadProviderDocument,
  loadRunIndex,
  saveIndicatorRules,
} from "~/lib/provider-recon.server";

const S3 = "http://s3.test:9000";

function recorder(respond: (url: string, method: string) => Response) {
  const calls: { url: string; method: string; body: unknown }[] = [];
  const fetchImpl = vi.fn(async (input: string | URL | Request, init?: RequestInit) => {
    const call = { url: String(input), method: init?.method ?? "GET", body: init?.body };
    calls.push(call);
    return respond(call.url, call.method);
  }) as unknown as typeof fetch;
  return { calls, fetchImpl };
}

const notFound = () => new Response("<Error><Code>NoSuchKey</Code></Error>", { status: 404 });

beforeEach(() => {
  vi.stubEnv("CORPSCOUT_S3_ENDPOINT", S3);
  vi.stubEnv("CORPSCOUT_S3_ACCESS_KEY", "access");
  vi.stubEnv("CORPSCOUT_S3_SECRET_KEY", "secret");
  vi.stubEnv("PROVIDER_RECON_BUCKET", "");
});

afterEach(() => vi.unstubAllEnvs());

describe("provider-recon object store", () => {
  it("reads the run index from the default bucket and normalizes legacy feeds", async () => {
    const { calls, fetchImpl } = recorder(() =>
      Response.json({ runs: [{ run_id: "20260927T060000Z-collect", published_at: "x", scope: { command: "collect" }, changed: [], unchanged_count: 1, issues: 0, feeds: [{ slug: "aws", collector: "aws_ip_ranges", status: "ok", items: 3 }] }], providers: ["aws"] }),
    );
    const index = await loadRunIndex({ fetchImpl });
    expect(calls[0].url).toBe(`${S3}/provider-recon/changes/index.json`);
    expect(index.runs[0].feeds[0].churn.missing).toBe(0);
    expect(index.providers).toEqual(["aws"]);
  });

  it("treats a missing index and missing rules as empty and default", async () => {
    const { fetchImpl } = recorder(notFound);
    expect(await loadRunIndex({ fetchImpl })).toEqual({ runs: [], providers: [] });
    expect(await loadIndicatorRules({ fetchImpl })).toEqual(DEFAULT_INDICATOR_RULES);
    expect(await loadManifest("20260927T060000Z-collect", { fetchImpl })).toBeNull();
    expect(await loadProviderDocument("aws", { fetchImpl })).toBeNull();
  });

  it("rejects unsafe run ids and slugs without fetching", async () => {
    const { calls, fetchImpl } = recorder(() => Response.json({}));
    expect(await loadManifest("../settings/indicator-rules", { fetchImpl })).toBeNull();
    expect(await loadManifest("20260927T060000Z-collect/../x", { fetchImpl })).toBeNull();
    expect(await loadProviderDocument("aws/../../x", { fetchImpl })).toBeNull();
    expect(await loadProviderDocument("AWS", { fetchImpl })).toBeNull();
    expect(calls).toHaveLength(0);
  });

  it("reads a manifest and a provider document by key", async () => {
    const { calls, fetchImpl } = recorder(() => Response.json({ slug: "aws" }));
    await loadManifest("20260927T060000Z-collect", { fetchImpl });
    await loadProviderDocument("one-com", { fetchImpl });
    expect(calls.map((c) => c.url)).toEqual([
      `${S3}/provider-recon/changes/20260927T060000Z-collect.json`,
      `${S3}/provider-recon/providers/one-com/latest.json`,
    ]);
  });

  it("surfaces other HTTP errors", async () => {
    const { fetchImpl } = recorder(() => new Response("denied", { status: 403 }));
    await expect(loadRunIndex({ fetchImpl })).rejects.toThrow("HTTP 403");
  });

  it("saves rules as JSON to the settings key, honouring PROVIDER_RECON_BUCKET", async () => {
    vi.stubEnv("PROVIDER_RECON_BUCKET", "provider-recon-test");
    const { calls, fetchImpl } = recorder(() => new Response("", { status: 200 }));
    await saveIndicatorRules({ ...DEFAULT_INDICATOR_RULES, missingPercent: 9 }, { fetchImpl });
    expect(calls[0].method).toBe("PUT");
    expect(calls[0].url).toBe(`${S3}/provider-recon-test/settings/indicator-rules.json`);
    expect(JSON.parse(new TextDecoder().decode(calls[0].body as Uint8Array)).missingPercent).toBe(9);
  });
});
