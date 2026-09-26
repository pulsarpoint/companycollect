import { createHash } from "node:crypto";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { ensureBucket, putObject } from "~/lib/object-store.server";
import {
  GEOLITE2_MAX_BYTES,
  GeoLite2UploadError,
  geolite2Edition,
  loadGeolite2Status,
  uploadGeolite2,
} from "~/lib/geolite2.server";

const S3 = "http://s3.test:9000";
const DAGSTER = "http://dagster.test/graphql";

interface Call {
  url: string;
  method: string;
  headers: Record<string, string>;
  body: unknown;
}

/** A fetch double that records every call and answers from `respond`. */
function recorder(respond: (call: Call) => Response) {
  const calls: Call[] = [];
  const fetchImpl = vi.fn(async (input: string | URL | Request, init?: RequestInit) => {
    const call = {
      url: String(input),
      method: init?.method ?? "GET",
      headers: (init?.headers ?? {}) as Record<string, string>,
      body: init?.body,
    };
    calls.push(call);
    return respond(call);
  }) as unknown as typeof fetch;
  return { calls, fetchImpl };
}

function graphqlBody(call: Call): { query: string; variables: Record<string, unknown> } {
  return JSON.parse(String(call.body));
}

function json(data: unknown): Response {
  return new Response(JSON.stringify({ data }), { status: 200 });
}

beforeEach(() => {
  vi.stubEnv("CORPSCOUT_S3_ENDPOINT", S3);
  vi.stubEnv("CORPSCOUT_S3_ACCESS_KEY", "access");
  vi.stubEnv("CORPSCOUT_S3_SECRET_KEY", "secret");
  vi.stubEnv("DAGSTER_GRAPHQL_URL", DAGSTER);
  vi.stubEnv("DAGSTER_UI_URL", "");
});

afterEach(() => {
  vi.unstubAllEnvs();
});

describe("geolite2Edition", () => {
  it.each([
    ["GeoLite2-City_20260925.tar.gz", "City"],
    ["GeoLite2-ASN_20260925.tar.gz", "ASN"],
    ["GeoLite2-City.mmdb", "City"],
    ["GeoLite2-ASN.tar.gz", "ASN"],
  ])("accepts %s", (name, edition) => {
    expect(geolite2Edition(name, 10)).toBe(edition);
  });

  it.each([
    "GeoLite2-Country_20260925.tar.gz",
    "GeoLite2-City_2026092.tar.gz",
    "GeoLite2-City.zip",
    "../GeoLite2-City.mmdb",
    "GeoLite2-City.mmdb.exe",
    "geolite2-city.mmdb",
  ])("refuses %s", (name) => {
    expect(() => geolite2Edition(name, 10)).toThrow(GeoLite2UploadError);
  });

  it("refuses empty and oversized files", () => {
    expect(() => geolite2Edition("GeoLite2-City.mmdb", 0)).toThrow(/empty/);
    expect(() => geolite2Edition("GeoLite2-City.mmdb", GEOLITE2_MAX_BYTES + 1)).toThrow(/200 MB/);
    expect(geolite2Edition("GeoLite2-City.mmdb", GEOLITE2_MAX_BYTES)).toBe("City");
  });
});

describe("object store", () => {
  it("signs a PUT with the payload's sha256", async () => {
    const { calls, fetchImpl } = recorder(() => new Response("", { status: 200 }));
    const body = new Uint8Array(Buffer.from("mmdb bytes"));

    await putObject("geolite2", "uploads/u-1/GeoLite2-City (1).mmdb", body, {
      fetchImpl,
      now: new Date("2026-09-26T10:00:00.000Z"),
    });

    expect(calls).toHaveLength(1);
    const [call] = calls;
    expect(call.method).toBe("PUT");
    expect(call.url).toBe(`${S3}/geolite2/uploads/u-1/GeoLite2-City%20%281%29.mmdb`);
    expect(call.body).toBe(body);
    expect(call.headers["x-amz-content-sha256"]).toBe(createHash("sha256").update(body).digest("hex"));
    expect(call.headers["x-amz-date"]).toBe("20260926T100000Z");
    expect(call.headers.Authorization).toMatch(
      /^AWS4-HMAC-SHA256 Credential=access\/20260926\/us-east-1\/s3\/aws4_request, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature=[0-9a-f]{64}$/,
    );
  });

  it("reports a refused PUT", async () => {
    const { fetchImpl } = recorder(() => new Response("AccessDenied", { status: 403 }));
    await expect(putObject("geolite2", "k", new Uint8Array(1), { fetchImpl })).rejects.toThrow(/HTTP 403 AccessDenied/);
  });

  it("creates the bucket when HEAD answers 404", async () => {
    const { calls, fetchImpl } = recorder((call) =>
      new Response("", { status: call.method === "HEAD" ? 404 : 200 }),
    );
    await ensureBucket("geolite2", { fetchImpl });
    expect(calls.map((call) => `${call.method} ${call.url}`)).toEqual([
      `HEAD ${S3}/geolite2`,
      `PUT ${S3}/geolite2`,
    ]);
    expect(calls[1].body).toBeUndefined();
  });

  it("leaves an existing bucket alone and fails on other errors", async () => {
    const existing = recorder(() => new Response("", { status: 200 }));
    await ensureBucket("geolite2", { fetchImpl: existing.fetchImpl });
    expect(existing.calls.map((call) => call.method)).toEqual(["HEAD"]);

    const broken = recorder(() => new Response("", { status: 500 }));
    await expect(ensureBucket("geolite2", { fetchImpl: broken.fetchImpl })).rejects.toThrow(/HTTP 500/);
  });
});

describe("uploadGeolite2", () => {
  it("stores both files unchanged and launches one install run", async () => {
    const s3 = recorder((call) => new Response("", { status: call.method === "HEAD" ? 404 : 200 }));
    const dagster = recorder(() =>
      json({ launchRun: { __typename: "LaunchRunSuccess", run: { runId: "run-1", status: "QUEUED" } } }),
    );
    const ids = ["u-city", "u-asn"];
    const city = new File([new Uint8Array([1, 2, 3])], "GeoLite2-City_20260925.tar.gz");
    const asn = new File([new Uint8Array([4, 5])], "GeoLite2-ASN_20260925.tar.gz");

    const result = await uploadGeolite2([city, asn], {
      objectStore: { fetchImpl: s3.fetchImpl },
      dagster: { fetchImpl: dagster.fetchImpl },
      uuid: () => ids.shift()!,
    });

    expect(s3.calls.map((call) => `${call.method} ${call.url}`)).toEqual([
      `HEAD ${S3}/geolite2`,
      `PUT ${S3}/geolite2`,
      `PUT ${S3}/geolite2/uploads/u-city/GeoLite2-City_20260925.tar.gz`,
      `PUT ${S3}/geolite2/uploads/u-asn/GeoLite2-ASN_20260925.tar.gz`,
    ]);
    expect([...(s3.calls[2].body as Uint8Array)]).toEqual([1, 2, 3]);
    const uploads = [
      { edition: "City", key: "uploads/u-city/GeoLite2-City_20260925.tar.gz" },
      { edition: "ASN", key: "uploads/u-asn/GeoLite2-ASN_20260925.tar.gz" },
    ];
    expect(result).toEqual({ runId: "run-1", uploads });
    const { variables } = graphqlBody(dagster.calls[0]);
    expect(variables.executionParams).toMatchObject({
      selector: { jobName: "geolite2_install_job", repositoryLocationName: "dagster_v3" },
      runConfigData: { ops: { geolite2_databases: { config: { uploads } } } },
    });
    expect((variables.executionParams as { selector: object }).selector).not.toHaveProperty("assetSelection");
  });

  it("validates every file before storing any", async () => {
    const s3 = recorder(() => new Response("", { status: 200 }));
    const files = [
      new File([new Uint8Array(1)], "GeoLite2-City.mmdb"),
      new File([new Uint8Array(1)], "GeoLite2-Country.mmdb"),
    ];
    await expect(uploadGeolite2(files, { objectStore: { fetchImpl: s3.fetchImpl } })).rejects.toThrow(
      GeoLite2UploadError,
    );
    const twice = [new File([new Uint8Array(1)], "GeoLite2-City.mmdb"), new File([new Uint8Array(1)], "GeoLite2-City_20260925.tar.gz")];
    await expect(uploadGeolite2(twice, { objectStore: { fetchImpl: s3.fetchImpl } })).rejects.toThrow(/at most one/);
    await expect(uploadGeolite2([], { objectStore: { fetchImpl: s3.fetchImpl } })).rejects.toThrow(/Choose/);
    expect(s3.calls).toHaveLength(0);
  });
});

function statusResponder(metadataEntries: unknown[] | null, run: Record<string, unknown> | null) {
  return recorder((call) => {
    const { query } = graphqlBody(call);
    if (query.includes("BackofficeAssetMetadata")) {
      return json({
        assetNodes: [
          {
            id: "geolite2_databases",
            assetMaterializations: metadataEntries
              ? [{ runId: "install-1", timestamp: "1790000000000", metadataEntries }]
              : [],
          },
        ],
      });
    }
    return json({ runsOrError: { __typename: "Runs", results: run ? [run] : [] } });
  });
}

const METADATA_ENTRIES = [
  { __typename: "TextMetadataEntry", label: "city_build", text: "2026-09-25T10:00:00+00:00" },
  { __typename: "TextMetadataEntry", label: "asn_build", text: "2026-09-01T10:00:00+00:00" },
  { __typename: "TextMetadataEntry", label: "city_sha256", text: "a".repeat(64) },
  { __typename: "TextMetadataEntry", label: "asn_sha256", text: "b".repeat(64) },
  { __typename: "JsonMetadataEntry", label: "installed", jsonString: '["City"]' },
  { __typename: "JsonMetadataEntry", label: "source_keys", jsonString: '["uploads/x/GeoLite2-City.mmdb"]' },
  { __typename: "BoolMetadataEntry", label: "fresh", boolValue: false },
  { __typename: "IntMetadataEntry", label: "city_age_days", intValue: 1 },
];

const RUN = {
  runId: "install-2",
  status: "STARTED",
  jobName: "geolite2_install_job",
  startTime: 1790000100,
  endTime: null,
  runConfig: {},
  assetSelection: null,
  tags: [],
};

describe("loadGeolite2Status", () => {
  it("reads the installed builds from the latest materialization and the last run", async () => {
    const { calls, fetchImpl } = statusResponder(METADATA_ENTRIES, RUN);

    const status = await loadGeolite2Status({ fetchImpl }, new Date("2026-09-26T12:00:00Z"));

    const metadataQuery = calls.map(graphqlBody).find((body) => body.query.includes("BackofficeAssetMetadata"));
    expect(metadataQuery?.variables).toEqual({ assetKeys: [{ path: ["geolite2_databases"] }], limit: 1 });
    const runsQuery = calls.map(graphqlBody).find((body) => body.query.includes("BackofficeRuns"));
    expect(runsQuery?.variables).toMatchObject({ filter: { pipelineName: "geolite2_install_job" }, limit: 1 });
    expect(status.installed).toEqual({
      installedAt: 1790000000000,
      runId: "install-1",
      replaced: ["City"],
      editions: [
        { edition: "City", build: "2026-09-25T10:00:00+00:00", ageDays: 1, stale: false, sha256: "a".repeat(64) },
        { edition: "ASN", build: "2026-09-01T10:00:00+00:00", ageDays: 25, stale: true, sha256: "b".repeat(64) },
      ],
    });
    expect(status.lastRun).toEqual({
      runId: "install-2",
      status: "STARTED",
      url: "http://dagster.test/runs/install-2",
      startTime: 1790000100,
    });
  });

  it("reports nothing installed before the first install and a missing edition as stale", async () => {
    const none = statusResponder(null, null);
    expect(await loadGeolite2Status({ fetchImpl: none.fetchImpl })).toEqual({ installed: null, lastRun: null });

    const missing = statusResponder(
      [{ __typename: "TextMetadataEntry", label: "city_build", text: "missing" }],
      null,
    );
    const status = await loadGeolite2Status({ fetchImpl: missing.fetchImpl });
    expect(status.installed?.editions[0]).toMatchObject({ edition: "City", build: null, ageDays: null, stale: true });
  });
});

describe("admin-settings-geolite2 route", () => {
  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it("loader shows the latest materialization and the last install run", async () => {
    const { fetchImpl } = statusResponder(METADATA_ENTRIES, RUN);
    vi.stubGlobal("fetch", fetchImpl);
    const { loader } = await import("~/routes/admin-settings-geolite2");

    const result = await loader({
      request: new Request("http://backoffice.test/admin/settings/geolite2?run=install-2"),
      params: {},
      context: {},
    } as never);

    expect(result.error).toBeNull();
    expect(result.launched).toBe("install-2");
    expect(result.status.installed?.editions.map((edition) => edition.build)).toEqual([
      "2026-09-25T10:00:00+00:00",
      "2026-09-01T10:00:00+00:00",
    ]);
    expect(result.status.lastRun?.status).toBe("STARTED");
  });

  it("loader renders a Dagster outage instead of failing", async () => {
    vi.stubGlobal("fetch", vi.fn(async () => new Response("", { status: 502 })));
    const { loader } = await import("~/routes/admin-settings-geolite2");
    const result = await loader({
      request: new Request("http://backoffice.test/admin/settings/geolite2"),
      params: {},
      context: {},
    } as never);
    expect(result.status).toEqual({ installed: null, lastRun: null });
    expect(result.error).toMatch(/HTTP 502/);
  });

  it("action uploads the chosen files, launches the install and redirects to its run", async () => {
    const seen: string[] = [];
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: string | URL | Request, init?: RequestInit) => {
        const url = String(input);
        seen.push(`${init?.method ?? "GET"} ${url}`);
        if (url === DAGSTER) {
          return json({ launchRun: { __typename: "LaunchRunSuccess", run: { runId: "run-9", status: "QUEUED" } } });
        }
        return new Response("", { status: 200 });
      }),
    );
    const { action } = await import("~/routes/admin-settings-geolite2");
    const form = new FormData();
    form.append("files", new File([new Uint8Array([7])], "GeoLite2-ASN_20260925.tar.gz"));
    form.append("files", new File([], ""));

    const response = await action({
      request: new Request("http://backoffice.test/admin/settings/geolite2", { method: "POST", body: form }),
      params: {},
      context: {},
    } as never);

    expect(response).toBeInstanceOf(Response);
    expect((response as Response).headers.get("Location")).toBe("/admin/settings/geolite2?run=run-9");
    expect(seen[0]).toBe(`HEAD ${S3}/geolite2`);
    expect(seen[1]).toMatch(new RegExp(`^PUT ${S3}/geolite2/uploads/[0-9a-f-]{36}/GeoLite2-ASN_20260925\\.tar\\.gz$`));
    expect(seen[2]).toBe(`POST ${DAGSTER}`);
  });

  it("action refuses a wrong file name without touching the store", async () => {
    const fetchImpl = vi.fn();
    vi.stubGlobal("fetch", fetchImpl);
    const { action } = await import("~/routes/admin-settings-geolite2");
    const form = new FormData();
    form.append("files", new File([new Uint8Array([7])], "GeoIP2-City.mmdb"));

    const result = await action({
      request: new Request("http://backoffice.test/admin/settings/geolite2", { method: "POST", body: form }),
      params: {},
      context: {},
    } as never);

    expect(result).toMatchObject({ init: { status: 400 }, data: { error: expect.stringMatching(/not a GeoLite2 file/) } });
    expect(fetchImpl).not.toHaveBeenCalled();
  });
});
