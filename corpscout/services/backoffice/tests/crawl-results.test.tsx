import { renderToStaticMarkup } from "react-dom/server";
import { createMemoryRouter, RouterProvider } from "react-router";
import { beforeEach, describe, expect, it, vi } from "vitest";
const database = vi.hoisted(() => ({chQuery: vi.fn()}));
vi.mock("~/lib/clickhouse.server", () => database);
import { readCrawlArchive } from "~/lib/crawl-results.server";
import { resultWebUrl } from "~/lib/crawl-results";
import AdminCrawlResult, {loader} from "~/routes/admin-crawl-result";
import {loader as download} from "~/routes/admin-crawl-result-json";

const path = "crawls/company-crawls/test-request/attempts/0001/result.json.gz";
const payload = {schema_version: "company-crawl-result/1.2", crawl: {status: "partial", stop_reason: "page_budget", mode: "full"},
  documents: [{url: "https://novelic.com/", html: '<script>alert("unsafe")</script><h1>Saved HTML</h1>', input: {observations: {contacts: [{type: "email", value: "sales@novelic.com"}]}}}]};
const row = {domain: "novelic.com", website_url: "https://novelic.com/", request_id: "test-request", attempt: 1,
  status: "partial", schema_version: payload.schema_version, source_path: path, result_json: JSON.stringify(payload)};

beforeEach(() => {vi.clearAllMocks(); database.chQuery.mockResolvedValue([row]);});

describe("saved crawl results through ClickHouse", () => {
  it("reads exactly the selected object through a bound path, preserving the complete JSON", async () => {
    const result = await readCrawlArchive(path);
    expect(result).toEqual(row);
    const [sql, params] = database.chQuery.mock.calls[0];
    expect(sql).toContain("FROM website_crawl_results_s3_archive");
    expect(sql).toContain("WHERE _path = {path:String}");
    expect(sql).not.toContain(path);
    expect(params).toEqual({path});
    const loaded = await loader({request: new Request(`http://backoffice/admin/crawls/results?${new URLSearchParams({path})}`)} as Parameters<typeof loader>[0]);
    expect(loaded.payload).toEqual(payload);
    expect(loaded.archive).not.toHaveProperty("result_json");
  });

  it("rejects traversal, glob patterns, absolute URLs and missing paths before querying", async () => {
    for (const invalid of [null, "", "crawls/../result.json.gz", "crawls/*/result.json.gz", "https://external.test/result.json.gz", "/crawls/id/result.json.gz"]) {
      await expect(readCrawlArchive(invalid)).rejects.toMatchObject({status: 400});
    }
    expect(database.chQuery).not.toHaveBeenCalled();
  });

  it("distinguishes a missing archive from a database failure without leaking SQL or credentials", async () => {
    database.chQuery.mockResolvedValue([]);
    await expect(readCrawlArchive(path)).rejects.toMatchObject({status: 404});
    const log = vi.spyOn(console, "error").mockImplementation(() => {});
    database.chQuery.mockRejectedValue(new Error("private-credentials in database SQL"));
    try { await readCrawlArchive(path); throw new Error("Expected a failure"); }
    catch (error) {
      expect(error).toBeInstanceOf(Response);
      expect((error as Response).status).toBe(503);
      expect(await (error as Response).text()).not.toContain("private-credentials");
      expect(JSON.stringify(log.mock.calls)).not.toContain("private-credentials");
    } finally {log.mockRestore();}
  });

  it("downloads the unmodified result with attachment and nosniff headers", async () => {
    const response = await download({request: new Request(`http://backoffice/admin/crawls/result.json?${new URLSearchParams({path})}`)} as Parameters<typeof download>[0]);
    expect(response.headers.get("Content-Disposition")).toContain("attachment");
    expect(response.headers.get("X-Content-Type-Options")).toBe("nosniff");
    expect(await response.text()).toBe(row.result_json);
  });

  it("shows partial coverage and the exact archive identity without executing saved HTML", async () => {
    const loaded = await loader({request: new Request(`http://backoffice/admin/crawls/results?${new URLSearchParams({path})}`)} as Parameters<typeof loader>[0]);
    const element = <AdminCrawlResult {...({loaderData: loaded} as Parameters<typeof AdminCrawlResult>[0])} />;
    const router = createMemoryRouter([{path: "/", element}]);
    const html = renderToStaticMarkup(<RouterProvider router={router} />);
    for (const label of ["novelic.com", "Attempt 1", "partial", "configured page limit", "Collected pages (1)", "Download JSON", "Full JSON", "Read from S3 via ClickHouse", path]) expect(html).toContain(label);
    expect(html).not.toContain('<script>alert("unsafe")</script>');
  });

  it("never turns executable or credential-bearing archive URLs into clickable links", () => {
    for (const url of ["javascript:alert(1)", "data:text/html,unsafe", "https://secret:password@example.com", "//example.com"]) expect(resultWebUrl(url)).toBeUndefined();
    expect(resultWebUrl("https://example.com/jobs")).toBe("https://example.com/jobs");
  });
});
