import { renderToStaticMarkup } from "react-dom/server";
import { createMemoryRouter, RouterProvider } from "react-router";
import { beforeEach, expect, it, vi } from "vitest";
const database = vi.hoisted(() => ({chQuery: vi.fn()}));
const archive = vi.hoisted(() => ({readCrawlArchive: vi.fn()}));
vi.mock("~/lib/clickhouse.server", () => database);
vi.mock("~/lib/crawl-results.server", () => archive);
import DomainCrawl, {loader} from "~/routes/admin-se-domain-crawl";
import { domainCrawlStatus } from "~/lib/domain-crawl-status";
import type { DomainCrawlResult } from "~/lib/domain-crawls.server";

const saved = {type: "site_info", kind: "saved", request_id: "older", attempt: 1, state: "completed", crawl_status: "skip_crawling", successful: true,
  finished_at: "2026-09-20T21:46:03Z", error: "", s3_path: "crawls/older/result.json.gz", s3_state: "uploaded"};
const latest = {...saved, kind: "latest", request_id: "latest", crawl_status: "needs_review", successful: false, finished_at: "2026-09-25T14:50:24Z", s3_path: "crawls/latest/result.json.gz"};
const failure = {crawl: {status: "needs_review", mode: "site_info", stop_reason: "model_unavailable", errors: [{error: "OpenRouter HTTP 404"}], usage: {by_call: [{error: "OpenRouter HTTP 404", provider_error: {message: "No allowed providers are available for the selected model."}}]}}};
const success = {crawl: {status: "skip_crawling", site_info: {operator_name: "100%", site_description: "Swedish news website", business_activities: ["Publishing news"]}}, documents: []};
function get(query = "") {return loader({params: {domain: "100.se"}, request: new Request(`http://backoffice/admin/se/companies/domains/100.se/crawl${query}`)} as never);}
function render(data: Awaited<ReturnType<typeof loader>>) {
  const router = createMemoryRouter([{path: "*", element: <DomainCrawl {...({loaderData: data} as Parameters<typeof DomainCrawl>[0])} />}], {initialEntries: ["/admin/se/companies/domains/100.se/crawl"]});
  return renderToStaticMarkup(<RouterProvider router={router} />);
}
beforeEach(() => {
  vi.clearAllMocks();
  database.chQuery.mockResolvedValue([latest, saved]);
  archive.readCrawlArchive.mockImplementation(async path => ({domain: "100.se", result_json: JSON.stringify(path === saved.s3_path ? success : failure)}));
});

it("shows completed but unsuccessful classification as Failed with the JSON provider reason", async () => {
  const data = await get();
  const html = render(data);
  expect(html).toContain(">Failed<");
  expect(html).toContain("No allowed providers are available for the selected model.");
  expect(html).toContain("OpenRouter HTTP 404");
  expect(html).toContain("model unavailable");
  expect(html).toContain("needs_review");
  expect(html).toContain("Processing: completed");
  expect(html).toContain("2026-09-25T14:50:24Z");
  expect(html).toContain("From an earlier attempt");
  expect(html).not.toContain(">Successful<");
  expect(data.details?.result?.request_id).toBe("latest");
});

it("renders earlier saved classification as parsed fields while retaining the latest failed status", async () => {
  const data = await get("?type=site_info&result=saved");
  const html = render(data);
  expect(html).toContain("Earlier saved result");
  expect(html).toContain(">Failed<");
  expect(html).toContain("Site classification");
  expect(html).toContain("Swedish news website");
  expect(html).toContain("Publishing news");
  expect(html).toContain("<dt");
  expect(html).toContain("Full JSON");
  expect(data.details?.result?.request_id).toBe("older");
});

it("retains Failed when JSON cannot be read and reports the archive error separately", async () => {
  archive.readCrawlArchive.mockRejectedValue(new Error("private credentials"));
  const html = render(await get());
  expect(html).toContain(">Failed<");
  expect(html).toContain("Saved JSON unavailable");
  expect(html).not.toContain("private credentials");
  expect(html.match(/Not crawled/g)).toHaveLength(2);
});

it("keeps explicit row errors without requiring an archive", async () => {
  database.chQuery.mockResolvedValue([{...latest, state: "failed", crawl_status: "failed", error: "Navigation timed out", s3_path: "", s3_state: "pending"}]);
  const html = render(await get());
  expect(html).toContain("Navigation timed out");
  expect(html).toContain(">Failed<");
  expect(archive.readCrawlArchive).not.toHaveBeenCalled();
});

it("distinguishes no history from an unavailable database", async () => {
  database.chQuery.mockResolvedValue([]);
  const empty = render(await get());
  expect(empty.match(/Not crawled/g)).toHaveLength(3);
  expect(empty).not.toContain(">Failed<");
  database.chQuery.mockRejectedValue(new Error("private"));
  const unavailable = render(await get());
  expect(unavailable).toContain("Crawl status unavailable");
  expect(unavailable).not.toContain("Not crawled");
});

it("ignores arbitrary archive parameters and rejects a mismatched archive domain", async () => {
  archive.readCrawlArchive.mockResolvedValue({domain: "other.se", result_json: JSON.stringify(success)});
  const data = await get("?type=unknown&path=crawls/other/result.json.gz");
  expect(archive.readCrawlArchive).toHaveBeenCalledWith(latest.s3_path);
  expect(data.details?.payload).toBeNull();
  expect(data.details?.archiveError).toBeTruthy();
});

it("preserves partial, cancelled, skipped, and successful outcomes", () => {
  expect(domainCrawlStatus({...latest, crawl_status: "partial"} as DomainCrawlResult)).toBe("Partial");
  expect(domainCrawlStatus({...latest, state: "cancelled"} as DomainCrawlResult)).toBe("Cancelled");
  expect(domainCrawlStatus(saved as DomainCrawlResult)).toBe("Skipped by classification");
  expect(domainCrawlStatus({...saved, crawl_status: "finished"} as DomainCrawlResult)).toBe("Crawled");
});

it("renders successful collected pages and escapes source HTML", async () => {
  database.chQuery.mockResolvedValue([{...latest, successful: true, crawl_status: "finished"}]);
  archive.readCrawlArchive.mockResolvedValue({domain: "100.se", result_json: JSON.stringify({crawl: {status: "finished", site_info: {operator_name: '<script>alert("unsafe")</script>'}, pages: [{source_url: "https://100.se/", status_code: 200, fetch_status: "fetched"}]}, documents: [{url: "https://100.se/", input: {observations: {contacts: [{email: "hello@100.se"}]}}}]})});
  const html = render(await get());
  expect(html).toContain(">Crawled<");
  expect(html).toContain("Collected pages (1)");
  expect(html).toContain("Fetched pages");
  expect(html).toContain("200");
  expect(html).not.toContain('<script>alert("unsafe")</script>');
});
