import { renderToStaticMarkup } from "react-dom/server";
import { createMemoryRouter, RouterProvider } from "react-router";
import { beforeEach, expect, it, vi } from "vitest";
const database = vi.hoisted(() => ({chQuery: vi.fn()}));
const archive = vi.hoisted(() => ({readCrawlArchive: vi.fn()}));
vi.mock("~/lib/clickhouse.server", () => database);
vi.mock("~/lib/crawl-results.server", () => archive);
import DomainCrawl, {loader} from "~/routes/admin-se-domain-crawl";
import { domainCrawlStatus, crawlFailureReason } from "~/lib/domain-crawl-status";
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
function panels(data: Awaited<ReturnType<typeof loader>>) {
  const html = render(data);
  const sidebarStart = html.indexOf("<aside");
  return {html, main: html.slice(html.indexOf("<section"), sidebarStart), sidebar: html.slice(sidebarStart)};
}
function rows(summary = [latest, saved], history: DomainCrawlResult[] = summary, requested = history) {
  database.chQuery.mockImplementation(async (query: string, params: {requestId?: string; attempt?: number}) => {
    if (query.includes("UNION ALL")) return summary;
    if (query.includes("request_id = {requestId:String}")) return requested.filter(row => row.request_id === params.requestId && row.attempt === params.attempt);
    return history;
  });
}
beforeEach(() => {
  vi.clearAllMocks();
  rows();
  archive.readCrawlArchive.mockImplementation(async path => ({domain: "100.se", result_json: JSON.stringify(path === saved.s3_path ? success : failure)}));
});

it("defaults to the last good parsed result while the latest failed attempt and reason stay in the sidebar", async () => {
  const data = await get();
  const {main, sidebar} = panels(data);
  expect(data.details?.result?.request_id).toBe("older");
  expect(main).toContain("Last good result");
  expect(main).toContain(">Successful<");
  expect(main).toContain("Swedish news website");
  expect(main).toContain("Publishing news");
  expect(main).toContain("Site classification");
  expect(main).toContain("Full JSON");
  expect(main).not.toContain(">Failed<");
  expect(sidebar).toContain('aria-label="Recent crawl attempts"');
  expect(sidebar).toContain(">Failed<");
  expect(sidebar).toContain("No allowed providers are available for the selected model.");
  expect(sidebar).toContain("2026-09-25T14:50:24Z");
  expect(sidebar).toContain('?type=site_info&amp;request=latest&amp;attempt=1"');
  expect(sidebar.match(/<a [^>]*aria-current="true"[^>]*>/)?.[0]).toContain('?type=site_info&amp;request=older&amp;attempt=1"');
  expect(archive.readCrawlArchive).toHaveBeenCalledWith(saved.s3_path);
  expect(archive.readCrawlArchive).toHaveBeenCalledWith(latest.s3_path);
});

it("keeps explicit latest and saved result links compatible", async () => {
  const data = await get("?type=site_info&result=latest");
  const {main} = panels(data);
  expect(data.details?.result?.request_id).toBe("latest");
  expect(main).toContain("Latest attempt details");
  expect(main).toContain(">Failed<");
  expect(main).toContain("No allowed providers are available for the selected model.");
  expect(main).toContain("OpenRouter HTTP 404");
  expect(main).toContain("model unavailable");
  expect(main).toContain("needs_review");
  expect(main).toContain("Processing: completed");
  expect(main).toContain("View last good result");
  const savedData = await get("?type=site_info&result=saved");
  expect(savedData.details?.result?.request_id).toBe("older");
  expect(panels(savedData).main).toContain("Last good result");
});

it("selects an attempt by its URL identity and restores the same result on refresh", async () => {
  const query = "?type=site_info&request=latest&attempt=1";
  const data = await get(query);
  const reloaded = await get(query);
  expect(data.details?.result?.request_id).toBe("latest");
  expect(reloaded.details?.result).toEqual(data.details?.result);
  const {main, sidebar} = panels(data);
  expect(main).toContain(">Failed<");
  expect(main).toContain("No allowed providers are available for the selected model.");
  expect(main).toContain("View last good result");
  expect(sidebar.match(/<a [^>]*aria-current="true"[^>]*>/)?.[0]).toContain('?type=site_info&amp;request=latest&amp;attempt=1"');
  expect(database.chQuery).toHaveBeenCalledWith(expect.stringContaining("AND request_id = {requestId:String} AND attempt = {attempt:UInt32}"), {domain: "100.se", requestId: "latest", attempt: 1});
});

it("keeps retries distinct and can select an attempt outside the recent history window", async () => {
  const retried = {...latest, attempt: 2};
  const older = {...saved, request_id: "outside-window", finished_at: "2026-09-10T10:00:00Z"};
  rows([retried, saved], [retried, latest], [retried, latest, older]);
  const retry = await get("?type=site_info&request=latest&attempt=1");
  expect(retry.details?.result?.attempt).toBe(1);
  const {sidebar} = panels(retry);
  expect(sidebar).toContain('?type=site_info&amp;request=latest&amp;attempt=2"');
  expect(sidebar.match(/<a [^>]*aria-current="true"[^>]*>/)?.[0]).toContain('?type=site_info&amp;request=latest&amp;attempt=1"');
  const data = await get("?type=site_info&request=outside-window&attempt=1");
  expect(data.details?.result?.request_id).toBe("outside-window");
  expect(panels(data).main).toContain("Attempt details");
  expect(panels(data).main).toContain("Swedish news website");
  expect(data.details?.attempts).toHaveLength(2);
  expect(database.chQuery).toHaveBeenCalledWith(expect.stringContaining("LIMIT 20"), {domain: "100.se"});
});

it("falls back to the latest failed attempt when there has been no good result", async () => {
  rows([latest]);
  const data = await get();
  const {main} = panels(data);
  expect(data.details?.result?.request_id).toBe("latest");
  expect(main).toContain("Latest attempt details");
  expect(main).toContain(">Failed<");
  expect(main).toContain("No allowed providers are available for the selected model.");
  expect(main).not.toContain("View last good result");
});

it("retains Failed when JSON cannot be read and reports the archive error separately", async () => {
  archive.readCrawlArchive.mockRejectedValue(new Error("private credentials"));
  const {html, sidebar} = panels(await get());
  expect(sidebar).toContain(">Failed<");
  expect(sidebar).toContain("Failure reason unavailable because the saved JSON could not be read.");
  expect(html).toContain("Saved JSON unavailable");
  expect(html).not.toContain("private credentials");
});

it("keeps explicit row errors without requiring an archive", async () => {
  rows([{...latest, state: "failed", crawl_status: "failed", error: "Navigation timed out", s3_path: "", s3_state: "pending"}]);
  const html = render(await get());
  expect(html).toContain("Navigation timed out");
  expect(html).toContain(">Failed<");
  expect(archive.readCrawlArchive).not.toHaveBeenCalled();
});

it("distinguishes no history from an unavailable database", async () => {
  rows([]);
  const empty = render(await get());
  expect(empty).toContain("Not crawled. No result has been recorded for this type.");
  expect(empty).toContain("No recorded attempts for this crawl type.");
  expect(empty).not.toContain(">Failed<");
  database.chQuery.mockRejectedValue(new Error("private"));
  const unavailable = render(await get());
  expect(unavailable).toContain("Crawl status unavailable");
  expect(unavailable).not.toContain("Not crawled");
});

it("ignores arbitrary archive parameters and rejects a mismatched archive domain", async () => {
  archive.readCrawlArchive.mockResolvedValue({domain: "other.se", result_json: JSON.stringify(success)});
  const data = await get("?type=site_info&path=crawls/other/result.json.gz");
  expect(archive.readCrawlArchive).toHaveBeenCalledWith(saved.s3_path);
  expect(archive.readCrawlArchive).not.toHaveBeenCalledWith("crawls/other/result.json.gz");
  expect(data.details?.payload).toBeNull();
  expect(data.details?.archiveError).toBeTruthy();
});

it.each([
  "?type=unknown",
  "?request=latest&attempt=1",
  "?type=site_info&request=latest",
  "?type=site_info&attempt=1",
  "?type=site_info&request=latest&attempt=0",
  "?type=site_info&request=latest&attempt=1.5",
  "?type=site_info&request=latest&attempt=4294967296",
  "?type=site_info&request=../other&attempt=1",
])("rejects an incomplete or invalid attempt identity: %s", async query => {
  await expect(get(query)).rejects.toMatchObject({status: 400});
  expect(database.chQuery).not.toHaveBeenCalled();
  expect(archive.readCrawlArchive).not.toHaveBeenCalled();
});

it("returns not found for an unknown valid identity instead of silently displaying the default result", async () => {
  await expect(get("?type=site_info&request=missing&attempt=1")).rejects.toMatchObject({status: 404});
  expect(archive.readCrawlArchive).not.toHaveBeenCalled();
});

it("preserves partial, cancelled, skipped, and successful outcomes", () => {
  expect(domainCrawlStatus({...latest, crawl_status: "partial"} as DomainCrawlResult)).toBe("Partial");
  expect(domainCrawlStatus({...latest, state: "cancelled"} as DomainCrawlResult)).toBe("Cancelled");
  expect(domainCrawlStatus(saved as DomainCrawlResult)).toBe("Skipped by classification");
  expect(domainCrawlStatus({...saved, crawl_status: "finished"} as DomainCrawlResult)).toBe("Crawled");
});

it("shows a successful latest attempt as the last good result with parsed pages and escaped source HTML", async () => {
  const completed = {...latest, successful: true, crawl_status: "finished"};
  rows([completed, {...completed, kind: "saved"}], [completed]);
  archive.readCrawlArchive.mockResolvedValue({domain: "100.se", result_json: JSON.stringify({crawl: {status: "finished", site_info: {operator_name: '<script>alert("unsafe")</script>'}, pages: [{source_url: "https://100.se/", status_code: 200, fetch_status: "fetched"}]}, documents: [{url: "https://100.se/", input: {observations: {contacts: [{email: "hello@100.se"}]}}}]})});
  const data = await get();
  const {main, sidebar} = panels(data);
  expect(data.details?.result?.request_id).toBe("latest");
  expect(main).toContain("Last good result");
  expect(main).toContain(">Successful<");
  expect(main).toContain("Collected pages (1)");
  expect(main).toContain("Fetched pages");
  expect(main).toContain("200");
  expect(main).not.toContain('<script>alert("unsafe")</script>');
  expect(main).not.toContain("A newer attempt");
  expect(sidebar).not.toContain(">Failed<");
  expect(archive.readCrawlArchive).toHaveBeenCalledTimes(1);
});


it("explains the excluded site type without treating an explicit override as a failure", () => {
  const crawl = {status: "skip_crawling", stop_reason: "not_company_website", site_gate: {reason: "Excluded primary site type: online_store"}};
  expect(crawlFailureReason({crawl})).toContain("Excluded primary site type: online store");
  expect(crawlFailureReason({crawl: {...crawl, status: "finished", stop_reason: "no_promising_candidates", site_gate: {...crawl.site_gate, overridden: true}}})).not.toContain("Excluded");
});
