import { renderToStaticMarkup } from "react-dom/server";
import { createMemoryRouter, RouterProvider } from "react-router";
import { beforeEach, expect, it, vi } from "vitest";

const database = vi.hoisted(() => ({ chQuery: vi.fn() }));
const archive = vi.hoisted(() => ({ readCrawlArchive: vi.fn() }));
const company = vi.hoisted(() => ({ getCompanyDomains: vi.fn() }));
vi.mock("~/lib/clickhouse.server", () => database);
vi.mock("~/lib/crawl-results.server", () => archive);
vi.mock("~/lib/queries.server", () => company);

import CompanyTechnologyCrawl, { loader } from "~/routes/admin-se-company-technology-crawl";
import CompanyTechnologyLayout, { loader as layoutLoader } from "~/routes/admin-se-company-technology-layout";

const companyId = "5594643297";
const basePath = `/admin/se/company/${companyId}/technology`;
const relatedDomain = { domain: "100.se", is_primary: 0, review_status: "confirmed_related" };
const primaryDomain = { domain: "primary.se", is_primary: 1, review_status: "confirmed_primary" };
const saved = {
  type: "site_info", kind: "saved", request_id: "older", attempt: 1,
  state: "completed", crawl_status: "finished", successful: true,
  finished_at: "2026-09-20T21:46:03Z", error: "", s3_state: "uploaded",
};
const latest = {
  ...saved, kind: "latest", request_id: "latest", attempt: 2,
  crawl_status: "needs_review", successful: false, finished_at: "2026-09-25T14:50:24Z",
};
const success = {
  crawl: { status: "finished", site_info: { operator_name: "100%", site_description: "Swedish news website", business_activities: ["Publishing news"] } },
  documents: [],
};
const failure = {
  crawl: { status: "needs_review", mode: "site_info", stop_reason: "model_unavailable", errors: [{ error: "OpenRouter HTTP 404" }], usage: { by_call: [{ provider_error: { message: "No allowed providers are available for the selected model." } }] } },
};

function args(query = "") {
  return {
    params: { companyId },
    request: new Request(`http://backoffice${basePath}/crawl${query}`),
  };
}

function get(query = "") {
  return loader(args(query) as never);
}

function render(data: Awaited<ReturnType<typeof loader>>, query = "") {
  const router = createMemoryRouter([
    { path: "*", element: <CompanyTechnologyCrawl {...({ loaderData: data } as Parameters<typeof CompanyTechnologyCrawl>[0])} /> },
  ], { initialEntries: [`${basePath}/crawl${query}`] });
  return renderToStaticMarkup(<RouterProvider router={router} />);
}

beforeEach(() => {
  vi.clearAllMocks();
  company.getCompanyDomains.mockResolvedValue([relatedDomain, primaryDomain]);
  database.chQuery.mockImplementation(async (query: string, params: { domain: string; requestId?: string; attempt?: number }) => {
    const rows = [latest, saved].map(row => ({ ...row, s3_path: `crawls/${params.domain}/${row.request_id}/result.json.gz` }));
    if (query.includes("UNION ALL")) return rows;
    if (query.includes("request_id = {requestId:String}")) {
      return rows.filter(row => row.request_id === params.requestId && row.attempt === params.attempt);
    }
    return rows;
  });
  archive.readCrawlArchive.mockImplementation(async (path: string) => ({
    domain: path.split("/")[1],
    result_json: JSON.stringify(path.includes("/older/") ? success : failure),
  }));
});

it("defaults to the company's primary domain rather than the first association", async () => {
  const data = await get();
  expect(company.getCompanyDomains).toHaveBeenCalledWith(expect.objectContaining({ code: "se" }), companyId);
  expect(data?.domain).toBe("primary.se");
  expect(database.chQuery.mock.calls.every(([, params]) => params.domain === "primary.se")).toBe(true);
  expect(render(data)).toContain("?domain=primary.se&amp;type=site_info");
});

it("normalizes an explicitly selected related domain and keeps its last good data beside the latest failure", async () => {
  const data = await get("?domain=%20100.SE.%20");
  expect(data?.domain).toBe("100.se");
  expect(data?.details?.result?.request_id).toBe("older");
  expect(database.chQuery.mock.calls.every(([, params]) => params.domain === "100.se")).toBe(true);
  const html = render(data);
  const sidebarStart = html.indexOf("<aside");
  const main = html.slice(html.indexOf("<section"), sidebarStart);
  const sidebar = html.slice(sidebarStart);
  expect(main).toContain("Last good result");
  expect(main).toContain(">Successful<");
  expect(main).toContain("Swedish news website");
  expect(main).toContain("Publishing news");
  expect(main).not.toContain(">Failed<");
  expect(sidebar).toContain('aria-label="Recent crawl attempts"');
  expect(sidebar).toContain(">Failed<");
  expect(sidebar).toContain("No allowed providers are available for the selected model.");
  expect(sidebar).toContain("2026-09-25T14:50:24Z");
});

it("limits selection to associated domains and falls back to the first association when there is no primary", async () => {
  expect((await get("?domain=unrelated.se"))?.domain).toBe("primary.se");
  company.getCompanyDomains.mockResolvedValue([relatedDomain, { ...primaryDomain, is_primary: 0 }]);
  expect((await get())?.domain).toBe("100.se");
});

it("shows the shared no-domains state without querying or reading crawl results", async () => {
  company.getCompanyDomains.mockResolvedValue([]);
  const data = await get("?domain=unrelated.se");
  expect(data).toBeNull();
  expect(render(data)).toContain("No domains resolved for this company");
  expect(database.chQuery).not.toHaveBeenCalled();
  expect(archive.readCrawlArchive).not.toHaveBeenCalled();
});

it("retains the selected company domain in crawl types, attempt links, and the link back to the last good result", async () => {
  const query = "?domain=100.se&type=site_info&request=latest&attempt=2";
  const data = await get(query);
  const html = render(data, query);
  for (const type of ["site_info", "jobs", "full"]) {
    expect(html).toContain(`href="${basePath}/crawl?domain=100.se&amp;type=${type}"`);
  }
  expect(html).toContain(`href="${basePath}/crawl?domain=100.se&amp;type=site_info&amp;request=latest&amp;attempt=2"`);
  expect(html).toContain(`href="${basePath}/crawl?domain=100.se&amp;type=site_info&amp;request=older&amp;attempt=1"`);
  expect(html.match(/<a [^>]*>View last good result<\/a>/)?.[0]).toContain(`href="${basePath}/crawl?domain=100.se&amp;type=site_info"`);
  expect(html).not.toContain(`href="${basePath}/crawl?type=`);
});

it("restores the exact selected request and retry number when its company URL is reloaded", async () => {
  const query = "?domain=100.se&type=site_info&request=latest&attempt=2";
  const data = await get(query);
  const reloaded = await get(query);
  expect(data?.details?.result).toMatchObject({ request_id: "latest", attempt: 2, successful: false });
  expect(reloaded?.details?.result).toEqual(data?.details?.result);
  expect(database.chQuery).toHaveBeenCalledWith(
    expect.stringContaining("AND request_id = {requestId:String} AND attempt = {attempt:UInt32}"),
    { domain: "100.se", requestId: "latest", attempt: 2 },
  );
  const html = render(reloaded, query);
  expect(html).toContain("Latest attempt details");
  expect(html).toContain("No allowed providers are available for the selected model.");
  expect(html.match(/<a [^>]*aria-current="true"[^>]*>/)?.[0]).toContain("request=latest&amp;attempt=2");
});

it("includes the shared Crawl navigation tab in company technology and preserves the selected related domain", async () => {
  const query = "?domain=100.se&type=site_info&request=latest&attempt=2";
  const data = await layoutLoader(args(query) as never);
  const router = createMemoryRouter([
    { path: "*", element: <CompanyTechnologyLayout {...({ loaderData: data, params: { companyId } } as Parameters<typeof CompanyTechnologyLayout>[0])} /> },
  ], { initialEntries: [`${basePath}/crawl${query}`] });
  const html = renderToStaticMarkup(<RouterProvider router={router} />);
  expect(html).toContain('aria-label="Domain technology sections"');
  const crawl = html.match(/<a [^>]*>Crawl<\/a>/)?.[0];
  expect(crawl).toContain(`href="${basePath}/crawl?domain=100.se"`);
  expect(crawl).toContain('aria-selected="true"');
  for (const section of ["web-technologies", "web-intelligence", "infrastructure", "ip-addresses", "mail-security"]) {
    expect(html).toContain(`href="${basePath}/${section}?domain=100.se"`);
  }
  expect(html).not.toContain("request=latest");
});
