import { renderToStaticMarkup } from "react-dom/server";
import { MemoryRouter } from "react-router";
import { expect, it } from "vitest";
import { BraveResults } from "~/components/admin/brave-results";
import type { BraveResultDetail, BraveResultsPage } from "~/lib/brave-results.server";
import { braveResultsPath, braveTaskResultsPath } from "~/lib/brave-results";

const taskId = "11111111-1111-4111-8111-111111111111";
const resultId = "22222222-2222-4222-8222-222222222222";
const basePath = "/admin/se/company/5565245619/brave";
const answer: BraveResultDetail = {
  result_id: resultId, task_id: taskId, country_code: "SE", company_id: "5565245619", company_name: "Example AB",
  query_type: "official_website", search_id: "", search_name: "", search_revision: 0, query: "Find the official website of Example AB.", status: "success",
  completed_at: "2026-09-25 16:54:13.512344", error_type: "", error_stage: "", answer_preview: "Preview only",
  answer_text: "The **official website** is [example.com](https://example.com).\n\nFull answer beyond the preview.",
  source_url: "https://search.brave.com/", route: "direct", elapsed_ms: 1500, attempt: 1, source_run_id: "run-1", execution_id: "execution-1",
  runUrl: "http://dagster:3000/runs/run-1",
};
const results: BraveResultsPage = {rows: [answer], selected: answer, total: 26, succeeded: 25, failed: 1, page: 1, totalPages: 2};
const render = (page = results, showCompany = false) => renderToStaticMarkup(<MemoryRouter><BraveResults results={page} basePath={basePath} showCompany={showCompany} /></MemoryRouter>);

it("shows the question and complete formatted answer with a stable result link", () => {
  const html = render();
  expect(html).toContain(answer.query);
  expect(html).toContain("<strong>official website</strong>");
  expect(html).toContain("Full answer beyond the preview.");
  expect(html).toContain(`href="${basePath}?result=${resultId}#brave-result"`);
  expect(html).toContain(`href="${braveTaskResultsPath(taskId)}?result=${resultId}"`);
  expect(html).toContain('href="http://dagster:3000/runs/run-1"');
  expect(html).toContain(`${basePath}?page=2`);
});

it("displays failed attempts and the recorded reason, without a successful badge", () => {
  const failed: BraveResultDetail = {...answer, status: "error", error_type: "AnswerTimeout", error_stage: "answer", answer_text: "", answer_preview: ""};
  const html = render({...results, rows: [failed], selected: failed, succeeded: 0, failed: 1, total: 1});
  expect(html).toContain("Search failed");
  expect(html).toContain("AnswerTimeout");
  expect(html).toContain("Stage: ");
  expect(html).toContain("No answer was saved for this attempt.");
  expect(html).not.toContain(">Successful<");
});

it("links task companies to the same exact result in their Brave tab", () => {
  const html = render(results, true);
  expect(html).toContain(`href="${basePath}?result=${resultId}"`);
  expect(html).toContain("SE:5565245619");
});

it("renders an empty state for a company without results", () => {
  const html = render({...results, rows: [], selected: null, total: 0, succeeded: 0, failed: 0, totalPages: 1});
  expect(html).toContain("No Brave results yet");
  expect(html).not.toContain("id=\"brave-result\"");
});

it("does not execute HTML or unsafe URLs in a saved answer", () => {
  const html = render({...results, selected: {...answer, answer_text: '<script>alert(1)</script>\n\n[unsafe](javascript:alert(1))\n\n![remote](https://example.com/pixel.png)'}});
  expect(html).not.toContain("<script>");
  expect(html).not.toContain("javascript:");
  expect(html).not.toContain('src="https://example.com/pixel.png"');
});

it("pagination clears the selected result and selection preserves its page", () => {
  expect(braveResultsPath(basePath, 2)).toBe(`${basePath}?page=2`);
  expect(braveResultsPath(basePath, 2, resultId)).toBe(`${basePath}?page=2&result=${resultId}`);
});

it("shows CAPTCHA, reported usage, confirmation time and proxy beside the request", () => {
  const row = {...answer, captcha_requests: [{external_request_id: "browser-one", presented: true,
    detected_at: "2026-09-26T10:00:00Z", detection_source: "page" as const, confirmed_at: "2026-09-26T10:00:10Z",
    cleared: true, attempts: 2, prompt_tokens: 1200, completion_tokens: 100, models: ["model-one"],
    proxy_route: "crawl_proxy1", request_status: "success"}]};
  const html = render({...results, rows: [row], selected: row});
  for (const text of ["CAPTCHA", "Presented", "Cleared", "1,200", "2026-09-26 10:00:10 UTC", "crawl_proxy1", "model-one"]) expect(html).toContain(text);
  const historical = render({...results, selected: {...row, captcha_requests: [{...row.captcha_requests[0], confirmed_at: null}]}});
  expect(historical).toContain("<dt>Confirmed at</dt><dd>Not recorded</dd>");
});
