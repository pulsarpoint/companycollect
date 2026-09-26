import { beforeEach, expect, it, vi } from "vitest";

const db = vi.hoisted(() => ({ query: vi.fn() }));
const pg = vi.hoisted(() => ({ query: vi.fn() }));
vi.mock("~/lib/clickhouse.server", () => ({ chQuery: db.query }));
vi.mock("~/lib/llm-control.server", () => ({ llmControl: () => pg }));
import { loadBraveResults } from "~/lib/brave-results.server";
import { loader as companyLoader } from "~/routes/admin-se-company-brave";
import { loader as taskLoader } from "~/routes/admin-brave-results";

const taskId = "11111111-1111-4111-8111-111111111111";
const resultId = "22222222-2222-4222-8222-222222222222";
const companyId = "5565245619";
beforeEach(() => { db.query.mockReset(); pg.query.mockReset().mockResolvedValue({rows: []}); });

it("reads company attempt history with country identity, errors, and deduplication", async () => {
  db.query.mockResolvedValueOnce([{total: "51", succeeded: "50", failed: "1"}])
    .mockResolvedValueOnce([{result_id: resultId, status: "error"}])
    .mockResolvedValueOnce([{result_id: resultId, status: "error", error_type: "TimeoutError", source_run_id: ""}]);
  const page = await loadBraveResults({countryCode: "SE", companyId}, new URLSearchParams("page=2"));
  expect(page).toMatchObject({total: 51, succeeded: 50, failed: 1, page: 2, totalPages: 3,
    selected: {result_id: resultId, status: "error", error_type: "TimeoutError"}});
  for (const [sql, params] of db.query.mock.calls) {
    expect(sql).toContain("company_brave_search_results AS r FINAL");
    expect(sql).toContain("r.country_code = {countryCode:String} AND r.company_id = {companyId:String}");
    expect(sql).not.toContain(companyId);
    expect(sql).not.toContain("latest_success");
    expect(params).toMatchObject({countryCode: "SE", companyId});
  }
  expect(db.query.mock.calls[1][0]).toContain("ORDER BY r.completed_at DESC, r.result_id DESC LIMIT 25 OFFSET");
  expect(db.query.mock.calls[1][1].offset).toBe(25);
});

it("can open a saved result outside the current page but always inside its task", async () => {
  db.query.mockResolvedValueOnce([{total: "60", succeeded: "60", failed: "0"}])
    .mockResolvedValueOnce([{result_id: "33333333-3333-4333-8333-333333333333"}])
    .mockResolvedValueOnce([{result_id: resultId, source_run_id: ""}]);
  const page = await loadBraveResults({taskId}, new URLSearchParams({result: resultId}));
  expect(page.selected?.result_id).toBe(resultId);
  expect(db.query.mock.calls[2][0]).toContain("r.task_id = {taskId:UUID} AND r.result_id = {resultId:UUID}");
  expect(db.query.mock.calls[2][1]).toEqual({taskId, resultId});
  expect(db.query.mock.calls[1][0]).not.toContain("answer_text, source_url");
  expect(db.query.mock.calls[2][0]).toContain("answer_text, source_url");
});

it.each([{taskId}, {countryCode: "SE", companyId}])("does not return a result belonging to another scope: %j", async scope => {
  db.query.mockResolvedValueOnce([{total: "0", succeeded: "0", failed: "0"}]).mockResolvedValue([]);
  await expect(loadBraveResults(scope, new URLSearchParams({result: resultId}))).rejects.toMatchObject({status: 404});
});

it("returns an empty state without requesting an answer when nothing has been saved", async () => {
  db.query.mockResolvedValueOnce([{total: "0", succeeded: "0", failed: "0"}]).mockResolvedValueOnce([]);
  expect(await loadBraveResults({taskId}, new URLSearchParams())).toMatchObject({rows: [], selected: null, total: 0, page: 1, totalPages: 1});
  expect(db.query).toHaveBeenCalledTimes(2);
});

it.each(["0", "-1", "bad", "1.5", "99999999999999999"])("normalizes invalid page %s before using an offset", async page => {
  db.query.mockResolvedValueOnce([{total: "60"}]).mockResolvedValueOnce([]);
  expect((await loadBraveResults({taskId}, new URLSearchParams({page}))).page).toBe(1);
  expect(db.query.mock.calls[1][1].offset).toBe(0);
});

it("clamps an out-of-range page to the last page", async () => {
  db.query.mockResolvedValueOnce([{total: "60"}]).mockResolvedValueOnce([]);
  expect((await loadBraveResults({taskId}, new URLSearchParams("page=100"))).page).toBe(3);
  expect(db.query.mock.calls[1][1].offset).toBe(50);
});

it("rejects malformed task or result IDs before querying", async () => {
  await expect(loadBraveResults({taskId: "not-a-task"}, new URLSearchParams())).rejects.toMatchObject({status: 400});
  await expect(loadBraveResults({taskId}, new URLSearchParams("result=wrong"))).rejects.toMatchObject({status: 400});
  expect(db.query).not.toHaveBeenCalled();
});

it("propagates database errors instead of displaying an empty history", async () => {
  db.query.mockRejectedValue(new Error("unavailable"));
  await expect(loadBraveResults({taskId}, new URLSearchParams())).rejects.toThrow("unavailable");
});

it("wires company and task routes to their respective saved result scopes", async () => {
  db.query.mockResolvedValue([]);
  await companyLoader({params: {companyId}, request: new Request("http://localhost/admin/se/company/5565245619/brave")} as never);
  expect(db.query.mock.calls[0][1]).toEqual({countryCode: "SE", companyId});
  db.query.mockClear();
  await taskLoader({params: {taskId}, request: new Request(`http://localhost/admin/queues/brave/results/${taskId}`)} as never);
  expect(db.query.mock.calls[0][1]).toEqual({taskId});
});

it("loads PostgreSQL CAPTCHA stats for the page and selected result, deduplicating resumed request owners", async () => {
  db.query.mockResolvedValueOnce([{total: "1"}]).mockResolvedValueOnce([{result_id: resultId}])
    .mockResolvedValueOnce([{result_id: resultId, source_run_id: ""}]);
  pg.query.mockResolvedValue({rows: [{brave_result_id: resultId, external_request_id: "browser-1",
    captcha_stats: {presented: true, prompt_tokens: 150, proxy_route: "crawl_proxy1"}}]});
  const page = await loadBraveResults({taskId}, new URLSearchParams());
  expect(page.rows[0].captcha_requests).toEqual(page.selected?.captcha_requests);
  expect(page.selected?.captcha_requests).toEqual([{external_request_id: "browser-1", presented: true, prompt_tokens: 150, proxy_route: "crawl_proxy1"}]);
  expect(pg.query.mock.calls[0][0]).toContain("DISTINCT ON (external_request_id)");
  expect(pg.query.mock.calls[0][1]).toEqual([[resultId]]);
});
