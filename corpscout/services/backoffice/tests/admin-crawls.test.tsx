import { renderToStaticMarkup } from "react-dom/server";
import { createMemoryRouter, RouterProvider } from "react-router";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { CrawlAttempt, CrawlSnapshot } from "~/lib/crawler";
import type { CrawlInputsSnapshot } from "~/lib/crawl-inputs";

const server = vi.hoisted(() => ({loadCrawls: vi.fn(), crawlAction: vi.fn()}));
vi.mock("~/lib/crawler.server", () => server);
const publisher = vi.hoisted(() => ({publishTestCrawl: vi.fn()}));
vi.mock("~/lib/crawl-submit.server", () => publisher);
const agent = vi.hoisted(() => ({runChallengeAgent: vi.fn()}));
vi.mock("~/lib/challenge-agent.server", () => agent);
const inputs = vi.hoisted(() => ({loadCrawlInputs: vi.fn(), startSavedCrawls: vi.fn()}));
vi.mock("~/lib/crawl-inputs.server", () => inputs);
const progress = vi.hoisted(() => ({loadCrawlProgress: vi.fn(), resumeCrawlTask: vi.fn()}));
vi.mock("~/lib/crawl-progress.server", () => progress);
import AdminCrawls, {action, loader} from "~/routes/admin-crawls";
import { CrawlBrowser } from "~/components/admin/crawl-browser";

const failure: CrawlAttempt = {
  request_id: "failed-one", attempt: 1, url: "https://melexis.com/", domain: "melexis.com",
  state: "failed", source: "rest", submitted_at: "2026-09-18T12:00:00Z", updated_at: "2026-09-18T12:00:10Z",
  current_url: "https://melexis.com/", reason: "Assistance timed out", blocked_reason: "captcha", error: "human_assistance_timeout",
  assistance_deadline: null, browser_available: false, verification_available: false, browser_session_id: null, retry_of: null, retry_of_attempt: null,
  s3_state: "uploaded", s3_error: null, s3_event: {result: {bucket: "crawls", key: "failed-one/attempts/0001/result.json.gz"}},
};
const snapshot: CrawlSnapshot = {attempts: [failure], total: 1, limit: 50, offset: 0, revision: 5, human_enabled: true};

beforeEach(() => vi.clearAllMocks());

describe("crawler backoffice", () => {
  it("shows the saved inventory separately from attempts with bounded manual controls", () => {
    const saved: CrawlInputsSnapshot = {stats: [{type: "site_info", total: 50, enabled: 49}], type: "site_info", domain: "", total: 50, offset: 0, limit: 25,
      rows: [{domain: "novelic.com", website_url: "https://novelic.com", enabled: true, priority: 50, headless: true, proxy_route: "direct", pages: [], page_mode: "discover", updated_at: failure.updated_at},
        {domain: "disabled.example", website_url: "https://disabled.example", enabled: false, priority: 40, headless: true, proxy_route: "direct", pages: [], page_mode: "discover", updated_at: failure.updated_at}]};
    const element = <AdminCrawls {...({loaderData: {snapshot, inputs: saved, error: null}} as Parameters<typeof AdminCrawls>[0])} />;
    const router = createMemoryRouter([{path: "/admin/crawls", element}], {initialEntries: ["/admin/crawls"]});
    const html = renderToStaticMarkup(<RouterProvider router={router} />);
    for (const label of ["Basic info inputs", "Basic info only", "Saved domains", "Disabled", "Activate 1 shown", "Next inputs", "Crawl attempts", "Processing status"]) expect(html).toContain(label);
    expect(html).toMatch(/<button[^>]*disabled=""[^>]*>Activate<span[^>]*> disabled.example<\/span>/);
    expect(html).toContain('role="tablist"');
    expect(html).not.toContain('id="input-captcha-model"');
    expect(inputs.startSavedCrawls).not.toHaveBeenCalled();
  });
  it("starts saved inputs only from a same-origin explicit action", async () => {
    inputs.startSavedCrawls.mockResolvedValue({runId: "run-one", count: 1});
    const body = new URLSearchParams({intent: "start-inputs", crawl_type: "site_info", domains: '["novelic.com"]', batch_id: "batch"});
    const request = new Request("http://backoffice/admin/crawls", {method: "POST", headers: {Origin: "http://backoffice"}, body});
    expect(await action({request} as Parameters<typeof action>[0])).toMatchObject({intent: "start-inputs", batch: {runId: "run-one"}});
    expect(inputs.startSavedCrawls).toHaveBeenCalledTimes(1);
    const rejected = new Request(request.url, {method: "POST", headers: {Origin: "http://other.test"}, body});
    expect((await action({request: rejected} as Parameters<typeof action>[0])).error).toContain("Cross-origin");
    expect(inputs.startSavedCrawls).toHaveBeenCalledTimes(1);
    expect(publisher.publishTestCrawl).not.toHaveBeenCalled();
  });

  it("keeps saved inputs visible when the crawler service is unavailable and never launches from a read", async () => {
    const saved = {stats: [{type: "site_info", total: 50, enabled: 50}], type: "site_info", domain: "", rows: [], total: 50, offset: 0, limit: 25};
    inputs.loadCrawlInputs.mockResolvedValue(saved);
    server.loadCrawls.mockRejectedValue(new Error("Crawler unavailable"));
    const result = await loader({request: new Request("http://backoffice/admin/crawls")} as Parameters<typeof loader>[0]);
    expect(result.inputs).toEqual(saved);
    expect(result.error).toBe("Crawler unavailable");
    expect(inputs.startSavedCrawls).not.toHaveBeenCalled();
  });
  it("submits automatic retry options while preserving automatic budget selection", async () => {
    server.crawlAction.mockResolvedValue({...failure, request_id: "retry-glm", interactive: false, challenge_agent_max_runs: 6});
    const payload = {intent: "retry", request_id: "failed-one", attempt: "1", retry_id: "retry-glm", interactive: "false", challenge_agent_max_runs: "", challenge_agent_model: "z-ai/glm-5.3-flash"};
    const request = new Request("http://backoffice/admin/crawls", {method: "POST", headers: {Origin: "http://backoffice"}, body: new URLSearchParams(payload)});
    const result = await action({request} as Parameters<typeof action>[0]);
    expect(result.error).toBeNull();
    expect(server.crawlAction).toHaveBeenCalledWith("failed-one", "retry", {attempt: 1, request_id: "retry-glm", interactive: false, challenge_agent_model: "z-ai/glm-5.3-flash"});
    const override = new Request("http://backoffice/admin/crawls", {method: "POST", headers: {Origin: "http://backoffice"}, body: new URLSearchParams({...payload, challenge_agent_max_runs: "12"})});
    await action({request: override} as Parameters<typeof action>[0]);
    expect(server.crawlAction).toHaveBeenLastCalledWith("failed-one", "retry", {attempt: 1, request_id: "retry-glm", interactive: false, challenge_agent_max_runs: 12, challenge_agent_model: "z-ai/glm-5.3-flash"});
  });

  it("shows saved partial data and the stopping URL on an older failed attempt", () => {
    const partial = {...failure, collected_pages: 0, current_url: "https://frame.work/laptop16?tab=specs", s3_event: {...failure.s3_event, page_count: 47}};
    const element = <AdminCrawls {...({loaderData: {snapshot: {...snapshot, attempts: [partial]}, error: null}} as Parameters<typeof AdminCrawls>[0])} />;
    const router = createMemoryRouter([{path: "/admin/crawls", element}], {initialEntries: ["/admin/crawls"]});
    const html = renderToStaticMarkup(<RouterProvider router={router} />);
    expect(html).toContain("partial · stopped");
    expect(html).toContain("47");
    expect(html).toContain("pages saved");
    expect(html).toContain("https://frame.work/laptop16?tab=specs");
    expect(html).toContain("Retry interactively");
  });

  it("keeps separate results for verification on different pages", () => {
    const results = ["https://frame.work/", "https://frame.work/laptop16"].map((pageUrl,index) => ({
      runId: `auto-${index}`, state: "appears_clear", reason: null, pageUrl,
      trigger: "automatic" as const, accessVerified: true, steps: [], elapsedSeconds: 5,
      usage: {prompt_tokens: 10, completion_tokens: 5},
    }));
    const html = renderToStaticMarkup(<CrawlBrowser attempt={{...failure, challenge_agent_results: results, challenge_agent_result: results[1]}} browserUrl={null}
      onVerify={vi.fn()} onResume={vi.fn()} onCancel={vi.fn()} onReconnect={vi.fn()} busy={false} />);
    expect(html).toContain("auto-0");
    expect(html).toContain("auto-1");
    expect(html).toContain("https://frame.work/laptop16");
    expect(html.match(/crawler verified access and continued/g)).toHaveLength(2);
  });
  it("shows server-driven automatic assistance and disables competing controls", () => {
    const running = {...failure, state: "captcha" as const, browser_available: true, challenge_agent_running: true};
    const html = renderToStaticMarkup(<CrawlBrowser attempt={running} browserUrl={null}
      onVerify={vi.fn()} onResume={vi.fn()} onCancel={vi.fn()} onReconnect={vi.fn()} onAgent={vi.fn()} busy={false} />);
    expect(html).toContain("Agent running");
    expect(html).toContain("continue automatically");
    expect(html).toMatch(/<button[^>]*\sdisabled=""[^>]*>Resume crawl<\/button>/);
    expect(html).not.toMatch(/<button[^>]*\sdisabled=""[^>]*>Cancel<\/button>/);
  });

  it("shows a persisted automatic result without asking the user to resume", () => {
    const completed = {...failure, state: "completed" as const, challenge_agent_result: {
      runId: "auto-one", state: "appears_clear", reason: "Challenge cleared", trigger: "automatic" as const,
      accessVerified: true, steps: [], elapsedSeconds: 5, usage: {prompt_tokens: 100, completion_tokens: 10},
    }};
    const html = renderToStaticMarkup(<CrawlBrowser attempt={completed} browserUrl={null}
      onVerify={vi.fn()} onResume={vi.fn()} onCancel={vi.fn()} onReconnect={vi.fn()} busy={false} />);
    expect(html).toContain("crawler verified access and continued");
    expect(html).toContain("110 tokens");
    expect(html).toContain("auto-one");
    expect(html).not.toContain("Resume the crawl to check access");
  });
  it("runs the agent only from an explicit same-origin action", async () => {
    agent.runChallengeAgent.mockResolvedValue({runId: "agent-one", state: "needs_human"});
    const request = new Request("http://backoffice/admin/crawls", {method: "POST", headers: {Origin: "http://backoffice"}, body: new URLSearchParams({intent: "challenge-agent", request_id: "retry-one"})});
    const result = await action({request} as Parameters<typeof action>[0]);
    expect(result).toMatchObject({intent: "challenge-agent", requestId: "retry-one", agent: {runId: "agent-one"}});
    expect(agent.runChallengeAgent).toHaveBeenCalledWith("retry-one");
    expect(server.crawlAction).not.toHaveBeenCalled();
    const crossOrigin = new Request("http://backoffice/admin/crawls", {method: "POST", headers: {Origin: "https://other.test"}, body: new URLSearchParams({intent: "challenge-agent", request_id: "retry-one"})});
    expect((await action({request: crossOrigin} as Parameters<typeof action>[0])).error).toContain("Cross-origin");
    expect(agent.runChallengeAgent).toHaveBeenCalledTimes(1);
  });

  it("shows agent limits and keeps Cancel available while the agent runs", () => {
    const paused = {...failure, state: "captcha" as const, browser_available: true};
    const html = renderToStaticMarkup(<CrawlBrowser attempt={paused} browserUrl={null}
      onVerify={vi.fn()} onResume={vi.fn()} onCancel={vi.fn()} onReconnect={vi.fn()} onAgent={vi.fn()} busy={false} agentBusy />);
    expect(html).toContain("DeepSeek V4.1 Flash");
    expect(html).toContain("Agent running");
    expect(html).toMatch(/<button[^>]*\sdisabled=""[^>]*>Resume crawl<\/button>/);
    expect(html).not.toMatch(/<button[^>]*\sdisabled=""[^>]*>Cancel<\/button>/);
    expect(html).toContain("crawl stays paused");
  });

  it("submits test requests through the test-crawl submitter, not a crawler action", async () => {
    const receipt = {request_id: "test-one", url: "https://novelic.com/", state: "queued"};
    publisher.publishTestCrawl.mockResolvedValue(receipt);
    const body = JSON.stringify({request_id: "test-one", url: "novelic.com", crawl: "full"});
    const request = new Request("http://backoffice/admin/crawls", {method: "POST", headers: {Origin: "http://backoffice"}, body: new URLSearchParams({intent: "submit", body})});
    expect(await action({request} as Parameters<typeof action>[0])).toEqual({error: null, intent: "submit", receipt});
    expect(publisher.publishTestCrawl).toHaveBeenCalledWith(body);
    expect(server.crawlAction).not.toHaveBeenCalled();
  });

  it("resumes a crawl task through the route action", async () => {
    const resumed = {runId: "new-run", status: "QUEUED", runUrl: "http://dagster/runs/new-run", taskId: "task", executionId: "1562550f-3625-44ab-b03e-779df9a1adc8"};
    progress.resumeCrawlTask.mockResolvedValue(resumed);
    const request = new Request("http://backoffice/admin/crawls", {method: "POST", headers: {Origin: "http://backoffice"},
      body: new URLSearchParams({intent: "resume-task", crawl_type: "site_info", execution_id: resumed.executionId})});
    expect(await action({request} as Parameters<typeof action>[0])).toEqual({error: null, intent: "resume-task", resumed});
    expect(progress.resumeCrawlTask).toHaveBeenCalledWith("site_info", resumed.executionId);
    progress.resumeCrawlTask.mockRejectedValue(new Error("This crawl task is still running."));
    const again = new Request("http://backoffice/admin/crawls", {method: "POST", headers: {Origin: "http://backoffice"},
      body: new URLSearchParams({intent: "resume-task", crawl_type: "site_info", execution_id: resumed.executionId})});
    expect(await action({request: again} as Parameters<typeof action>[0])).toEqual({error: "This crawl task is still running."});
  });

  it("rejects cross-origin test submissions before publishing", async () => {
    const request = new Request("http://backoffice/admin/crawls", {method: "POST", headers: {Origin: "https://untrusted.test"}, body: new URLSearchParams({intent: "submit", body: "{}"})});
    expect((await action({request} as Parameters<typeof action>[0])).error).toContain("Cross-origin");
    expect(publisher.publishTestCrawl).not.toHaveBeenCalled();
  });

  it("offers manual activation on the paused attempt before browser or resume controls", () => {
    const paused = {...failure, state: "captcha" as const, verification_available: true,
      reason: "Brave searches are paused. Start verification.", blocked_reason: "brave_captcha"};
    const element = <AdminCrawls {...({loaderData: {snapshot: {...snapshot, attempts: [paused]}, error: null}} as Parameters<typeof AdminCrawls>[0])} />;
    const router = createMemoryRouter([{path: "/admin/crawls", element}], {initialEntries: ["/admin/crawls"]});
    const html = renderToStaticMarkup(<RouterProvider router={router} />);
    expect(html).toContain("Start verification");
    expect(html).toContain("Brave searches are paused");
    expect(html).not.toContain("Retry interactively");
    const panel = renderToStaticMarkup(<CrawlBrowser attempt={paused} browserUrl={null}
      onVerify={vi.fn()} onResume={vi.fn()} onCancel={vi.fn()} onReconnect={vi.fn()} busy={false} />);
    expect(panel).toMatch(/<button[^>]*disabled[^>]*>Resume crawl<\/button>/);
    expect(panel).not.toContain("Reconnect browser");
  });

  it("activates verification on the existing request without creating a retry", async () => {
    server.crawlAction.mockResolvedValue({state: "verification_requested"});
    const request = new Request("http://backoffice/admin/crawls", {method: "POST", headers: {Origin: "http://backoffice"},
      body: new URLSearchParams({intent: "verify-search", request_id: "paused-one"})});
    await action({request} as Parameters<typeof action>[0]);
    expect(server.crawlAction).toHaveBeenCalledWith("paused-one", "verify-search", undefined);
  });

  it("shows saved failure, archive status, filters and interactive retry", () => {
    const element = <AdminCrawls {...({loaderData: {snapshot, error: null}} as Parameters<typeof AdminCrawls>[0])} />;
    const router = createMemoryRouter([{path: "/admin/crawls", element}], {initialEntries: ["/admin/crawls"]});
    const html = renderToStaticMarkup(<RouterProvider router={router} />);
    for (const label of ["melexis.com", "captcha", "S3 saved", "Retry interactively", "All statuses", "REST", "SQLite history"]) expect(html).toContain(label);
    expect(html).toContain("/admin/crawls/results?path=crawls%2Ffailed-one%2Fattempts%2F0001%2Fresult.json.gz");
  });

  it("loads crawler filters and represents an unavailable service", async () => {
    server.loadCrawls.mockResolvedValue(snapshot);
    await loader({request: new Request("http://backoffice/admin/crawls?state=failed&domain=melexis")} as Parameters<typeof loader>[0]);
    expect(server.loadCrawls.mock.calls[0][0].get("state")).toBe("failed");
    server.loadCrawls.mockRejectedValue(new Error("Crawler unavailable"));
    const result = await loader({request: new Request("http://backoffice/admin/crawls")} as Parameters<typeof loader>[0]);
    expect(result.error).toBe("Crawler unavailable");
  });

  it("preserves the failed attempt identity in a separate manual retry", async () => {
    server.crawlAction.mockResolvedValue({...failure, request_id: "manual-one", state: "queued"});
    const form = new URLSearchParams({intent: "retry", request_id: "failed-one", attempt: "1", retry_id: "manual-one"});
    const request = new Request("http://backoffice/admin/crawls", {method: "POST", headers: {Origin: "http://backoffice"}, body: form});
    await action({request} as Parameters<typeof action>[0]);
    expect(server.crawlAction).toHaveBeenCalledWith("failed-one", "retry", {attempt: 1, request_id: "manual-one"});
  });

  it("rejects cross-origin controls before contacting the crawler", async () => {
    const request = new Request("http://backoffice/admin/crawls", {method: "POST", headers: {Origin: "https://untrusted.test"}, body: new URLSearchParams({intent: "cancel", request_id: "failed-one"})});
    const result = await action({request} as Parameters<typeof action>[0]);
    expect(result.error).toContain("Cross-origin");
    expect(server.crawlAction).not.toHaveBeenCalled();
  });
});
