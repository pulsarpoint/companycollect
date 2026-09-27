import { renderToStaticMarkup } from "react-dom/server";
import { createMemoryRouter, Form, RouterProvider } from "react-router";
import { describe, expect, it, vi } from "vitest";
import { LlmTestDetails } from "~/components/admin/llm-test-sheet";
import { LlmSettingsWorkspace } from "~/components/admin/llm-settings-workspace";
import type { LlmProfile } from "~/lib/llm-settings.server";

const mocks = vi.hoisted(() => ({fetcher: vi.fn()}));
vi.mock("react-router", async importOriginal => ({
  ...await importOriginal<typeof import("react-router")>(),
  useFetcher: mocks.fetcher,
}));

const profiles: LlmProfile[] = [
  {
    profileId: "first", name: "Primary model", provider: "openrouter",
    baseUrl: "https://openrouter.ai/api/v1", model: "example/primary",
    reasoningEffort: null, isActive: true, revision: 1, state: "enabled", disabledReason: null, lastCheck: null, apiKeyAvailable: true,
    createdAt: "2026-09-25T12:00:00.000Z", updatedAt: "2026-09-25T12:00:00.000Z",
  },
  {
    profileId: "second", name: "Other model", provider: "openrouter",
    baseUrl: "https://openrouter.ai/api/v1", model: "example/other",
    reasoningEffort: null, isActive: false, revision: 1, state: "enabled", disabledReason: null, lastCheck: null, apiKeyAvailable: false,
    createdAt: "2026-09-25T12:00:00.000Z", updatedAt: "2026-09-25T12:00:00.000Z",
  },
];

function render() {
  const router = createMemoryRouter([{
    path: "*", action: () => null,
    element: <LlmSettingsWorkspace profiles={profiles} editingProfile={profiles[0]} />,
  }], {initialEntries: ["/admin/settings/llms?edit=first"]});
  return renderToStaticMarkup(<RouterProvider router={router} />);
}

describe("saved LLM profile testing", () => {
  it("opens a model-specific preview instead of immediately posting a test", () => {
    mocks.fetcher.mockReturnValue({state: "idle", data: undefined, Form});
    const html = render();
    expect(html).toContain('/admin/settings/llms?test=first');
    expect(html).toContain('/admin/settings/llms?test=second');
    expect(html).not.toContain('name="intent" value="test"');
    expect(html).toContain("No API key");
  });
  const preview = {request: {method: "POST", url: "http://local/v1/chat/completions", body: {model: "local-model", messages: [{role: "user", content: "Test JSON"}]}}, response: null, elapsed_ms: null};
  it("shows the request before running and full JSON usage and reasoning afterward", () => {
    const before = renderToStaticMarkup(<LlmTestDetails preview={preview} result={null} testing={false} error="" />);
    expect(before).toContain("Request preview");
    expect(before).toContain("local-model");
    expect(before).toContain("Run the test");
    const body = JSON.stringify({choices: [{message: {content: "ok", reasoning_content: "reason ".repeat(1000)}}], usage: {total_tokens: 42}});
    const after = renderToStaticMarkup(<LlmTestDetails preview={preview} testing={false} error="" result={{profileId: "first", ok: true, message: "Verified", checkedAt: "now", exchange: {...preview, response: {status: 200, content_type: "application/json", body}, elapsed_ms: 1234}}} />);
    expect(after).toContain("Request sent");
    expect(after).toContain("HTTP 200");
    expect(after).toContain("1.23 s");
    expect(after).toContain("total_tokens");
    expect(after).toContain("reason ".repeat(1000));
  });
  it("shows pending feedback and renders non-JSON provider errors as text", () => {
    const result = {profileId: "first", ok: false, message: "Provider failed", checkedAt: "now", exchange: {...preview, response: {status: 502, content_type: "text/html", body: '<script>alert("error")</script>'}, elapsed_ms: 200}};
    const pending = renderToStaticMarkup(<LlmTestDetails preview={preview} result={result} testing error="" />);
    expect(pending).toContain("Waiting for the model response");
    expect(pending).not.toContain("HTTP 502");
    const failed = renderToStaticMarkup(<LlmTestDetails preview={preview} result={result} testing={false} error="" />);
    expect(failed).toContain("HTTP 502");
    expect(failed).toContain("Test failed");
    expect(failed).toContain("&lt;script&gt;");
    expect(failed).not.toContain("<script>");
  });
});
