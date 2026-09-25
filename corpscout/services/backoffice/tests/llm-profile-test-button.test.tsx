import { renderToStaticMarkup } from "react-dom/server";
import { createMemoryRouter, Form, RouterProvider } from "react-router";
import { beforeEach, describe, expect, it, vi } from "vitest";
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
    isActive: true, revision: 1, state: "enabled", disabledReason: null, lastCheck: null, apiKeyAvailable: true,
    createdAt: "2026-09-25T12:00:00.000Z", updatedAt: "2026-09-25T12:00:00.000Z",
  },
  {
    profileId: "second", name: "Other model", provider: "openrouter",
    baseUrl: "https://openrouter.ai/api/v1", model: "example/other",
    isActive: false, revision: 1, state: "enabled", disabledReason: null, lastCheck: null, apiKeyAvailable: false,
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

function testForms(html: string) {
  return [...html.matchAll(/<form\b[^>]*aria-label="Test [^"]+"[^>]*>[\s\S]*?<\/form>/g)].map(match => match[0]);
}

beforeEach(() => {
  vi.resetAllMocks();
  mocks.fetcher.mockReturnValue({state: "idle", data: undefined, Form});
});

describe("saved LLM profile testing", () => {
  it("posts only the saved profile identity and lets a missing key produce a server explanation", () => {
    const html = render();
    const forms = testForms(html);
    expect(forms).toHaveLength(2);
    forms.forEach((form, index) => {
      expect(form).toContain('method="post"');
      expect(form).toContain('action="/admin/settings/llms"');
      expect([...form.matchAll(/<input\b[^>]*name="([^"]+)"/g)].map(match => match[1])).toEqual(["intent", "profile_id"]);
      expect(form).toContain('name="intent" value="test"');
      expect(form).toContain(`name="profile_id" value="${profiles[index].profileId}"`);
      expect(form).not.toContain('disabled=""');
    });
    expect(html).toContain("Key missing");
    expect(html).toContain("short text request");
    expect(html).toContain("additional vision or CAPTCHA capabilities");
  });

  it("shows pending feedback only for the profile being tested", () => {
    mocks.fetcher.mockReturnValueOnce({state: "submitting", data: undefined, Form});
    const html = render();
    const forms = testForms(html);
    expect(forms[0]).toContain("Testing…");
    expect(forms[0]).toContain('disabled=""');
    expect(forms[1]).toContain(">Test</button>");
    expect(forms[1]).not.toContain('disabled=""');
    expect(html).toContain("Testing the saved configuration for Primary model…");
    expect(html).not.toContain("Testing the saved configuration for Other model…");
    expect(html).toContain('role="status" aria-live="polite"');
    expect(html).toContain('value="Primary model"');
  });

  it("shows independent success and failure messages without a save error", () => {
    for (const [index, ok] of [true, false].entries()) {
      mocks.fetcher.mockReturnValueOnce({state: "idle", Form, data: {
        testResult: {profileId: profiles[index].profileId, ok,
          message: ok ? "The saved model returned valid JSON." : "Save an API key before testing this profile.",
          checkedAt: "2026-09-25T12:01:00.000Z"}, values: null, error: "",
      }});
    }
    const html = render();
    expect(html).toContain("Test passed");
    expect(html).toContain("The saved model returned valid JSON.");
    expect(html).toContain("Test failed");
    expect(html).toContain("Save an API key before testing this profile.");
    expect(html.match(/role="status"/g)).toHaveLength(2);
    expect(html).not.toContain("Could not save LLM profile");
    expect(html).toContain('value="Primary model"');
  });
});
