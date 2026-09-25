import { renderToStaticMarkup } from "react-dom/server";
import {
  createMemoryRouter,
  MemoryRouter,
  RouterProvider,
} from "react-router";
import { describe, expect, it } from "vitest";
import { AdminSidebar } from "~/components/admin/admin-sidebar";
import { LlmSettingsWorkspace } from "~/components/admin/llm-settings-workspace";
import { SidebarProvider } from "~/components/ui/sidebar";
import type { LlmProfile } from "~/lib/llm-settings.server";

const profile: LlmProfile = {
  profileId: "profile-1", name: "DeepSeek production", provider: "DeepSeek",
  baseUrl: "https://api.deepseek.com", model: "deepseek-v4-flash",
  isActive: true, apiKeyAvailable: true,
  createdAt: "2026-08-20T12:00:00.000Z", updatedAt: "2026-08-20T12:00:00.000Z",
};

function renderWorkspace(element: React.ReactElement) {
  const router = createMemoryRouter([{path: "*", element, action: () => null}], {
    initialEntries: ["/admin/settings/llms"],
  });
  return renderToStaticMarkup(<RouterProvider router={router} />);
}

function apiKeyInput(html: string) {
  return html.match(/<input\b[^>]*name="api_key"[^>]*>/)?.[0] ?? "";
}

describe("admin LLM settings", () => {
  it("shows active model metadata and saved-key status with a blank password field", () => {
    const router = createMemoryRouter(
      [
        {
          path: "*",
          element: (
            <LlmSettingsWorkspace
              profiles={[profile]}
              editingProfile={null}
            />
          ),
          action: () => null,
        },
      ],
      { initialEntries: ["/admin/settings/llms"] },
    );
    const html = renderToStaticMarkup(<RouterProvider router={router} />);

    expect(html).toContain("LLM settings");
    expect(html).toContain("DeepSeek production");
    expect(html).toContain("deepseek-v4-flash");
    expect(html).toContain("API keys are encrypted in the settings database");
    expect(html).toContain("Key saved");
    expect(apiKeyInput(html)).toContain('required=""');
    expect(apiKeyInput(html)).toContain('type="password"');
    expect(apiKeyInput(html)).not.toContain("value=");
    expect(html).not.toContain("api_key_environment_variable");
    // The parameters card is split into Remote / Local tabs.
    expect(html).toContain("Remote");
    expect(html).toContain("Local");
  });

  it("lets an edit retain its saved key without ever populating the password field", () => {
    const html = renderWorkspace(<LlmSettingsWorkspace profiles={[profile]} editingProfile={profile} />);
    expect(html).toContain("Leave blank to keep the saved API key");
    expect(apiKeyInput(html)).toContain('type="password"');
    expect(apiKeyInput(html)).not.toContain("required=");
    expect(apiKeyInput(html)).not.toContain("value=");
  });

  it("requires a key when editing a profile whose key is missing", () => {
    const missing = {...profile, apiKeyAvailable: false};
    const html = renderWorkspace(<LlmSettingsWorkspace profiles={[missing]} editingProfile={missing} />);
    expect(html).toContain("Key missing");
    expect(apiKeyInput(html)).toContain('required=""');
    expect(html).not.toContain("Leave blank to keep the saved API key");
  });

  it("restores non-secret metadata after a save error while leaving the key blank", () => {
    const html = renderWorkspace(<LlmSettingsWorkspace profiles={[]} editingProfile={null}
      submittedValues={{profileId: "", name: "Invalid profile", provider: "Provider", baseUrl: "https://example.com", model: "model"}}
      error="Profile name is already in use." />);
    expect(html).toContain('value="Invalid profile"');
    expect(html).toContain("Profile name is already in use.");
    expect(apiKeyInput(html)).not.toContain("value=");
  });

  it("shows the local codex on/off radio on the Local tab", () => {
    const router = createMemoryRouter(
      [
        {
          path: "*",
          element: (
            <LlmSettingsWorkspace
              profiles={[]}
              editingProfile={null}
              localCodexEnabled={true}
              initialTab="local"
            />
          ),
          action: () => null,
        },
      ],
      { initialEntries: ["/admin/settings/llms"] },
    );
    const html = renderToStaticMarkup(<RouterProvider router={router} />);

    expect(html).toContain("local_codex");
    // A single switch, reflecting the stored toggle state.
    expect(html).toContain('data-slot="switch"');
    expect(html).toContain("data-checked");
    expect(html).toContain('name="intent" value="set_local_codex"');
  });

  it("renders the local codex switch unchecked when the toggle is off", () => {
    const router = createMemoryRouter(
      [
        {
          path: "*",
          element: (
            <LlmSettingsWorkspace
              profiles={[]}
              editingProfile={null}
              localCodexEnabled={false}
              initialTab="local"
            />
          ),
          action: () => null,
        },
      ],
      { initialEntries: ["/admin/settings/llms"] },
    );
    const html = renderToStaticMarkup(<RouterProvider router={router} />);

    expect(html).toContain('data-slot="switch"');
    expect(html).toContain("data-unchecked");
  });

  it("adds a Settings navigation section with the LLM page active", () => {
    const html = renderToStaticMarkup(
      <MemoryRouter initialEntries={["/admin/settings/llms"]}>
        <SidebarProvider>
          <AdminSidebar />
        </SidebarProvider>
      </MemoryRouter>,
    );

    expect(html).toContain("Settings");
    expect(html).toContain("LLMs");
    expect(html).toContain('href="/admin/settings/llms"');
    expect(html).toContain('data-open=""');
  });
});
