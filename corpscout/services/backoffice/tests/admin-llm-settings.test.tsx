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

describe("admin LLM settings", () => {
  it("shows active model metadata and only the API-key variable name", () => {
    const router = createMemoryRouter(
      [
        {
          path: "*",
          element: (
            <LlmSettingsWorkspace
              profiles={[
                {
                  profileId: "profile-1",
                  name: "DeepSeek production",
                  provider: "DeepSeek",
                  baseUrl: "https://api.deepseek.com",
                  model: "deepseek-v4-flash",
                  apiKeyEnvironmentVariable: "DEEPSEEK_API_KEY",
                  isActive: true,
                  apiKeyAvailable: true,
                  createdAt: "2026-08-20T12:00:00.000Z",
                  updatedAt: "2026-08-20T12:00:00.000Z",
                },
              ]}
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
    expect(html).toContain("DEEPSEEK_API_KEY");
    expect(html).toContain("API keys remain outside the settings database");
    expect(html).toContain("Key available");
    expect(html).not.toContain('type="password"');
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
