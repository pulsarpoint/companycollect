import { renderToStaticMarkup } from "react-dom/server";
import { createMemoryRouter, RouterProvider } from "react-router";
import { describe, expect, it } from "vitest";
import { LocalCodexWorkspaceView } from "~/routes/admin-settings-llms-local";

const THREADS = [
  {
    threadId: "thread-1",
    page: "/admin/settings/llms/local",
    title: "hello codex",
    createdAt: "2026-09-01T10:00:00.000Z",
    updatedAt: "2026-09-01T10:05:00.000Z",
  },
];

const HISTORY = {
  thread: THREADS[0],
  messages: [
    {
      messageId: "m1",
      threadId: "thread-1",
      role: "user" as const,
      content: "hello codex",
      createdAt: "2026-09-01T10:00:00.000Z",
    },
    {
      messageId: "m2",
      threadId: "thread-1",
      role: "agent" as const,
      content: "hi there",
      createdAt: "2026-09-01T10:05:00.000Z",
    },
  ],
};

function render(view: React.ReactElement): string {
  const router = createMemoryRouter(
    [{ path: "*", element: view, action: () => null }],
    { initialEntries: ["/admin/settings/llms/local"] },
  );
  return renderToStaticMarkup(<RouterProvider router={router} />);
}

describe("local codex workspace", () => {
  it("prompts to enable the toggle when local codex is off", () => {
    const html = render(
      <LocalCodexWorkspaceView
        localCodexEnabled={false}
        threads={[]}
        history={null}
        error=""
      />,
    );
    expect(html).toContain("local_codex");
    expect(html).toContain("Enable local codex");
    expect(html).not.toContain("New conversation");
  });

  it("shows threads, history, and the send form when enabled", () => {
    const html = render(
      <LocalCodexWorkspaceView
        localCodexEnabled={true}
        threads={THREADS}
        history={HISTORY}
        error=""
      />,
    );
    expect(html).toContain("New conversation");
    expect(html).toContain("hello codex");
    expect(html).toContain("hi there");
    expect(html).toContain("?thread=thread-1");
    expect(html).toContain('name="intent" value="send"');
    expect(html).toContain('name="intent" value="remove"');
  });

  it("offers only the new-conversation form when no thread is selected", () => {
    const html = render(
      <LocalCodexWorkspaceView
        localCodexEnabled={true}
        threads={THREADS}
        history={null}
        error=""
      />,
    );
    expect(html).toContain('name="intent" value="new-thread"');
    expect(html).not.toContain('name="intent" value="send"');
  });
});
