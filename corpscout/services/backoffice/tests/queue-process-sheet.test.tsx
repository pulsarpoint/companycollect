import { Children, isValidElement, type FormEventHandler, type ReactNode } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { MemoryRouter } from "react-router";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { QueueProcessSheet } from "~/components/admin/queue-process-sheet";
import { QUEUE_TEMPLATES, parseQueueFilters, type QueueType } from "~/lib/queues";

const mocks = vi.hoisted(() => ({
  fetcher: vi.fn(), submit: vi.fn(),
  onSubmit: undefined as FormEventHandler<HTMLFormElement> | undefined,
}));
vi.mock("react-router", async importOriginal => ({...await importOriginal<typeof import("react-router")>(), useFetcher: mocks.fetcher}));
vi.mock("~/components/ui/sheet", () => {
  const Wrapper = ({children}: {children: ReactNode}) => <div>{children}</div>;
  return {
    Sheet: Wrapper, SheetHeader: Wrapper, SheetTitle: Wrapper, SheetDescription: Wrapper, SheetFooter: Wrapper,
    SheetContent: ({children}: {children: ReactNode}) => {
      const form = Children.toArray(children).find(child => isValidElement(child) && child.type === "form");
      if (isValidElement<{onSubmit: FormEventHandler<HTMLFormElement>}>(form)) mocks.onSubmit = form.props.onSubmit;
      return <div>{children}</div>;
    },
  };
});

const profiles = [
  {profileId: "123", name: "Vision model", provider: "openrouter", model: "example/vision", apiKeyAvailable: true},
  {profileId: "missing-key", name: "Missing key", provider: "deepseek", model: "other/model", apiKeyAvailable: false},
];
const task = "85fa92ee-12ad-4c01-ad1c-508f5c83b925";

function render(type: QueueType = "brave", state = "idle", llmProfileId?: string) {
  mocks.fetcher.mockReturnValueOnce({state, data: undefined, submit: mocks.submit})
    .mockReturnValue({state: "idle", data: {profiles, error: null}, load: vi.fn()});
  return renderToStaticMarkup(<MemoryRouter><QueueProcessSheet
    filters={parseQueueFilters(type, new URLSearchParams({task}))} total={4} asset="results"
    llmProfileId={llmProfileId} onClose={vi.fn()} /></MemoryRouter>);
}

beforeEach(() => { vi.resetAllMocks(); mocks.onSubmit = undefined; });
afterEach(() => { vi.unstubAllGlobals(); });

describe("Brave queue LLM selection", () => {
  it("requires an initially empty saved-model choice before the other processing options", () => {
    const html = render();
    expect(html).toContain("Browser assistant LLM");
    expect(html).toContain("browser and CAPTCHA assistant");
    expect(html).toContain("image and JSON response check");
    expect(html).toMatch(/<select[^>]*name="llm_profile_id"[^>]*required=""/);
    expect(html).toMatch(/<option[^>]*value=""[^>]*selected=""/);
    expect(html.indexOf('name="llm_profile_id"')).toBeLessThan(html.indexOf('name="force_rescan"'));
    expect(html).toMatch(/<option[^>]*value="missing-key"[^>]*disabled=""/);
    for (const field of ["api", "model", "api_key", "api_key_encrypted"]) expect(html).not.toContain(`name="${field}"`);
  });

  it("keeps an explicitly restored profile while preserving the shared selector", () => {
    expect(render("brave", "idle", "123")).toMatch(/<option[^>]*value="123"[^>]*selected=""/);
  });

  it("shows the verification stage while the start request is running", () => {
    const html = render("brave", "submitting");
    expect(html).toContain("Checking LLM and starting…");
    expect(html).toMatch(/<button[^>]*type="submit"[^>]*disabled=""/);
  });

  it("submits the chosen profile as a string in JSON without raw model configuration", () => {
    render("brave", "idle", "123");
    const OriginalFormData = FormData;
    const form = new OriginalFormData();
    for (const [key, value] of Object.entries(QUEUE_TEMPLATES.brave)) form.set(key, String(value));
    form.set("llm_profile_id", "123");
    form.set("force_rescan", "true");
    form.set("execution_id", task);
    vi.stubGlobal("FormData", class extends OriginalFormData {
      constructor(source: FormData) { super(); source.forEach((value, key) => this.append(key, value)); }
    });
    mocks.onSubmit?.({preventDefault: vi.fn(), currentTarget: form} as unknown as Parameters<FormEventHandler<HTMLFormElement>>[0]);
    expect(mocks.submit).toHaveBeenCalledOnce();
    const [payload, options] = mocks.submit.mock.calls[0];
    expect(options).toEqual({method: "post", action: "/admin/queues/brave"});
    const config = JSON.parse(payload.config);
    expect(config).toEqual({...QUEUE_TEMPLATES.brave, llm_profile_id: "123", force_rescan: true, execution_id: task});
    for (const field of ["api", "model", "api_key", "api_key_encrypted", "llm"]) expect(config).not.toHaveProperty(field);
  });

  it.each(["webtech", "ip-enrichment"] as const)("keeps %s processing independent of an LLM choice", type => {
    const html = render(type);
    expect(html).not.toContain('name="llm_profile_id"');
    expect(html).not.toContain("Browser assistant LLM");
  });
});
