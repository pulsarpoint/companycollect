import { beforeEach, describe, expect, it, vi } from "vitest";
import { action } from "~/routes/admin-settings-llms";
import { LlmSettingsValidationError } from "~/lib/llm-settings.server";

const settings = vi.hoisted(() => ({save: vi.fn(), activate: vi.fn(), local: vi.fn()}));
vi.mock("~/lib/llm-settings.server", () => ({
  LlmSettingsValidationError: class extends Error {},
  saveAndActivateLlmProfile: settings.save,
  activateLlmProfile: settings.activate,
  setLocalCodexEnabled: settings.local,
  isLocalCodexEnabled: vi.fn(), listLlmProfiles: vi.fn(), getLlmProfile: vi.fn(),
}));

const metadata = {
  profileId: "", name: "Provider model", provider: "Provider",
  baseUrl: "https://provider.example/v1", model: "selected-model",
};

function submit(values: Record<string, string> = {}) {
  const form = new FormData();
  for (const [key, value] of Object.entries({
    intent: "save", profile_id: "", name: metadata.name, provider: metadata.provider,
    base_url: metadata.baseUrl, model: metadata.model, api_key: "private-test-api-key", ...values,
  })) form.set(key, value);
  const request = new Request("http://localhost/admin/settings/llms", {method: "POST", body: form});
  return action({request} as Parameters<typeof action>[0]);
}

beforeEach(() => { vi.resetAllMocks(); });

describe("LLM settings key handling", () => {
  it("passes the submitted key only to encrypted storage and redirects without it", async () => {
    const response = await submit();
    expect(settings.save).toHaveBeenCalledWith({...metadata, apiKey: "private-test-api-key"});
    expect(response).toBeInstanceOf(Response);
    expect((response as Response).headers.get("Location")).toBe("/admin/settings/llms?saved=yes");
    expect(await (response as Response).text()).not.toContain("private-test-api-key");
  });

  it("passes a blank editing key so storage preserves the existing encrypted value", async () => {
    await submit({profile_id: "saved-profile", api_key: ""});
    expect(settings.save).toHaveBeenCalledWith({...metadata, profileId: "saved-profile", apiKey: ""});
  });

  it("never returns the key with validation-error form values", async () => {
    settings.save.mockImplementation(() => { throw new LlmSettingsValidationError("Profile name is already in use."); });
    const response = await submit();
    expect(response).toEqual({error: "Profile name is already in use.", values: metadata});
    expect(JSON.stringify(response)).not.toContain("private-test-api-key");
    expect(JSON.stringify(response)).not.toContain("apiKey");
    expect(JSON.stringify(response)).not.toContain("api_key");
  });

  it("redacts the submitted key even if a validation message accidentally includes it", async () => {
    settings.save.mockImplementation(() => { throw new LlmSettingsValidationError("Key private-test-api-key was rejected."); });
    const response = await submit();
    expect(response).toEqual({error: "Key [redacted] was rejected.", values: metadata});
    expect(JSON.stringify(response)).not.toContain("private-test-api-key");
  });

  it("does not accept the retired environment-variable field as a stored key", async () => {
    await submit({api_key: "", api_key_environment_variable: "PRIVATE_ENVIRONMENT_NAME"});
    expect(settings.save).toHaveBeenCalledWith({...metadata, apiKey: ""});
  });
});
