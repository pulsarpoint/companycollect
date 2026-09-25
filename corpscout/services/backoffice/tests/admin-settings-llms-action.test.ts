import { beforeEach, describe, expect, it, vi } from "vitest";
import { action } from "~/routes/admin-settings-llms";
import { LlmSettingsValidationError } from "~/lib/llm-settings.server";
import { CrawlLlmError } from "~/lib/crawl-llm.server";

const settings = vi.hoisted(() => ({save: vi.fn(), activate: vi.fn(), local: vi.fn(), verify: vi.fn()}));
vi.mock("~/lib/crawl-llm.server", () => ({
  CrawlLlmError: class extends Error {}, verifySelectedLlm: settings.verify,
}));
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


describe("saved LLM configuration tests", () => {
  it("tests the saved profile without activation, saving or returning credentials", async () => {
    settings.verify.mockResolvedValue({provider: "Provider", model: "model", base_url: "https://provider.example/v1", api_key_encrypted: "encrypted-private-credential"});
    const response = await submit({intent: "test", profile_id: "saved-profile", api_key: "untrusted-override", model: "untrusted-model"});
    expect(settings.verify).toHaveBeenCalledExactlyOnceWith("saved-profile", "crawler");
    expect(settings.save).not.toHaveBeenCalled();
    expect(settings.activate).not.toHaveBeenCalled();
    expect(settings.local).not.toHaveBeenCalled();
    expect(response).toEqual({testResult: {
      profileId: "saved-profile", ok: true, message: expect.stringContaining("Connection successful"), checkedAt: expect.any(String),
    }, values: null, error: ""});
    expect(JSON.stringify(response)).not.toContain("encrypted-private-credential");
    expect(JSON.stringify(response)).not.toContain("untrusted-override");
  });

  it.each([
    new CrawlLlmError("LLM verification failed: Model provider rejected the request (HTTP 401)."),
    new LlmSettingsValidationError("The selected LLM API key is missing. Add it in LLM settings."),
    new CrawlLlmError("Could not verify the selected LLM through the crawler."),
  ])("shows a safe verification failure beside the tested profile", async error => {
    settings.verify.mockRejectedValue(error);
    expect(await submit({intent: "test", profile_id: "saved-profile"})).toEqual({testResult: {
      profileId: "saved-profile", ok: false, message: error.message, checkedAt: expect.any(String),
    }, values: null, error: ""});
    expect(settings.activate).not.toHaveBeenCalled();
  });

  it("does not expose unexpected errors or raw provider credentials", async () => {
    settings.verify.mockRejectedValue(new Error("private-test-api-key encrypted-private-credential"));
    const response = await submit({intent: "test", profile_id: "saved-profile"});
    expect(response).toMatchObject({testResult: {profileId: "saved-profile", ok: false, message: "Could not test this model. Try again or check the service connection."}});
    expect(JSON.stringify(response)).not.toContain("private-test-api-key");
  });
});
