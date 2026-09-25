import { createCipheriv, randomBytes } from "node:crypto";
import { browserFetch } from "~/lib/browser-service.server";
import { crawlerFetch } from "~/lib/crawler.server";
import { getLlmProfile, getLlmProfileApiKey, LlmSettingsValidationError } from "~/lib/llm-settings.server";

export class CrawlLlmError extends Error {}

export type EncryptedCrawlLlm = {
  provider: string;
  base_url: string;
  model: string;
  api_key_encrypted: string;
};

/** Encrypt for the crawler; Dagster carries this envelope without the shared key. */
export function encryptCrawlLlm(profile: {provider: string; baseUrl: string; model: string}, apiKey: string, sharedKey: string): EncryptedCrawlLlm {
  if (!/^[a-fA-F0-9]{64}$/.test(sharedKey)) {
    throw new CrawlLlmError("Configure the same 64-character hexadecimal CRAWLER_LLM_ENCRYPTION_KEY in Backoffice and the crawler.");
  }
  if (!apiKey.trim() || Buffer.byteLength(apiKey, "utf8") > 8192 || /[\x00-\x1f\x7f]/.test(apiKey)) throw new CrawlLlmError("The selected LLM API key is missing or invalid.");
  for (const [value, maxLength] of [[profile.provider, 100], [profile.baseUrl, 2048], [profile.model, 200]] as const) {
    if (!value.trim() || value !== value.trim() || value.length > maxLength || /[\x00-\x1f\x7f]/.test(value)) throw new CrawlLlmError("The selected LLM profile is invalid. Update it in LLM settings.");
  }
  let url: URL;
  try { url = new URL(profile.baseUrl); }
  catch { throw new CrawlLlmError("The selected LLM base URL is invalid. Update it in LLM settings."); }
  if (!["http:", "https:"].includes(url.protocol) || url.username || url.password || profile.baseUrl.includes("?") || profile.baseUrl.includes("#") || /\s/.test(profile.baseUrl)) {
    throw new CrawlLlmError("The selected LLM base URL must be HTTP(S) without credentials, query parameters, or fragments.");
  }
  const nonce = randomBytes(12);
  const cipher = createCipheriv("aes-256-gcm", Buffer.from(sharedKey, "hex"), nonce);
  cipher.setAAD(Buffer.from(`corpscout-crawler-llm:v1\0${profile.provider}\0${profile.baseUrl}\0${profile.model}`, "utf8"));
  const encrypted = Buffer.concat([cipher.update(apiKey, "utf8"), cipher.final(), cipher.getAuthTag()]);
  return {provider: profile.provider, base_url: profile.baseUrl, model: profile.model,
    api_key_encrypted: `v1.${nonce.toString("base64url")}.${encrypted.toString("base64url")}`};
}

/** Verify the selected model through the service that will actually use it. */
export async function verifySelectedLlm(profileId: unknown, target: "crawler" | "brave"): Promise<EncryptedCrawlLlm> {
  if (typeof profileId !== "string" || !profileId.trim()) throw new CrawlLlmError("Choose an LLM from LLM settings before starting processing.");
  const tokenName = target === "crawler" ? "CRAWLER_API_TOKEN" : "BROWSER_API_TOKEN";
  const serviceName = target === "crawler" ? "crawler" : "Brave browser assistant";
  if (!process.env[tokenName]?.trim()) throw new CrawlLlmError(`Configure ${tokenName} on Backoffice before verifying models.`);
  const profile = getLlmProfile(profileId);
  if (!profile) throw new CrawlLlmError("The selected LLM no longer exists. Choose another LLM.");
  let apiKey: string;
  try { apiKey = getLlmProfileApiKey(profileId); }
  catch (error) {
    if (error instanceof LlmSettingsValidationError) throw new CrawlLlmError(error.message);
    throw error;
  }
  const llm = encryptCrawlLlm(profile, apiKey, process.env.CRAWLER_LLM_ENCRYPTION_KEY ?? "");
  let result: unknown;
  try {
    const init: RequestInit = {
      method: "POST", headers: {"Content-Type": "application/json"}, redirect: "error",
      body: JSON.stringify({llm}), signal: AbortSignal.timeout(35_000),
    };
    const response = target === "crawler" ? await crawlerFetch("/v1/llm/verify", init)
      : await browserFetch("/v1/brave/llm/verify", init);
    result = await response.json();
  } catch {
    throw new CrawlLlmError(`Could not verify the selected LLM through the ${serviceName}. Check service connectivity, API authentication, and the shared encryption key before trying again.`);
  }
  if (typeof result !== "object" || result === null || !("ok" in result) || result.ok !== true) {
    const detail = typeof result === "object" && result !== null && "error" in result && typeof result.error === "string"
      ? result.error.replaceAll(apiKey, "[redacted]").replaceAll(llm.api_key_encrypted, "[redacted]").slice(0, 500) : "The service did not confirm the model is working.";
    throw new CrawlLlmError(`LLM verification failed: ${detail}`);
  }
  return llm;
}

export async function prepareCrawlSettings<T extends Record<string, unknown>>(settings: T) {
  const {llm_profile_id: profileId, ...rest} = settings;
  const llm = await verifySelectedLlm(profileId, "crawler");
  return {...rest, api: llm.provider === "deepseek" || new URL(llm.base_url).hostname === "api.deepseek.com" ? "deepseek" : "openrouter",
    model: llm.model, llm, crawler_config: {provider: null}};
}
