import { createCipheriv, randomBytes } from "node:crypto";
import { browserFetch } from "~/lib/browser-service.server";
import { crawlerFetch } from "~/lib/crawler.server";
import { isDecisionModel, type ReasoningEffort } from "~/lib/llm-model-options";
import { getLlmProfile, getLlmProfileApiKey, LlmSettingsValidationError, recordLlmCheck, type LlmFailureKind } from "~/lib/llm-settings.server";

export interface LlmTestExchange {
  request: {method: string; url: string; body: unknown} | null;
  response: {status: number; content_type: string; body: string} | null;
  elapsed_ms: number | null;
}

export class CrawlLlmError extends Error {
  constructor(message: string, public readonly exchange: LlmTestExchange | null = null) {
    super(message);
  }
}

function readTestExchange(result: unknown, secrets: string[]): LlmTestExchange | null {
  if (!result || typeof result !== "object" || !("exchange" in result)) return null;
  const exchange = result.exchange as LlmTestExchange | null;
  if (!exchange || typeof exchange !== "object") return null;
  if (exchange.request !== null && (!exchange.request || typeof exchange.request.method !== "string"
    || typeof exchange.request.url !== "string" || !("body" in exchange.request))) return null;
  if (exchange.response !== null && (!exchange.response || !Number.isInteger(exchange.response.status)
    || typeof exchange.response.content_type !== "string" || typeof exchange.response.body !== "string")) return null;
  if (exchange.elapsed_ms !== null && (typeof exchange.elapsed_ms !== "number" || !Number.isFinite(exchange.elapsed_ms))) return null;
  function redact(value: unknown): unknown {
    if (typeof value === "string") {
      let text = value;
      for (const secret of secrets.filter(Boolean)) {
        text = text.replaceAll(secret, "[redacted]").replaceAll(JSON.stringify(secret).slice(1, -1), "[redacted]");
      }
      return text;
    }
    if (Array.isArray(value)) return value.map(redact);
    if (value && typeof value === "object") return Object.fromEntries(Object.entries(value).map(([key, item]) => [redact(key), redact(item)]));
    return value;
  }
  // Only expose the provider exchange, never the verification transport envelope.
  return redact({
    request: exchange.request && {method: exchange.request.method, url: exchange.request.url, body: exchange.request.body},
    response: exchange.response && {status: exchange.response.status, content_type: exchange.response.content_type, body: exchange.response.body},
    elapsed_ms: exchange.elapsed_ms,
  }) as LlmTestExchange;
}

export type EncryptedCrawlLlm = {
  profile_id?: string;
  profile_revision?: number;
  provider: string;
  base_url: string;
  model: string;
  reasoning_effort?: ReasoningEffort;
  api_key_encrypted: string;
};

/** Encrypt for the crawler; Dagster carries this envelope without the shared key. */
export function encryptCrawlLlm(profile: {profileId?: string; revision?: number; provider: string; baseUrl: string; model: string; reasoningEffort?: ReasoningEffort | null}, apiKey: string, sharedKey: string): EncryptedCrawlLlm {
  if (!/^[a-fA-F0-9]{64}$/.test(sharedKey)) {
    throw new CrawlLlmError("Configure the same 64-character hexadecimal CRAWLER_LLM_ENCRYPTION_KEY in Backoffice and the crawler.");
  }
  if ((apiKey !== "" && !apiKey.trim()) || Buffer.byteLength(apiKey, "utf8") > 8192 || /[\x00-\x1f\x7f]/.test(apiKey)) throw new CrawlLlmError("The selected LLM API key is missing or invalid.");
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
  return {...(profile.profileId && profile.revision ? {profile_id: profile.profileId, profile_revision: profile.revision} : {}), provider: profile.provider, base_url: profile.baseUrl, model: profile.model,
    ...(profile.reasoningEffort ? {reasoning_effort: profile.reasoningEffort} : {}),
    api_key_encrypted: `v1.${nonce.toString("base64url")}.${encrypted.toString("base64url")}`};
}

/** Verify the selected model through the service that will actually use it. */
async function verifyLlmProfile(
  profileId: unknown, target: "crawler" | "brave", enable: boolean, role: "processing" | "decision",
  inspection?: {previewOnly: boolean; revision: number},
): Promise<{llm: EncryptedCrawlLlm; exchange: LlmTestExchange | null}> {
  if (typeof profileId !== "string" || !profileId.trim()) throw new CrawlLlmError("Choose an LLM from LLM settings before starting processing.");
  const tokenName = target === "crawler" ? "CRAWLER_API_TOKEN" : "BROWSER_API_TOKEN";
  const serviceName = target === "crawler" ? "crawler" : "Brave browser assistant";
  if (!process.env[tokenName]?.trim()) throw new CrawlLlmError(`Configure ${tokenName} on Backoffice before verifying models.`);
  const profile = await getLlmProfile(profileId);
  if (!profile) throw new CrawlLlmError("The selected LLM no longer exists. Choose another LLM.");
  if (inspection && profile.revision !== inspection.revision)
    throw new CrawlLlmError("This model configuration changed. Reopen Test to preview the current revision before running it.");
  if (isDecisionModel(profile.model) && (target === "brave" || !enable && role !== "decision"))
    throw new CrawlLlmError("Jev returns typed decisions. Select a text-generation model for this processing task.");
  if (role === "decision" && (target !== "crawler" || !isDecisionModel(profile.model))) throw new CrawlLlmError("Choose a saved Jev decision model.");
  if (profile.state === 'disabled' && !enable) throw new CrawlLlmError("This model is disabled. Test and enable it in LLM settings first.");
  const startedAt = new Date().toISOString();
  let apiKey: string;
  try { apiKey = await getLlmProfileApiKey(profileId, profile.revision); }
  catch (error) {
    if (error instanceof LlmSettingsValidationError) throw new CrawlLlmError(error.message);
    throw error;
  }
  const llm = encryptCrawlLlm(profile, apiKey, process.env.CRAWLER_LLM_ENCRYPTION_KEY ?? "");
  let result: unknown;
  try {
    const init: RequestInit = {
      method: "POST", headers: {"Content-Type": "application/json"}, redirect: "error",
      body: JSON.stringify({llm, ...(inspection ? {preview_only: inspection.previewOnly, include_exchange: true} : {})}), signal: AbortSignal.timeout(35_000),
    };
    const response = target === "crawler" ? await crawlerFetch("/v1/llm/verify", init)
      : await browserFetch("/v1/brave/llm/verify", init);
    if (!response.ok) throw new Error("Verification service unavailable");
    result = await response.json();
  } catch {
    if (!inspection?.previewOnly) await recordLlmCheck(profile, target, startedAt, false, "Verification service could not be reached or authenticated.", 'service');
    throw new CrawlLlmError(`Could not verify the selected LLM through the ${serviceName}. Check service connectivity, API authentication, and the shared encryption key before trying again.`);
  }
  const exchange = inspection ? readTestExchange(result, [apiKey, llm.api_key_encrypted]) : null;
  if (typeof result !== "object" || result === null || !("ok" in result) || result.ok !== true) {
    const detail = typeof result === "object" && result !== null && "error" in result && typeof result.error === "string"
      ? (apiKey ? result.error.replaceAll(apiKey, "[redacted]") : result.error).replaceAll(llm.api_key_encrypted, "[redacted]").slice(0, 500) : "The service did not confirm the model is working.";
    const kind = typeof result === 'object' && result !== null && 'failure_kind' in result
      && ['configuration','transient','capability','service'].includes(String(result.failure_kind))
      ? result.failure_kind as LlmFailureKind : 'service';
    if (!inspection?.previewOnly) await recordLlmCheck(profile, target, startedAt, false, detail, kind);
    throw new CrawlLlmError(`LLM verification failed: ${detail}${kind === 'configuration' && !inspection?.previewOnly ? ' Model disabled; dependent tasks are being stopped.' : ''}`, exchange);
  }
  if (inspection && !exchange) throw new CrawlLlmError("The crawler did not return the test request and response. Update the crawler service and retry.");
  if (!inspection?.previewOnly) await recordLlmCheck(profile, target, startedAt, true, "Configuration verified successfully.", null, enable);
  return {llm, exchange};
}

export async function verifySelectedLlm(profileId: unknown, target: "crawler" | "brave", enable = false, role: "processing" | "decision" = "processing"): Promise<EncryptedCrawlLlm> {
  return (await verifyLlmProfile(profileId, target, enable, role)).llm;
}

/** Inspect the real crawler request without exposing the encrypted transport profile. */
export async function inspectLlmTest(profileId: unknown, revision: number, previewOnly: boolean): Promise<LlmTestExchange | null> {
  if (!Number.isSafeInteger(revision) || revision < 1) throw new CrawlLlmError("Reopen Test to load the saved model revision.");
  return (await verifyLlmProfile(profileId, "crawler", true, "processing", {previewOnly, revision})).exchange;
}

export async function prepareCrawlSettings<T extends Record<string, unknown>>(settings: T) {
  const {llm_profile_id: profileId, ...rest} = settings;
  const llm = await verifySelectedLlm(profileId, "crawler");
  return {...rest, api: llm.provider === "deepseek" || new URL(llm.base_url).hostname === "api.deepseek.com" ? "deepseek" : "openrouter",
    model: llm.model, llm, crawler_config: {provider: null}};
}
