import { fetchObject, ObjectStoreError, putObject, type ObjectStoreOptions } from "~/lib/object-store.server";
import {
  normalizeFeedRun,
  normalizeIndicatorRules,
  type IndicatorRules,
  type Manifest,
  type ProviderDocument,
  type RunIndex,
} from "~/lib/provider-recon";

/**
 * Read access to provider-recon's bucket (index, manifests, provider
 * documents) plus the backoffice's own warning-rules object. Keys are built
 * only from validated run ids and slugs.
 */

export const INDEX_KEY = "changes/index.json";
export const RULES_KEY = "settings/indicator-rules.json";

const RUN_ID = /^\d{8}T\d{6}Z-[a-z]+$/;
const SLUG = /^[a-z0-9][a-z0-9-]*[a-z0-9]$/;

export function providerReconBucket(): string {
  return process.env.PROVIDER_RECON_BUCKET || "provider-recon";
}

async function getJson<T>(key: string, options: ObjectStoreOptions): Promise<T | null> {
  const bucket = providerReconBucket();
  const response = await fetchObject(bucket, key, options);
  if (response.status === 404) return null;
  if (!response.ok) throw new ObjectStoreError(`Reading ${key} from ${bucket} failed: HTTP ${response.status}.`);
  return (await response.json()) as T;
}

export async function loadRunIndex(options: ObjectStoreOptions = {}): Promise<RunIndex> {
  const index = await getJson<RunIndex>(INDEX_KEY, options);
  if (!index) return { runs: [], providers: [] };
  return {
    providers: index.providers ?? [],
    runs: (index.runs ?? []).map((run) => ({ ...run, changed: run.changed ?? [], feeds: (run.feeds ?? []).map(normalizeFeedRun) })),
  };
}

export async function loadManifest(runId: string, options: ObjectStoreOptions = {}): Promise<Manifest | null> {
  if (!RUN_ID.test(runId)) return null;
  const manifest = await getJson<Manifest>(`changes/${runId}.json`, options);
  if (!manifest) return null;
  return { ...manifest, changed: manifest.changed ?? [], feeds: (manifest.feeds ?? []).map(normalizeFeedRun) };
}

export async function loadProviderDocument(slug: string, options: ObjectStoreOptions = {}): Promise<ProviderDocument | null> {
  if (!SLUG.test(slug)) return null;
  return getJson<ProviderDocument>(`providers/${slug}/latest.json`, options);
}

export async function loadIndicatorRules(options: ObjectStoreOptions = {}): Promise<IndicatorRules> {
  return normalizeIndicatorRules(await getJson<unknown>(RULES_KEY, options));
}

export async function saveIndicatorRules(rules: IndicatorRules, options: ObjectStoreOptions = {}): Promise<void> {
  const body = new TextEncoder().encode(`${JSON.stringify(rules, null, 2)}\n`);
  await putObject(providerReconBucket(), RULES_KEY, body, options);
}
