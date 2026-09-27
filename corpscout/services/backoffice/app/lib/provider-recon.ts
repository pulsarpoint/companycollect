/**
 * Client-safe model of provider-recon output (changes/index.json, change
 * manifests, provider documents) and the backoffice's display-only warning
 * rules. Shared by loaders and components; no server imports.
 */

export interface Churn {
  added: number;
  reappeared: number;
  missing: number;
  removed: number;
  purged: number;
}

export interface FeedRun {
  slug: string;
  collector: string;
  status: string; // ok | stale | failed
  items: number;
  churn: Churn;
  unmapped_tags: string[];
  error?: string;
}

export interface RunScope {
  command: string;
  providers?: string[];
}

export interface RunSummary {
  run_id: string;
  published_at: string;
  scope: RunScope;
  changed: string[];
  unchanged_count: number;
  issues: number;
  feeds: FeedRun[];
}

export interface RunIndex {
  runs: RunSummary[];
  providers: string[];
}

export interface KindDiff {
  added?: string[];
  missing?: string[];
  reappeared?: string[];
  restored?: string[];
  removed?: string[];
  purged?: string[];
  updated?: string[];
  added_count: number;
  missing_count: number;
  reappeared_count: number;
  restored_count: number;
  removed_count: number;
  purged_count: number;
  updated_count: number;
  truncated?: boolean;
}

export interface ProviderChange {
  slug: string;
  created?: boolean;
  old_hash?: string;
  new_hash: string;
  evidence?: Record<string, KindDiff>;
  feed_versions?: Record<string, { old: string; new: string }>;
}

export interface Manifest {
  run_id: string;
  scope: RunScope;
  published_at: string;
  changed: ProviderChange[];
  unchanged: string[];
  collector_issues: { slug: string; collector: string; status: string; error?: string }[];
  feeds: FeedRun[];
}

export interface Lifecycle {
  status?: string; // active | missing | removed; absent in pre-lifecycle documents
  first_seen?: string;
  last_seen?: string;
  missing_since?: string;
  removed_at?: string;
  removal_action?: string;
  restored_at?: string;
}

export interface IPRange extends Lifecycle {
  cidr: string;
  region?: string;
  feed_tag?: string;
  confidence: number;
  source: string;
  collector?: string;
  source_url?: string;
  source_version?: string;
}

export interface ProviderService {
  service_key: string;
  display_name: string;
  service_types: string[];
  traits: string[];
  removed_at?: string;
  evidence: { ip_ranges: IPRange[] };
}

export interface CollectorStatus {
  status: string;
  source_url?: string;
  source_version?: string;
  items: number;
  fetched_at: string;
  last_success_at?: string;
  error?: string;
  unmapped_tags?: string[];
  skipped_lines?: number;
  churn?: Churn;
}

export interface ProviderDocument {
  version: string;
  slug: string;
  display_name: string;
  category: string;
  website?: string;
  country?: string;
  aliases: string[];
  provider_keys: string[];
  services: ProviderService[];
  collection: { collected_at: string; collectors: Record<string, CollectorStatus>; content_hash: string };
}

/** Display-only warning rules; percentages of the ranges a feed had before the run. */
export interface IndicatorRules {
  missingPercent: number;
  removedPercent: number;
  addedPercent: number;
  flagUnmappedTags: boolean;
}

export const DEFAULT_INDICATOR_RULES: IndicatorRules = {
  missingPercent: 5,
  removedPercent: 5,
  addedPercent: 50,
  flagUnmappedTags: true,
};

export type FlagKind = "failed" | "stale" | "missing" | "removed" | "added" | "unmapped";

export interface FeedFlag {
  kind: FlagKind;
  message: string;
}

const EMPTY_CHURN: Churn = { added: 0, reappeared: 0, missing: 0, removed: 0, purged: 0 };

/** Fill fields that older manifests (before lifecycle tracking) lack. */
export function normalizeFeedRun(raw: Omit<Partial<FeedRun>, "churn"> & { churn?: Partial<Churn> }): FeedRun {
  return {
    slug: raw.slug ?? "",
    collector: raw.collector ?? "",
    status: raw.status ?? "ok",
    items: raw.items ?? 0,
    churn: { ...EMPTY_CHURN, ...(raw.churn ?? {}) },
    unmapped_tags: raw.unmapped_tags ?? [],
    ...(raw.error ? { error: raw.error } : {}),
  };
}

/**
 * Flags for one feed run. Percentages are relative to the live ranges the
 * feed had before the run (items - added + removed); a feed without such a
 * baseline (its first run) gets no percentage flags.
 */
export function flagFeed(feed: FeedRun, rules: IndicatorRules): FeedFlag[] {
  const flags: FeedFlag[] = [];
  if (feed.status === "failed") flags.push({ kind: "failed", message: feed.error ? `Failed: ${feed.error}` : "Failed" });
  if (feed.status === "stale") {
    flags.push({ kind: "stale", message: feed.error ? `Stale: ${feed.error}` : "Stale: kept the previous ranges" });
  }
  const before = feed.items - feed.churn.added + feed.churn.removed;
  if (before > 0) {
    const check = (kind: FlagKind, count: number, limit: number, verb: string) => {
      const share = (count * 100) / before;
      if (count > 0 && share > limit) {
        flags.push({ kind, message: `${count} of ${before} ranges ${verb} (${share.toFixed(1)}% > ${limit}%)` });
      }
    };
    check("missing", feed.churn.missing, rules.missingPercent, "went missing");
    check("removed", feed.churn.removed, rules.removedPercent, "were removed");
    check("added", feed.churn.added, rules.addedPercent, "were added");
  }
  if (rules.flagUnmappedTags && feed.unmapped_tags.length > 0) {
    flags.push({ kind: "unmapped", message: `Unmapped tags: ${feed.unmapped_tags.join(", ")}` });
  }
  return flags;
}

function percentOr(value: unknown, fallback: number): number {
  return typeof value === "number" && Number.isFinite(value) && value >= 0 && value <= 100 ? value : fallback;
}

/** Stored rules merged over the defaults; unusable fields fall back. */
export function normalizeIndicatorRules(raw: unknown): IndicatorRules {
  const r = (raw && typeof raw === "object" ? raw : {}) as Record<string, unknown>;
  return {
    missingPercent: percentOr(r.missingPercent, DEFAULT_INDICATOR_RULES.missingPercent),
    removedPercent: percentOr(r.removedPercent, DEFAULT_INDICATOR_RULES.removedPercent),
    addedPercent: percentOr(r.addedPercent, DEFAULT_INDICATOR_RULES.addedPercent),
    flagUnmappedTags: typeof r.flagUnmappedTags === "boolean" ? r.flagUnmappedTags : DEFAULT_INDICATOR_RULES.flagUnmappedTags,
  };
}

/** Validate the rules form. An unchecked checkbox is absent from FormData: false. */
export function parseIndicatorRules(form: FormData): { rules: IndicatorRules } | { error: string } {
  const percent = (name: string, label: string): number | string => {
    const raw = String(form.get(name) ?? "").trim();
    const n = Number(raw);
    if (raw === "" || !Number.isFinite(n) || n < 0 || n > 100) return `${label} must be a number from 0 to 100.`;
    return n;
  };
  const missing = percent("missingPercent", "Missing");
  const removed = percent("removedPercent", "Removed");
  const added = percent("addedPercent", "Added");
  for (const v of [missing, removed, added]) if (typeof v === "string") return { error: v };
  return {
    rules: {
      missingPercent: missing as number,
      removedPercent: removed as number,
      addedPercent: added as number,
      flagUnmappedTags: form.get("flagUnmappedTags") === "on",
    },
  };
}

/** Status of a range; documents from before lifecycle tracking have none: active. */
export function rangeStatus(range: IPRange): "active" | "missing" | "removed" {
  return range.status === "missing" || range.status === "removed" ? range.status : "active";
}

export function serviceRangeCounts(service: ProviderService): { active: number; missing: number; removed: number } {
  const counts = { active: 0, missing: 0, removed: 0 };
  for (const r of service.evidence.ip_ranges ?? []) counts[rangeStatus(r)]++;
  return counts;
}

/** Missing ranges (newest first), then removed ranges (newest first). */
export function attentionRanges(doc: ProviderDocument): { serviceKey: string; range: IPRange }[] {
  const out: { serviceKey: string; range: IPRange }[] = [];
  for (const s of doc.services) {
    for (const r of s.evidence.ip_ranges ?? []) {
      if (rangeStatus(r) !== "active") out.push({ serviceKey: s.service_key, range: r });
    }
  }
  const rank = (r: IPRange) => (rangeStatus(r) === "missing" ? 0 : 1);
  const when = (r: IPRange) => (rangeStatus(r) === "missing" ? r.missing_since : r.removed_at) ?? "";
  return out.sort((a, b) => rank(a.range) - rank(b.range) || when(b.range).localeCompare(when(a.range)) || a.range.cidr.localeCompare(b.range.cidr));
}

/**
 * Restore choices per feed: one per distinct removal date of grace-expired
 * ranges, newest first. restore brings back everything removed on or after
 * its date, so the newest date is the narrowest undo; count is how many
 * listed removals that option would cover.
 */
export function restoreOptions(doc: ProviderDocument): { collector: string; since: string; count: number }[] {
  const byCollector = new Map<string, string[]>();
  for (const s of doc.services) {
    for (const r of s.evidence.ip_ranges ?? []) {
      if (rangeStatus(r) !== "removed" || r.removal_action !== "grace_expired" || !r.collector || !r.removed_at) continue;
      byCollector.set(r.collector, [...(byCollector.get(r.collector) ?? []), r.removed_at]);
    }
  }
  const out: { collector: string; since: string; count: number }[] = [];
  for (const collector of [...byCollector.keys()].sort()) {
    const dates = byCollector.get(collector) ?? [];
    for (const since of [...new Set(dates)].sort().reverse()) {
      out.push({ collector, since, count: dates.filter((d) => d >= since).length });
    }
  }
  return out;
}

export function restoreCommand(slug: string, collector: string, since: string): string {
  return `provider-recon restore -provider ${slug} -collector ${collector} -removed-since ${since}`;
}
