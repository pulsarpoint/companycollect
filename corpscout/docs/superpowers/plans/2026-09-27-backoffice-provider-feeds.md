# Backoffice provider feed indicator Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a backoffice section, **Provider feeds**, that shows every `provider-recon` run and each feed's churn. It highlights unusual updates using warning rules an operator can edit (display only, never blocking), and lists each provider's missing and removed ranges with the exact `restore` command.

**Architecture:**
- The backoffice reads the `provider-recon` bucket through the existing SigV4 client (`~/lib/object-store.server`):
  - `changes/index.json`: the run index; one read lists runs and providers.
  - `changes/<run_id>.json`: a run's change manifest.
  - `providers/<slug>/latest.json`: a provider document.
  - `settings/indicator-rules.json`: the warning rules, written by the settings form.
- Flag evaluation is a pure, client-safe function shared by loaders and components.
- No ClickHouse and no Dagster.

**Tech Stack:** React Router 8 (framework mode), TypeScript, shadcn/ui, vitest (`renderToStaticMarkup` for component tests, a recording `fetchImpl` for object-store tests, as in `tests/geolite2.server.test.ts`).

**Spec:** `docs/superpowers/specs/2026-09-27-provider-recon-service-design.md`, section "Evidence lifecycle and removal" ("No blocking gate") and "Change manifest additions".

**Depends on:** `docs/superpowers/plans/2026-09-27-provider-recon-lifecycle.md` being executed and run at least once, since that is what writes `changes/index.json`, `feeds` churn and lifecycle fields. Execute that plan first.

## Global Constraints

- Work in `corpscout/services/backoffice`.
  - Commands: `npm run typecheck`, `npx vitest run <files>`.
  - Run git from the `corpscout` root and commit by explicit path only.
  - Conventional Commits, each ending with `Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>`.
- Route components never use values from `~/lib/*.server` in the component body; `import type` is fine (backoffice CLAUDE.md). Flag logic lives in the client-safe `~/lib/provider-recon`.
- Bucket: `PROVIDER_RECON_BUCKET`, default `provider-recon`. Credentials: the existing `CORPSCOUT_S3_*`.
- Object keys are built only from validated inputs: run id `^\d{8}T\d{6}Z-[a-z]+$`, slug `^[a-z0-9][a-z0-9-]*[a-z0-9]$`. Anything else is a 404 without touching S3.
- Warning rules are display-only; nothing here writes to provider documents. Undoing a removal stays a CLI command (`provider-recon restore`), which the provider page shows ready to copy.
- Default rules: missing > 5%, removed > 5%, added > 50% of the ranges a feed had before the run, and flag unmapped tags. A feed with no previous ranges (first run) gets no percentage flags. `stale` and `failed` are always flagged.
- New route files break the long-running dev server on :5183 (stale Vite fs cache). Verify on a second port; the owner restarts :5183.
- Work in a git worktree; copy the main checkout's backoffice `.env` into it. **Merge caveat:** the main checkout has uncommitted in-progress edits to `app/routes.ts` and `app/components/admin/admin-sidebar.tsx`, so a fast-forward of `main` will refuse to overwrite them. Before merging, the owner commits or stashes those edits; don't touch them.
- The full vitest suite hits prod ClickHouse (~9 min) and has known failures on untouched main. Run only this plan's test files, plus `npm run typecheck`.

## Review Focus

1. **A manifest or document from before the lifecycle slice** (no `feeds`, no `churn`, items without `status`). Expected: pages render with zero churn and treat a missing status as active, with no crash. *(Task 1: `normalizes legacy feed runs`; Task 3: legacy range renders as active.)*
2. **Path-shaped input**: a run id or slug in the URL such as `../settings/indicator-rules` or `aws/../../x`. Expected: 404, and no S3 request. *(Task 2: `rejects unsafe run ids and slugs without fetching`.)*
3. **An empty bucket or missing index** (no run yet). Expected: the page says no runs yet; the rules show defaults; saving rules works. *(Task 2: 404 → empty index/defaults.)*
4. **Rule form input**: blank, negative, >100, non-numeric, or the checkbox off. Expected: a validation message and nothing saved. An off checkbox saves `false`. *(Task 1: `parseIndicatorRules`.)*
5. **The first run of a feed** (all ranges "added"). Expected: no "added" flag. *(Task 1: `no percentage flags without a baseline`.)*

---

## File Structure

```
app/lib/provider-recon.ts                        client-safe: JSON types, rules, flagFeed, parse/normalize, range helpers, restoreCommand
app/lib/provider-recon.server.ts                 S3 reads/writes for index, manifests, documents, rules
app/components/admin/provider-feeds.tsx          FeedTable, RangeTable
app/routes/admin-provider-feeds.tsx              /admin/provider-feeds (runs, latest feeds, providers, rules form)
app/routes/admin-provider-feeds-run.tsx          /admin/provider-feeds/runs/:runId
app/routes/admin-provider-feeds-provider.tsx     /admin/provider-feeds/providers/:slug
app/routes.ts                                    three routes
app/components/admin/admin-sidebar.tsx           "Provider feeds" item
tests/provider-recon.test.ts                     pure logic
tests/provider-recon.server.test.ts              object-store access
tests/provider-feeds.test.tsx                    component rendering
```

---

### Task 1: Client-safe types, rules and flags

**Files:**
- Create: `app/lib/provider-recon.ts`
- Test: `tests/provider-recon.test.ts`

**Interfaces:**
- Produces:
  - Types matching the Go JSON: `Churn`, `FeedRun`, `RunScope`, `RunSummary`, `RunIndex`, `KindDiff`, `ProviderChange`, `Manifest`, `Lifecycle`, `IPRange`, `ProviderService`, `CollectorStatus`, `ProviderDocument`
  - Rules: `IndicatorRules`, `DEFAULT_INDICATOR_RULES`
  - Flags: `FeedFlag`, `flagFeed(feed, rules)`
  - Parsing: `normalizeFeedRun(raw)`, `normalizeIndicatorRules(raw)`, `parseIndicatorRules(form)`
  - Range helpers: `rangeStatus(range)`, `serviceRangeCounts(service)`, `attentionRanges(doc)`
  - `restoreCommand(slug, collector, since)`

- [ ] **Step 1: Write the failing tests** — `tests/provider-recon.test.ts`

```ts
import { describe, expect, it } from "vitest";
import {
  DEFAULT_INDICATOR_RULES,
  attentionRanges,
  flagFeed,
  normalizeFeedRun,
  normalizeIndicatorRules,
  parseIndicatorRules,
  restoreCommand,
  serviceRangeCounts,
  type FeedRun,
  type ProviderDocument,
} from "~/lib/provider-recon";

function feed(partial: Partial<FeedRun> & { churn?: Partial<FeedRun["churn"]> }): FeedRun {
  return normalizeFeedRun({ slug: "aws", collector: "aws_ip_ranges", status: "ok", items: 100, ...partial });
}

describe("flagFeed", () => {
  it("flags nothing for an ordinary run", () => {
    expect(flagFeed(feed({ churn: { added: 2, missing: 1 } }), DEFAULT_INDICATOR_RULES)).toEqual([]);
  });

  it("flags a share of missing ranges above the rule, relative to the ranges before the run", () => {
    // before = items - added + removed = 100 - 0 + 0 = 100; 10 missing = 10% > 5%
    const flags = flagFeed(feed({ churn: { missing: 10 } }), DEFAULT_INDICATOR_RULES);
    expect(flags).toHaveLength(1);
    expect(flags[0].kind).toBe("missing");
    expect(flags[0].message).toContain("10 of 100 ranges went missing");
  });

  it("flags removals and additions against their own rules", () => {
    // before = 90 - 0 + 10 = 100 → 10 removed = 10%
    expect(flagFeed(feed({ items: 90, churn: { removed: 10 } }), DEFAULT_INDICATOR_RULES).map((f) => f.kind)).toEqual(["removed"]);
    // before = 200 - 100 = 100 → 100 added = 100% > 50%
    expect(flagFeed(feed({ items: 200, churn: { added: 100 } }), DEFAULT_INDICATOR_RULES).map((f) => f.kind)).toEqual(["added"]);
  });

  it("no percentage flags without a baseline", () => {
    expect(flagFeed(feed({ items: 50, churn: { added: 50 } }), DEFAULT_INDICATOR_RULES)).toEqual([]);
  });

  it("always flags stale and failed feeds, and unmapped tags when enabled", () => {
    const stale = flagFeed(feed({ status: "stale", error: "status 503" }), DEFAULT_INDICATOR_RULES);
    expect(stale.map((f) => f.kind)).toEqual(["stale"]);
    expect(stale[0].message).toContain("status 503");
    expect(flagFeed(feed({ status: "failed" }), DEFAULT_INDICATOR_RULES).map((f) => f.kind)).toEqual(["failed"]);
    const tags = feed({ unmapped_tags: ["NEW_SERVICE"] });
    expect(flagFeed(tags, DEFAULT_INDICATOR_RULES).map((f) => f.kind)).toEqual(["unmapped"]);
    expect(flagFeed(tags, { ...DEFAULT_INDICATOR_RULES, flagUnmappedTags: false })).toEqual([]);
  });
});

describe("normalizers", () => {
  it("normalizes legacy feed runs", () => {
    const f = normalizeFeedRun({ slug: "aws", collector: "x", status: "ok", items: 3 });
    expect(f.churn).toEqual({ added: 0, reappeared: 0, missing: 0, removed: 0, purged: 0 });
    expect(f.unmapped_tags).toEqual([]);
  });

  it("fills missing rule fields with defaults and ignores junk", () => {
    expect(normalizeIndicatorRules(null)).toEqual(DEFAULT_INDICATOR_RULES);
    expect(normalizeIndicatorRules({ missingPercent: 12, removedPercent: "x" })).toEqual({ ...DEFAULT_INDICATOR_RULES, missingPercent: 12 });
  });
});

describe("parseIndicatorRules", () => {
  function form(entries: Record<string, string>) {
    const f = new FormData();
    for (const [k, v] of Object.entries(entries)) f.set(k, v);
    return f;
  }

  it("parses a valid form; an absent checkbox means false", () => {
    expect(parseIndicatorRules(form({ missingPercent: "3", removedPercent: "4.5", addedPercent: "80" }))).toEqual({
      rules: { missingPercent: 3, removedPercent: 4.5, addedPercent: 80, flagUnmappedTags: false },
    });
    expect(parseIndicatorRules(form({ missingPercent: "3", removedPercent: "4", addedPercent: "80", flagUnmappedTags: "on" }))).toEqual({
      rules: { missingPercent: 3, removedPercent: 4, addedPercent: 80, flagUnmappedTags: true },
    });
  });

  it.each([
    [{ missingPercent: "", removedPercent: "4", addedPercent: "80" }, "Missing"],
    [{ missingPercent: "3", removedPercent: "-1", addedPercent: "80" }, "Removed"],
    [{ missingPercent: "3", removedPercent: "4", addedPercent: "101" }, "Added"],
    [{ missingPercent: "abc", removedPercent: "4", addedPercent: "80" }, "Missing"],
  ])("rejects %o", (entries, label) => {
    const result = parseIndicatorRules(form(entries));
    expect("error" in result && result.error).toContain(label);
  });
});

describe("ranges", () => {
  const doc = {
    version: "provider-recon/v1", slug: "aws", display_name: "AWS", category: "cloud", aliases: [], provider_keys: [],
    services: [{
      service_key: "aws.cloudfront", display_name: "CloudFront", service_types: ["cdn"], traits: [],
      evidence: {
        ip_ranges: [
          { cidr: "10.0.1.0/24", confidence: 1, source: "official_feed", collector: "aws_ip_ranges", status: "active", first_seen: "2026-09-01", last_seen: "2026-10-01" },
          { cidr: "10.0.2.0/24", confidence: 1, source: "official_feed", collector: "aws_ip_ranges", status: "missing", first_seen: "2026-09-01", last_seen: "2026-09-28", missing_since: "2026-09-29" },
          { cidr: "10.0.3.0/24", confidence: 1, source: "official_feed", collector: "aws_ip_ranges", status: "removed", first_seen: "2026-09-01", last_seen: "2026-09-20", missing_since: "2026-09-21", removed_at: "2026-09-28", removal_action: "grace_expired" },
          { cidr: "10.0.4.0/24", confidence: 1, source: "official_feed", collector: "aws_ip_ranges", first_seen: "", last_seen: "" },
        ],
        asns: [], dns_rules: [], http_rules: [], ptr_rules: [], certificate_identities: [],
      },
    }],
    collection: { collected_at: "2026-10-01T06:00:00Z", collectors: {}, content_hash: "sha256:x" },
  } as unknown as ProviderDocument;

  it("counts ranges by status; a legacy range without status counts as active", () => {
    expect(serviceRangeCounts(doc.services[0])).toEqual({ active: 2, missing: 1, removed: 1 });
  });

  it("lists missing first, then removed, newest first", () => {
    expect(attentionRanges(doc).map((r) => r.range.cidr)).toEqual(["10.0.2.0/24", "10.0.3.0/24"]);
  });

  it("builds the restore command", () => {
    expect(restoreCommand("aws", "aws_ip_ranges", "2026-09-28")).toBe(
      "provider-recon restore -provider aws -collector aws_ip_ranges -removed-since 2026-09-28",
    );
  });
});
```

- [ ] **Step 2: Run to verify failure**

Run: `npx vitest run tests/provider-recon.test.ts`
Expected: FAIL: cannot resolve `~/lib/provider-recon`.

- [ ] **Step 3: Implement** — `app/lib/provider-recon.ts`

```ts
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
export function normalizeFeedRun(raw: Partial<FeedRun> & { churn?: Partial<Churn> }): FeedRun {
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

export function restoreCommand(slug: string, collector: string, since: string): string {
  return `provider-recon restore -provider ${slug} -collector ${collector} -removed-since ${since}`;
}
```

- [ ] **Step 4: Run to verify pass**

Run: `npx vitest run tests/provider-recon.test.ts && npm run typecheck`
Expected: PASS. If typecheck fails in files this plan didn't touch, compare with `git stash`-free main: pre-existing failures are not this task's, so record them and continue.

- [ ] **Step 5: Commit**

```bash
git add services/backoffice/app/lib/provider-recon.ts services/backoffice/tests/provider-recon.test.ts
git commit -m "feat(backoffice): provider-recon model and display-only feed warning rules"
```

---

### Task 2: Object-store access

**Files:**
- Create: `app/lib/provider-recon.server.ts`
- Test: `tests/provider-recon.server.test.ts`

**Interfaces:**
- Consumes: `fetchObject`, `putObject`, `ObjectStoreError`, `ObjectStoreOptions` (`~/lib/object-store.server`); Task 1 types and normalizers.
- Produces:
  - `providerReconBucket()`
  - `INDEX_KEY`, `RULES_KEY`
  - `loadRunIndex(options?)`, which normalizes every run's feeds
  - `loadManifest(runId, options?)`, which returns `null` for an invalid id or a 404
  - `loadProviderDocument(slug, options?)`
  - `loadIndicatorRules(options?)`, `saveIndicatorRules(rules, options?)`

- [ ] **Step 1: Write the failing tests** — `tests/provider-recon.server.test.ts`

```ts
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { DEFAULT_INDICATOR_RULES } from "~/lib/provider-recon";
import {
  loadIndicatorRules,
  loadManifest,
  loadProviderDocument,
  loadRunIndex,
  saveIndicatorRules,
} from "~/lib/provider-recon.server";

const S3 = "http://s3.test:9000";

function recorder(respond: (url: string, method: string) => Response) {
  const calls: { url: string; method: string; body: unknown }[] = [];
  const fetchImpl = vi.fn(async (input: string | URL | Request, init?: RequestInit) => {
    const call = { url: String(input), method: init?.method ?? "GET", body: init?.body };
    calls.push(call);
    return respond(call.url, call.method);
  }) as unknown as typeof fetch;
  return { calls, fetchImpl };
}

const notFound = () => new Response("<Error><Code>NoSuchKey</Code></Error>", { status: 404 });

beforeEach(() => {
  vi.stubEnv("CORPSCOUT_S3_ENDPOINT", S3);
  vi.stubEnv("CORPSCOUT_S3_ACCESS_KEY", "access");
  vi.stubEnv("CORPSCOUT_S3_SECRET_KEY", "secret");
  vi.stubEnv("PROVIDER_RECON_BUCKET", "");
});

afterEach(() => vi.unstubAllEnvs());

describe("provider-recon object store", () => {
  it("reads the run index from the default bucket and normalizes legacy feeds", async () => {
    const { calls, fetchImpl } = recorder(() =>
      Response.json({ runs: [{ run_id: "20260927T060000Z-collect", published_at: "x", scope: { command: "collect" }, changed: [], unchanged_count: 1, issues: 0, feeds: [{ slug: "aws", collector: "aws_ip_ranges", status: "ok", items: 3 }] }], providers: ["aws"] }),
    );
    const index = await loadRunIndex({ fetchImpl });
    expect(calls[0].url).toBe(`${S3}/provider-recon/changes/index.json`);
    expect(index.runs[0].feeds[0].churn.missing).toBe(0);
    expect(index.providers).toEqual(["aws"]);
  });

  it("treats a missing index and missing rules as empty and default", async () => {
    const { fetchImpl } = recorder(notFound);
    expect(await loadRunIndex({ fetchImpl })).toEqual({ runs: [], providers: [] });
    expect(await loadIndicatorRules({ fetchImpl })).toEqual(DEFAULT_INDICATOR_RULES);
    expect(await loadManifest("20260927T060000Z-collect", { fetchImpl })).toBeNull();
    expect(await loadProviderDocument("aws", { fetchImpl })).toBeNull();
  });

  it("rejects unsafe run ids and slugs without fetching", async () => {
    const { calls, fetchImpl } = recorder(() => Response.json({}));
    expect(await loadManifest("../settings/indicator-rules", { fetchImpl })).toBeNull();
    expect(await loadManifest("20260927T060000Z-collect/../x", { fetchImpl })).toBeNull();
    expect(await loadProviderDocument("aws/../../x", { fetchImpl })).toBeNull();
    expect(await loadProviderDocument("AWS", { fetchImpl })).toBeNull();
    expect(calls).toHaveLength(0);
  });

  it("reads a manifest and a provider document by key", async () => {
    const { calls, fetchImpl } = recorder(() => Response.json({ slug: "aws" }));
    await loadManifest("20260927T060000Z-collect", { fetchImpl });
    await loadProviderDocument("one-com", { fetchImpl });
    expect(calls.map((c) => c.url)).toEqual([
      `${S3}/provider-recon/changes/20260927T060000Z-collect.json`,
      `${S3}/provider-recon/providers/one-com/latest.json`,
    ]);
  });

  it("surfaces other HTTP errors", async () => {
    const { fetchImpl } = recorder(() => new Response("denied", { status: 403 }));
    await expect(loadRunIndex({ fetchImpl })).rejects.toThrow("HTTP 403");
  });

  it("saves rules as JSON to the settings key, honouring PROVIDER_RECON_BUCKET", async () => {
    vi.stubEnv("PROVIDER_RECON_BUCKET", "provider-recon-test");
    const { calls, fetchImpl } = recorder(() => new Response("", { status: 200 }));
    await saveIndicatorRules({ ...DEFAULT_INDICATOR_RULES, missingPercent: 9 }, { fetchImpl });
    expect(calls[0].method).toBe("PUT");
    expect(calls[0].url).toBe(`${S3}/provider-recon-test/settings/indicator-rules.json`);
    expect(JSON.parse(new TextDecoder().decode(calls[0].body as Uint8Array)).missingPercent).toBe(9);
  });
});
```

- [ ] **Step 2: Run to verify failure**

Run: `npx vitest run tests/provider-recon.server.test.ts`
Expected: FAIL: cannot resolve `~/lib/provider-recon.server`.

- [ ] **Step 3: Implement** — `app/lib/provider-recon.server.ts`

```ts
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
```

- [ ] **Step 4: Run to verify pass**

Run: `npx vitest run tests/provider-recon.server.test.ts tests/provider-recon.test.ts && npm run typecheck`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add services/backoffice/app/lib/provider-recon.server.ts services/backoffice/tests/provider-recon.server.test.ts
git commit -m "feat(backoffice): read provider-recon runs, manifests and documents from the object store"
```

---

### Task 3: Feed and range tables

**Files:**
- Create: `app/components/admin/provider-feeds.tsx`
- Test: `tests/provider-feeds.test.tsx`

**Interfaces:**
- Consumes: Task 1 (`flagFeed`, `rangeStatus`, types).
- Produces:
  - `FeedTable({ feeds, rules })`: one row per feed with churn columns. Flagged rows are highlighted and list their flag messages. Provider names link to `/admin/provider-feeds/providers/:slug`.
  - `RangeTable({ rows })`: rows from `attentionRanges`, with the status badge and dates.

- [ ] **Step 1: Write the failing tests** — `tests/provider-feeds.test.tsx`

```tsx
import { renderToStaticMarkup } from "react-dom/server";
import { MemoryRouter } from "react-router";
import { describe, expect, it } from "vitest";
import { FeedTable, RangeTable } from "~/components/admin/provider-feeds";
import { DEFAULT_INDICATOR_RULES, normalizeFeedRun } from "~/lib/provider-recon";

describe("FeedTable", () => {
  it("highlights flagged feeds with their reasons and links providers", () => {
    const html = renderToStaticMarkup(
      <MemoryRouter>
        <FeedTable
          rules={DEFAULT_INDICATOR_RULES}
          feeds={[
            normalizeFeedRun({ slug: "aws", collector: "aws_ip_ranges", status: "ok", items: 100, churn: { missing: 20 } }),
            normalizeFeedRun({ slug: "oracle", collector: "oracle_public_ip_ranges", status: "ok", items: 50 }),
          ]}
        />
      </MemoryRouter>,
    );
    expect(html).toContain('href="/admin/provider-feeds/providers/aws"');
    expect(html).toContain("20 of 100 ranges went missing");
    expect(html).toContain('data-flagged="true"');
    expect(html).toContain('data-flagged="false"');
  });

  it("says so when there are no feeds", () => {
    const html = renderToStaticMarkup(<MemoryRouter><FeedTable rules={DEFAULT_INDICATOR_RULES} feeds={[]} /></MemoryRouter>);
    expect(html).toContain("No feeds in this run");
  });
});

describe("RangeTable", () => {
  it("shows status, dates and removal action; a legacy range reads as active", () => {
    const html = renderToStaticMarkup(
      <RangeTable
        rows={[
          { serviceKey: "aws.cloudfront", range: { cidr: "10.0.3.0/24", confidence: 1, source: "official_feed", collector: "aws_ip_ranges", status: "removed", first_seen: "2026-09-01", last_seen: "2026-09-20", missing_since: "2026-09-21", removed_at: "2026-09-28", removal_action: "grace_expired" } },
          { serviceKey: "aws.cloudfront", range: { cidr: "10.0.9.0/24", confidence: 1, source: "official_feed" } },
        ]}
      />,
    );
    expect(html).toContain("10.0.3.0/24");
    expect(html).toContain("grace_expired");
    expect(html).toContain("2026-09-28");
    expect(html).toContain(">active<");
  });
});
```

- [ ] **Step 2: Run to verify failure**

Run: `npx vitest run tests/provider-feeds.test.tsx`
Expected: FAIL: cannot resolve `~/components/admin/provider-feeds`.

- [ ] **Step 3: Implement** — `app/components/admin/provider-feeds.tsx`

```tsx
import { Link } from "react-router";
import { Badge } from "~/components/ui/badge";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "~/components/ui/table";
import { flagFeed, rangeStatus, type FeedRun, type IndicatorRules, type IPRange } from "~/lib/provider-recon";

const STATUS_VARIANT: Record<string, "default" | "secondary" | "destructive" | "outline"> = {
  ok: "secondary",
  active: "secondary",
  stale: "outline",
  missing: "outline",
  failed: "destructive",
  removed: "destructive",
};

export function StatusBadge({ status }: { status: string }) {
  return <Badge variant={STATUS_VARIANT[status] ?? "outline"}>{status}</Badge>;
}

/** One row per feed; rows the warning rules flag are highlighted with their reasons. */
export function FeedTable({ feeds, rules }: { feeds: FeedRun[]; rules: IndicatorRules }) {
  if (feeds.length === 0) return <p className="text-sm text-muted-foreground">No feeds in this run.</p>;
  return (
    <Table>
      <TableHeader>
        <TableRow>
          <TableHead>Provider</TableHead>
          <TableHead>Feed</TableHead>
          <TableHead>Status</TableHead>
          <TableHead className="text-right">Ranges</TableHead>
          <TableHead className="text-right">Added</TableHead>
          <TableHead className="text-right">Missing</TableHead>
          <TableHead className="text-right">Reappeared</TableHead>
          <TableHead className="text-right">Removed</TableHead>
          <TableHead className="text-right">Purged</TableHead>
          <TableHead>Warnings</TableHead>
        </TableRow>
      </TableHeader>
      <TableBody>
        {feeds.map((feed) => {
          const flags = flagFeed(feed, rules);
          const flagged = flags.length > 0;
          return (
            <TableRow key={`${feed.slug}/${feed.collector}`} data-flagged={flagged ? "true" : "false"} className={flagged ? "bg-destructive/5" : undefined}>
              <TableCell>
                <Link className="font-medium underline-offset-4 hover:underline" to={`/admin/provider-feeds/providers/${feed.slug}`}>
                  {feed.slug}
                </Link>
              </TableCell>
              <TableCell className="font-mono text-xs">{feed.collector}</TableCell>
              <TableCell><StatusBadge status={feed.status} /></TableCell>
              <TableCell className="text-right tabular-nums">{feed.items}</TableCell>
              <TableCell className="text-right tabular-nums">{feed.churn.added}</TableCell>
              <TableCell className="text-right tabular-nums">{feed.churn.missing}</TableCell>
              <TableCell className="text-right tabular-nums">{feed.churn.reappeared}</TableCell>
              <TableCell className="text-right tabular-nums">{feed.churn.removed}</TableCell>
              <TableCell className="text-right tabular-nums">{feed.churn.purged}</TableCell>
              <TableCell>
                {flagged ? (
                  <ul className="flex flex-col gap-1 text-sm text-destructive">
                    {flags.map((f) => <li key={f.kind}>{f.message}</li>)}
                  </ul>
                ) : (
                  <span className="text-sm text-muted-foreground">—</span>
                )}
              </TableCell>
            </TableRow>
          );
        })}
      </TableBody>
    </Table>
  );
}

/** Missing and removed ranges with their lifecycle dates. */
export function RangeTable({ rows }: { rows: { serviceKey: string; range: IPRange }[] }) {
  if (rows.length === 0) return <p className="text-sm text-muted-foreground">Every range is active.</p>;
  return (
    <Table>
      <TableHeader>
        <TableRow>
          <TableHead>Range</TableHead>
          <TableHead>Service</TableHead>
          <TableHead>Feed</TableHead>
          <TableHead>Status</TableHead>
          <TableHead>First seen</TableHead>
          <TableHead>Last seen</TableHead>
          <TableHead>Missing since</TableHead>
          <TableHead>Removed</TableHead>
          <TableHead>Action</TableHead>
          <TableHead>Restored</TableHead>
        </TableRow>
      </TableHeader>
      <TableBody>
        {rows.map(({ serviceKey, range }) => (
          <TableRow key={`${serviceKey}/${range.cidr}/${range.first_seen ?? ""}`}>
            <TableCell className="font-mono text-xs">{range.cidr}</TableCell>
            <TableCell className="text-sm">{serviceKey}</TableCell>
            <TableCell className="font-mono text-xs">{range.collector ?? "curated"}</TableCell>
            <TableCell><StatusBadge status={rangeStatus(range)} /></TableCell>
            <TableCell className="tabular-nums">{range.first_seen || "—"}</TableCell>
            <TableCell className="tabular-nums">{range.last_seen || "—"}</TableCell>
            <TableCell className="tabular-nums">{range.missing_since ?? "—"}</TableCell>
            <TableCell className="tabular-nums">{range.removed_at ?? "—"}</TableCell>
            <TableCell className="text-sm">{range.removal_action ?? "—"}</TableCell>
            <TableCell className="tabular-nums">{range.restored_at ?? "—"}</TableCell>
          </TableRow>
        ))}
      </TableBody>
    </Table>
  );
}
```

- [ ] **Step 4: Run to verify pass**

Run: `npx vitest run tests/provider-feeds.test.tsx && npm run typecheck`
Expected: PASS. If `Badge` does not accept `variant="outline"`, check `app/components/ui/badge.tsx` for the available variants, map to the closest one, and ledger it.

- [ ] **Step 5: Commit**

```bash
git add services/backoffice/app/components/admin/provider-feeds.tsx services/backoffice/tests/provider-feeds.test.tsx
git commit -m "feat(backoffice): provider feed and range lifecycle tables"
```

---

### Task 4: Pages, routes and sidebar

**Files:**
- Create: `app/routes/admin-provider-feeds.tsx`, `app/routes/admin-provider-feeds-run.tsx`, `app/routes/admin-provider-feeds-provider.tsx`
- Modify: `app/routes.ts`, `app/components/admin/admin-sidebar.tsx`

**Interfaces:**
- Consumes: Tasks 1–3.
- Produces:
  - `/admin/provider-feeds` — the latest full collect's feeds, the recent runs with flagged-feed counts, providers, and the warning-rules form (action).
  - `/admin/provider-feeds/runs/:runId` — the run's feeds and changed providers with their lifecycle diffs.
  - `/admin/provider-feeds/providers/:slug` — collectors, services with range counts by status, missing/removed ranges, and restore commands.

- [ ] **Step 1: Index page** — `app/routes/admin-provider-feeds.tsx`

```tsx
import { data, Form, Link, redirect, useNavigation } from "react-router";
import type { Route } from "./+types/admin-provider-feeds";
import { FeedTable } from "~/components/admin/provider-feeds";
import { Alert, AlertDescription, AlertTitle } from "~/components/ui/alert";
import { Badge } from "~/components/ui/badge";
import { Button } from "~/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "~/components/ui/card";
import { Field, FieldDescription, FieldGroup, FieldLabel } from "~/components/ui/field";
import { Input } from "~/components/ui/input";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "~/components/ui/table";
import { ObjectStoreError } from "~/lib/object-store.server";
import { flagFeed, parseIndicatorRules } from "~/lib/provider-recon";
import { loadIndicatorRules, loadRunIndex, saveIndicatorRules } from "~/lib/provider-recon.server";

const RECENT_RUNS = 60;

export async function loader() {
  const [index, rules] = await Promise.all([loadRunIndex(), loadIndicatorRules()]);
  const latestFull = index.runs.find((r) => r.scope.command === "collect" && !(r.scope.providers?.length)) ?? null;
  return { runs: index.runs.slice(0, RECENT_RUNS), providers: index.providers, rules, latestFull };
}

export async function action({ request }: Route.ActionArgs) {
  const origin = request.headers.get("origin");
  if (origin && origin !== new URL(request.url).origin) return data({ error: "Invalid request origin." }, { status: 403 });
  const parsed = parseIndicatorRules(await request.formData());
  if ("error" in parsed) return data({ error: parsed.error }, { status: 400 });
  try {
    await saveIndicatorRules(parsed.rules);
  } catch (error) {
    if (error instanceof ObjectStoreError) return data({ error: error.message }, { status: 502 });
    throw error;
  }
  return redirect("/admin/provider-feeds");
}

export function meta() {
  return [{ title: "Provider feeds | CompanyCollect" }];
}

export default function AdminProviderFeeds({ loaderData, actionData }: Route.ComponentProps) {
  const { runs, providers, rules, latestFull } = loaderData;
  const busy = useNavigation().state === "submitting";
  return (
    <div className="flex flex-col gap-6 p-4 md:p-6">
      <header className="flex flex-col gap-2">
        <h1 className="text-2xl font-semibold">Provider feeds</h1>
        <p className="text-sm text-muted-foreground">
          Every provider-recon run and what each feed changed. Warnings are display only; nothing is blocked. A wrong removal is undone
          with <code className="font-mono">provider-recon restore</code> (see a provider's page).
        </p>
      </header>
      {actionData?.error && (
        <Alert variant="destructive">
          <AlertTitle>Rules not saved</AlertTitle>
          <AlertDescription>{actionData.error}</AlertDescription>
        </Alert>
      )}
      <div className="grid min-w-0 items-start gap-6 xl:grid-cols-[minmax(0,1fr)_minmax(20rem,26rem)]">
        <section className="flex min-w-0 flex-col gap-6">
          <Card>
            <CardHeader>
              <CardTitle>Latest full collect</CardTitle>
              <CardDescription>
                {latestFull ? (
                  <Link className="underline-offset-4 hover:underline" to={`/admin/provider-feeds/runs/${latestFull.run_id}`}>{latestFull.run_id}</Link>
                ) : "No runs yet."}
              </CardDescription>
            </CardHeader>
            <CardContent>{latestFull && <FeedTable feeds={latestFull.feeds} rules={rules} />}</CardContent>
          </Card>
          <Card>
            <CardHeader>
              <CardTitle>Runs</CardTitle>
              <CardDescription>The most recent {runs.length} runs, newest first.</CardDescription>
            </CardHeader>
            <CardContent>
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Run</TableHead>
                    <TableHead>Command</TableHead>
                    <TableHead>Providers</TableHead>
                    <TableHead className="text-right">Changed</TableHead>
                    <TableHead className="text-right">Warnings</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {runs.map((run) => {
                    const warned = run.feeds.filter((f) => flagFeed(f, rules).length > 0).length;
                    return (
                      <TableRow key={run.run_id}>
                        <TableCell>
                          <Link className="font-mono text-xs underline-offset-4 hover:underline" to={`/admin/provider-feeds/runs/${run.run_id}`}>{run.run_id}</Link>
                        </TableCell>
                        <TableCell>{run.scope.command}</TableCell>
                        <TableCell className="text-sm">{run.scope.providers?.join(", ") || "all"}</TableCell>
                        <TableCell className="text-right tabular-nums">{run.changed.length}</TableCell>
                        <TableCell className="text-right">
                          {warned > 0 ? <Badge variant="destructive">{warned}</Badge> : <span className="text-muted-foreground">0</span>}
                        </TableCell>
                      </TableRow>
                    );
                  })}
                </TableBody>
              </Table>
            </CardContent>
          </Card>
        </section>
        <aside className="flex min-w-0 flex-col gap-6">
          <Card>
            <CardHeader>
              <CardTitle>Warning rules</CardTitle>
              <CardDescription>Percent of the ranges a feed had before the run. Stale and failed feeds are always flagged.</CardDescription>
            </CardHeader>
            <CardContent>
              <Form method="post">
                <FieldGroup>
                  <Field>
                    <FieldLabel htmlFor="missingPercent">Missing above (%)</FieldLabel>
                    <Input id="missingPercent" name="missingPercent" type="number" step="0.1" min="0" max="100" defaultValue={rules.missingPercent} />
                  </Field>
                  <Field>
                    <FieldLabel htmlFor="removedPercent">Removed above (%)</FieldLabel>
                    <Input id="removedPercent" name="removedPercent" type="number" step="0.1" min="0" max="100" defaultValue={rules.removedPercent} />
                  </Field>
                  <Field>
                    <FieldLabel htmlFor="addedPercent">Added above (%)</FieldLabel>
                    <Input id="addedPercent" name="addedPercent" type="number" step="0.1" min="0" max="100" defaultValue={rules.addedPercent} />
                    <FieldDescription>A feed's first run has no baseline and is never flagged.</FieldDescription>
                  </Field>
                  <Field orientation="horizontal">
                    <input id="flagUnmappedTags" name="flagUnmappedTags" type="checkbox" defaultChecked={rules.flagUnmappedTags} />
                    <FieldLabel htmlFor="flagUnmappedTags">Flag new unmapped feed tags</FieldLabel>
                  </Field>
                  <Button type="submit" disabled={busy}>{busy ? "Saving…" : "Save rules"}</Button>
                </FieldGroup>
              </Form>
            </CardContent>
          </Card>
          <Card>
            <CardHeader>
              <CardTitle>Providers</CardTitle>
              <CardDescription>{providers.length} published.</CardDescription>
            </CardHeader>
            <CardContent>
              <ul className="grid grid-cols-2 gap-1 text-sm">
                {providers.map((slug) => (
                  <li key={slug}>
                    <Link className="underline-offset-4 hover:underline" to={`/admin/provider-feeds/providers/${slug}`}>{slug}</Link>
                  </li>
                ))}
              </ul>
            </CardContent>
          </Card>
        </aside>
      </div>
    </div>
  );
}
```

If `Field` has no `orientation` prop in `app/components/ui/field.tsx`, drop the prop and wrap the checkbox and its label in `<div className="flex items-center gap-2">`.

- [ ] **Step 2: Run page** — `app/routes/admin-provider-feeds-run.tsx`

```tsx
import { data, Link } from "react-router";
import type { Route } from "./+types/admin-provider-feeds-run";
import { FeedTable } from "~/components/admin/provider-feeds";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "~/components/ui/card";
import type { KindDiff } from "~/lib/provider-recon";
import { loadIndicatorRules, loadManifest } from "~/lib/provider-recon.server";

export async function loader({ params }: Route.LoaderArgs) {
  const [manifest, rules] = await Promise.all([loadManifest(params.runId), loadIndicatorRules()]);
  if (!manifest) throw data(`Run ${params.runId} not found.`, { status: 404 });
  return { manifest, rules };
}

export function meta({ params }: Route.MetaArgs) {
  return [{ title: `${params.runId} | Provider feeds` }];
}

const TRANSITIONS: [keyof KindDiff, keyof KindDiff, string][] = [
  ["added", "added_count", "Added"],
  ["missing", "missing_count", "Missing"],
  ["reappeared", "reappeared_count", "Reappeared"],
  ["restored", "restored_count", "Restored"],
  ["removed", "removed_count", "Removed"],
  ["purged", "purged_count", "Purged"],
  ["updated", "updated_count", "Updated"],
];

function DiffList({ kind, diff }: { kind: string; diff: KindDiff }) {
  return (
    <div className="flex flex-col gap-1">
      <h4 className="font-mono text-xs text-muted-foreground">{kind}</h4>
      {TRANSITIONS.map(([listKey, countKey, label]) => {
        const count = diff[countKey] as number;
        if (!count) return null;
        const items = (diff[listKey] as string[] | undefined) ?? [];
        return (
          <details key={label} className="text-sm">
            <summary>{label}: {count}</summary>
            {items.length > 0 ? (
              <ul className="ml-4 font-mono text-xs">{items.map((item) => <li key={item}>{item}</li>)}</ul>
            ) : (
              <p className="ml-4 text-xs text-muted-foreground">Counts only (new provider).</p>
            )}
          </details>
        );
      })}
      {diff.truncated && <p className="text-xs text-muted-foreground">Lists are truncated; counts are exact.</p>}
    </div>
  );
}

export default function AdminProviderFeedsRun({ loaderData }: Route.ComponentProps) {
  const { manifest, rules } = loaderData;
  return (
    <div className="flex flex-col gap-6 p-4 md:p-6">
      <header className="flex flex-col gap-1">
        <Link className="text-sm text-muted-foreground underline-offset-4 hover:underline" to="/admin/provider-feeds">Provider feeds</Link>
        <h1 className="font-mono text-xl font-semibold">{manifest.run_id}</h1>
        <p className="text-sm text-muted-foreground">
          {manifest.scope.command} · {manifest.scope.providers?.join(", ") || "all providers"} · {manifest.changed.length} changed, {manifest.unchanged.length} unchanged
        </p>
      </header>
      <Card>
        <CardHeader><CardTitle>Feeds</CardTitle></CardHeader>
        <CardContent><FeedTable feeds={manifest.feeds} rules={rules} /></CardContent>
      </Card>
      <Card>
        <CardHeader>
          <CardTitle>Changed providers</CardTitle>
          <CardDescription>Lifecycle transitions per evidence kind.</CardDescription>
        </CardHeader>
        <CardContent className="flex flex-col gap-6">
          {manifest.changed.length === 0 && <p className="text-sm text-muted-foreground">Nothing changed in this run.</p>}
          {manifest.changed.map((change) => (
            <section key={change.slug} className="flex flex-col gap-3">
              <h3 className="font-medium">
                <Link className="underline-offset-4 hover:underline" to={`/admin/provider-feeds/providers/${change.slug}`}>{change.slug}</Link>
                {change.created && <span className="ml-2 text-xs text-muted-foreground">first publication</span>}
              </h3>
              <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
                {Object.entries(change.evidence ?? {}).map(([kind, diff]) => <DiffList key={kind} kind={kind} diff={diff} />)}
              </div>
            </section>
          ))}
        </CardContent>
      </Card>
    </div>
  );
}
```

- [ ] **Step 3: Provider page** — `app/routes/admin-provider-feeds-provider.tsx`

```tsx
import { data, Link } from "react-router";
import type { Route } from "./+types/admin-provider-feeds-provider";
import { RangeTable, StatusBadge } from "~/components/admin/provider-feeds";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "~/components/ui/card";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "~/components/ui/table";
import { attentionRanges, restoreCommand, serviceRangeCounts } from "~/lib/provider-recon";
import { loadProviderDocument } from "~/lib/provider-recon.server";

export async function loader({ params }: Route.LoaderArgs) {
  const doc = await loadProviderDocument(params.slug);
  if (!doc) throw data(`Provider ${params.slug} not found.`, { status: 404 });
  return { doc };
}

export function meta({ params }: Route.MetaArgs) {
  return [{ title: `${params.slug} | Provider feeds` }];
}

export default function AdminProviderFeedsProvider({ loaderData }: Route.ComponentProps) {
  const { doc } = loaderData;
  const attention = attentionRanges(doc);
  // One restore command per feed, from the earliest grace-expired removal still listed.
  const restores = new Map<string, string>();
  for (const { range } of attention) {
    if (range.status === "removed" && range.removal_action === "grace_expired" && range.collector && range.removed_at) {
      const current = restores.get(range.collector);
      if (!current || range.removed_at < current) restores.set(range.collector, range.removed_at);
    }
  }
  return (
    <div className="flex flex-col gap-6 p-4 md:p-6">
      <header className="flex flex-col gap-1">
        <Link className="text-sm text-muted-foreground underline-offset-4 hover:underline" to="/admin/provider-feeds">Provider feeds</Link>
        <h1 className="text-2xl font-semibold">{doc.display_name}</h1>
        <p className="text-sm text-muted-foreground">
          {doc.slug} · {doc.category}{doc.country ? ` · ${doc.country}` : ""} · collected {doc.collection.collected_at.slice(0, 16).replace("T", " ")} UTC
        </p>
      </header>
      <Card>
        <CardHeader><CardTitle>Feeds</CardTitle></CardHeader>
        <CardContent>
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Feed</TableHead>
                <TableHead>Status</TableHead>
                <TableHead className="text-right">Ranges</TableHead>
                <TableHead>Last success</TableHead>
                <TableHead>Version</TableHead>
                <TableHead>Error</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {Object.entries(doc.collection.collectors).map(([id, st]) => (
                <TableRow key={id}>
                  <TableCell className="font-mono text-xs">{id}</TableCell>
                  <TableCell><StatusBadge status={st.status} /></TableCell>
                  <TableCell className="text-right tabular-nums">{st.items}</TableCell>
                  <TableCell className="tabular-nums">{st.last_success_at?.slice(0, 16).replace("T", " ") ?? "—"}</TableCell>
                  <TableCell className="max-w-48 truncate font-mono text-xs" title={st.source_version}>{st.source_version ?? "—"}</TableCell>
                  <TableCell className="text-sm text-destructive">{st.error ?? ""}</TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </CardContent>
      </Card>
      <Card>
        <CardHeader><CardTitle>Services</CardTitle></CardHeader>
        <CardContent>
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Service</TableHead>
                <TableHead>Types</TableHead>
                <TableHead className="text-right">Active</TableHead>
                <TableHead className="text-right">Missing</TableHead>
                <TableHead className="text-right">Removed</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {doc.services.map((s) => {
                const c = serviceRangeCounts(s);
                return (
                  <TableRow key={s.service_key}>
                    <TableCell>
                      <span className="font-medium">{s.display_name}</span>{" "}
                      <span className="font-mono text-xs text-muted-foreground">{s.service_key}</span>
                      {s.removed_at && <span className="ml-2 text-xs text-destructive">removed from definition {s.removed_at}</span>}
                    </TableCell>
                    <TableCell className="text-sm">{s.service_types.join(", ")}</TableCell>
                    <TableCell className="text-right tabular-nums">{c.active}</TableCell>
                    <TableCell className="text-right tabular-nums">{c.missing}</TableCell>
                    <TableCell className="text-right tabular-nums">{c.removed}</TableCell>
                  </TableRow>
                );
              })}
            </TableBody>
          </Table>
        </CardContent>
      </Card>
      <Card>
        <CardHeader>
          <CardTitle>Missing and removed ranges</CardTitle>
          <CardDescription>Removed ranges stay here for 90 days; history keeps them forever.</CardDescription>
        </CardHeader>
        <CardContent className="flex flex-col gap-4">
          {restores.size > 0 && (
            <div className="flex flex-col gap-1">
              <p className="text-sm">If a removal was wrong, fix the collector, then run:</p>
              {[...restores].map(([collector, since]) => (
                <code key={collector} className="rounded bg-muted px-2 py-1 font-mono text-xs">{restoreCommand(doc.slug, collector, since)}</code>
              ))}
            </div>
          )}
          <RangeTable rows={attention} />
        </CardContent>
      </Card>
    </div>
  );
}
```

- [ ] **Step 4: Routes and sidebar**

In `app/routes.ts`, directly after `route("technologies/:slug", "routes/admin-technology-detail.tsx"),` add:
```ts
    // provider-recon runs, per-feed churn and warnings (display only), and
    // each provider's missing/removed ranges.
    route("provider-feeds", "routes/admin-provider-feeds.tsx"),
    route("provider-feeds/runs/:runId", "routes/admin-provider-feeds-run.tsx"),
    route("provider-feeds/providers/:slug", "routes/admin-provider-feeds-provider.tsx"),
```

In `app/components/admin/admin-sidebar.tsx`, add a menu item directly after the Technologies `SidebarMenuItem`:
```tsx
            <SidebarMenuItem>
              <SidebarMenuButton
                isActive={pathname === "/admin/provider-feeds" || pathname.startsWith("/admin/provider-feeds/")}
                tooltip="Provider feed updates and warnings"
                render={<Link to="/admin/provider-feeds" />}
              >
                <NetworkIcon />
                <span>Provider feeds</span>
              </SidebarMenuButton>
            </SidebarMenuItem>
```
(`NetworkIcon` is already imported.)

- [ ] **Step 5: Typecheck and tests**

Run: `npm run typecheck && npx vitest run tests/provider-recon.test.ts tests/provider-recon.server.test.ts tests/provider-feeds.test.tsx`
Expected: PASS. Typegen creates `./+types/admin-provider-feeds*`. Fix any prop mismatch against the actual shadcn components in `app/components/ui` and ledger it.

- [ ] **Step 6: Commit**

```bash
git add services/backoffice/app/routes.ts services/backoffice/app/components/admin/admin-sidebar.tsx services/backoffice/app/routes/admin-provider-feeds.tsx services/backoffice/app/routes/admin-provider-feeds-run.tsx services/backoffice/app/routes/admin-provider-feeds-provider.tsx
git commit -m "feat(backoffice): provider feeds pages with display-only warnings and restore hints"
```

---

### Task 5: Smoke test against the real bucket

**Prerequisite:** the provider-recon lifecycle plan has run at least one `collect` to S3, so `changes/index.json` exists.

- [ ] **Step 1: Start a second dev server** (never restart the owner's :5183)

In the worktree, copy the main checkout's `.env` (`cp ../../../../companycollect/corpscout/services/backoffice/.env .`; adjust the path to the main checkout). Then run `npx react-router dev --port 5184` as a background command.

- [ ] **Step 2: Load each page**

```bash
curl -s -o /tmp/pf.html -w "%{http_code}\n" http://localhost:5184/admin/provider-feeds
rg -c "Latest full collect|aws_ip_ranges" /tmp/pf.html
RUN=$(curl -s http://localhost:5184/admin/provider-feeds | rg -o '/admin/provider-feeds/runs/[0-9TZ]+-collect' | head -1)
curl -s -o /dev/null -w "%{http_code}\n" "http://localhost:5184$RUN"
curl -s -o /dev/null -w "%{http_code}\n" http://localhost:5184/admin/provider-feeds/providers/microsoft
curl -s -o /dev/null -w "%{http_code}\n" "http://localhost:5184/admin/provider-feeds/providers/..%2Fsettings"
```
Expected:
- The first three pages return 200. The index shows the latest full collect with `aws_ip_ranges` among its feeds.
- The last request returns 404.
- If admin routes require a session, log in first (the backoffice's local login) and repeat with the session cookie.

- [ ] **Step 3: Save rules once through the form**

Post the form (browser, or `curl -X POST -d missingPercent=5 -d removedPercent=5 -d addedPercent=50 -d flagUnmappedTags=on`). Confirm `settings/indicator-rules.json` exists in the bucket, and that reloading the page shows the saved values in the form.

- [ ] **Step 4: Stop the :5184 server.** Report that the owner's :5183 server needs a restart to pick up the new route files.
