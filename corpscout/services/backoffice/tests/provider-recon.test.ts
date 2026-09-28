import { describe, expect, it } from "vitest";
import {
  DEFAULT_INDICATOR_RULES,
  feedProtocol,
  safeHttpUrl,
  attentionRanges,
  flagFeed,
  normalizeFeedRun,
  normalizeIndicatorRules,
  parseIndicatorRules,
  restoreCommand,
  restoreOptions,
  serviceRangeCounts,
  type FeedRun,
  type ProviderDocument,
} from "~/lib/provider-recon";

function feed(partial: Omit<Partial<FeedRun>, "churn"> & { churn?: Partial<FeedRun["churn"]> }): FeedRun {
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

describe("restoreOptions", () => {
  it("offers one option per removal date, newest first, each counting what it would restore", () => {
    const r = (cidr: string, removed_at: string, collector = "aws_ip_ranges", removal_action = "grace_expired") => ({
      cidr, confidence: 1, source: "official_feed", collector, status: "removed", first_seen: "2026-06-01", last_seen: "2026-06-01", removed_at, removal_action,
    });
    const doc = {
      slug: "aws",
      services: [{ service_key: "aws.cloudfront", evidence: { ip_ranges: [
        r("10.0.1.0/24", "2026-09-01"),
        r("10.0.2.0/24", "2026-09-28"),
        r("10.0.3.0/24", "2026-09-28"),
        r("10.0.4.0/24", "2026-09-28", "aws_ip_ranges", "definition_removed"),
        r("10.0.5.0/24", "2026-09-20", "google_goog"),
      ] } }],
    } as unknown as ProviderDocument;
    expect(restoreOptions(doc)).toEqual([
      { collector: "aws_ip_ranges", since: "2026-09-28", count: 2 },
      { collector: "aws_ip_ranges", since: "2026-09-01", count: 3 },
      { collector: "google_goog", since: "2026-09-20", count: 1 },
    ]);
  });
});

describe("feedProtocol", () => {
  it.each([
    ["https://ip-ranges.amazonaws.com/ip-ranges.json", "JSON", "HTTPS · JSON"],
    ["http://example.test/feed.csv", "CSV (RFC 8805 geofeed)", "HTTP · CSV (RFC 8805 geofeed)"],
    ["not a url", "JSON", "JSON"],
    [undefined, undefined, "—"],
    ["ftp://x/y", "", "FTP"],
  ])("%s + %s → %s", (url, format, expected) => {
    expect(feedProtocol(url, format)).toBe(expected);
  });

  it("only links http(s) URLs", () => {
    expect(safeHttpUrl("https://a.test/x")).toBe("https://a.test/x");
    expect(safeHttpUrl("javascript:alert(1)")).toBeNull();
    expect(safeHttpUrl("ftp://x/y")).toBeNull();
    expect(safeHttpUrl(undefined)).toBeNull();
  });
});
