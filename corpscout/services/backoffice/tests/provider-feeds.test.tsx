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
