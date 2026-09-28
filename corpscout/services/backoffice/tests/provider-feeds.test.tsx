import { renderToStaticMarkup } from "react-dom/server";
import { createMemoryRouter, MemoryRouter, RouterProvider } from "react-router";
import type { ReactElement } from "react";
import { describe, expect, it } from "vitest";
import { DagsterPanel, FeedTable, RangeTable } from "~/components/admin/provider-feeds";
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

describe("FeedTable source column", () => {
  it("shows protocol, full URL link and version", () => {
    const html = renderToStaticMarkup(
      <MemoryRouter>
        <FeedTable rules={DEFAULT_INDICATOR_RULES} feeds={[normalizeFeedRun({
          slug: "aws", collector: "aws_ip_ranges", status: "ok", items: 10,
          source_url: "https://ip-ranges.amazonaws.com/ip-ranges.json", source_version: "syncToken=1", format: "JSON",
        })]} />
      </MemoryRouter>,
    );
    expect(html).toContain("HTTPS · JSON");
    expect(html).toContain('href="https://ip-ranges.amazonaws.com/ip-ranges.json"');
    expect(html).toContain("syncToken=1");
  });

  it("falls back to a dash for manifests without source details", () => {
    const html = renderToStaticMarkup(
      <MemoryRouter>
        <FeedTable rules={DEFAULT_INDICATOR_RULES} feeds={[normalizeFeedRun({ slug: "aws", collector: "aws_ip_ranges", status: "ok", items: 10 })]} />
      </MemoryRouter>,
    );
    expect(html).toContain("—");
    expect(html).not.toContain("href=\"http");
  });
});

/** Forms need a data router; MemoryRouter is not one. */
function inDataRouter(element: ReactElement) {
  const router = createMemoryRouter([{ path: "/admin/provider-feeds", element }], { initialEntries: ["/admin/provider-feeds"] });
  return renderToStaticMarkup(<RouterProvider router={router} />);
}

describe("DagsterPanel", () => {
  it("renders assets with links, the schedule and runs, and controls", () => {
    const html = inDataRouter(
        <DagsterPanel dagster={{
          assets: [{ asset: "provider_recon_documents", url: "http://d/assets/provider_recon_documents", materializedAt: 1790565200, runId: "r1", runUrl: "http://d/runs/r1", numbers: { documents: 37 } }],
          schedule: { name: "provider_recon_daily", status: "RUNNING", stateId: "s", cronSchedule: "12 3 * * *", timezone: "UTC", nextTick: 1790565120 },
          runs: [{ runId: "r1", status: "SUCCESS", startTime: 1790565100, endTime: 1790565160, url: "http://d/runs/r1" }],
          error: null,
        }} />,
    );
    expect(html).toContain('href="http://d/assets/provider_recon_documents"');
    expect(html).toContain("12 3 * * *");
    expect(html).toContain("RUNNING");
    expect(html).toMatch(/<button[^>]*value="run-now"[^>]*name="intent"|<button[^>]*name="intent"[^>]*value="run-now"/);
    expect(html).toContain('value="schedule-stop"');
    expect(html).toContain("documents: 37");
  });

  it("disables the controls while a submission is in flight and Run now while a run is active", () => {
    const base = {
      assets: [], schedule: { name: "provider_recon_daily", status: "RUNNING", stateId: "s", cronSchedule: "12 3 * * *", timezone: "UTC", nextTick: null },
      error: null,
    };
    const busy = inDataRouter(<DagsterPanel busy dagster={{ ...base, runs: [] }} />);
    expect(busy.match(/<button[^>]*disabled=""[^>]*>/g)?.length).toBe(2);
    const active = inDataRouter(<DagsterPanel dagster={{ ...base, runs: [{ runId: "r2", status: "STARTED", startTime: 1, endTime: null, url: null }] }} />);
    expect(active).toMatch(/<button[^>]*disabled=""[^>]*value="run-now"|<button[^>]*value="run-now"[^>]*disabled=""/);
    expect(active).not.toMatch(/<button[^>]*disabled=""[^>]*value="schedule-stop"|<button[^>]*value="schedule-stop"[^>]*disabled=""/);
  });

  it("shows the Dagster error instead of the panel content", () => {
    const html = inDataRouter(<DagsterPanel dagster={{ assets: [], schedule: null, runs: [], error: "Dagster at x did not answer" }} />);
    expect(html).toContain("Dagster at x did not answer");
    expect(html).not.toContain("run-now");
  });
});
