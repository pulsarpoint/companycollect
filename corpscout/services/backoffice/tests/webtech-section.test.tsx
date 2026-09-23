import { renderToStaticMarkup } from "react-dom/server";
import { MemoryRouter } from "react-router";
import { expect, it } from "vitest";
import { WebtechSection } from "~/components/detail/webtech-section";

const data: Parameters<typeof WebtechSection>[0]["data"] = {
  domain: "example.se",
  scan: {
    crawl_id: "crawl",
    scan_id: "scan",
    report_sha256: "hash",
    detector_version: "mywappalyzer-1.4.1",
    scanned_at: "2026-09-05 12:00:00",
    outcome: "success",
    technology_count: 2,
    requested_url: "https://example.se",
    final_url: "https://www.example.se",
  },
  detections: [
    {
      detected_name: "React",
      technology: "React",
      technology_id: "123",
      catalog_match: "exact",
      version: "19.1",
      confidence: 100,
      categories: ["JavaScript frameworks"],
      analysis_complete: 1,
    },
    {
      detected_name: "Unlisted tool",
      technology: "",
      technology_id: null,
      catalog_match: "unmapped",
      version: "",
      confidence: 75,
      categories: [],
      analysis_complete: 1,
    },
  ],
  catalog: {
    React: {
      slug: "react",
      description: "A UI library",
      website: "https://react.dev",
      categories: [],
      saas: false,
      oss: true,
      icon: true,
    },
  },
};

function render(value = data, linkTechnologies = true) {
  return renderToStaticMarkup(
    <MemoryRouter>
      <WebtechSection data={value} linkTechnologies={linkTechnologies} />
    </MemoryRouter>,
  );
}

it("shows detection details, catalog enrichment and unmatched technologies", () => {
  const html = render();
  for (const text of [
    "19.1",
    "JavaScript frameworks",
    "100%",
    "75%",
    "Unlisted tool",
    "Not in catalog",
    "A UI library",
    "https://www.example.se",
  ])
    expect(html).toContain(text);
  expect(html).toContain('href="/admin/technologies/react"');
  expect(render(data, false)).not.toContain('href="/admin/technologies/');
});

it("distinguishes no scan, a zero-result scan and an incomplete scan", () => {
  expect(render({ ...data, scan: null, detections: [] })).toContain(
    "No Webtech scan available",
  );
  const empty = {
    ...data,
    scan: { ...data.scan!, technology_count: 0 },
    detections: [],
  };
  expect(render(empty)).toContain("No technologies detected in this scan");
  expect(render(empty)).not.toContain("Scan did not complete fully");
  expect(
    render({ ...empty, scan: { ...empty.scan, outcome: "hard_timeout" } }),
  ).toContain("Scan did not complete fully");
  expect(render({ ...data, detections: [] })).toContain(
    "Detection details are incomplete",
  );
});
