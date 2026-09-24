import { renderToStaticMarkup } from "react-dom/server";
import { MemoryRouter } from "react-router";
import { expect, it } from "vitest";
import { WebtechSection } from "~/components/detail/webtech-section";

const data: Parameters<typeof WebtechSection>[0]["data"] = {
  pages: [],
  domain: "example.se",
  scan: {
    website_origin: "https://example.se",
    page_url: "https://example.se/",
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
      website_origin: "https://example.se",
      page_url: "https://example.se/",
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
      website_origin: "https://example.se",
      page_url: "https://example.se/",
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

it("renders separate requested pages and observed redirect destinations", () => {
  const first = { scan: data.scan!, detections: data.detections };
  const second = {
    scan: { ...data.scan!, page_url: "https://example.se/admin", final_url: "https://shop.example.net/login" },
    detections: [{ ...data.detections[0], version: "18.0" }],
  };
  const html = render({ ...data, pages: [first, second] });
  expect(html).toContain("https://example.se/admin");
  expect(html).toContain("https://shop.example.net/login");
  expect(html.match(/Requested page/g)).toHaveLength(2);
  expect(html).toContain("19.1");
  expect(html).toContain("18.0");
});

it("labels historical results without presenting them as the latest scan", () => {
  const html = renderToStaticMarkup(<MemoryRouter><WebtechSection data={data} historical /></MemoryRouter>);
  expect(html).toContain("Results recorded for this historical Webtech scan");
  expect(html).not.toContain("Results from the latest Webtech scan");
  expect(html).toContain("Scan ID");
});
