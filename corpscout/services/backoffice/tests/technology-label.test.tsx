/**
 * TechnologyLabel's admin link mode: opt-in only (public pages keep plain
 * labels), and only when the enrichment map actually knows a catalog slug.
 * Also pins that the WebTechnologyHistorySection threads the flag through to
 * its detection tables.
 */
import { renderToStaticMarkup } from "react-dom/server";
import { createMemoryRouter, RouterProvider } from "react-router";
import { describe, expect, it } from "vitest";
import { TechnologyLabel } from "~/components/detail/technology-label";
import { WebTechnologyHistorySection } from "~/components/detail/web-technology-history-section";
import type { TechnologyCatalogEntry } from "~/lib/technology-catalog.server";
import type { CompanyWebTechnologyHistory } from "~/lib/web-technology-history";

const ENTRY: TechnologyCatalogEntry = {
  slug: "wordpress",
  description: "The web's most common CMS.",
  website: "https://wordpress.org",
  categories: ["CMS"],
  saas: false,
  oss: true,
  icon: true,
};

function render(element: React.ReactElement): string {
  const router = createMemoryRouter([{ path: "*", element }], {
    initialEntries: ["/admin/technologies"],
  });
  return renderToStaticMarkup(<RouterProvider router={router} />);
}

describe("TechnologyLabel link mode", () => {
  it("stays a plain label by default -- public pages never link into the admin area", () => {
    const html = render(<TechnologyLabel name="WordPress" entry={ENTRY} />);
    expect(html).toContain("WordPress");
    expect(html).not.toContain("href=");
  });

  it("links the name to the catalog detail page when opted in and the slug is known", () => {
    const html = render(
      <TechnologyLabel name="WordPress" entry={ENTRY} linkToCatalog />,
    );
    expect(html).toContain('href="/admin/technologies/wordpress"');
    expect(html).toContain(">WordPress</a>");
  });

  it("degrades to a plain label when the catalog has no entry for the name", () => {
    const html = render(<TechnologyLabel name="MysteryTech" linkToCatalog />);
    expect(html).toContain("MysteryTech");
    expect(html).not.toContain("href=");
  });
});

const HISTORY: CompanyWebTechnologyHistory = {
  domain: "example.se",
  latestCrawlId: "2026-30",
  latestProcessedAt: "2026-08-01 00:00:00",
  technologies: [
    {
      name: "WordPress",
      categories: ["CMS"],
      versions: [],
      state: "detected_latest",
      firstDetectedCrawl: "2026-26",
      lastDetectedCrawl: "2026-30",
      detectedCrawlCount: 2,
      lastDetectedPages: 10,
      lastDetectedConfidence: 100,
    },
  ],
  snapshots: [
    {
      crawlId: "2026-30",
      processedAt: "2026-08-01 00:00:00",
      observedPages: 12,
      detections: [
        {
          name: "WordPress",
          categories: ["CMS"],
          versions: [],
          confidence: 100,
          detectedPages: 10,
          sampleUrls: [],
        },
      ],
      newlyDetected: [],
      noLongerDetected: [],
    },
  ],
};

describe("WebTechnologyHistorySection admin links", () => {
  it("links detected technologies to their catalog pages when linkTechnologies is set", () => {
    const html = render(
      <WebTechnologyHistorySection
        history={HISTORY}
        catalog={{ WordPress: ENTRY }}
        linkTechnologies
      />,
    );
    expect(html).toContain('href="/admin/technologies/wordpress"');
  });

  it("keeps plain labels by default (the public technology pages)", () => {
    const html = render(
      <WebTechnologyHistorySection
        history={HISTORY}
        catalog={{ WordPress: ENTRY }}
      />,
    );
    expect(html).not.toContain('href="/admin/technologies/wordpress"');
  });
});
