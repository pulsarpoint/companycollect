import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";
import {
  WebTechnologyHistorySection,
  type TechnologyCatalog,
} from "~/components/detail/web-technology-history-section";
import type { CompanyWebTechnologyHistory } from "~/lib/web-technology-history";

const history: CompanyWebTechnologyHistory = {
  domain: "example.se",
  latestCrawlId: "CC-MAIN-2026-25",
  latestProcessedAt: "2026-07-20 12:00:00.000",
  technologies: [
    {
      name: "React",
      categories: ["JavaScript frameworks"],
      versions: ["19.1"],
      state: "detected_latest",
      firstDetectedCrawl: "CC-MAIN-2026-17",
      lastDetectedCrawl: "CC-MAIN-2026-25",
      detectedCrawlCount: 2,
      lastDetectedPages: 12,
      lastDetectedConfidence: 100,
    },
    {
      name: "C3.js",
      categories: ["JavaScript libraries"],
      versions: [],
      state: "not_detected_latest",
      firstDetectedCrawl: "CC-MAIN-2026-17",
      lastDetectedCrawl: "CC-MAIN-2026-17",
      detectedCrawlCount: 1,
      lastDetectedPages: 8,
      lastDetectedConfidence: 100,
    },
  ],
  snapshots: [
    {
      crawlId: "CC-MAIN-2026-25",
      observedPages: 12,
      processedAt: "2026-07-20 12:00:00.000",
      newlyDetected: ["YouTube"],
      noLongerDetected: ["C3.js"],
      detections: [
        {
          name: "React",
          categories: ["JavaScript frameworks"],
          versions: ["19.1"],
          confidence: 100,
          detectedPages: 12,
          sampleUrls: ["https://example.se/"],
        },
        {
          name: "YouTube",
          categories: ["Video players"],
          versions: [],
          confidence: 100,
          detectedPages: 2,
          sampleUrls: ["https://example.se/video"],
        },
      ],
    },
    {
      crawlId: "CC-MAIN-2026-17",
      observedPages: 8,
      processedAt: "2026-07-10 12:00:00.000",
      newlyDetected: [],
      noLongerDetected: [],
      detections: [
        {
          name: "C3.js",
          categories: ["JavaScript libraries"],
          versions: [],
          confidence: 100,
          detectedPages: 8,
          sampleUrls: ["https://example.se/"],
        },
        {
          name: "React",
          categories: ["JavaScript frameworks"],
          versions: ["19.1"],
          confidence: 100,
          detectedPages: 8,
          sampleUrls: ["https://example.se/"],
        },
      ],
    },
  ],
};

describe("WebTechnologyHistorySection", () => {
  it("shows latest detections and crawl-to-crawl changes", () => {
    const html = renderToStaticMarkup(
      <WebTechnologyHistorySection history={history} />,
    );

    expect(html).toContain("Web application technologies");
    expect(html).toContain("Detected in latest crawl");
    expect(html).toContain("Not detected in latest crawl");
    expect(html).toContain("Detection history");
    expect(html).toContain("Newly detected");
    expect(html).toContain("No longer detected in this crawl");
    expect(html).toContain("https://example.se/video");
    expect(html).toContain("does not prove installation or removal");
  });

  const catalog: TechnologyCatalog = {
    React: {
      slug: "react",
      description:
        "React is an open-source JavaScript library for building user interfaces.",
      website: "https://react.dev",
      categories: ["JavaScript frameworks"],
      saas: false,
      oss: true,
      icon: true,
    },
    YouTube: {
      slug: "youtube",
      description: "YouTube is a video sharing service.",
      website: "https://www.youtube.com",
      categories: ["Video players"],
      saas: true,
      oss: false,
      icon: true,
    },
  };

  it("renders catalog icons, descriptions, and website links", () => {
    const html = renderToStaticMarkup(
      <WebTechnologyHistorySection history={history} catalog={catalog} />,
    );

    // Icons go through the proxy route, never straight to the object store.
    expect(html).toContain('src="/icons/tech/react"');
    expect(html).toContain('src="/icons/tech/youtube"');
    expect(html).toContain(
      "React is an open-source JavaScript library for building user interfaces.",
    );
    expect(html).toContain('href="https://react.dev"');
    expect(html).toContain("react.dev");
  });

  it("falls back to a monogram block for technologies the catalog misses", () => {
    const html = renderToStaticMarkup(
      <WebTechnologyHistorySection history={history} catalog={catalog} />,
    );

    // C3.js has no catalog entry: no icon URL, a monogram stand-in instead.
    expect(html).not.toContain("/icons/tech/c3");
    expect(html).toContain('data-slot="technology-monogram"');
  });

  it("renders every technology without a catalog at all", () => {
    const html = renderToStaticMarkup(
      <WebTechnologyHistorySection history={history} />,
    );
    expect(html).toContain("React");
    expect(html).not.toContain("/icons/tech/");
    expect(html).toContain('data-slot="technology-monogram"');
  });
});
