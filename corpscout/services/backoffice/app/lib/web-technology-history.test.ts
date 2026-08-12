import { describe, expect, it } from "vitest";
import { buildWebTechnologyHistory } from "~/lib/web-technology-history";

describe("buildWebTechnologyHistory", () => {
  it("compares technology detections in crawl order", () => {
    const history = buildWebTechnologyHistory(
      "example.se",
      [
        {
          crawlId: "CC-MAIN-2026-17",
          observedPages: 20,
          processedAt: "2026-07-27 20:00:00.000",
        },
        {
          crawlId: "CC-MAIN-2026-25",
          observedPages: 15,
          processedAt: "2026-07-20 12:00:00.000",
        },
        {
          crawlId: "CC-MAIN-2026-30",
          observedPages: 10,
          processedAt: "2026-08-06 10:00:00.000",
        },
      ],
      [
        {
          crawlId: "CC-MAIN-2026-17",
          name: "C3.js",
          categories: ["JavaScript libraries"],
          versions: [],
          confidence: 100,
          detectedPages: 20,
          sampleUrls: ["https://example.se/"],
        },
        {
          crawlId: "CC-MAIN-2026-17",
          name: "React",
          categories: ["JavaScript frameworks"],
          versions: ["19"],
          confidence: 100,
          detectedPages: 20,
          sampleUrls: ["https://example.se/"],
        },
        {
          crawlId: "CC-MAIN-2026-25",
          name: "React",
          categories: ["JavaScript frameworks"],
          versions: ["19.1"],
          confidence: 100,
          detectedPages: 15,
          sampleUrls: ["https://example.se/"],
        },
        {
          crawlId: "CC-MAIN-2026-25",
          name: "YouTube",
          categories: ["Video players"],
          versions: [],
          confidence: 100,
          detectedPages: 2,
          sampleUrls: ["https://example.se/video"],
        },
        {
          crawlId: "CC-MAIN-2026-30",
          name: "React",
          categories: ["JavaScript frameworks"],
          versions: ["19.1"],
          confidence: 100,
          detectedPages: 10,
          sampleUrls: ["https://example.se/"],
        },
      ],
    );

    expect(history.latestCrawlId).toBe("CC-MAIN-2026-30");
    expect(history.snapshots.map((snapshot) => snapshot.crawlId)).toEqual([
      "CC-MAIN-2026-30",
      "CC-MAIN-2026-25",
      "CC-MAIN-2026-17",
    ]);
    expect(history.snapshots[0].newlyDetected).toEqual([]);
    expect(history.snapshots[0].noLongerDetected).toEqual(["YouTube"]);
    expect(history.snapshots[1].newlyDetected).toEqual(["YouTube"]);
    expect(history.snapshots[1].noLongerDetected).toEqual(["C3.js"]);
    expect(history.snapshots[2].newlyDetected).toEqual([]);
    expect(history.snapshots[2].noLongerDetected).toEqual([]);

    expect(history.technologies).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          name: "React",
          state: "detected_latest",
          firstDetectedCrawl: "CC-MAIN-2026-17",
          lastDetectedCrawl: "CC-MAIN-2026-30",
          detectedCrawlCount: 3,
          versions: ["19", "19.1"],
        }),
        expect.objectContaining({
          name: "C3.js",
          state: "not_detected_latest",
          lastDetectedCrawl: "CC-MAIN-2026-17",
        }),
        expect.objectContaining({
          name: "YouTube",
          state: "not_detected_latest",
          lastDetectedCrawl: "CC-MAIN-2026-25",
        }),
      ]),
    );
  });
});
