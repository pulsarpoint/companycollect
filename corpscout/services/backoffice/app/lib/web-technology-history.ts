export interface WebTechnologyCrawlCoverage {
  crawlId: string;
  observedPages: number;
  processedAt: string;
}

export interface WebTechnologyCrawlDetection {
  crawlId: string;
  name: string;
  categories: string[];
  versions: string[];
  confidence: number;
  detectedPages: number;
  sampleUrls: string[];
}

export interface WebTechnologySnapshotDetection {
  name: string;
  categories: string[];
  versions: string[];
  confidence: number;
  detectedPages: number;
  sampleUrls: string[];
}

export interface WebTechnologyCrawlSnapshot
  extends WebTechnologyCrawlCoverage {
  detections: WebTechnologySnapshotDetection[];
  newlyDetected: string[];
  noLongerDetected: string[];
}

export interface WebTechnologySummary {
  name: string;
  categories: string[];
  versions: string[];
  state: "detected_latest" | "not_detected_latest";
  firstDetectedCrawl: string;
  lastDetectedCrawl: string;
  detectedCrawlCount: number;
  lastDetectedPages: number;
  lastDetectedConfidence: number;
}

export interface CompanyWebTechnologyHistory {
  domain: string;
  latestCrawlId: string | null;
  latestProcessedAt: string | null;
  technologies: WebTechnologySummary[];
  /** Newest crawl first for direct timeline rendering. */
  snapshots: WebTechnologyCrawlSnapshot[];
}

function compareCrawlIds(left: string, right: string): number {
  const parse = (value: string) => {
    const match = /^CC-MAIN-(\d{4})-(\d+)$/.exec(value);
    return match ? [Number(match[1]), Number(match[2])] : null;
  };
  const leftParts = parse(left);
  const rightParts = parse(right);
  if (!leftParts || !rightParts) return left.localeCompare(right);
  return leftParts[0] - rightParts[0] || leftParts[1] - rightParts[1];
}

function uniqueSorted(values: Iterable<string>): string[] {
  return Array.from(new Set(values)).filter(Boolean).sort();
}

function mergeCrawlDetections(
  detections: WebTechnologyCrawlDetection[],
): WebTechnologySnapshotDetection[] {
  const merged = new Map<string, WebTechnologySnapshotDetection>();
  for (const detection of detections) {
    const existing = merged.get(detection.name);
    merged.set(detection.name, {
      name: detection.name,
      categories: uniqueSorted([
        ...(existing?.categories ?? []),
        ...detection.categories,
      ]),
      versions: uniqueSorted([
        ...(existing?.versions ?? []),
        ...detection.versions,
      ]),
      confidence: Math.max(existing?.confidence ?? 0, detection.confidence),
      detectedPages: Math.max(
        existing?.detectedPages ?? 0,
        detection.detectedPages,
      ),
      sampleUrls: uniqueSorted([
        ...(existing?.sampleUrls ?? []),
        ...detection.sampleUrls,
      ]).slice(0, 5),
    });
  }
  return Array.from(merged.values()).sort((left, right) =>
    left.name.localeCompare(right.name),
  );
}

export function buildWebTechnologyHistory(
  domain: string,
  coverage: WebTechnologyCrawlCoverage[],
  detections: WebTechnologyCrawlDetection[],
): CompanyWebTechnologyHistory {
  const coverageByCrawl = new Map(
    coverage.map((item) => [item.crawlId, { ...item }]),
  );
  for (const detection of detections) {
    if (!coverageByCrawl.has(detection.crawlId)) {
      coverageByCrawl.set(detection.crawlId, {
        crawlId: detection.crawlId,
        observedPages: detection.detectedPages,
        processedAt: "",
      });
    }
  }

  const detectionsByCrawl = new Map<string, WebTechnologyCrawlDetection[]>();
  for (const detection of detections) {
    const values = detectionsByCrawl.get(detection.crawlId) ?? [];
    values.push(detection);
    detectionsByCrawl.set(detection.crawlId, values);
  }

  const crawlIds = Array.from(coverageByCrawl.keys()).sort(compareCrawlIds);
  let previousNames = new Set<string>();
  const chronologicalSnapshots = crawlIds.map<WebTechnologyCrawlSnapshot>(
    (crawlId, index) => {
      const crawlCoverage = coverageByCrawl.get(crawlId)!;
      const crawlDetections = mergeCrawlDetections(
        detectionsByCrawl.get(crawlId) ?? [],
      );
      const names = new Set(crawlDetections.map((item) => item.name));
      const snapshot = {
        ...crawlCoverage,
        detections: crawlDetections,
        newlyDetected:
          index === 0
            ? []
            : uniqueSorted(
                Array.from(names).filter((name) => !previousNames.has(name)),
              ),
        noLongerDetected:
          index === 0
            ? []
            : uniqueSorted(
                Array.from(previousNames).filter((name) => !names.has(name)),
              ),
      };
      previousNames = names;
      return snapshot;
    },
  );

  const technologyDetections = new Map<
    string,
    Array<{
      crawlId: string;
      detection: WebTechnologySnapshotDetection;
    }>
  >();
  for (const snapshot of chronologicalSnapshots) {
    for (const detection of snapshot.detections) {
      const values = technologyDetections.get(detection.name) ?? [];
      values.push({ crawlId: snapshot.crawlId, detection });
      technologyDetections.set(detection.name, values);
    }
  }

  const latestSnapshot = chronologicalSnapshots.at(-1) ?? null;
  const latestNames = new Set(
    latestSnapshot?.detections.map((detection) => detection.name) ?? [],
  );
  const technologies = Array.from(technologyDetections.entries())
    .map<WebTechnologySummary>(([name, observations]) => {
      const last = observations.at(-1)!;
      return {
        name,
        categories: uniqueSorted(
          observations.flatMap(({ detection }) => detection.categories),
        ),
        versions: uniqueSorted(
          observations.flatMap(({ detection }) => detection.versions),
        ),
        state: latestNames.has(name)
          ? "detected_latest"
          : "not_detected_latest",
        firstDetectedCrawl: observations[0].crawlId,
        lastDetectedCrawl: last.crawlId,
        detectedCrawlCount: observations.length,
        lastDetectedPages: last.detection.detectedPages,
        lastDetectedConfidence: last.detection.confidence,
      };
    })
    .sort((left, right) => {
      if (left.state !== right.state) {
        return left.state === "detected_latest" ? -1 : 1;
      }
      return left.name.localeCompare(right.name);
    });

  return {
    domain,
    latestCrawlId: latestSnapshot?.crawlId ?? null,
    latestProcessedAt: latestSnapshot?.processedAt || null,
    technologies,
    snapshots: chronologicalSnapshots.reverse(),
  };
}
