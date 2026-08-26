import { renderToStaticMarkup } from "react-dom/server";
import { createMemoryRouter, RouterProvider } from "react-router";
import { describe, expect, it } from "vitest";
import { EsefOperationsWorkspace } from "~/components/admin/esef-operations-workspace";
import type { DagsterRun } from "~/lib/dagster.server";
import type {
  EsefInventoryAsset,
  EsefOverview,
} from "~/lib/esef-operations.server";

const SUCCESSFUL_RUN: DagsterRun = {
  runId: "run-output-1234",
  status: "SUCCESS",
  jobName: "esef_document_company_information_job",
  startTime: 1_786_900_000,
  endTime: 1_786_900_100,
  runConfig: {
    ops: {
      esef_document_company_information_clickhouse: {
        config: { model: "deepseek-v4-flash", max_documents: 50 },
      },
    },
  },
  selectedAssets: null,
  tags: {
    "corpscout/requested_by": "operator@example.com",
    "corpscout/request_id": "request-1",
    "corpscout/llm_model": "deepseek-v4-flash",
  },
};

function asset(
  name: string,
  staleStatus: "FRESH" | "STALE" | "MISSING",
  timestamp: number | null,
  activeRuns: DagsterRun[] = [],
): EsefInventoryAsset {
  return {
    asset: name,
    description: `${name} operational data`,
    groupName: "esef",
    kinds: [name.endsWith("_clickhouse") ? "clickhouse" : "duckdb"],
    dependencies: [],
    jobNames: ["__ASSET_JOB"],
    staleStatus,
    partitioned: name.endsWith("_duckdb"),
    materialization:
      timestamp === null
        ? null
        : { runId: `${name}-run`, timestamp, numbers: {} },
    activeRuns,
  };
}

const OVERVIEW: EsefOverview = {
  inventory: {
    assets: [
      asset("esef_facts_clickhouse", "FRESH", 1_786_900_300_000),
      asset("esef_disclosures_duckdb", "STALE", 1_786_900_200_000),
      asset("esef_document_people_clickhouse", "MISSING", null),
      asset(
        "esef_document_company_information_clickhouse",
        "FRESH",
        1_786_900_000_000,
      ),
    ],
    activeRuns: [],
  },
  enrichment: {
    syncState: "out_of_sync",
    canLaunch: true,
    blockingReasons: [],
    latestEnrichmentRun: SUCCESSFUL_RUN,
    recentEnrichmentRuns: [SUCCESSFUL_RUN],
    unfinishedInputRuns: [],
    latestBatch: {
      runId: SUCCESSFUL_RUN.runId,
      timestamp: 1_786_900_000_000,
      numbers: {
        attempted_document_count: 50,
        processed_document_count: 48,
        enriched_document_count: 40,
        reused_enrichment_count: 8,
        failed_document_count: 2,
        rate_limited_document_count: 1,
      },
    },
    assets: [],
  },
};

function render(overview: EsefOverview | null, error = ""): string {
  const router = createMemoryRouter(
    [
      {
        path: "*",
        element: (
          <EsefOperationsWorkspace
            overview={overview}
            error={error}
            profiles={[
              {
                profileId: "deepseek",
                name: "DeepSeek production",
                provider: "deepseek",
                model: "deepseek-v4-flash",
                baseUrl: "https://api.deepseek.com",
                isActive: true,
              },
            ]}
            countries={["FI", "SE"]}
            countryError=""
            runtimeDefaults={{
              temperature: 0,
              concurrency: 1,
              maxDocuments: 50,
              maxEvidenceChars: 64_000,
              timeoutSeconds: 180,
            }}
            runUrls={{
              "run-output-1234": "https://dagster/runs/run-output-1234",
            }}
          />
        ),
      },
    ],
    { initialEntries: ["/admin/esef"] },
  );
  return renderToStaticMarkup(<RouterProvider router={router} />);
}

describe("EsefOperationsWorkspace", () => {
  it("makes the complete ESEF asset inventory the main page", () => {
    const html = render(OVERVIEW);

    expect(html).toContain("ESEF filings");
    expect(html).toContain("Assets");
    expect(html).toContain("Needs attention");
    expect(html).toContain("esef_facts_clickhouse");
    expect(html).toContain("esef_disclosures_duckdb");
    expect(html).toContain("esef_document_people_clickhouse");
    expect(html).toContain("Fresh");
    expect(html).toContain("Stale");
    expect(html).toContain("Missing");
  });

  it("presents enrichment as the page action instead of a second page", () => {
    const html = render(OVERVIEW);

    expect(html).toContain("Enhance company information");
    expect(html).not.toContain("Update available");
  });

  it("shows the latest Dagster batch document outcomes", () => {
    const html = render(OVERVIEW);

    expect(html).toContain("Latest completed enrichment batch");
    expect(html).toContain("Attempted");
    expect(html).toContain("Processed");
    expect(html).toContain("Rate limited");
    expect(html).toContain("Some documents need another attempt");
  });

  it("keeps the route usable when Dagster is unavailable", () => {
    const html = render(null, "Dagster did not answer.");

    expect(html).toContain("Dagster status is unavailable");
    expect(html).toContain("Dagster did not answer.");
    expect(html).toContain("Refresh");
  });
});
