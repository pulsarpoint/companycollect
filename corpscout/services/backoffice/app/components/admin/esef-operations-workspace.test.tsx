import { renderToStaticMarkup } from "react-dom/server";
import { createMemoryRouter, RouterProvider } from "react-router";
import { describe, expect, it } from "vitest";
import { EsefOperationsWorkspace } from "~/components/admin/esef-operations-workspace";
import type { EsefOperationsStatus } from "~/lib/esef-operations.server";

const OUT_OF_SYNC_STATUS: EsefOperationsStatus = {
  syncState: "out_of_sync",
  canLaunch: true,
  blockingReasons: [],
  latestEnrichmentRun: {
    runId: "run-output-1234",
    status: "SUCCESS",
    jobName: "esef_document_company_information_job",
    startTime: 1_786_900_000,
    endTime: 1_786_900_100,
    tags: {
      "corpscout/requested_by": "operator@example.com",
      "corpscout/request_id": "request-1",
    },
  },
  recentEnrichmentRuns: [
    {
      runId: "run-output-1234",
      status: "SUCCESS",
      jobName: "esef_document_company_information_job",
      startTime: 1_786_900_000,
      endTime: 1_786_900_100,
      tags: { "corpscout/requested_by": "operator@example.com" },
    },
  ],
  unfinishedInputRuns: [],
  assets: [
    {
      asset: "esef_filings_clickhouse",
      role: "input",
      materialization: {
        runId: "run-input-1",
        timestamp: 1_786_900_200_000,
        numbers: {},
      },
      newerThanOutput: true,
    },
    {
      asset: "esef_document_concept_labels_clickhouse",
      role: "input",
      materialization: {
        runId: "run-input-2",
        timestamp: 1_786_899_000_000,
        numbers: {},
      },
      newerThanOutput: false,
    },
    {
      asset: "esef_disclosures_clickhouse",
      role: "input",
      materialization: {
        runId: "run-input-3",
        timestamp: 1_786_899_000_000,
        numbers: {},
      },
      newerThanOutput: false,
    },
    {
      asset: "esef_document_company_information_clickhouse",
      role: "output",
      materialization: {
        runId: "run-output-1234",
        timestamp: 1_786_900_000_000,
        numbers: {},
      },
      newerThanOutput: false,
    },
  ],
};

function render(status: EsefOperationsStatus | null, error = ""): string {
  const router = createMemoryRouter(
    [
      {
        path: "*",
        element: (
          <EsefOperationsWorkspace
            status={status}
            error={error}
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
  it("distinguishes an out-of-sync asset from a blocked launch", () => {
    const html = render(OUT_OF_SYNC_STATUS);

    expect(html).toContain("Update available");
    expect(html).toContain("Launch guard open");
    expect(html).toContain("Newer than output");
    expect(html).toContain("operator@example.com");
    expect(html).toContain("https://dagster/runs/run-output-1234");
  });

  it("shows every reason that a launch is blocked", () => {
    const reason =
      "Required input job esef_filings_backfill_job has unfinished run run-1 (STARTED).";
    const html = render({
      ...OUT_OF_SYNC_STATUS,
      syncState: "inputs_updating",
      canLaunch: false,
      blockingReasons: [reason],
    });

    expect(html).toContain("Launch blocked");
    expect(html).toContain("Waiting for Dagster");
    expect(html).toContain(reason);
  });

  it("keeps the route usable when Dagster is unavailable", () => {
    const html = render(null, "Dagster did not answer.");

    expect(html).toContain("Dagster status is unavailable");
    expect(html).toContain("Dagster did not answer.");
    expect(html).toContain("Refresh status");
  });
});
