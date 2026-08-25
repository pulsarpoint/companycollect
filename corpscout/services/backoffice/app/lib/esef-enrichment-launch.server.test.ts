import { describe, expect, it, vi } from "vitest";

import {
  ESEF_DOCUMENT_COMPANY_INFORMATION_JOB,
  launchEsefDocumentCompanyInformation,
  type LaunchEsefDocumentCompanyInformationInput,
} from "~/lib/esef-enrichment-launch.server";

const DAGSTER_URL = "http://dagster:3000/graphql";

const INPUT: LaunchEsefDocumentCompanyInformationInput = {
  requestedBy: " operator@example.com ",
  countryIso2: " se ",
  companyIds: [" 5566692850 ", "5566692850", "5560125220"],
  sourceDocumentIds: [" filing-1 ", "", "filing-2"],
  maxDocuments: 250,
  refreshBehavior: "reuse_existing",
  maxEvidenceChars: 64_000,
  timeoutSeconds: 180,
  llm: {
    provider: " deepseek ",
    model: " deepseek-v4-flash ",
    baseUrl: " https://api.deepseek.com ",
    temperature: 0,
    maxTokens: 8_000,
    promptVersion: " esef-company-enrichment-v2 ",
    concurrency: 4,
  },
};

function successfulDagsterFetch() {
  const calls: Array<{ query: string; variables: Record<string, unknown> }> = [];
  const fetchImpl = vi.fn(async (_input: RequestInfo | URL, init?: RequestInit) => {
    const body = JSON.parse(String(init?.body)) as {
      query: string;
      variables: Record<string, unknown>;
    };
    calls.push(body);
    const data = body.query.includes("query BackofficeRuns")
      ? { runsOrError: { __typename: "Runs", results: [] } }
      : body.query.includes("query BackofficeAssetMaterializations")
        ? {
            assetNodes: [
              {
                id: "asset",
                assetMaterializations: [
                  {
                    runId: "input-run",
                    timestamp: "1000",
                    metadataEntries: [],
                  },
                ],
              },
            ],
          }
        : {
            launchRun: {
              __typename: "LaunchRunSuccess",
              run: { runId: "run-1", status: "QUEUED" },
            },
          };
    return new Response(
      JSON.stringify({ data }),
      { status: 200, headers: { "content-type": "application/json" } },
    );
  });
  return { calls, fetchImpl: fetchImpl as unknown as typeof fetch };
}

function launchCall(
  calls: Array<{ query: string; variables: Record<string, unknown> }>,
) {
  const call = calls.find(({ query }) =>
    query.includes("mutation BackofficeLaunchRun"),
  );
  if (!call) throw new Error("No Dagster launch mutation was sent");
  return call;
}

function executionParams(call: { variables: Record<string, unknown> }) {
  return call.variables.executionParams as {
    selector: Record<string, unknown>;
    runConfigData: Record<string, unknown>;
    executionMetadata: { tags: Array<{ key: string; value: string }> };
  };
}

describe("launchEsefDocumentCompanyInformation", () => {
  it("fixes the job, constructs the complete config and adds server-owned tags", async () => {
    const { calls, fetchImpl } = successfulDagsterFetch();
    const hostileBrowserFields = {
      ...INPUT,
      job: "some_other_job",
      assetSelection: ["some_other_asset"],
      requestId: "browser-request-id",
      tags: { "corpscout/trigger_source": "browser" },
      llm: { ...INPUT.llm, apiKey: "browser-secret" },
    } as LaunchEsefDocumentCompanyInformationInput;

    const launched = await launchEsefDocumentCompanyInformation(
      hostileBrowserFields,
      { fetchImpl, url: DAGSTER_URL },
    );

    const params = executionParams(launchCall(calls));
    expect(params.selector).toEqual({
      repositoryLocationName: "dagster_v3",
      repositoryName: "__repository__",
      jobName: ESEF_DOCUMENT_COMPANY_INFORMATION_JOB,
    });
    expect(params.selector).not.toHaveProperty("assetSelection");
    expect(params.runConfigData).toEqual({
      ops: {
        esef_document_company_information_clickhouse: {
          config: {
            provider: "deepseek",
            model: "deepseek-v4-flash",
            base_url: "https://api.deepseek.com",
            temperature: 0,
            max_tokens: 8_000,
            prompt_version: "esef-company-enrichment-v2",
            concurrency: 4,
            country_iso2: "SE",
            company_ids: ["5566692850", "5560125220"],
            source_document_ids: ["filing-1", "filing-2"],
            max_documents: 250,
            refresh_existing: false,
            reprocess_existing_without_model: false,
            max_evidence_chars: 64_000,
            timeout_seconds: 180,
          },
        },
      },
    });
    expect(JSON.stringify(params.runConfigData)).not.toContain("browser-secret");

    const tags = Object.fromEntries(
      params.executionMetadata.tags.map(({ key, value }) => [key, value]),
    );
    expect(tags).toEqual({
      "corpscout/trigger_source": "backoffice",
      "corpscout/request_id": launched.requestId,
      "corpscout/requested_by": "operator@example.com",
    });
    expect(launched).toMatchObject({ runId: "run-1", status: "QUEUED" });
    expect(launched.requestId).toMatch(
      /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/,
    );
  });

  it.each([
    ["reuse_existing", false, false],
    ["refresh_existing", true, false],
    ["reprocess_existing_without_model", false, true],
  ] as const)(
    "maps %s to one valid pair of Dagster refresh flags",
    async (refreshBehavior, refreshExisting, reprocessExistingWithoutModel) => {
      const { calls, fetchImpl } = successfulDagsterFetch();

      await launchEsefDocumentCompanyInformation(
        { ...INPUT, refreshBehavior },
        { fetchImpl, url: DAGSTER_URL },
      );

      const config = (
        executionParams(launchCall(calls)).runConfigData as {
          ops: {
            esef_document_company_information_clickhouse: {
              config: Record<string, unknown>;
            };
          };
        }
      ).ops.esef_document_company_information_clickhouse.config;
      expect(config.refresh_existing).toBe(refreshExisting);
      expect(config.reprocess_existing_without_model).toBe(
        reprocessExistingWithoutModel,
      );
    },
  );

  it("refuses a launch without an authenticated operator", async () => {
    const fetchImpl = vi.fn();

    await expect(
      launchEsefDocumentCompanyInformation(
        { ...INPUT, requestedBy: "   " },
        { fetchImpl: fetchImpl as unknown as typeof fetch, url: DAGSTER_URL },
      ),
    ).rejects.toThrow("authenticated operator");
    expect(fetchImpl).not.toHaveBeenCalled();
  });
});
