import {
  assetMaterializations,
  listRuns,
  type AssetMaterialization,
  type DagsterOptions,
  type DagsterRun,
} from "~/lib/dagster.server";

export const ESEF_ENRICHMENT_ASSET =
  "esef_document_company_information_clickhouse";

export const ESEF_INPUT_ASSETS = [
  "esef_filings_clickhouse",
  "esef_document_concept_labels_clickhouse",
  "esef_disclosures_clickhouse",
] as const;

const ESEF_ENRICHMENT_JOB = "esef_document_company_information_job";
const ESEF_INPUT_JOBS = [
  "esef_filings_refresh_job",
  "esef_filings_backfill_job",
] as const;

const UNFINISHED_RUN_STATUSES = [
  "NOT_STARTED",
  "QUEUED",
  "STARTING",
  "STARTED",
  "CANCELING",
] as const;

export type EsefSyncState =
  | "synced"
  | "out_of_sync"
  | "never_materialized"
  | "inputs_updating"
  | "materializing";

export interface EsefAssetStatus {
  asset: string;
  role: "input" | "output";
  materialization: AssetMaterialization | null;
  newerThanOutput: boolean;
}

export interface EsefOperationsStatus {
  syncState: EsefSyncState;
  canLaunch: boolean;
  blockingReasons: string[];
  latestEnrichmentRun: DagsterRun | null;
  recentEnrichmentRuns: DagsterRun[];
  unfinishedInputRuns: DagsterRun[];
  assets: EsefAssetStatus[];
}

function latest(
  materializations: AssetMaterialization[],
): AssetMaterialization | null {
  return materializations[0] ?? null;
}

function unfinished(run: DagsterRun): boolean {
  return (UNFINISHED_RUN_STATUSES as readonly string[]).includes(run.status);
}

/** Read the live state that both the ESEF page and its launch guard use. */
export async function loadEsefOperationsStatus(
  options: DagsterOptions = {},
): Promise<EsefOperationsStatus> {
  const [recentEnrichmentRuns, inputRunsByJob, materializationLists] =
    await Promise.all([
      listRuns({ job: ESEF_ENRICHMENT_JOB, limit: 8 }, options),
      Promise.all(
        ESEF_INPUT_JOBS.map((job) =>
          listRuns(
            { job, limit: 20, statuses: UNFINISHED_RUN_STATUSES },
            options,
          ),
        ),
      ),
      Promise.all(
        [...ESEF_INPUT_ASSETS, ESEF_ENRICHMENT_ASSET].map((asset) =>
          assetMaterializations({ asset, limit: 1 }, options),
        ),
      ),
    ]);
  const unfinishedInputRuns = inputRunsByJob.flat().filter(unfinished);
  const inputMaterializations = materializationLists
    .slice(0, ESEF_INPUT_ASSETS.length)
    .map(latest);
  const outputMaterialization = latest(
    materializationLists[ESEF_INPUT_ASSETS.length] ?? [],
  );
  const activeEnrichmentRun = recentEnrichmentRuns.find(unfinished) ?? null;
  const outputTimestamp = outputMaterialization?.timestamp ?? null;

  const assets: EsefAssetStatus[] = [
    ...ESEF_INPUT_ASSETS.map((asset, index) => ({
      asset,
      role: "input" as const,
      materialization: inputMaterializations[index] ?? null,
      newerThanOutput:
        outputTimestamp === null ||
        (inputMaterializations[index]?.timestamp ?? 0) > outputTimestamp,
    })),
    {
      asset: ESEF_ENRICHMENT_ASSET,
      role: "output" as const,
      materialization: outputMaterialization,
      newerThanOutput: false,
    },
  ];

  const blockingReasons: string[] = [];
  if (activeEnrichmentRun) {
    blockingReasons.push(
      `ESEF company-information run ${activeEnrichmentRun.runId} is ${activeEnrichmentRun.status}.`,
    );
  }
  for (const run of unfinishedInputRuns) {
    blockingReasons.push(
      `Required input job ${run.jobName} has unfinished run ${run.runId} (${run.status}).`,
    );
  }
  for (const asset of assets.filter(
    (item) => item.role === "input" && item.materialization === null,
  )) {
    blockingReasons.push(
      `Required input asset ${asset.asset} has never materialized.`,
    );
  }

  let syncState: EsefSyncState;
  if (unfinishedInputRuns.length > 0) {
    syncState = "inputs_updating";
  } else if (activeEnrichmentRun) {
    syncState = "materializing";
  } else if (outputMaterialization === null) {
    syncState = "never_materialized";
  } else if (
    assets.some((asset) => asset.role === "input" && asset.newerThanOutput)
  ) {
    syncState = "out_of_sync";
  } else {
    syncState = "synced";
  }

  return {
    syncState,
    canLaunch: blockingReasons.length === 0,
    blockingReasons,
    latestEnrichmentRun: recentEnrichmentRuns[0] ?? null,
    recentEnrichmentRuns,
    unfinishedInputRuns,
    assets,
  };
}

export async function assertEsefLaunchAllowed(
  options: DagsterOptions = {},
): Promise<void> {
  const status = await loadEsefOperationsStatus(options);
  if (!status.canLaunch) {
    throw new Error(
      `ESEF company-information launch is blocked: ${status.blockingReasons.join(" ")}`,
    );
  }
}
