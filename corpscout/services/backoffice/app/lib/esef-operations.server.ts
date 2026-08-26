import {
  assetGroup,
  assetMaterializations,
  listRuns,
  type AssetMaterialization,
  type DagsterOptions,
  type DagsterAsset,
  type DagsterRun,
} from "~/lib/dagster.server";

export const ESEF_GROUP_NAME = "esef";

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
  | "partially_processed"
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
  latestBatch: AssetMaterialization | null;
  assets: EsefAssetStatus[];
}

export interface EsefInventoryAsset extends DagsterAsset {
  activeRuns: DagsterRun[];
}

export interface EsefAssetInventory {
  assets: EsefInventoryAsset[];
  activeRuns: DagsterRun[];
}

export interface EsefOverview {
  inventory: EsefAssetInventory;
  enrichment: EsefOperationsStatus;
}

export class EsefLaunchBlockedError extends Error {
  readonly reasons: string[];

  constructor(reasons: string[]) {
    super(`ESEF company-information launch is blocked: ${reasons.join(" ")}`);
    this.name = "EsefLaunchBlockedError";
    this.reasons = reasons;
  }
}

function unfinished(run: DagsterRun): boolean {
  return (UNFINISHED_RUN_STATUSES as readonly string[]).includes(run.status);
}

function uniqueRuns(runs: DagsterRun[]): DagsterRun[] {
  return [...new Map(runs.map((run) => [run.runId, run])).values()];
}

function runTargetsAsset(run: DagsterRun, asset: DagsterAsset): boolean {
  if (run.selectedAssets !== null) {
    return run.selectedAssets.includes(asset.asset);
  }
  // A full launch of Dagster's repository-wide implicit asset job is too broad
  // to attribute safely. UI materializations of an asset subset carry an
  // explicit selection; named ESEF jobs can be mapped from the asset metadata.
  return run.jobName !== "__ASSET_JOB" && asset.jobNames.includes(run.jobName);
}

/** All assets in Dagster's ESEF group plus any live run that currently targets them. */
export async function loadEsefAssetInventory(
  options: DagsterOptions = {},
): Promise<EsefAssetInventory> {
  const assets = await assetGroup(ESEF_GROUP_NAME, options);
  const jobs = new Set([
    "__ASSET_JOB",
    ...ESEF_INPUT_JOBS,
    ESEF_ENRICHMENT_JOB,
    ...assets.flatMap((asset) => asset.jobNames),
  ]);
  const runsByJob = await Promise.all(
    [...jobs].map((job) =>
      listRuns({ job, limit: 20, statuses: UNFINISHED_RUN_STATUSES }, options),
    ),
  );
  const unfinishedRuns = uniqueRuns(runsByJob.flat().filter(unfinished));
  const activeRuns = unfinishedRuns.filter((run) =>
    assets.some((asset) => runTargetsAsset(run, asset)),
  );
  return {
    assets: assets.map((asset) => ({
      ...asset,
      activeRuns: activeRuns.filter((run) => runTargetsAsset(run, asset)),
    })),
    activeRuns,
  };
}

function enrichmentStatus(
  inventory: EsefAssetInventory,
  recentEnrichmentRuns: DagsterRun[],
  latestBatch: AssetMaterialization | null,
): EsefOperationsStatus {
  const inventoryByAsset = new Map(
    inventory.assets.map((asset) => [asset.asset, asset]),
  );
  const inputInventory = ESEF_INPUT_ASSETS.map((asset) =>
    inventoryByAsset.get(asset),
  );
  const outputInventory = inventoryByAsset.get(ESEF_ENRICHMENT_ASSET);
  const unfinishedInputRuns = uniqueRuns(
    inputInventory.flatMap((asset) => asset?.activeRuns ?? []),
  );
  const activeEnrichmentRun = outputInventory?.activeRuns[0] ?? null;
  const outputMaterialization = outputInventory?.materialization ?? null;
  const outputTimestamp = outputMaterialization?.timestamp ?? null;

  const assets: EsefAssetStatus[] = [
    ...ESEF_INPUT_ASSETS.map((asset, index) => ({
      asset,
      role: "input" as const,
      materialization: inputInventory[index]?.materialization ?? null,
      newerThanOutput:
        outputTimestamp === null ||
        (inputInventory[index]?.materialization?.timestamp ?? 0) >
          outputTimestamp,
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
  const inputRunsByJob = new Map<string, DagsterRun[]>();
  for (const run of unfinishedInputRuns) {
    inputRunsByJob.set(run.jobName, [
      ...(inputRunsByJob.get(run.jobName) ?? []),
      run,
    ]);
  }
  for (const [jobName, runs] of inputRunsByJob) {
    if (runs.length === 1) {
      blockingReasons.push(
        `Required input job ${jobName} has unfinished run ${runs[0].runId} (${runs[0].status}).`,
      );
      continue;
    }
    const statusCounts = new Map<string, number>();
    for (const run of runs) {
      statusCounts.set(run.status, (statusCounts.get(run.status) ?? 0) + 1);
    }
    const statuses = [...statusCounts]
      .map(([status, count]) => `${count} ${status}`)
      .join(", ");
    blockingReasons.push(
      `Required input job ${jobName} has ${runs.length} unfinished runs (${statuses}); first run ${runs[0].runId}.`,
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
  } else if ((latestBatch?.numbers.failed_document_count ?? 0) > 0) {
    syncState = "partially_processed";
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
    latestBatch,
    assets,
  };
}

/** One shared read for the ESEF page and the server-side launch guard. */
export async function loadEsefOverview(
  options: DagsterOptions = {},
): Promise<EsefOverview> {
  const [inventory, recentEnrichmentRuns, enrichmentMaterializations] =
    await Promise.all([
      loadEsefAssetInventory(options),
      listRuns({ job: ESEF_ENRICHMENT_JOB, limit: 8 }, options),
      assetMaterializations(
        { asset: ESEF_ENRICHMENT_ASSET, limit: 1 },
        options,
      ),
    ]);
  return {
    inventory,
    enrichment: enrichmentStatus(
      inventory,
      recentEnrichmentRuns,
      enrichmentMaterializations[0] ?? null,
    ),
  };
}

/** Read the live state that both the ESEF page and its launch guard use. */
export async function loadEsefOperationsStatus(
  options: DagsterOptions = {},
): Promise<EsefOperationsStatus> {
  return (await loadEsefOverview(options)).enrichment;
}

export async function assertEsefLaunchAllowed(
  options: DagsterOptions = {},
): Promise<void> {
  const status = await loadEsefOperationsStatus(options);
  if (!status.canLaunch) {
    throw new EsefLaunchBlockedError(status.blockingReasons);
  }
}
