import {
  assetGroup,
  assetMaterializations,
  dagsterAssetUrl,
  dagsterRunUrl,
  launchRun,
  listRuns,
  scheduleDetails,
  startSchedule,
  stopSchedule,
  type DagsterOptions,
  type ScheduleDetails,
} from "~/lib/dagster.server";

/** Dagster objects and the service behind the Provider feeds section. */
export const PROVIDER_RECON_GROUP = "provider_recon";
export const PROVIDER_RECON_JOB = "provider_recon_job";
export const PROVIDER_RECON_SCHEDULE = "provider_recon_daily";

export interface ProviderReconDagster {
  assets: {
    asset: string;
    url: string | null;
    /** Seconds since the epoch. */
    materializedAt: number | null;
    runId: string | null;
    runUrl: string | null;
    numbers: Record<string, number>;
  }[];
  schedule: ScheduleDetails | null;
  runs: {
    runId: string;
    status: string;
    startTime: number | null;
    endTime: number | null;
    url: string | null;
  }[];
  error: string | null;
}

/** Everything the panel shows; a Dagster failure becomes `error`, never a throw. */
export async function loadProviderReconDagster(
  options: DagsterOptions = {},
): Promise<ProviderReconDagster> {
  try {
    const [assets, schedule, runs] = await Promise.all([
      assetGroup(PROVIDER_RECON_GROUP, options),
      scheduleDetails(PROVIDER_RECON_SCHEDULE, options),
      listRuns({ job: PROVIDER_RECON_JOB, limit: 5 }, options),
    ]);
    // assetGroup carries no metadata; the counts come from the latest materialization.
    const latest = await Promise.all(
      assets.map((a) =>
        assetMaterializations({ asset: a.asset, limit: 1 }, options).then((m) => m[0] ?? null),
      ),
    );
    return {
      assets: assets.map((a, i) => {
        const m = latest[i] ?? a.materialization;
        return {
          asset: a.asset,
          url: dagsterAssetUrl(a.asset, options.url),
          // Dagster reports materialization timestamps in milliseconds.
          materializedAt: m?.timestamp ? m.timestamp / 1000 : null,
          runId: m?.runId ?? null,
          runUrl: m?.runId ? dagsterRunUrl(m.runId, options.url) : null,
          numbers: m?.numbers ?? {},
        };
      }),
      schedule,
      runs: runs.map((r) => ({
        runId: r.runId,
        status: r.status,
        startTime: r.startTime,
        endTime: r.endTime,
        url: dagsterRunUrl(r.runId, options.url),
      })),
      error: null,
    };
  } catch (error) {
    return {
      assets: [],
      schedule: null,
      runs: [],
      error: error instanceof Error ? error.message : String(error),
    };
  }
}

/** Launch the whole provider_recon_job; returns the run id. */
export async function runProviderReconNow(
  options: DagsterOptions = {},
): Promise<string> {
  const { runId } = await launchRun(
    {
      job: PROVIDER_RECON_JOB,
      runConfig: {},
      tags: { "backoffice/trigger": "provider-feeds" },
    },
    options,
  );
  return runId;
}

export async function setProviderReconSchedule(
  on: boolean,
  options: DagsterOptions = {},
): Promise<string> {
  return on
    ? startSchedule(PROVIDER_RECON_SCHEDULE, options)
    : stopSchedule(PROVIDER_RECON_SCHEDULE, options);
}

/** A provider-recon service answer, phrased for the operator. */
export class ProviderReconServiceError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "ProviderReconServiceError";
  }
}

export function providerReconApiUrl(): string {
  return (
    process.env.PROVIDER_RECON_API_URL ||
    "http://companycollect.taileb086.ts.net:8095"
  ).replace(/\/+$/, "");
}

/** POST /v1/restore on the service; maps its answers to operator-facing messages. */
export async function restoreProviderRanges(
  input: { provider: string; collector: string; removedSince: string },
  fetchImpl: typeof fetch = fetch,
): Promise<{ runId: string; restored: number }> {
  let response: Response;
  try {
    response = await fetchImpl(`${providerReconApiUrl()}/v1/restore`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        provider: input.provider,
        collector: input.collector,
        removed_since: input.removedSince,
      }),
      signal: AbortSignal.timeout(120_000),
    });
  } catch (error) {
    throw new ProviderReconServiceError(
      `provider-recon service unreachable: ${error instanceof Error ? error.message : String(error)}`,
    );
  }
  const body = (await response.json().catch(() => ({}))) as {
    run_id?: string;
    restored?: number;
    error?: string;
  };
  if (response.status === 200) {
    return { runId: body.run_id ?? "", restored: body.restored ?? 0 };
  }
  if (response.status === 409) {
    throw new ProviderReconServiceError(
      `provider-recon is busy with run ${body.run_id ?? "?"}; try again when it finishes.`,
    );
  }
  throw new ProviderReconServiceError(
    body.error ?? `provider-recon answered HTTP ${response.status}`,
  );
}
