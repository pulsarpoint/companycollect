/**
 * GeoLite2 upload: the owner downloads MaxMind's archives by hand, this page
 * stores them unchanged in the corpscout object store and launches
 * `geolite2_install_job`, which validates and installs them on the Dagster host.
 *
 * Deliberately thin: only the file name and size are checked here. The Dagster
 * asset `geolite2_databases` is the authority -- it opens the MMDB, checks the
 * database type and build and refuses older builds -- and its latest
 * materialization metadata is what this page shows as "installed".
 */
import { randomUUID } from "node:crypto";

import {
  dagsterRunUrl,
  launchRun,
  latestAssetMetadata,
  listRuns,
  type DagsterOptions,
} from "~/lib/dagster.server";
import { ensureBucket, putObject, type ObjectStoreOptions } from "~/lib/object-store.server";

export const GEOLITE2_BUCKET = "geolite2";
export const GEOLITE2_ASSET = "geolite2_databases";
export const GEOLITE2_INSTALL_JOB = "geolite2_install_job";
export const GEOLITE2_MAX_BYTES = 200 * 1024 * 1024;
/** Matches `freshness.py` MAX_AGE on the Dagster side. */
export const GEOLITE2_MAX_AGE_DAYS = 14;
const DAY_MS = 86_400_000;
const NAME = /^GeoLite2-(City|ASN)(_\d{8})?\.(tar\.gz|mmdb)$/;

export type GeoLite2Edition = "City" | "ASN";

export class GeoLite2UploadError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "GeoLite2UploadError";
  }
}

/** The edition named by an accepted file name; throws for anything else. */
export function geolite2Edition(name: string, size: number): GeoLite2Edition {
  const match = NAME.exec(name);
  if (!match) {
    throw new GeoLite2UploadError(
      `${name} is not a GeoLite2 file: expected GeoLite2-City or GeoLite2-ASN as .tar.gz (optionally _YYYYMMDD) or .mmdb.`,
    );
  }
  if (size <= 0) throw new GeoLite2UploadError(`${name} is empty.`);
  if (size > GEOLITE2_MAX_BYTES) throw new GeoLite2UploadError(`${name} is larger than 200 MB.`);
  return match[1] as GeoLite2Edition;
}

export interface GeoLite2UploadResult {
  runId: string;
  uploads: { edition: GeoLite2Edition; key: string }[];
}

export interface GeoLite2Dependencies {
  objectStore?: ObjectStoreOptions;
  dagster?: DagsterOptions;
  uuid?: () => string;
}

/** Validate every file first, then store them and launch one install run. */
export async function uploadGeolite2(
  files: readonly File[],
  deps: GeoLite2Dependencies = {},
): Promise<GeoLite2UploadResult> {
  if (files.length === 0) throw new GeoLite2UploadError("Choose a City and/or an ASN file.");
  const editions = files.map((file) => geolite2Edition(file.name, file.size));
  if (new Set(editions).size !== editions.length) {
    throw new GeoLite2UploadError("Upload at most one City and one ASN file.");
  }
  await ensureBucket(GEOLITE2_BUCKET, deps.objectStore);
  const uuid = deps.uuid ?? randomUUID;
  const uploads = [];
  for (const [index, file] of files.entries()) {
    const key = `uploads/${uuid()}/${file.name}`;
    await putObject(GEOLITE2_BUCKET, key, new Uint8Array(await file.arrayBuffer()), deps.objectStore);
    uploads.push({ edition: editions[index], key });
  }
  const run = await launchRun(
    {
      job: GEOLITE2_INSTALL_JOB,
      runConfig: { ops: { [GEOLITE2_ASSET]: { config: { uploads } } } },
      tags: { "backoffice/action": "geolite2_upload" },
    },
    deps.dagster,
  );
  return { runId: run.runId, uploads };
}

export interface InstalledEdition {
  edition: GeoLite2Edition;
  /** ISO timestamp of the MMDB build, or null when the file is missing. */
  build: string | null;
  ageDays: number | null;
  stale: boolean;
  sha256: string | null;
}

export interface GeoLite2Status {
  installed: {
    installedAt: number;
    runId: string;
    editions: InstalledEdition[];
    replaced: string[];
  } | null;
  lastRun: { runId: string; status: string; url: string | null; startTime: number | null } | null;
}

function text(value: unknown): string | null {
  return typeof value === "string" && value !== "missing" ? value : null;
}

/** The installed builds from the asset's latest materialization, and the latest install run. */
export async function loadGeolite2Status(
  options: DagsterOptions = {},
  now: Date = new Date(),
): Promise<GeoLite2Status> {
  const [materialization, runs] = await Promise.all([
    latestAssetMetadata(GEOLITE2_ASSET, options),
    listRuns({ job: GEOLITE2_INSTALL_JOB, limit: 1 }, options),
  ]);
  const installed = materialization
    ? {
        installedAt: materialization.timestamp,
        runId: materialization.runId,
        replaced: Array.isArray(materialization.metadata.installed)
          ? materialization.metadata.installed.map(String)
          : [],
        editions: (["City", "ASN"] as const).map((edition) => {
          const prefix = edition.toLowerCase();
          const build = text(materialization.metadata[`${prefix}_build`]);
          const ageMs = build === null ? null : now.getTime() - Date.parse(build);
          return {
            edition,
            build,
            ageDays: ageMs === null ? null : Math.floor(ageMs / DAY_MS),
            // Same rule as the Dagster check: stale when older than exactly 14 days.
            stale: ageMs === null || ageMs > GEOLITE2_MAX_AGE_DAYS * DAY_MS,
            sha256: text(materialization.metadata[`${prefix}_sha256`]),
          };
        }),
      }
    : null;
  const run = runs[0];
  return {
    installed,
    lastRun: run
      ? { runId: run.runId, status: run.status, url: dagsterRunUrl(run.runId, options.url), startTime: run.startTime }
      : null,
  };
}
