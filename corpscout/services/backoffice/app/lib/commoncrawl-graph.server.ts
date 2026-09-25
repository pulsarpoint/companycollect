import "dotenv/config";
import { randomUUID } from "node:crypto";
import { Pool } from "pg";
import { chQuery } from "~/lib/clickhouse.server";
import {
  dagsterRunUrl,
  instigatorStates,
  launchRun,
} from "~/lib/dagster.server";

let pool: Pool | undefined;
export function graphCatalog() {
  const connectionString = process.env.COMMONCRAWL_GRAPH_PG_URL;
  if (!connectionString) throw new Error("Graph catalog is not configured.");
  if (pool) return pool;
  pool = new Pool({
    connectionString,
    max: 3,
    connectionTimeoutMillis: 5000,
    idleTimeoutMillis: 30000,
    statement_timeout: 10000,
  });
  pool.on("error", () => console.error("Graph catalog connection unavailable"));
  return pool;
}
export interface GraphCatalogRelease {
  graph_release: string;
  coverage_start: string | null;
  coverage_end: string | null;
  graph_status: "not_imported" | "retained" | "retired";
  source_index_url: string;
  available_files: number;
  ranks_available: boolean;
  cached_files: number;
  compressed_bytes: string;
  status: string | null;
  selection: "full" | "ranks" | null;
  request_id: string | null;
  dagster_run_id: string | null;
  last_error: string | null;
}
export interface GraphCatalogState {
  active_graph_release: string | null;
  automatic_imports_enabled: boolean;
  discovery_succeeded_at: string | null;
  discovery_error: string | null;
}

export async function graphReleases(): Promise<GraphCatalogRelease[]> {
  const result = await graphCatalog()
    .query<GraphCatalogRelease>(`SELECT r.graph_release,r.coverage_start::text,r.coverage_end::text,
    r.graph_status,r.source_index_url,
    count(*) FILTER (WHERE f.availability='available' AND f.source_etag IS NOT NULL AND f.source_bytes>0 AND f.expected_rows>0)::int AS available_files,
    coalesce(bool_or(f.artifact_kind='ranks' AND f.availability='available' AND f.source_etag IS NOT NULL AND f.source_bytes>0 AND f.expected_rows>0),false) AS ranks_available,
    count(*) FILTER (WHERE f.cache_key IS NOT NULL AND f.cached_source_etag=f.source_etag)::int AS cached_files,
    coalesce(sum(f.source_bytes),0)::text AS compressed_bytes,
    q.status,q.selection,q.request_id::text,q.dagster_run_id::text,q.last_error
    FROM commoncrawl_graph_releases r LEFT JOIN commoncrawl_graph_release_files f USING(graph_release)
    LEFT JOIN LATERAL (SELECT * FROM commoncrawl_graph_import_requests WHERE graph_release=r.graph_release ORDER BY created_at DESC LIMIT 1) q ON true
    GROUP BY r.graph_release,q.status,q.selection,q.request_id,q.dagster_run_id,q.last_error
    ORDER BY r.coverage_end DESC NULLS LAST,r.graph_release`);
  return result.rows;
}
export async function graphState(): Promise<GraphCatalogState> {
  const result = await graphCatalog()
    .query<GraphCatalogState>(`SELECT active_graph_release,automatic_imports_enabled,
    discovery_succeeded_at::text,discovery_error FROM commoncrawl_graph_state WHERE singleton`);
  if (!result.rows[0]) throw new Error("Graph catalog has no state.");
  return result.rows[0];
}
// Eight minutes plus the explorer's bounded queries fit inside ten-minute retirement grace.
let lastActiveGraph: { release: string | null; readAt: number } | undefined;
export async function activeGraphRelease(): Promise<string | null> {
  try {
    const state = await graphState();
    lastActiveGraph = {
      release: state.active_graph_release,
      readAt: Date.now(),
    };
    return state.active_graph_release;
  } catch (error) {
    if (lastActiveGraph && Date.now() - lastActiveGraph.readAt < 8 * 60 * 1000)
      return lastActiveGraph.release;
    throw error;
  }
}

export async function loadedRankingReleases() {
  return chQuery<{
    graph_release: string;
    rows: string;
  }>(`SELECT partition AS graph_release,toString(sum(rows)) AS rows
    FROM system.parts WHERE database='corpscout' AND table='commoncrawl_domain_graph_ranks' AND active GROUP BY partition`);
}
export async function graphDashboard() {
  const [releases, state, loaded, automation, retainedGraphs] =
    await Promise.all([
      graphReleases(),
      graphState(),
      loadedRankingReleases(),
      instigatorStates(
        {
          names: [
            "commoncrawl_graph_discovery_schedule",
            "commoncrawl_graph_cleanup_schedule",
            "commoncrawl_graph_import_sensor",
          ],
        },
        { timeoutMs: 5000 },
      ).catch(() => null),
      chQuery<{ graph_release: string }>(
        "SELECT DISTINCT partition AS graph_release FROM system.parts WHERE database='corpscout' AND table IN ('commoncrawl_domain_graph_nodes','commoncrawl_domain_graph_edges') AND active",
      ),
    ]);
  const fullCandidate = releases.find(
    (r) =>
      r.available_files === 3 &&
      r.coverage_end !== null &&
      r.graph_status !== "retired",
  );
  return {
    state,
    automation,
    latestRanking:
      releases.find(
        (r) =>
          r.coverage_end !== null &&
          loaded.some((l) => l.graph_release === r.graph_release),
      )?.graph_release ?? null,
    releases: releases.map((r) => ({
      ...r,
      graphStored: retainedGraphs.some(
        (graph) => graph.graph_release === r.graph_release,
      ),
      rankRows:
        loaded.find((l) => l.graph_release === r.graph_release)?.rows ?? null,
      canImportFull:
        r === fullCandidate && r.graph_release !== state.active_graph_release,
      runUrl: r.dagster_run_id ? dagsterRunUrl(r.dagster_run_id) : null,
      submissionId: randomUUID(),
    })),
  };
}

export class GraphRequestError extends Error {}

export async function queueGraphImport(
  release: string,
  selection: string,
  requestId: string,
  origin = "manual",
) {
  if (
    !/^cc-main-[a-z0-9-]+$/.test(release) ||
    !["ranks", "full"].includes(selection) ||
    !/^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/i.test(requestId)
  ) {
    throw new GraphRequestError(
      "Choose a discovered release and a valid import request.",
    );
  }
  const loaded = await loadedRankingReleases();
  if (selection === "ranks" && loaded.some((r) => r.graph_release === release))
    return false;
  const client = await graphCatalog().connect();
  try {
    await client.query("BEGIN");
    await client.query(
      "SELECT singleton FROM commoncrawl_graph_state WHERE singleton FOR UPDATE",
    );
    const files = await client.query(
      `SELECT artifact_kind FROM commoncrawl_graph_release_files WHERE graph_release=$1
      AND availability='available' AND source_etag IS NOT NULL AND source_bytes>0 AND expected_rows>0`,
      [release],
    );
    const required =
      selection === "full" ? ["nodes", "edges", "ranks"] : ["ranks"];
    if (
      required.some((kind) => !files.rows.some((r) => r.artifact_kind === kind))
    )
      throw new GraphRequestError(
        "Required files are unavailable. Refresh the catalog first.",
      );
    if (selection === "full") {
      const candidate =
        await client.query(`SELECT r.graph_release FROM commoncrawl_graph_releases r
        WHERE coverage_end IS NOT NULL AND graph_status<>'retired'
        AND (SELECT count(*) FROM commoncrawl_graph_release_files f WHERE f.graph_release=r.graph_release AND availability='available'
          AND source_etag IS NOT NULL AND source_bytes>0 AND expected_rows>0)=3
        ORDER BY coverage_end DESC,graph_release LIMIT 1`);
      if (candidate.rows[0]?.graph_release !== release)
        throw new GraphRequestError(
          "Only the newest complete graph can be imported. Use rankings for historical releases.",
        );
    }
    const prior = await client.query(
      `SELECT request_id,status,selection,source_manifest FROM commoncrawl_graph_import_requests
      WHERE graph_release=$1 ORDER BY created_at DESC LIMIT 1`,
      [release],
    );
    const retry =
      prior.rows[0] &&
      ["failed", "canceled"].includes(prior.rows[0].status) &&
      prior.rows[0].selection === selection
        ? prior.rows[0]
        : null;
    const inserted = await client.query(
      `INSERT INTO commoncrawl_graph_import_requests
      (request_id,graph_release,selection,origin,requested_by,retry_of,source_manifest)
      VALUES ($1,$2,$3,$4,$5,$6,$7) ON CONFLICT DO NOTHING RETURNING request_id`,
      [
        requestId,
        release,
        selection,
        origin,
        process.env.BACKOFFICE_OPERATOR?.trim() || "backoffice",
        retry?.request_id ?? null,
        retry?.source_manifest ?? null,
      ],
    );
    await client.query("COMMIT");
    return inserted.rowCount === 1;
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
}

export async function graphAction(form: FormData) {
  const intent = String(form.get("intent") ?? "");
  if (intent === "discover") {
    await launchRun({
      job: "commoncrawl_graph_discovery_job",
      runConfig: {},
      tags: {
        "backoffice/operator":
          process.env.BACKOFFICE_OPERATOR?.trim() || "backoffice",
      },
    });
    return "Catalog refresh started in Dagster.";
  }
  if (intent === "pause" || intent === "resume") {
    await graphCatalog().query(
      "UPDATE commoncrawl_graph_state SET automatic_imports_enabled=$1,updated_at=now(),updated_by=$2 WHERE singleton",
      [
        intent === "resume",
        process.env.BACKOFFICE_OPERATOR?.trim() || "backoffice",
      ],
    );
    return intent === "pause"
      ? "Automatic imports paused. Discovery and running imports continue."
      : "Automatic imports enabled. The Dagster sensor must also be running.";
  }
  if (intent === "backfill") {
    const releases = await graphReleases();
    const loaded = new Set(
      (await loadedRankingReleases()).map((r) => r.graph_release),
    );
    let count = 0;
    for (const release of releases) {
      if (release.ranks_available && !loaded.has(release.graph_release))
        count += Number(
          await queueGraphImport(
            release.graph_release,
            "ranks",
            randomUUID(),
            "backfill",
          ),
        );
    }
    return `Queued ${count} missing ranking releases. Imports run one at a time.`;
  }
  if (intent === "import") {
    const queued = await queueGraphImport(
      String(form.get("release")),
      String(form.get("selection")),
      String(form.get("requestId")),
    );
    return queued
      ? "Import queued. Follow its run in Dagster."
      : "This release is already loaded or queued.";
  }
  throw new GraphRequestError("Unknown graph action.");
}
