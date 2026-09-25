import { execFileSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { randomUUID } from "node:crypto";
import { Pool } from "pg";
import { afterAll, beforeAll, expect, it, vi } from "vitest";

const { chQuery, instigatorStates } = vi.hoisted(() => ({
  chQuery: vi.fn(),
  instigatorStates: vi.fn(),
}));
vi.mock("~/lib/clickhouse.server", () => ({ chQuery }));
vi.mock("~/lib/dagster.server", () => ({
  instigatorStates,
  launchRun: vi.fn(),
  dagsterRunUrl: (id: string) => `https://dagster.example/runs/${id}`,
}));
const graph = await import("~/lib/commoncrawl-graph.server");
const name = `backoffice-graph-test-${randomUUID()}`;
let admin: Pool;
const old = "cc-main-2026-may-jun-jul",
  latest = "cc-main-2026-jul-aug-sep";

beforeAll(async () => {
  execFileSync(
    "docker",
    [
      "run",
      "-d",
      "--rm",
      "--name",
      name,
      "-p",
      "127.0.0.1::5432",
      "-e",
      "POSTGRES_PASSWORD=graph-test",
      "postgres:17",
    ],
    { stdio: "pipe" },
  );
  let port = "";
  for (let i = 0; i < 60; i++) {
    port =
      execFileSync("docker", ["port", name, "5432"], { encoding: "utf8" })
        .trim()
        .split(":")
        .at(-1) ?? "";
    if (port) break;
    await new Promise((resolve) => setTimeout(resolve, 250));
  }
  const url = `postgresql://postgres:graph-test@127.0.0.1:${port}/postgres`;
  admin = new Pool({ connectionString: url, connectionTimeoutMillis: 1000 });
  for (let i = 0; i < 60; i++) {
    try {
      await admin.query("SELECT 1");
      break;
    } catch (error) {
      if (i === 59) throw error;
      await new Promise((resolve) => setTimeout(resolve, 250));
    }
  }
  await admin.query(
    readFileSync(
      new URL(
        "../../../database/migrations/000126_commoncrawl_graph_catalog.up.sql",
        import.meta.url,
      ),
      "utf8",
    ),
  );
  for (const [release, date] of [
    [old, "2026-07-01"],
    [latest, "2026-09-01"],
  ]) {
    await admin.query(
      "INSERT INTO commoncrawl_graph_releases (graph_release,source_index_url,coverage_end) VALUES ($1,'https://example.test',$2)",
      [release, date],
    );
    for (const kind of ["nodes", "edges", "ranks"])
      await admin.query(
        `INSERT INTO commoncrawl_graph_release_files
      (graph_release,artifact_kind,source_url,source_etag,source_bytes,expected_rows,availability) VALUES ($1,$2,'https://example.test','etag',10,2,'available')`,
        [release, kind],
      );
  }
  vi.stubEnv("COMMONCRAWL_GRAPH_PG_URL", url);
  chQuery.mockResolvedValue([]);
  instigatorStates.mockResolvedValue({ schedules: [], sensors: [] });
}, 60000);
afterAll(async () => {
  await graph.graphCatalog().end();
  await admin?.end();
  execFileSync("docker", ["rm", "-f", name], { stdio: "pipe" });
  vi.unstubAllEnvs();
});

it("rejects historical full graphs and serializes concurrent requests", async () => {
  await expect(
    graph.queueGraphImport(old, "full", randomUUID()),
  ).rejects.toThrow("newest");
  const result = await Promise.all([
    graph.queueGraphImport(latest, "full", randomUUID()),
    graph.queueGraphImport(latest, "ranks", randomUUID()),
  ]);
  expect(result.sort()).toEqual([false, true]);
  const dashboard = await graph.graphDashboard();
  expect(dashboard.releases.map((r) => r.graph_release)).toEqual([latest, old]);
  expect(dashboard.releases[0].status).toBe("queued");
  expect(dashboard.releases[0].available_files).toBe(3);
});
it("queues only missing ranks and pauses automatic work without clearing the ledger", async () => {
  chQuery.mockResolvedValue([{ graph_release: latest, rows: "2" }]);
  const form = new FormData();
  form.set("intent", "backfill");
  expect(await graph.graphAction(form)).toContain("Queued 1");
  expect(await graph.graphAction(form)).toContain("Queued 0");
  form.set("intent", "resume");
  await graph.graphAction(form);
  expect((await graph.graphState()).automatic_imports_enabled).toBe(true);
  form.set("intent", "pause");
  await graph.graphAction(form);
  expect((await graph.graphState()).automatic_imports_enabled).toBe(false);
  expect((await graph.graphDashboard()).latestRanking).toBe(latest);
});

it("serves a known active pointer briefly during catalog failure, then fails closed", async () => {
  await admin.query(
    "UPDATE commoncrawl_graph_state SET active_graph_release=$1",
    [latest],
  );
  expect(await graph.activeGraphRelease()).toBe(latest);
  const query = vi
    .spyOn(graph.graphCatalog(), "query")
    .mockRejectedValue(new Error("catalog unavailable"));
  expect(await graph.activeGraphRelease()).toBe(latest);
  const now = Date.now();
  const clock = vi.spyOn(Date, "now").mockReturnValue(now + 9 * 60 * 1000);
  await expect(graph.activeGraphRelease()).rejects.toThrow(
    "catalog unavailable",
  );
  query.mockRestore();
  clock.mockRestore();
});
