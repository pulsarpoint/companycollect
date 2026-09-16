// Opt-in, disposable local ClickHouse; never uses the application's database.
// VITEST_LIVE=1 npx vitest run tests/domain-graph.live.test.ts
import { createClient } from "@clickhouse/client";
import { execFileSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { randomUUID } from "node:crypto";
import { setTimeout } from "node:timers/promises";
import { afterAll, beforeAll, describe, expect, it, vi } from "vitest";
import type { ClickHouseClient } from "@clickhouse/client";

const state = vi.hoisted(() => ({
  client: undefined as ClickHouseClient | undefined,
}));
vi.mock("~/lib/clickhouse.server", () => ({
  chQuery: async (query: string, query_params: Record<string, unknown>) => {
    const result = await state.client!.query({
      query,
      query_params,
      format: "JSONEachRow",
      clickhouse_settings: { readonly: "2", use_top_k_dynamic_filtering: 0 },
    });
    return result.json();
  },
}));
const { getDomainGraphReleases, searchDomainGraph } = await import(
  "~/lib/domain-graph.server"
);

const container = `backoffice-graph-test-${randomUUID()}`;
const release = "cc-main-2026-jun-jul-aug";
const olderRelease = "cc-main-2026-mar-apr-may";
const search = {
  domain: "original.com",
  release,
  direction: "all" as const,
  page: 1,
  pageSize: 50,
};

beforeAll(async () => {
  execFileSync(
    "docker",
    [
      "run",
      "-d",
      "--rm",
      "--name",
      container,
      "-p",
      "127.0.0.1::8123",
      "-e",
      "CLICKHOUSE_USER=test",
      "-e",
      "CLICKHOUSE_PASSWORD=test",
      "clickhouse/clickhouse-server:26.5",
    ],
    { stdio: "pipe" },
  );
  const address = execFileSync("docker", ["port", container, "8123"], {
    encoding: "utf8",
  }).trim();
  state.client = createClient({
    url: `http://${address}`,
    username: "test",
    password: "test",
  });
  const deadline = Date.now() + 30_000;
  while (true) {
    const ping = await state.client.ping();
    if (ping.success) break;
    if (Date.now() > deadline) throw new Error("Test ClickHouse did not start");
    await setTimeout(200);
  }
  const ddl = readFileSync(
    new URL(
      "../../../clickhouse/migrations/000418_corpscout_commoncrawl_domain_graph.up.sql",
      import.meta.url,
    ),
    "utf8",
  );
  for (const query of ddl.split(";").filter((sql) => sql.trim()))
    await state.client.command({ query });
  for (const graph_release of [olderRelease, release]) {
    const domains =
      graph_release === release
        ? [
            "original.com",
            "related.se",
            "oneway.com",
            "cycle.com",
            "return.com",
            "inbound.com",
            "isolated.com",
          ]
        : ["original.com", "old-neighbor.se"];
    await state.client.insert({
      table: "corpscout.commoncrawl_domain_graph_nodes",
      format: "JSONEachRow",
      values: domains.map((root_domain, node_id) => ({
        graph_release,
        node_id,
        root_domain,
        n_hosts: 1,
        source_etag: "fixture",
        source_run_id: "test",
      })),
    });
    const edges =
      graph_release === release
        ? [
            [0, 1],
            [1, 0],
            [0, 0],
            [0, 2],
            [0, 3],
            [3, 4],
            [4, 0],
            [5, 0],
          ]
        : [[0, 1]];
    await state.client.insert({
      table: "corpscout.commoncrawl_domain_graph_edges",
      format: "JSONEachRow",
      values: edges.map(([source_node_id, target_node_id]) => ({
        graph_release,
        source_node_id,
        target_node_id,
        source_etag: "fixture",
        source_run_id: "test",
      })),
    });
    await state.client.insert({
      table: "corpscout.commoncrawl_domain_graph_snapshots",
      format: "JSONEachRow",
      values: [
        {
          graph_release,
          node_count: domains.length,
          edge_count: edges.length,
          loop_count: graph_release === release ? 1 : 0,
          vertices_url: "",
          edges_url: "",
          vertices_etag: "fixture",
          edges_etag: "fixture",
          source_index_url: "",
          source_run_id: "test",
          published_at:
            graph_release === release
              ? "2026-09-16 12:00:00.000"
              : "2026-06-16 12:00:00.000",
        },
      ],
    });
  }
  const indexDdl = readFileSync(
    new URL(
      "../../../clickhouse/migrations/000419_corpscout_domain_graph_lookup_index.up.sql",
      import.meta.url,
    ),
    "utf8",
  );
  for (const query of indexDdl.split(";").filter((sql) => sql.trim()))
    await state.client.command({ query });
}, 60_000);

afterAll(async () => {
  await state.client?.close();
  execFileSync("docker", ["rm", "-f", container], { stdio: "pipe" });
});

describe("real ClickHouse graph queries", () => {
  it("materializes the replacement lookup index for existing releases", async () => {
    const response = await state.client!.query({
      query: `
      SELECT name, sum(rows) AS rows FROM system.projection_parts
      WHERE active AND database='corpscout' AND table='commoncrawl_domain_graph_nodes'
      GROUP BY name`,
      format: "JSONEachRow",
    });
    const parts = await response.json<{ name: string; rows: string | number }>();
    expect(parts.map((part) => ({ ...part, rows: Number(part.rows) }))).toEqual([
      { name: "by_node_id_lookup", rows: 9 },
    ]);
  });
  it("counts both directions, ignores self-links, and identifies direct reciprocity", async () => {
    expect(
      (await getDomainGraphReleases()).map((row) => row.graph_release),
    ).toEqual([release, olderRelease]);
    const result = await searchDomainGraph(search);
    expect(result.counts).toEqual({
      all: 5,
      mutual: 1,
      outgoing: 2,
      incoming: 2,
    });
    expect(
      result.rows.map((row) => [
        row.connected_domain,
        row.outgoing,
        row.incoming,
        row.reciprocal,
      ]),
    ).toEqual([
      ["related.se", 1, 1, 1],
      ["cycle.com", 1, 0, 0],
      ["inbound.com", 0, 1, 0],
      ["oneway.com", 1, 0, 0],
      ["return.com", 0, 1, 0],
    ]);
  });

  it("filters one-way and mutual connections without treating longer cycles as reciprocal", async () => {
    expect(
      (await searchDomainGraph({ ...search, direction: "mutual" })).rows.map(
        (row) => row.connected_domain,
      ),
    ).toEqual(["related.se"]);
    expect(
      (await searchDomainGraph({ ...search, direction: "outgoing" })).rows.map(
        (row) => row.connected_domain,
      ),
    ).toEqual(["cycle.com", "oneway.com"]);
    expect(
      (await searchDomainGraph({ ...search, direction: "incoming" })).rows.map(
        (row) => row.connected_domain,
      ),
    ).toEqual(["inbound.com", "return.com"]);
  });

  it("keeps reused numeric IDs isolated by release", async () => {
    const result = await searchDomainGraph({
      ...search,
      release: olderRelease,
    });
    expect(result.rows.map((row) => row.connected_domain)).toEqual([
      "old-neighbor.se",
    ]);
    expect(result.counts).toEqual({
      all: 1,
      mutual: 0,
      outgoing: 1,
      incoming: 0,
    });
  });

  it("distinguishes missing and isolated domains and hides unpublished releases", async () => {
    expect(
      await searchDomainGraph({ ...search, domain: "absent.com" }),
    ).toMatchObject({ found: false, rows: [] });
    expect(
      await searchDomainGraph({ ...search, domain: "isolated.com" }),
    ).toMatchObject({ found: true, total: 0, rows: [] });
    expect(
      await searchDomainGraph({ ...search, release: "not-published" }),
    ).toMatchObject({ found: false, rows: [] });
  });
});
