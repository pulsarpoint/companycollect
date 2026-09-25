import { execFileSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { randomUUID } from "node:crypto";
import { createClient, ClickHouseLogLevel, type ClickHouseClient } from "@clickhouse/client";
import { afterAll, beforeAll, beforeEach, describe, expect, it, vi } from "vitest";
import { loadCrawlInputs } from "~/lib/crawl-inputs.server";
import { loadDomainCrawls } from "~/lib/domain-crawls.server";

// Opt-in integration suite: starts its own database, never uses .env credentials.
describe.skipIf(process.env.VITEST_CLICKHOUSE_DOCKER !== "1")("SE crawl input inserts in ClickHouse", () => {
  const container = `backoffice-crawl-input-${randomUUID()}`;
  const targets = ["website_full_crawl_requests", "website_jobs_crawl_requests", "website_site_info_requests"];
  let client: ClickHouseClient;
  const docker = (...args: string[]) => execFileSync("docker", args, { encoding: "utf8", stdio: ["ignore", "pipe", "pipe"] }).trim();
  const read = async (query: string) => (await client.query({ query, format: "JSONEachRow" })).json();

  beforeAll(async () => {
    docker("run", "-d", "--rm", "--name", container, "-p", "127.0.0.1::8123", "-e", "CLICKHOUSE_USER=test", "-e", "CLICKHOUSE_PASSWORD=test", "clickhouse/clickhouse-server:26.5");
    const port = docker("port", container, "8123").split(":").at(-1);
    vi.stubEnv("CLICKHOUSE_URL", `http://127.0.0.1:${port}`);
    vi.stubEnv("CLICKHOUSE_USER", "test");
    vi.stubEnv("CLICKHOUSE_PASSWORD", "test");
    vi.stubEnv("CLICKHOUSE_DATABASE", "corpscout");
    // Inventory reads do not depend on Dagster.
    vi.stubEnv("DAGSTER_GRAPHQL_URL", "http://127.0.0.1:1/graphql");
    client = createClient({ url: `http://127.0.0.1:${port}`, username: "test", password: "test", database: "default", log: { level: ClickHouseLogLevel.OFF } });
    const deadline = Date.now() + 25_000;
    while (!(await client.ping()).success) {
      if (Date.now() > deadline) throw new Error("Test ClickHouse failed to start");
      await new Promise((resolve) => setTimeout(resolve, 100));
    }
    const migration = readFileSync(new URL("../../../clickhouse/migrations/000429_corpscout_website_crawl_requests.up.sql", import.meta.url), "utf8");
    for (const query of migration.split(";").filter((statement) => statement.trim())) await client.command({ query });
    const resultsMigration = readFileSync(new URL("../../../clickhouse/migrations/000430_corpscout_website_crawl_type_results.up.sql", import.meta.url), "utf8");
    for (const query of resultsMigration.split(";").filter((statement) => statement.trim())) await client.command({ query });
    await client.command({ query: `CREATE TABLE corpscout.se_company_domain (
      company_id String, root_domain String, website_url String DEFAULT '', website_host String DEFAULT '',
      association String, is_primary UInt8 DEFAULT 0, confidence Float64, sources Array(String),
      supporting_sources Array(String) DEFAULT [], verification_status String DEFAULT '', review_status String DEFAULT '',
      active UInt8, inactive_reason String DEFAULT '', revision UInt64
    ) ENGINE=ReplacingMergeTree(revision) ORDER BY (company_id, root_domain)` });
    await client.command({ query: "SYSTEM STOP MERGES corpscout.se_company_domain" });
    await client.insert({ table: "corpscout.se_company_domain", format: "JSONEachRow", values: [
      { company_id: "100", root_domain: "shared.example", sources: ["brave"], association: "connected", active: 1, confidence: .8, revision: 1 },
      { company_id: "100", root_domain: "shared.example", sources: ["brave"], association: "connected", active: 0, confidence: .5, revision: 2 },
      { company_id: "200", root_domain: "shared.example", sources: ["wikidata"], association: "connected", active: 1, confidence: .9, revision: 1 },
      { company_id: "100", root_domain: "solo.example", sources: ["brave", "brave"], association: "uncertain", active: 1, confidence: .7, revision: 1 },
      { company_id: "300", root_domain: "rejected.example", sources: ["esef_filing"], association: "not_connected", active: 0, confidence: .2, revision: 1 },
    ] });
  }, 40_000);

  afterAll(async () => {
    await client?.close();
    docker("rm", "-f", container);
    vi.unstubAllEnvs();
  });
  beforeEach(async () => {
    for (const table of targets) await client.command({ query: `TRUNCATE TABLE corpscout.${table}` });
    for (const table of ["website_site_info_results", "website_full_crawl_results", "website_jobs_crawl_results"]) {
      await client.command({query: `TRUNCATE TABLE corpscout.${table}`});
    }
  });

  it("separates the latest attempt from retained data, preserves timestamp precision and scopes to the exact domain", async () => {
    const base = {domain: "shared.example", website_url: "https://shared.example", request_id: "saved", attempt: 1,
      state: "completed", crawl_status: "finished", successful: true, finished_at: "2026-09-20 10:00:00.000000",
      error: "", s3_path: "crawls/saved/result.json.gz", s3_state: "uploaded"};
    await client.insert({table: "corpscout.website_site_info_results", format: "JSONEachRow", values: [
      base,
      {...base, request_id: "z-earlier", state: "failed", crawl_status: "failed", successful: false, finished_at: "2026-09-25 10:00:00.100000"},
      {...base, request_id: "a-latest", state: "failed", crawl_status: "failed", successful: false, finished_at: "2026-09-25 10:00:00.900000"},
      {...base, domain: "other.example", request_id: "unrelated", finished_at: "2026-09-26 10:00:00.000000"},
    ]});
    await client.insert({table: "corpscout.website_jobs_crawl_results", format: "JSONEachRow", values: [{...base, crawl_status: "skip_crawling"}]});
    const rows = await loadDomainCrawls("shared.example");
    expect(rows[0].latest?.request_id).toBe("a-latest");
    expect(rows[0].saved?.request_id).toBe("saved");
    expect(rows[0].latest?.finished_at).toBe("2026-09-25T10:00:00Z");
    expect(rows[1].saved?.crawl_status).toBe("skip_crawling");
    expect(rows[2]).toMatchObject({type: "full", latest: null, saved: null});
    expect((await loadDomainCrawls("' OR 1=1 --")).every(row => row.latest === null && row.saved === null)).toBe(true);
  });

  it("reads current input counts, priority, disabled revisions and bounded filtered pages", async () => {
    const result = {table: "corpscout.website_site_info_requests"};
    await client.command({query: `INSERT INTO ${result.table} (domain, website_url, revision)
      VALUES ('shared.example', 'https://shared.example', 1), ('solo.example', 'https://solo.example', 1), ('rejected.example', 'https://rejected.example', 1)`});
    await client.command({query: `INSERT INTO ${result.table}
      SELECT * EXCEPT bucket REPLACE (false AS enabled, 2 AS revision, 99 AS priority)
      FROM ${result.table}_current WHERE domain='solo.example'`});
    const snapshot = await loadCrawlInputs(new URLSearchParams());
    expect(snapshot.type).toBe("site_info");
    expect(snapshot.stats).toContainEqual({type: "site_info", total: 3, enabled: 2});
    expect(snapshot.stats).toContainEqual({type: "full", total: 0, enabled: 0});
    expect(snapshot.rows[0]).toMatchObject({domain: "solo.example", priority: 99, enabled: false});
    expect(snapshot.rows[0].updated_at).toMatch(/Z$/);
    const filtered = await loadCrawlInputs(new URLSearchParams({input_type: "site_info", input_domain: "SHARED"}));
    expect(filtered.total).toBe(1);
    expect(filtered.rows.map(row => row.domain)).toEqual(["shared.example"]);
    const next = await loadCrawlInputs(new URLSearchParams({input_type: "site_info", input_offset: "2"}));
    expect(next.rows).toHaveLength(1);
    expect((await loadCrawlInputs(new URLSearchParams({input_domain: "' OR 1=1 --"}))).rows).toEqual([]);
  });
});
