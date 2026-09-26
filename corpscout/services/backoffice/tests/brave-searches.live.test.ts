import { randomUUID } from "node:crypto";
import { Pool, type PoolClient } from "pg";
import { expect, it, vi } from "vitest";
import { listBraveSearches, removeBraveSearch, resolveBraveSearch, saveBraveSearch } from "~/lib/brave-searches.server";

const state = vi.hoisted(() => ({client: undefined as PoolClient | undefined}));
vi.mock("~/lib/llm-control.server", () => ({llmControl: () => state.client}));

it.skipIf(process.env.VITEST_LIVE !== "1")("creates, edits, resolves and removes a search using the deployed database permissions", async () => {
  const pool = new Pool({connectionString: process.env.LLM_CONTROL_PG_URL, connectionTimeoutMillis: 5000});
  const client = await pool.connect();
  state.client = client;
  try {
    await client.query("BEGIN");
    const name = `Brave search test ${randomUUID()}`;
    await saveBraveSearch({searchId: "", revision: 0, name, queryTemplate: "Find jobs at {company_name}."});
    const created = (await listBraveSearches()).find(row => row.name === name)!;
    expect(created).toMatchObject({name, revision: 1});
    const task = randomUUID();
    expect(await resolveBraveSearch(task, created.searchId, 1)).toMatchObject({search_name: name, search_revision: 1});
    await saveBraveSearch({...created, queryTemplate: "Find careers at {company_name} in {country_code}."});
    await expect(resolveBraveSearch(task, created.searchId, 1)).rejects.toThrow("has changed");
    expect(await resolveBraveSearch(task, created.searchId, 2)).toMatchObject({query_template: "Find careers at {company_name} in {country_code}."});
    await expect(removeBraveSearch(created.searchId, 1)).rejects.toThrow("changed or removed");
    await removeBraveSearch(created.searchId, 2);
    expect((await listBraveSearches()).some(row => row.searchId === created.searchId)).toBe(false);
    await expect(resolveBraveSearch(task, created.searchId, 2)).rejects.toThrow("removed");
  } finally {
    await client.query("ROLLBACK");
    client.release();
    await pool.end();
  }
}, 30_000);
