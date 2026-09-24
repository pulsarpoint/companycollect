import { renderToStaticMarkup } from "react-dom/server";
import { MemoryRouter } from "react-router";
import { beforeEach, expect, it, vi } from "vitest";
import { WebtechViewTabs } from "~/components/admin/webtech-scans-table";
import { parseWebtechInputFilters, webtechInputPath } from "~/lib/webtech-input";
const db = vi.hoisted(() => ({ chQuery: vi.fn() }));
vi.mock("~/lib/clickhouse.server", () => db);
vi.mock("~/lib/webtech-maintenance.server", () => ({ assertWebtechAvailable: vi.fn() }));
const { listWebtechInputs } = await import("~/lib/webtech-input.server");
beforeEach(() => vi.resetAllMocks());

it("reads submitted inputs and matches indexed results by task and input identity", async () => {
  db.chQuery.mockResolvedValueOnce([{ task_id: "task-a", input_id: "page", root_domain: "novelic.com" }, { task_id: "task-b", input_id: "page", root_domain: "novelic.com" }])
    .mockResolvedValueOnce([{ total: "2", tasks: "2", domains: "1" }])
    .mockResolvedValueOnce([{ task_id: "task-a", input_id: "page", scanned_at: "2026-09-24" }]);
  const result = await listWebtechInputs(parseWebtechInputFilters(new URLSearchParams("prefix=novelic")));
  expect(result.rows.map(row => row.scanned_at)).toEqual(["2026-09-24", null]);
  expect(result.total).toBe(2);
  for (const [sql] of db.chQuery.mock.calls.slice(0, 2)) {
    expect(sql).toContain("corpscout.webtech_scan_input");
    expect(sql).not.toContain("commoncrawl_domain_graph_signals");
    expect(sql).not.toContain("harmonic_rank");
  }
});

it("preserves source and task filters during pagination", () => {
  const filters = parseWebtechInputFilters(new URLSearchParams("task=test&source=manual&prefix=NOVELIC"));
  expect(webtechInputPath(filters, 2)).toBe("/admin/webtech/input?prefix=novelic&task=test&source=manual&page=2");
});

it("exposes Inputs from workspace navigation", () => {
  const html = renderToStaticMarkup(<MemoryRouter><WebtechViewTabs basePath="/admin/webtech" view="input" showInputs /></MemoryRouter>);
  expect(html).toContain('href="/admin/webtech/input"');
});
