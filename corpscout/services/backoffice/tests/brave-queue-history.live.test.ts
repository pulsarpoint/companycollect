import { expect, it } from "vitest";
import { chQuery } from "~/lib/clickhouse.server";
import { loadBraveSourcePage, loadBraveSourceSummaries } from "~/lib/brave-queue-history.server";

it.skipIf(process.env.VITEST_LIVE !== "1")("queries retained Brave membership and pages it with the live ClickHouse engine", async () => {
  const tasks = await chQuery<{task_id: string}>(`SELECT task_id FROM corpscout.company_brave_task_sources
    UNION ALL SELECT task_id FROM corpscout.company_brave_search_input WHERE task_id != '' LIMIT 1`, {});
  expect(tasks).toHaveLength(1);
  const task = tasks[0].task_id;
  const [summaries, page] = await Promise.all([loadBraveSourceSummaries([task]), loadBraveSourcePage(task, 1)]);
  expect(summaries).toHaveLength(1);
  expect(summaries[0]).toMatchObject({task_id: task, task_type: "brave", complete: 1});
  expect(Number(summaries[0].total)).toBeGreaterThan(0);
  expect(page).toHaveLength(Math.min(50, Number(summaries[0].total)));
  expect(page.every(row => row.country_code === "SE" && row.company_id && row.company_name)).toBe(true);
}, 30_000);
