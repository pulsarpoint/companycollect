import { expect, it } from "vitest";
import { chQuery } from "~/lib/clickhouse.server";
import { loadBraveResults } from "~/lib/brave-results.server";

it.skipIf(process.env.VITEST_LIVE !== "1")("reads the same full answer from task history and company history", async () => {
  const [saved] = await chQuery<{country_code: string; company_id: string; task_id: string; result_id: string; answer_text: string}>(
    `SELECT country_code,company_id,toString(task_id) AS task_id,toString(result_id) AS result_id,answer_text
     FROM corpscout.company_brave_search_results FINAL WHERE status='success' ORDER BY completed_at DESC LIMIT 1`,
  );
  expect(saved).toBeDefined();
  const search = new URLSearchParams({result: saved.result_id});
  const [task, company] = await Promise.all([
    loadBraveResults({taskId: saved.task_id}, search),
    loadBraveResults({countryCode: saved.country_code, companyId: saved.company_id}, search),
  ]);
  expect(task.selected?.answer_text).toBe(saved.answer_text);
  expect(company.selected?.answer_text).toBe(saved.answer_text);
  expect(task.rows.every(row => row.task_id === saved.task_id)).toBe(true);
  expect(company.rows.every(row => row.country_code === saved.country_code && row.company_id === saved.company_id)).toBe(true);
  expect(task.total).toBe(task.succeeded + task.failed);
  const next = await loadBraveResults({taskId: saved.task_id}, new URLSearchParams("page=2"));
  if (task.total > 25) expect(next.rows.some(row => task.rows.some(first => first.result_id === row.result_id))).toBe(false);
  await expect(loadBraveResults({countryCode: "ZZ", companyId: saved.company_id}, search)).rejects.toMatchObject({status: 404});
}, 30_000);

it.skipIf(process.env.VITEST_LIVE !== "1")("reads a saved failure with its reason", async () => {
  const [failed] = await chQuery<{task_id: string; result_id: string; error_type: string}>(
    `SELECT toString(task_id) AS task_id,toString(result_id) AS result_id,error_type
     FROM corpscout.company_brave_search_results FINAL WHERE status='error' ORDER BY completed_at DESC LIMIT 1`,
  );
  expect(failed).toBeDefined();
  const history = await loadBraveResults({taskId: failed.task_id}, new URLSearchParams({result: failed.result_id}));
  expect(history.selected).toMatchObject({status: "error", result_id: failed.result_id, error_type: failed.error_type});
}, 30_000);
