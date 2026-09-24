import { chQuery } from "~/lib/clickhouse.server";
import { assertWebtechAvailable } from "~/lib/webtech-maintenance.server";
import { WEBTECH_PAGE_SIZE } from "~/lib/webtech";
import type { WebtechInputFilters } from "~/lib/webtech-input";

export interface WebtechInputRow {
  task_id: string;
  input_id: string;
  root_domain: string;
  website_origin: string;
  page_url: string;
  source_name: string;
  source_record_id: string;
  submitted_at: string;
  bucket: number;
}

export async function listWebtechInputs(filters: WebtechInputFilters) {
  assertWebtechAvailable();
  const params = { ...filters, limit: WEBTECH_PAGE_SIZE, offset: (filters.page - 1) * WEBTECH_PAGE_SIZE };
  const where = `startsWith(root_domain, {prefix:String})
    ${filters.task ? "AND task_id = {task:String}" : ""}
    ${filters.source ? "AND source_name = {source:String}" : ""}`;
  const [rows, counts] = await Promise.all([
    chQuery<WebtechInputRow>(
      `SELECT task_id,input_id,root_domain,website_origin,page_url,source_name,source_record_id,
        bucket,toString(submitted_at) AS submitted_at
       FROM corpscout.webtech_scan_input WHERE ${where}
       ORDER BY submitted_at DESC,task_id,input_id LIMIT {limit:UInt32} OFFSET {offset:UInt64}`,
      params,
    ),
    chQuery<{ total: string; tasks: string; domains: string }>(
      `SELECT toString(count()) AS total,toString(uniqExact(task_id)) AS tasks,
        toString(uniqExact(root_domain)) AS domains
       FROM corpscout.webtech_scan_input WHERE ${where}`,
      params,
    ),
  ]);
  const indexed = rows.length ? await chQuery<{ task_id: string; input_id: string; scanned_at: string }>(
    `SELECT task_id,input_id,toString(max(scanned_at)) AS scanned_at
     FROM corpscout.webtech_domain_scan_results
     WHERE (task_id,input_id) IN (
       SELECT arrayJoin(arrayZip({tasks:Array(String)}, {inputs:Array(String)}))
     )
       AND root_domain IN {domains:Array(String)} GROUP BY task_id,input_id`,
    { tasks: rows.map(row => row.task_id), inputs: rows.map(row => row.input_id), domains: [...new Set(rows.map(row => row.root_domain))] },
  ) : [];
  const completed = new Map(indexed.map(row => [JSON.stringify([row.task_id, row.input_id]), row.scanned_at]));
  return {
    rows: rows.map(row => ({ ...row, scanned_at: completed.get(JSON.stringify([row.task_id, row.input_id])) ?? null })),
    total: Number(counts[0]?.total ?? 0), tasks: Number(counts[0]?.tasks ?? 0), domains: Number(counts[0]?.domains ?? 0),
  };
}
