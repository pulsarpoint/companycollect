import { chQuery } from "~/lib/clickhouse.server";
import type { CrawlQueueType } from "~/lib/queues";

export interface QueueHistoryReference {
  taskId: string;
  crawlType: CrawlQueueType | null;
  executionId: string;
}
export interface QueueSourceSummary {
  task_id: string;
  task_type: string;
  total: string;
  complete: number;
  preview: [string, string][];
}
export interface QueueSourceRow { domain: string; website_url: string; sources: string[] }

function sourceSelection(type: "webtech" | "crawler", references: QueueHistoryReference[]) {
  const kinds = [...new Set(references.map(ref => ref.crawlType ?? "webtech"))];
  const queries = kinds.map(kind => {
    const condition = `task_id IN (SELECT task FROM refs WHERE kind = '${kind}')`;
    const queue = kind === "webtech" ? "webtech_scan_input" : "website_crawl_task_domains";
    const domain = kind === "webtech" ? "root_domain" : "domain";
    const url = kind === "webtech" ? "page_url" : "website_url";
    const results = kind === "webtech" ? "webtech_domain_scan_results" : kind === "site_info" ? "website_site_info_results" : `website_${kind}_crawl_results`;
    return `SELECT '${kind}' AS task_type, task_id, domain, website_url, source_name, 1 AS complete
      FROM corpscout.queue_task_sources WHERE task_type = '${kind}' AND ${condition}
      UNION ALL
      SELECT '${kind}', task_id, ${domain}, ${url}, source_name, 1
      FROM corpscout.${queue} WHERE ${condition} ${kind === "webtech" ? "" : `AND crawl_type = '${kind}'`}
      UNION ALL
      ${kind === "webtech"
        ? `SELECT '${kind}', task_id, root_domain, page_url, '', 0 FROM corpscout.${results} WHERE ${condition}`
        : `SELECT '${kind}', refs.task, r.domain, r.website_url, '', 0
           FROM corpscout.${results} r INNER JOIN refs ON r.run_id = refs.execution
           WHERE refs.kind = '${kind}' AND r.run_id IN (SELECT execution FROM refs WHERE kind = '${kind}')`}`;
  });
  return {
    sql: `WITH refs AS (
      SELECT tupleElement(ref, 1) AS task, tupleElement(ref, 2) AS kind, tupleElement(ref, 3) AS execution
      FROM (SELECT arrayJoin(arrayZip({tasks:Array(String)}, {kinds:Array(String)}, {executions:Array(String)})) AS ref)
    ), sources AS (${queries.join(" UNION ALL ")}), websites AS (
      SELECT task_type, task_id, domain, website_url, groupUniqArrayIf(source_name, source_name != '') AS sources,
        max(complete) AS complete FROM sources
      GROUP BY task_type, task_id, domain, website_url
    )`,
    params: {tasks: references.map(ref => ref.taskId), kinds: references.map(ref => type === "webtech" ? "webtech" : ref.crawlType!), executions: references.map(ref => ref.executionId)},
  };
}

export async function loadQueueSourceSummaries(type: "webtech" | "crawler", references: QueueHistoryReference[]) {
  if (!references.length) return [];
  const {sql, params} = sourceSelection(type, references);
  return chQuery<QueueSourceSummary>(`${sql}
    SELECT task_type, task_id, toString(count()) AS total, max(complete) AS complete,
      groupArray(3)((domain, website_url)) AS preview
    FROM (SELECT * FROM websites ORDER BY task_type, task_id, domain, website_url)
    GROUP BY task_type, task_id`, params);
}

export async function loadQueueSourcePage(type: "webtech" | "crawler", references: QueueHistoryReference[], page: number) {
  if (!references.length) return [];
  const {sql, params} = sourceSelection(type, references);
  return chQuery<QueueSourceRow>(`${sql}
    SELECT domain, website_url, arraySort(arrayDistinct(arrayFlatten(groupArray(sources)))) AS sources
    FROM websites GROUP BY domain, website_url ORDER BY domain, website_url
    LIMIT 50 OFFSET {offset:UInt64}`, {...params, offset: (page - 1) * 50});
}
