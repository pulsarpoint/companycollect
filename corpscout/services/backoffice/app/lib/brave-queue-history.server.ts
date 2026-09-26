import { chQuery } from "~/lib/clickhouse.server";
import type { QueueSourceSummary } from "~/lib/queue-history.server";

export interface BraveSourceCompany {country_code: string; company_id: string; company_name: string; sources: string[]}
const MEMBERS = `WITH members AS (
 SELECT task_id,input_id,country_code,company_id,company_name,source_name,1 AS complete
 FROM corpscout.company_brave_task_sources FINAL WHERE task_id IN {tasks:Array(String)}
 UNION ALL
 SELECT task_id,input_id,country_code,company_id,company_name,source_name,1
 FROM corpscout.company_brave_queue_input WHERE task_id IN {tasks:Array(String)}
 UNION ALL
 SELECT task_id,input_id,country_code,company_id,company_name,'legacy',1
 FROM corpscout.company_brave_search_input WHERE task_id IN {tasks:Array(String)}
 UNION ALL
 SELECT toString(task_id),input_id,country_code,company_id,company_name,'',0
 FROM corpscout.company_brave_search_results WHERE toString(task_id) IN {tasks:Array(String)}
), companies AS (
 SELECT m.task_id,m.country_code,m.company_id,argMax(m.company_name,m.complete) AS company_name,
 groupUniqArrayIf(m.source_name,m.source_name != '') AS sources,max(m.complete) AS complete
 FROM members AS m GROUP BY m.task_id,m.country_code,m.company_id
)`;

export async function loadBraveSourceSummaries(tasks: string[]): Promise<QueueSourceSummary[]> {
  if (!tasks.length) return [];
  return chQuery<QueueSourceSummary>(`${MEMBERS}
   SELECT 'brave' AS task_type,task_id,toString(count()) AS total,max(complete) AS complete,
   groupArray(3)((company_name,concat(country_code,':',company_id))) AS preview
   FROM (SELECT * FROM companies ORDER BY task_id,country_code,company_id) GROUP BY task_id`, {tasks});
}

export async function loadBraveSourcePage(task: string, page: number) {
  return chQuery<BraveSourceCompany>(`${MEMBERS}
   SELECT country_code,company_id,company_name,sources FROM companies
   ORDER BY country_code,company_id LIMIT 50 OFFSET {offset:UInt64}`, {tasks: [task], offset: (page - 1) * 50});
}
