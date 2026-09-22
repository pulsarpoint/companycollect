import {readFileSync,writeFileSync} from 'node:fs';
import {runStatus} from '../../app/lib/dagster.server';
import {chQuery} from '../../app/lib/clickhouse.server';
import {browserRequest} from '../../app/lib/browser-service.server';
const folder='output/redirect-deployment-20260921';
const resumed=JSON.parse(readFileSync(`${folder}/resumed-run.json`,'utf8'));
const [original]=JSON.parse(readFileSync(`${folder}/original-runs.json`,'utf8'));
const [run,counts,response]=await Promise.all([
 runStatus(resumed.runId),
 chQuery('SELECT count() AS total, countIf(status=\'success\') AS successful, countIf(status=\'error\') AS failed, uniqExact(input_id) AS distinct_inputs, countIf(source_run_id={resumed:String}) AS resumed_results FROM corpscout.company_brave_search_results FINAL WHERE execution_id={id:UUID}',{id:original.runId,resumed:resumed.runId}),
 browserRequest('/v1/server')]);
const browser=await response.json();
const status={time:new Date().toISOString(),runId:run.runId,status:run.status,execution:run.tags['brave/execution']?JSON.parse(run.tags['brave/execution']):null,counts,version:browser.version,healthy:browser.healthy,active:browser.leases.filter((r:any)=>r.state==='ready').map((r:any)=>({domain:r.domain,stage:r.operation?.stage,request_id:r.request_id}))};
writeFileSync(`${folder}/resumed-status.json`,JSON.stringify(status,null,2));console.log(JSON.stringify(status));
