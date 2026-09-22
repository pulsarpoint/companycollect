import {existsSync,readFileSync,writeFileSync} from 'node:fs';
import {runStatus} from '../../app/lib/dagster.server';
import {browserRequest} from '../../app/lib/browser-service.server';
import {crawlerFetch} from '../../app/lib/crawler.server';
import {chQuery} from '../../app/lib/clickhouse.server';
const folder='output/session-deployment-20260921';
const results:any={};
for(const [type,file]of [['brave','resumed-run.json'],['crawl','crawl-receipt.json']]){
 if(!existsSync(`${folder}/${file}`))continue;
 const receipt=JSON.parse(readFileSync(`${folder}/${file}`,'utf8'));const run=await runStatus(receipt.runId);
 results[type]={runId:run.runId,status:run.status};
 if(type==='brave')results.brave.rows=await chQuery("SELECT route,status,error_type,error_stage,count() n FROM company_brave_search_results FINAL WHERE source_run_id={run:String} GROUP BY route,status,error_type,error_stage",{run:run.runId});
 else {results.crawl.rows=await chQuery("SELECT domain,request_id,state,successful,error,s3_path FROM website_site_info_results FINAL WHERE run_id={run:String}",{run:run.runId});results.crawl.submissions=await chQuery('SELECT request_id FROM website_crawl_submissions FINAL WHERE run_id={run:String}',{run:run.runId});for(const row of results.crawl.submissions){const job=await(await crawlerFetch(`/v1/crawls/${row.request_id}/status`)).json();results.crawl.job={state:job.state,reason:job.reason,error:job.error,collected_pages:job.collected_pages,browser_lease_id:job.browser_lease_id,browser_execution_id:job.browser_execution_id,s3_state:job.s3_state,crawl_status:job.crawl_status};}}
}
const browser=await(await browserRequest('/v1/server')).json();results.browser={version:browser.version,settings:browser.settings,active:browser.leases.filter((r:any)=>!r.ended_at).map((r:any)=>({session_id:r.session_id,state:r.state,domain:r.domain}))};
writeFileSync(`${folder}/progress.json`,JSON.stringify(results,null,2));console.log(JSON.stringify(results,null,2));
