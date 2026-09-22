import {readFileSync,writeFileSync} from 'node:fs';
import {setTimeout} from 'node:timers/promises';
import {chQuery} from '../../app/lib/clickhouse.server';
import {runStatus,assetMaterializations} from '../../app/lib/dagster.server';
import {loadCrawls} from '../../app/lib/crawler.server';
const folder=process.argv[2]??'output/basic-info-validation-20260920';
const {runId} = JSON.parse(readFileSync(`${folder}/receipt.json`,'utf8'));
for (let i=0;i<120;i++) {
 const [run,counts,receipts,live] = await Promise.all([
 runStatus(runId,{timeoutMs:10000}),
 chQuery<{saved:number;successful_count:number;unsuccessful_count:number}>('SELECT toUInt32(count()) AS saved, toUInt32(countIf(successful)) AS successful_count, toUInt32(countIf(NOT successful)) AS unsuccessful_count FROM corpscout.website_site_info_results_current WHERE run_id={runId:String}',{runId}),
 chQuery<{request_id:string}>('SELECT request_id FROM corpscout.website_crawl_submissions FINAL WHERE run_id={runId:String}',{runId}),
 loadCrawls(new URLSearchParams({source:'rest'}))]);
 const ids=new Set(receipts.map(r=>r.request_id));
 const state={time:new Date().toISOString(),status:run.status,...counts[0],submitted:ids.size,active:live.attempts.filter(r=>ids.has(r.request_id)&&!['completed','failed','cancelled'].includes(r.state)).map(r=>({domain:r.domain,state:r.state,reason:r.reason,url:r.current_url})),failures:live.attempts.filter(r=>ids.has(r.request_id)&&r.state==='failed').map(r=>({domain:r.domain,error:r.error,reason:r.reason}))};
 console.log(JSON.stringify(state));writeFileSync(`${folder}/progress.json`,JSON.stringify(state,null,2));
 if (['SUCCESS','FAILURE','CANCELED'].includes(run.status)) {
 const metadata=(await assetMaterializations({asset:'website_site_info_results',limit:20},{timeoutMs:10000})).find(r=>r.runId===runId);
 writeFileSync(`${folder}/completion.json`,JSON.stringify({runId,status:run.status,startTime:run.startTime,endTime:run.endTime,metadata},null,2));
 console.log(JSON.stringify({complete:true,status:run.status,metadata}));break;
 }
 await setTimeout(30000);
}
