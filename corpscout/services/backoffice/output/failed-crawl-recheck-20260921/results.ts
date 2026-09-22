import {readFileSync,writeFileSync} from 'node:fs';
import {chQuery} from '../../app/lib/clickhouse.server';
import {readCrawlArchive} from '../../app/lib/crawl-results.server';
const folder='output/failed-crawl-recheck-20260921';
const receipt=JSON.parse(readFileSync(`${folder}/receipt.json`,'utf8'));
const rows=await chQuery<any>(`SELECT domain,request_id,state,crawl_status,successful,error,s3_state,s3_path,dateDiff('second',started_at,finished_at) elapsed_seconds FROM website_site_info_results FINAL WHERE run_id={run:String} ORDER BY domain`,{run:receipt.runId});
const reads=await Promise.allSettled(rows.map(async(row:any)=>{const archive=await readCrawlArchive(row.s3_path);const result=JSON.parse(archive.result_json);const crawl=result.crawl??{};return {...row,archive_verified:archive.request_id===row.request_id,stop_reason:crawl.stop_reason,errors:crawl.errors,site_info:crawl.site_info,final_url:crawl.site_url,pages:crawl.pages?.map((p:any)=>({requested_url:p.requested_url,source_url:p.source_url,status_code:p.status_code,fetch_status:p.fetch_status,errors:p.errors,navigation_attempts:p.navigation_attempts})),challenge_agent_results:crawl.challenge_agent_results??result.challenge_agent_results,usage:crawl.usage?{calls:crawl.usage.calls,prompt_tokens:crawl.usage.prompt_tokens,completion_tokens:crawl.usage.completion_tokens}:undefined};}));
const results=reads.map((r,i)=>r.status==='fulfilled'?r.value:{...rows[i],readError:String(r.reason)});
writeFileSync(`${folder}/results.json`,JSON.stringify(results,null,2));console.log(JSON.stringify(results,null,2));
