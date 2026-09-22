import {readFileSync,writeFileSync} from 'node:fs';
import {readCrawlArchive} from '../../app/lib/crawl-results.server';
const folder='output/failed-crawl-recheck-20260921';
const inspection=JSON.parse(readFileSync(`${folder}/inspection.json`,'utf8'));
const receipt=JSON.parse(readFileSync(`${folder}/receipt.json`,'utf8'));
const failures=inspection.failures.filter((f:any)=>f.run_id==='21641399-34d9-48fe-9747-7c011915c1d2'&&receipt.domains.includes(f.domain));
const reads=await Promise.allSettled(failures.map(async(f:any)=>{const archive=await readCrawlArchive(f.s3_path);const data=JSON.parse(archive.result_json);const crawl=data.crawl??{};return{domain:f.domain,s3_path:f.s3_path,schema:data.schema_version,status:crawl.status??data.status,stop_reason:crawl.stop_reason,error:data.error,errors:crawl.errors,site_info:crawl.site_info,pages:crawl.pages?.map((p:any)=>({url:p.source_url,status:p.status_code,errors:p.errors})),challenge_agent_results:crawl.challenge_agent_results??data.challenge_agent_results};}));
const output=reads.map((r,i)=>r.status==='fulfilled'?r.value:{domain:failures[i].domain,readError:String(r.reason)});
writeFileSync(`${folder}/baseline.json`,JSON.stringify(output,null,2));console.log(JSON.stringify(output,null,2));
