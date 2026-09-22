import {readFileSync,writeFileSync} from 'node:fs';
import {isDeepStrictEqual} from 'node:util';
import {createHash} from 'node:crypto';
import {chQuery} from '../../app/lib/clickhouse.server';
import {readCrawlArchive} from '../../app/lib/crawl-results.server';
const folder=process.argv[2]??'output/basic-info-validation-20260920';
const {runId,domains} = JSON.parse(readFileSync(`${folder}/receipt.json`,'utf8'));
const rows=await chQuery<Record<string,any>>('SELECT * FROM corpscout.website_site_info_results_current WHERE run_id={runId:String} ORDER BY domain',{runId});
const checked:Record<string,any>[]=[];
for(let i=0;i<rows.length;i+=3){
 const group=await Promise.all(rows.slice(i,i+3).map(async row=>{
  const info=row.site_info===null?null:JSON.parse(row.site_info);
  const pages=JSON.parse(row.pages);
  const check:Record<string,any>={domain:row.domain,request_id:row.request_id,attempt:row.attempt,successful:row.successful,state:row.state,crawl_status:row.crawl_status,error:row.error,
   started_at:row.started_at,finished_at:row.finished_at,site_info:info,pages:pages.length,model_usage:JSON.parse(row.model_usage),s3_path:row.s3_path,s3_state:row.s3_state,issues:[]};
  if(row.successful && (!info?.site_description?.trim() || !info?.source_url)) check.issues.push('Successful result lacks description or source URL');
  if(pages.length>1)check.issues.push('Basic-info page limit exceeded');
  if(!row.s3_path||row.s3_state!=='uploaded')check.issues.push('Missing uploaded S3 result');
  else try {
   const archive=await readCrawlArchive(row.s3_path);
   const payload=JSON.parse(archive.result_json);
   const crawl=payload.crawl??payload;
   check.s3_readable=true;check.s3_info_matches=isDeepStrictEqual(info,crawl.site_info??null);
   check.s3_identity_matches=archive.request_id===row.request_id && archive.attempt===row.attempt;
   check.document_count=Array.isArray(payload.documents)?payload.documents.length:0;
   check.document_fields=payload.documents?.[0]?Object.keys(payload.documents[0]):[];
   check.input_fields=payload.documents?.[0]?.input?Object.keys(payload.documents[0].input):[];
   check.stop_reason=crawl.stop_reason;
   check.crawl_errors=crawl.errors;
   check.html_bytes=(payload.documents??[]).reduce((sum:number,doc:any)=>sum+Buffer.byteLength(doc.html??''),0);
   check.html_hashes_match=(payload.documents??[]).every((doc:any)=>typeof doc.html==='string'&&createHash('sha256').update(doc.html).digest('hex')===doc.html_sha256);
   check.pages_match=isDeepStrictEqual(pages,crawl.pages??[]);
   check.usage_matches=isDeepStrictEqual(check.model_usage,crawl.usage??{});
   check.observations_match=isDeepStrictEqual(JSON.parse(row.page_observations),(payload.documents??[]).filter((doc:any)=>doc.input?.observations!==undefined).map((doc:any)=>doc.input.observations));
   if(!check.html_hashes_match||!check.pages_match||!check.usage_matches||!check.observations_match)check.issues.push('HTML hash or stored section mismatch');
   if(row.successful&&check.html_bytes===0)check.issues.push('Successful result has empty HTML');
   if(!check.s3_info_matches)check.issues.push('CH site_info differs from S3');
   if(!check.s3_identity_matches)check.issues.push('S3 identity mismatch');
   if(row.successful&&check.document_count===0)check.issues.push('Successful crawl missing HTML document');
  } catch(error) {check.s3_readable=false;check.issues.push(error instanceof Error?error.message:'S3 mapping read failed');}
  return check;
 }));
 checked.push(...group);console.log(JSON.stringify({verified:checked.length,total:rows.length,issues:group.filter(r=>r.issues.length).map(r=>({domain:r.domain,issues:r.issues}))}));
}
const found=new Set(rows.map(row=>row.domain));
const summary={runId,requested:domains.length,saved:rows.length,successful:rows.filter(r=>r.successful).length,unsuccessful:rows.filter(r=>!r.successful).length,
 missing:domains.filter((domain:string)=>!found.has(domain)),s3_verified:checked.filter(r=>r.s3_readable&&r.s3_info_matches&&r.s3_identity_matches).length,
 issues:checked.filter(r=>r.issues.length).map(r=>({domain:r.domain,issues:r.issues})),failures:checked.filter(r=>!r.successful).map(r=>({domain:r.domain,error:r.error,status:r.crawl_status,stop_reason:r.stop_reason})),results:checked};
writeFileSync(`${folder}/verification.json`,JSON.stringify(summary,null,2));
console.log(JSON.stringify({...summary,results:undefined}));
