import {readFileSync,writeFileSync} from 'node:fs';
import {readCrawlArchive} from '../../app/lib/crawl-results.server';
const folder='output/redirect-deployment-20260921/site-info';
const report=JSON.parse(readFileSync(`${folder}/verification.json`,'utf8'));
const details=[];
for(const row of report.results){
 const archive=await readCrawlArchive(row.s3_path);
 const {crawl}=JSON.parse(archive.result_json);
 const first=crawl.pages[0];
 if(!first.redirects?.length || !first.navigation_attempts?.length)throw new Error(`Missing redirect provenance: ${row.domain}`);
 details.push({domain:row.domain,input_url:crawl.input_url,site_url:crawl.site_url,status:crawl.status,description:crawl.site_info.site_description,pages:crawl.pages.map((p:any)=>({requested_url:p.requested_url,source_url:p.source_url,redirects:p.redirects,navigation_attempts:p.navigation_attempts})),s3_path:row.s3_path});
}
writeFileSync(`${folder}/provenance.json`,JSON.stringify(details,null,2));
console.log(JSON.stringify(details.map(({description,...rest})=>rest)));
