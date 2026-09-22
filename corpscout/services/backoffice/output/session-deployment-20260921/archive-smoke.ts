import 'dotenv/config';
import {writeFileSync} from 'node:fs';
import {chQuery} from '../../app/lib/clickhouse.server';
import {readCrawlArchive} from '../../app/lib/crawl-results.server';
const [row]=await chQuery<any>("SELECT domain,s3_path FROM website_site_info_results_latest_success WHERE domain='aga.se' ORDER BY finished_at DESC LIMIT 1");
if(!row)throw new Error('Existing crawl result not found');
const archive=await readCrawlArchive(row.s3_path);
const output={domain:archive.domain,request_id:archive.request_id,attempt:archive.attempt,status:archive.status,schema:archive.schema_version,jsonBytes:archive.result_json.length};
writeFileSync('output/session-deployment-20260921/archive-smoke.json',JSON.stringify(output,null,2));console.log(JSON.stringify(output));
