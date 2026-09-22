import 'dotenv/config';
import {createClient} from '@clickhouse/client';
import {readFileSync,writeFileSync} from 'node:fs';
const folder='output/session-deployment-20260921';
const log=readFileSync(`${folder}/dagster-deploy.log`,'utf8');
if(!/failed=0/.test(log)||!log.includes('Require hot-sync to preserve'))throw new Error('Dagster deployment not finished');
const client=createClient({url:process.env.CLICKHOUSE_URL,username:process.env.CLICKHOUSE_USER,password:process.env.CLICKHOUSE_PASSWORD,database:'corpscout'});
const names=['company_brave_search_latest','website_crawl_results_s3','website_full_crawl_results_current','website_jobs_crawl_results_current','website_site_info_results_current'];
const dropped=[];
for(const name of names){const rows=await(await client.query({query:'SELECT engine FROM system.tables WHERE database=\'corpscout\' AND name={name:String}',query_params:{name},format:'JSONEachRow'})).json<any>();if(!rows.length)continue;if(rows[0].engine!=='View')throw new Error(`Refusing to remove non-view ${name}`);await client.command({query:`DROP VIEW corpscout.${name}`});dropped.push(name);}
writeFileSync(`${folder}/schema-cleanup.json`,JSON.stringify({dropped},null,2));console.log(JSON.stringify({dropped}));await client.close();
