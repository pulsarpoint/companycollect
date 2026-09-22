import 'dotenv/config';
import {readFileSync,writeFileSync,existsSync} from 'node:fs';
import {createClient} from '@clickhouse/client';
const folder='output/session-deployment-20260921';
if(existsSync(`${folder}/schema-cutover.json`))throw new Error('Schema cutover receipt already exists');
const saved=JSON.parse(readFileSync(`${folder}/drained-checkpoint.json`,'utf8'));
if(saved.runStatus!=='CANCELED'||saved.browser.active.length)throw new Error('Workers were not drained');
const client=createClient({url:process.env.CLICKHOUSE_URL,username:process.env.CLICKHOUSE_USER,password:process.env.CLICKHOUSE_PASSWORD,database:'corpscout'});
const query=async(sql:string)=>(await client.query({query:sql,format:'JSONEachRow'})).json<any>();
const ddl=(file:string)=>readFileSync(`../../clickhouse/migrations/${file}`,'utf8').replace(/^--.*$/gm,'').split(';').map(s=>s.trim()).filter(Boolean);
const before=await query("SELECT name,uuid,create_table_query FROM system.tables WHERE database='corpscout' AND (name LIKE '%brave%' OR name LIKE 'website%results%') ORDER BY name");
const counts=await query("SELECT 'company_brave_search_results' name,count() rows FROM company_brave_search_results FINAL UNION ALL SELECT 'se_company_brave_domains',count() FROM se_company_brave_domains FINAL UNION ALL SELECT 'website_site_info_results',count() FROM website_site_info_results FINAL");
writeFileSync(`${folder}/schema-before.json`,JSON.stringify({before,counts},null,2),{mode:0o600});
const statements=[
 'DROP VIEW corpscout.se_company_brave_search_successes',
 'RENAME TABLE corpscout.se_company_brave_domains TO corpscout.se_company_brave_search_results_latest_success',
 ...ddl('000428_corpscout_brave_search_outcomes.up.sql').filter(s=>/^CREATE (MATERIALIZED )?VIEW/.test(s)),
 'RENAME TABLE corpscout.se_company_brave_domains_history TO corpscout.se_company_brave_search_results_s3_archive',
 ...ddl('000426_corpscout_website_crawl_s3_attempt_identity.up.sql').filter(s=>/^CREATE OR REPLACE VIEW/.test(s)),
 ...ddl('000430_corpscout_website_crawl_type_results.up.sql').filter(s=>/^CREATE VIEW/.test(s)).map(s=>s.replace('CREATE VIEW IF NOT EXISTS','CREATE OR REPLACE VIEW')),
 'GRANT SELECT, INSERT ON corpscout.se_company_brave_search_results_latest_success TO processing_publisher',
 'GRANT SELECT ON corpscout.company_brave_search_results_latest TO processing_publisher',
 'GRANT SELECT ON corpscout.se_company_brave_search_results_s3_archive TO processing_publisher',
];
writeFileSync(`${folder}/schema-statements.sql`,statements.join(';\n\n')+';\n');
for(const sql of statements){await client.command({query:sql});console.log(sql.split('\n')[0]);}
const after=await query("SELECT name,uuid,create_table_query FROM system.tables WHERE database='corpscout' AND (name LIKE '%brave%' OR name LIKE 'website%results%') ORDER BY name");
const countsAfter=await query("SELECT 'company_brave_search_results' name,count() rows FROM company_brave_search_results FINAL UNION ALL SELECT 'se_company_brave_search_results_latest_success',count() FROM se_company_brave_search_results_latest_success FINAL UNION ALL SELECT 'website_site_info_results',count() FROM website_site_info_results FINAL");
for(const row of counts){const name=row.name==='se_company_brave_domains'?'se_company_brave_search_results_latest_success':row.name;if(countsAfter.find((r:any)=>r.name===name)?.rows!==row.rows)throw new Error(`Row count changed: ${row.name}`);}
for(const row of before.filter((r:any)=>!r.create_table_query.startsWith('CREATE VIEW')&&!r.create_table_query.startsWith('CREATE MATERIALIZED VIEW'))){const name=row.name==='se_company_brave_domains'?'se_company_brave_search_results_latest_success':row.name;if(after.find((r:any)=>r.name===name)?.uuid!==row.uuid)throw new Error(`Physical identity changed: ${row.name}`);}
writeFileSync(`${folder}/schema-cutover.json`,JSON.stringify({after,countsAfter},null,2),{mode:0o600});
console.log(JSON.stringify({physicalTablesPreserved:true,countsAfter}));
await client.close();
