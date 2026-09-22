import {writeFileSync} from 'node:fs';
import {chQuery} from '../../app/lib/clickhouse.server';
import {startSavedCrawls} from '../../app/lib/crawl-inputs.server';
const rows = await chQuery<{domain: string}>('SELECT domain FROM corpscout.website_site_info_requests_current WHERE enabled ORDER BY priority DESC, domain ASC');
if (rows.length !== 50) throw new Error(`Expected 50 enabled inputs, found ${rows.length}`);
const batchId = crypto.randomUUID();
const settings = {intent:'start-inputs', crawl_type:'site_info', domains:JSON.stringify(rows.map(row=>row.domain)), batch_id:batchId,
  challenge_agent_model:'deepseek-flash', challenge_agent_max_runs:'3', api:'deepseek', model:'deepseek-flash', max_pages:'1', max_model_calls:'20',
  page_selection:'basic_info', max_in_flight:'3', refresh_interval_days:'30', force_refresh:'false'};
writeFileSync('output/basic-info-validation-20260920/submission.json', JSON.stringify(settings,null,2));
const form = new FormData(); for(const [k,v] of Object.entries(settings)) form.set(k,v);
const receipt = await startSavedCrawls(form);
writeFileSync('output/basic-info-validation-20260920/receipt.json', JSON.stringify(receipt,null,2));
console.log(JSON.stringify(receipt));
