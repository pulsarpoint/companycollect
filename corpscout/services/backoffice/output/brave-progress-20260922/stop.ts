import {readFileSync,writeFileSync} from 'node:fs';
import {dagsterGraphqlUrl,runStatus} from '../../app/lib/dagster.server';
const [saved]=JSON.parse(readFileSync('output/brave-progress-20260922/original-runs.json','utf8'));
const run=await runStatus(saved.runId);
if(run.status!=='STARTED'||run.tags['brave/execution']!==saved.tags['brave/execution'])throw new Error('Run identity or state changed');
const response=await fetch(dagsterGraphqlUrl(),{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({query:'mutation($id:String!){terminateRun(runId:$id,terminatePolicy:SAFE_TERMINATE){__typename ... on TerminateRunSuccess{run{runId status}} ... on TerminateRunFailure{message} ... on PythonError{message}}}',variables:{id:run.runId}})}).then(r=>r.json());
writeFileSync('output/brave-progress-20260922/termination.json',JSON.stringify(response,null,2));
console.log(JSON.stringify(response));
