import 'dotenv/config';
import {writeFileSync} from 'node:fs';
import {browserRequest} from '../../app/lib/browser-service.server';
const id=crypto.randomUUID().replaceAll('-','');
const requestId=`redirect-probe-${id}`;
async function call(path:string,body?:unknown,method='POST') {
 const response=await browserRequest(path,{method,headers:{'Content-Type':'application/json'},...(body?{body:JSON.stringify(body)}:{}),signal:AbortSignal.timeout(90000)});
 return {status:response.status,body:await response.json()};
}
const reserved=await call('/v1/browser/sessions',{id,requestId,domain:'aga.se',headless:false});
console.log(JSON.stringify({reserved:reserved.status}));
if(reserved.status!==201)throw new Error('No test browser available');
try {
 for(const url of ['http://aga.se/','https://aga.se/','https://www.advokatsamfundet.se/']){
  const result=await call('/v1/browser/extract',{session:{id},url,browserHtml:true,timeoutSeconds:45,checkRobotsTxt:true});
  writeFileSync(`output/redirect-validation-20260921/probe-${new URL(url).hostname}-${new URL(url).protocol.slice(0,-1)}.json`,JSON.stringify(result,null,2));
  console.log(JSON.stringify({url,status:result.status,final_url:result.body.url,statusCode:result.body.statusCode,error:result.body.error,detail:result.body.detail,html_bytes:result.body.browserHtml?.length}));
 }
}finally{console.log(JSON.stringify({released:(await call(`/v1/browser/sessions/${id}`,undefined,'DELETE')).status}));}
