import 'dotenv/config';
import {writeFileSync} from 'node:fs';
import {browserRequest,browserWebsocketUrl} from '../../app/lib/browser-service.server';
const id=crypto.randomUUID().replaceAll('-','');
let execution:string|undefined;
const request=async(path:string,method='GET',body?:object,exec?:string)=>{
 const response=await browserRequest(path,{method,headers:{'Content-Type':'application/json',...(exec?{'X-Browser-Execution-Id':exec}:{})},body:body?JSON.stringify(body):undefined,signal:AbortSignal.timeout(90000)});
 const data=await response.json();if(!response.ok)throw new Error(`${path}: ${response.status} ${JSON.stringify(data)}`);return data;
};
try{
 const first=await request('/v1/browser/sessions','POST',{id,requestId:`deployment-${id}`,domain:'example.com',headless:true});execution=first.executionId;
 const capture=await request('/v1/browser/extract','POST',{session:{id,executionId:execution},url:'https://example.com/',timeoutSeconds:30});
 if(capture.statusCode!==200||!capture.browserHtml.includes('Example Domain'))throw new Error('Headless capture failed');
 const closed=await request(`/v1/browser/sessions/${id}`,'DELETE',undefined,execution);execution=undefined;
 if(closed.state!=='closed'||closed.id!==id)throw new Error('Session was not preserved');
 const second=await request('/v1/browser/sessions','POST',{id,requestId:`deployment-headed-${id}`,domain:'example.com',headless:false});execution=second.executionId;
 if(second.executionId===first.executionId||!second.desktopAvailable)throw new Error('Headed reopen failed');
 const stale=await browserRequest(`/v1/browser/sessions/${id}`,{method:'DELETE',headers:{'X-Browser-Execution-Id':first.executionId}});
 if(stale.status!==409)throw new Error('Stale execution was not fenced');
 const headed=await request('/v1/browser/extract','POST',{session:{id,executionId:execution},url:'https://example.com/',timeoutSeconds:30});
 if(headed.statusCode!==200)throw new Error('Headed capture failed');
 const ticket=await request(`/v1/browser/sessions/${id}/browser-ticket`,'POST');
 const protocol=await new Promise<string>((resolve,reject)=>{const ws=new WebSocket(browserWebsocketUrl(ticket.websocket_path));ws.binaryType='arraybuffer';const timer=setTimeout(()=>{ws.close();reject(new Error('VNC timeout'));},10000);ws.onmessage=(event)=>{clearTimeout(timer);const prefix=Buffer.from(event.data).toString('ascii');ws.close();resolve(prefix)};ws.onerror=()=>{clearTimeout(timer);reject(new Error('VNC connection failed'))};});
 if(!protocol.startsWith('RFB '))throw new Error('Unexpected desktop protocol');
 await request(`/v1/browser/sessions/${id}`,'DELETE',undefined,execution);execution=undefined;
 const final=await request(`/v1/browser/sessions/${id}`);
 const result={sessionId:id,headlessStatus:capture.statusCode,headedStatus:headed.statusCode,newExecutionOnReopen:true,staleExecutionStatus:stale.status,vncProtocol:protocol.trim(),closedSessionRetained:final.state==='closed'};
 writeFileSync('output/session-deployment-20260921/browser-smoke.json',JSON.stringify(result,null,2));console.log(JSON.stringify(result));
}finally{if(execution)await request(`/v1/browser/sessions/${id}`,'DELETE',undefined,execution);}
