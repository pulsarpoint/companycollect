import 'dotenv/config';
import {browserRequest} from '../../app/lib/browser-service.server';
const server=await(await browserRequest('/v1/server')).json();
const saved=await(await browserRequest('/v1/browser-sessions')).json();
console.log(JSON.stringify({version:server.version,healthy:server.healthy,settings:server.settings,active:server.leases.filter((r:any)=>!r.ended_at).map((r:any)=>({id:r.id,session_id:r.session_id,state:r.state,domain:r.domain})),saved:saved.sessions.map((s:any)=>({id:s.id,state:s.state,headless:s.headless,route:s.route,label:s.label,pinned:s.pinned}))},null,2));
