import dotenv from 'dotenv';
dotenv.config({path:'.env',quiet:true});
import {getUnifiedCompanyDomains,searchCompanyDomainReviewQueue} from '../../app/lib/company-domains.server';
import {loadSeCompanyDomains} from '../../app/lib/se-company-domains.server';
import {listWorkspaceDomains} from '../../app/lib/workspace-domains.server';
import {getCompanySection} from '../../app/lib/company-sections.server';
import {parseWorkspaceDomainFilters} from '../../app/lib/workspace-domains';
for(const [label,read] of [
 ['company header/reviews',()=>getUnifiedCompanyDomains('SE','5563029726')],
 ['company Domains tab',()=>loadSeCompanyDomains('5563029726')],
 ['company Domains section',()=>getCompanySection('SE','5563029726','domains')],
 ['review queue',()=>searchCompanyDomainReviewQueue('SE',{query:'5563029726'})],
 ['workspace domain filters',()=>listWorkspaceDomains(parseWorkspaceDomainFilters(new URLSearchParams('prefix=addtech&companies=with')), '')],
] as const){try{const data:any=await read();console.log(JSON.stringify({label,ok:true,rows:Array.isArray(data)?data.length:data.rows?.length??data.domains?.length,total:data.total}));}catch(e){console.log(JSON.stringify({label,ok:false,error:e instanceof Error?e.message:'Error'}));process.exitCode=1;}}
