"""Isolated browser API + Basic info crawl on the crawler host."""
import asyncio
import json
import os
import sys
from pathlib import Path
from uuid import uuid4

import httpx
from dotenv import dotenv_values

from browser_service.api import create_app
from browser_service.browser_sessions import BrowserPoolSettings
from browser_service.runtime import BrowserService
from company_research.browser_client import BrowserLeaseClient
from company_research.crawl import crawl_company
from company_research.models import ResearchConfig

async def main():
    key = dotenv_values(stream=sys.stdin)['DEEPSEEK']
    root = Path('/tmp/corpscout-redirect-validation-20260921/live')
    service = BrowserService(root / 'browser', settings=BrowserPoolSettings(headless_count=1, headed_count=0), idle_timeout=120)
    await service.start()
    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(create_app(service, api_token='validation', deepseek_api_key=None)), base_url='http://browser-test', headers={'Authorization':'Bearer validation'}, timeout=90) as http:
            for domain in ['aga.se','advokatsamfundet.se']:
                async with BrowserLeaseClient(http).lease(identifier=uuid4().hex, request_id=f'redirect-validation-{domain.replace(".","-")}', domain=domain, headless=True) as lease:
                    result = await crawl_company(f'https://{domain}/', output_dir=root / domain, site_info=True, api='deepseek', api_key=key, config=ResearchConfig(model='deepseek-flash', provider=None, max_pages=1, max_model_calls=3, page_attempts=1, web_search=False, page_timeout_seconds=45), browser_client=lease)
                    print(json.dumps({'domain':domain,'status':result['status'],'input_url':result['input_url'],'site_url':result['site_url'],'site_info':result['site_info'],'pages':result['pages'],'usage':result['usage'],'errors':result['errors']},ensure_ascii=False),flush=True)
    finally:
        await service.close()

asyncio.run(main())
