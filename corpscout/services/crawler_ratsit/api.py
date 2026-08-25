import logging

import uvicorn
from fastapi import FastAPI, HTTPException
from playwright.async_api import Error as PlaywrightError
from pydantic import BaseModel

from rats import RATSIT_URL, crawl_ratsit_markdown


LOGGER = logging.getLogger(__name__)
PORT = 8000

app = FastAPI(title="Ratsit crawler")


class CrawlResponse(BaseModel):
    url: str
    markdown: str


@app.post("/rats_crawl", response_model=CrawlResponse)
async def crawl() -> CrawlResponse:
    """Crawl the fixed Ratsit company page in the local Chromium instance."""
    try:
        markdown = await crawl_ratsit_markdown()
    except (PlaywrightError, RuntimeError) as error:
        LOGGER.exception("Ratsit crawl failed")
        raise HTTPException(
            status_code=502,
            detail="Ratsit crawl failed; check the local Chromium service",
        ) from error

    return CrawlResponse(url=RATSIT_URL, markdown=markdown)


@app.post("/rats_crawl", response_model=CrawlResponse)
async def crawl() -> CrawlResponse:
    """Crawl the fixed Ratsit company page in the local Chromium instance."""
    try:
        markdown = await crawl_ratsit_markdown()
    except (PlaywrightError, RuntimeError) as error:
        LOGGER.exception("Ratsit crawl failed")
        raise HTTPException(
            status_code=502,
            detail="Ratsit crawl failed; check the local Chromium service",
        ) from error

    return CrawlResponse(url=RATSIT_URL, markdown=markdown)



if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=PORT)
