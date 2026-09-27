"""Check live Jev ranking against frozen, already verified website evidence."""

import asyncio
import json
from pathlib import Path

import httpx
from dotenv import dotenv_values

from crawler_service.company_lookup import CompanyAssessment, accept_match
from crawler_service.jev import JevClient
from crawler_service.llm import ModelClient
from crawler_service.llm_profile import EncryptedLLMProfile
from crawler_service.models import ResearchConfig
from crawler_service.storage import write_json

ROOT = Path(__file__).resolve().parents[2]


async def main() -> None:
    profile = EncryptedLLMProfile.model_validate_json(
        Path("/tmp/company-lookup-jev-envelope.json").read_text()
    )
    key = profile.decrypt_api_key(dotenv_values(ROOT.parent / "backoffice/.env"))
    output = ROOT / "data/company-lookup-v2-jev-replay-20260927"
    cases = []
    async with httpx.AsyncClient() as http:
        for domain in ["jm.se", "avanza.se", "brinova.se"]:
            source = (
                ROOT / "data/company-lookup-local-20260927" / domain / "result.json"
            )
            saved = json.loads(source.read_text())
            budget = ModelClient(
                http, "", ResearchConfig(max_model_calls=3), output / domain
            )
            client = JevClient(http, key, profile.model, ["company_match"], budget)
            ranked = await client.rank_companies(saved["identity"], saved["candidates"])
            company, reason = accept_match(
                CompanyAssessment.model_validate(ranked["assessment"]),
                saved["candidates"],
                saved["identity"],
            )
            result = {
                "domain": domain,
                "source": str(source.relative_to(ROOT)),
                "model": profile.model,
                "ranking": ranked,
                "accepted_company_id": company["company_id"] if company else None,
                "validation": reason,
                "usage": budget.usage(),
            }
            write_json(output / domain / "result.json", result)
            cases.append(result)
            print(
                json.dumps(
                    {
                        k: result[k]
                        for k in ["domain", "accepted_company_id", "validation"]
                    }
                ),
                flush=True,
            )
    write_json(output / "summary.json", {"cases": cases})


if __name__ == "__main__":
    asyncio.run(main())
