"""Test live Jev industry decisions on the same frozen local-model evidence."""

import asyncio
import json
from pathlib import Path

import httpx
from dotenv import dotenv_values

from crawler_service.company_industry import checked_industry_assessments
from crawler_service.company_lookup import (
    CompanyAssessment,
    IndustryAssessment,
    accept_match,
)
from crawler_service.jev import JevClient
from crawler_service.llm import ModelClient
from crawler_service.llm_profile import EncryptedLLMProfile
from crawler_service.models import ResearchConfig
from crawler_service.storage import write_json

ROOT = Path(__file__).resolve().parents[2]
OUTPUT = ROOT / "data/company-lookup-industry-v2-jev-20260927"


async def main() -> None:
    profile = EncryptedLLMProfile.model_validate_json(
        Path("/tmp/company-lookup-jev-envelope.json").read_text()
    )
    key = profile.decrypt_api_key(dotenv_values(ROOT.parent / "backoffice/.env"))
    results = []
    async with httpx.AsyncClient(timeout=180) as http:
        for domain in ["addtech.se", "jm.se", "neobo.se", "instalco.se"]:
            source = (
                ROOT / "data/company-lookup-industry-20260927" / domain / "result.json"
            )
            saved = json.loads(source.read_text())
            destination = OUTPUT / domain / "result.json"
            if destination.exists():
                results.append(json.loads(destination.read_text()))
                continue
            candidates = [
                {
                    **row,
                    "industries": [
                        code
                        for code in row["industries"]
                        if code["reference_status"] == "reference_consistent"
                    ],
                }
                for row in saved["candidates"]
            ]
            budget = ModelClient(
                http, "", ResearchConfig(max_model_calls=3), OUTPUT / domain
            )
            client = JevClient(http, key, profile.model, ["company_match"], budget)
            identity_candidates = [
                {key: value for key, value in row.items() if key != "industries"}
                for row in saved["candidates"]
            ]
            ranked = await client.rank_companies(saved["identity"], identity_candidates)
            assessment = CompanyAssessment.model_validate(ranked["assessment"])
            comparable = [
                {"company_id": row["company_id"], "industries": row["industries"]}
                for row in candidates
                if row["industries"]
            ]
            activities = [
                fact
                for fact in saved["identity"]
                if fact["kind"] == "business_activity"
            ]
            industry_checks = (
                await client.compare_company_industries(activities, comparable)
                if activities and comparable
                else []
            )
            checked = checked_industry_assessments(
                industry_checks,
                saved["candidates"],
                saved["identity"],
            )
            assessment.industry_checks = [
                IndustryAssessment.model_validate(
                    {key: row[key] for key in ("company_id", "status", "reasons")}
                )
                for row in checked
            ]
            matched, reason = accept_match(
                assessment, saved["candidates"], saved["identity"]
            )
            result = {
                "domain": domain,
                "source": str(source.relative_to(ROOT)),
                "model": profile.model,
                "ranking": ranked,
                "industry_assessments": checked,
                "accepted_company_id": matched["company_id"] if matched else None,
                "validation": reason,
                "usage": budget.usage(),
            }
            write_json(destination, result)
            results.append(result)
            print(
                json.dumps(
                    {
                        key: result[key]
                        for key in ("domain", "accepted_company_id", "validation")
                    }
                ),
                flush=True,
            )
    write_json(OUTPUT / "summary.json", {"cases": results})


if __name__ == "__main__":
    asyncio.run(main())
