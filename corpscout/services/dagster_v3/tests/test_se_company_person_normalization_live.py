import os
import uuid
from datetime import UTC, datetime

import pytest
from dotenv import load_dotenv
from openai import OpenAI

from dagster_v3.defs.company_people.normalization import (
    DraftPersonObservation,
    batch_company_observations,
    request_company_people,
)
from dagster_v3.defs.esef_filings.llm_enrichment import deepseek_settings

NOW = datetime(2026, 8, 19, 12, tzinfo=UTC)


def _observation(
    source: str,
    *,
    draft_number: int,
    source_value: dict[str, object],
) -> DraftPersonObservation:
    return DraftPersonObservation(
        draft_id=uuid.UUID(int=draft_number),
        source=source,
        source_record_uid=f"source-record-{draft_number}",
        fiscal_year=2025,
        source_observed_at=NOW,
        source_value=source_value,
    )


LIVE_CASES = (
    (
        "same person from Bolagsverket and ESEF",
        (
            _observation(
                "bolagsverket",
                draft_number=101,
                source_value={
                    "first_name": "David Gustaf",
                    "last_name": "Mindus",
                    "role_kind": "ceo",
                    "role_original": "Verkställande direktör",
                },
            ),
            _observation(
                "esef",
                draft_number=102,
                source_value={
                    "name": "David Mindus",
                    "role": "CEO",
                    "role_category": "chief_executive",
                    "organization": "AB Sagax",
                },
            ),
        ),
    ),
    (
        "two board members from two sources",
        (
            _observation(
                "esef",
                draft_number=201,
                source_value={
                    "name": "Anna Andersson",
                    "role": "Board member",
                    "role_category": "board_member",
                    "organization": "Example AB",
                },
            ),
            _observation(
                "wikidata",
                draft_number=202,
                source_value={
                    "name": "Erik Eriksson",
                    "description": "Swedish businessperson",
                    "role_property": "P3320",
                    "role_label": "board member",
                },
            ),
        ),
    ),
)


@pytest.mark.integration
@pytest.mark.skipif(
    os.getenv("RUN_DEEPSEEK_COMPANY_PEOPLE_TESTS") != "1",
    reason="set RUN_DEEPSEEK_COMPANY_PEOPLE_TESTS=1 to call DeepSeek",
)
@pytest.mark.parametrize(("case_name", "observations"), LIVE_CASES)
def test_deepseek_v4_flash_obeys_company_people_contract(
    case_name: str,
    observations: tuple[DraftPersonObservation, ...],
) -> None:
    del case_name
    load_dotenv()
    settings = deepseek_settings()
    assert settings.model == "deepseek-v4-flash"
    client = OpenAI(
        base_url=settings.base_url.rstrip("/"),
        api_key=settings.api_key,
        timeout=180,
        max_retries=2,
    )
    batch = batch_company_observations(
        observations,
        maximum_observations_per_request=50,
    )[0]

    result = request_company_people(
        client,
        company_id="5565200028",
        batch=batch,
        previous_profiles=(),
        model=settings.model,
    )

    returned_ids = {
        draft_id for person in result.response.people for draft_id in person.draft_ids
    }
    assert returned_ids == {observation.draft_id for observation in observations}
    assert all(person.name.strip() for person in result.response.people)
    assert result.model_name == "deepseek-v4-flash"
