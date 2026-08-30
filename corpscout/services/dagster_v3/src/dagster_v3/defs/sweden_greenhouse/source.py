from datetime import UTC, datetime

from dagster_v3.defs.common.ats_source import (
    BoardDefinition,
    BoardPayload,
    get_json,
)

GREENHOUSE_API_BASE_URL = "https://boards-api.greenhouse.io/v1/boards"

BOARDS = (
    BoardDefinition(
        provider_board_id="greenhouse:mentimeter",
        board_token="mentimeter",
        display_name="Mentimeter",
        company_id="5568925506",
        country_code="SE",
        board_url="https://job-boards.eu.greenhouse.io/mentimeter",
        evidence_url="https://www.mentimeter.com/careers",
        configured_at=datetime(2026, 8, 31, tzinfo=UTC),
    ),
)


def fetch_board(board: BoardDefinition) -> BoardPayload:
    url = f"{GREENHOUSE_API_BASE_URL}/{board.board_token}/jobs"
    payload = get_json(url, params={"content": "true"})
    jobs = payload.get("jobs") if isinstance(payload, dict) else None
    if not isinstance(jobs, list):
        raise ValueError(
            f"Greenhouse board {board.provider_board_id} did not return a jobs list"
        )
    return BoardPayload(payload=payload, source_url=url, job_count=len(jobs))
