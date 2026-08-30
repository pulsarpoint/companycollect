from datetime import UTC, datetime

from dagster_v3.defs.common.ats_source import (
    BoardDefinition,
    BoardPayload,
    get_json,
)

ASHBY_API_BASE_URL = "https://api.ashbyhq.com/posting-api/job-board"

BOARDS = (
    BoardDefinition(
        provider_board_id="ashby:lovable",
        board_token="lovable",
        display_name="Lovable",
        company_id="5595061739",
        country_code="SE",
        board_url="https://jobs.ashbyhq.com/lovable",
        evidence_url="https://lovable.dev/careers",
        configured_at=datetime(2026, 8, 31, tzinfo=UTC),
    ),
)


def fetch_board(board: BoardDefinition) -> BoardPayload:
    url = f"{ASHBY_API_BASE_URL}/{board.board_token}"
    payload = get_json(url, params={"includeCompensation": "true"})
    jobs = payload.get("jobs") if isinstance(payload, dict) else None
    if not isinstance(jobs, list):
        raise ValueError(f"Ashby board {board.provider_board_id} did not return jobs")
    return BoardPayload(payload=payload, source_url=url, job_count=len(jobs))
