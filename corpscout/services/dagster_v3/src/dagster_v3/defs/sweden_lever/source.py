from datetime import UTC, datetime

from dagster_v3.defs.common.ats_source import (
    BoardDefinition,
    BoardPayload,
    get_json,
)

LEVER_EU_API_BASE_URL = "https://api.eu.lever.co/v0/postings"
PAGE_SIZE = 100

BOARDS = (
    BoardDefinition(
        provider_board_id="lever:seb",
        board_token="seb",
        display_name="SEB",
        company_id="5020329081",
        country_code="SE",
        board_url="https://jobs.eu.lever.co/seb",
        evidence_url="https://sebgroup.com/career/find-your-new-job",
        configured_at=datetime(2026, 8, 31, tzinfo=UTC),
    ),
)


def fetch_board(board: BoardDefinition) -> BoardPayload:
    url = f"{LEVER_EU_API_BASE_URL}/{board.board_token}"
    jobs: list[object] = []
    skip = 0
    while True:
        page = get_json(
            url,
            params={"mode": "json", "skip": skip, "limit": PAGE_SIZE},
        )
        if not isinstance(page, list):
            raise ValueError(
                f"Lever board {board.provider_board_id} did not return a job list"
            )
        jobs.extend(page)
        if len(page) < PAGE_SIZE:
            break
        skip += PAGE_SIZE
    return BoardPayload(payload=jobs, source_url=url, job_count=len(jobs))
