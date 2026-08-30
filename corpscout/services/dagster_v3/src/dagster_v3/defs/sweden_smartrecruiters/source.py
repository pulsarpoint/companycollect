from datetime import UTC, datetime

from dagster_v3.defs.common.ats_source import (
    BoardDefinition,
    BoardPayload,
    get_json,
)

SMARTRECRUITERS_API_BASE_URL = "https://api.smartrecruiters.com/v1/companies"
PAGE_SIZE = 100

BOARDS = (
    BoardDefinition(
        provider_board_id="smartrecruiters:hmgroup",
        board_token="HMGroup",
        display_name="H&M Group Sweden",
        company_id="5560427220",
        country_code="SE",
        board_url="https://jobs.smartrecruiters.com/HMGroup",
        evidence_url="https://career.hm.com/se-en/",
        configured_at=datetime(2026, 8, 31, tzinfo=UTC),
    ),
)


def fetch_board(board: BoardDefinition) -> BoardPayload:
    postings_url = f"{SMARTRECRUITERS_API_BASE_URL}/{board.board_token}/postings"
    summaries: list[dict[str, object]] = []
    offset = 0
    while True:
        page = get_json(
            postings_url,
            params={"limit": PAGE_SIZE, "offset": offset, "country": "se"},
        )
        content = page.get("content") if isinstance(page, dict) else None
        if not isinstance(content, list):
            raise ValueError(
                f"SmartRecruiters board {board.provider_board_id} did not return content"
            )
        summaries.extend(content)
        total = int(page.get("totalFound", len(summaries)))
        if not content or len(summaries) >= total:
            break
        offset += len(content)

    details: list[object] = []
    for summary in summaries:
        posting_id = summary.get("id")
        if not isinstance(posting_id, str) or not posting_id:
            raise ValueError(
                f"SmartRecruiters board {board.provider_board_id} returned a posting without id"
            )
        details.append(get_json(f"{postings_url}/{posting_id}"))
    return BoardPayload(
        payload={"content": details},
        source_url=postings_url,
        job_count=len(details),
    )
