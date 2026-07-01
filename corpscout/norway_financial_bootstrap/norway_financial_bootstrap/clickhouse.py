from __future__ import annotations

import os
from typing import Any

import clickhouse_connect

from norway_financial_bootstrap.candidates import FinancialCandidate

CLICKHOUSE_DATABASE = "corpscout"
NO_COMPANIES_TABLE = "no_companies"
NO_COMPANIES_QUERY = f"""
select
    toString(org_number) as org_number,
    toString(name) as legal_name,
    ifNull(toString(primary_website_url), '') as website,
    toString(last_submitted_accounts_year) as last_submitted_accounts_year
from {CLICKHOUSE_DATABASE}.{NO_COMPANIES_TABLE}
where is_active = true
  and last_submitted_accounts_year is not null
order by org_number
"""


def clickhouse_from_env() -> Any:
    return clickhouse_connect.get_client(
        host=_required_env("CLICKHOUSE_HOST"),
        port=_int_env("CLICKHOUSE_HTTP_PORT", 8123),
        username=os.environ.get("CLICKHOUSE_USER", "default"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", ""),
        database=CLICKHOUSE_DATABASE,
        secure=_bool_env("CLICKHOUSE_SECURE", False),
    )


def financial_candidates_from_clickhouse(client: Any) -> list[FinancialCandidate]:
    result = client.query(NO_COMPANIES_QUERY)
    return [
        FinancialCandidate(
            org_number=str(row[0]),
            legal_name=str(row[1] or ""),
            website=str(row[2] or ""),
            last_submitted_accounts_year=str(row[3]),
        )
        for row in result.result_rows
    ]


def _required_env(name: str) -> str:
    value = os.environ.get(name)
    if value is None or value.strip() == "":
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def _int_env(name: str, default: int) -> int:
    value = os.environ.get(name)
    if value is None or value.strip() == "":
        return default
    return int(value)


def _bool_env(name: str, default: bool) -> bool:
    value = os.environ.get(name)
    if value is None or value.strip() == "":
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}
