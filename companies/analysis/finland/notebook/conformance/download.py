"""Download Finland raw source files directly to S3 (first asset).

URL constants live here (no separate urls module). Download functions are
added in Phase 3. Mirrors the existing source clients; copied, not imported.
"""

from __future__ import annotations

import requests

# --- Source URLs (Phase 2) ---------------------------------------------------
PRH_YTJ_COMPANIES_URL = "https://avoindata.prh.fi/opendata-ytj-api/v3/companies"
PRH_YTJ_DESCRIPTION_URL = "https://avoindata.prh.fi/opendata-ytj-api/v3/description"
PRH_XBRL_BASE_URL = "https://avoindata.prh.fi/opendata-xbrl-api/v3"
USER_AGENT = "corpscout-conformance/0.1 (finland)"

# prh_ytj code lists to fetch (code, lang), order per the source catalog.
CODE_LISTS: list[tuple[str, str]] = [
    ("REK", "en"), ("REK_KDI", "en"), ("VIRANOM", "en"), ("TLAJI", "en"),
    ("YRMU", "en"), ("STATUS3", "en"), ("KIELI", "en"),
]


def probe() -> dict[str, int]:
    """Return HTTP status for one probe request per source URL. Confirms the
    endpoints resolve before any bulk download is wired up (Phase 3)."""
    headers = {"User-Agent": USER_AGENT}
    statuses: dict[str, int] = {}
    r = requests.get(PRH_YTJ_COMPANIES_URL, params={"page": 1}, headers=headers, timeout=60)
    statuses["prh_ytj_companies"] = r.status_code
    r = requests.get(
        PRH_YTJ_DESCRIPTION_URL, params={"code": "STATUS3", "lang": "en"},
        headers=headers, timeout=60,
    )
    statuses["prh_ytj_description"] = r.status_code
    r = requests.get(
        f"{PRH_XBRL_BASE_URL}/all_financial_statements",
        params={"registeredDateStart": "2025-01-01", "registeredDateEnd": "2025-01-02", "page": 1},
        headers=headers, timeout=60,
    )
    statuses["prh_xbrl_discovery"] = r.status_code
    return statuses
