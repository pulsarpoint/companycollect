"""Overlay fetch: the public webappanalyzer catalog, pinned to one commit.

The run first resolves the repository's current HEAD SHA via the GitHub API,
then fetches every raw file pinned to that SHA — so the technologies,
categories, groups, and any icons all come from ONE consistent tree even if
the repository moves mid-run. The SHA is recorded as source_version on every
overlay-sourced row.

All HTTP goes through dlt's requests helper (retry/backoff on connection
errors and 429/5xx built in), never plain requests. Callers inject a fake
session in tests; nothing here is exercised live by the test suite.
"""

from typing import Any, Protocol

from dlt.sources.helpers import requests as dlt_requests
from requests import HTTPError

from dagster_v3.defs.technology_catalog import tables
from dagster_v3.defs.technology_catalog.catalog import (
    TECHNOLOGY_LETTERS,
    CatalogLayer,
    parse_categories,
    parse_groups,
)

OVERLAY_REPO = "enthec/webappanalyzer"
OVERLAY_HEAD_URL = f"https://api.github.com/repos/{OVERLAY_REPO}/commits/HEAD"
OVERLAY_RAW_BASE = f"https://raw.githubusercontent.com/{OVERLAY_REPO}"

REQUEST_TIMEOUT_SECONDS = 60


class HttpSession(Protocol):
    def get(self, url: str, *, timeout: float) -> Any: ...


def resolve_overlay_commit(session: HttpSession = dlt_requests) -> str:
    """The default branch's current commit SHA, used to pin every raw fetch."""
    response = session.get(OVERLAY_HEAD_URL, timeout=REQUEST_TIMEOUT_SECONDS)
    sha = str(response.json()["sha"])
    if not sha:
        raise ValueError(f"GitHub returned no commit SHA from {OVERLAY_HEAD_URL}")
    return sha


def overlay_raw_url(commit_sha: str, repo_path: str) -> str:
    return f"{OVERLAY_RAW_BASE}/{commit_sha}/{repo_path}"


def fetch_overlay_layer(
    commit_sha: str, session: HttpSession = dlt_requests
) -> CatalogLayer:
    """Fetch the full overlay catalog (technologies + categories + groups)."""
    technologies: dict[str, Any] = {}
    for letter in TECHNOLOGY_LETTERS:
        technologies.update(
            _get_json(session, commit_sha, f"src/technologies/{letter}.json")
        )
    return CatalogLayer(
        technologies=technologies,
        categories=parse_categories(
            _get_json(session, commit_sha, "src/categories.json")
        ),
        groups=parse_groups(_get_json(session, commit_sha, "src/groups.json")),
        source=tables.OVERLAY_SOURCE,
        source_version=commit_sha,
    )


def fetch_overlay_icon(
    commit_sha: str, icon_filename: str, session: HttpSession = dlt_requests
) -> bytes | None:
    """Icon bytes from the pinned tree, or None when the file does not exist.

    A 404 is a real state (a catalog entry naming an icon the repo dropped),
    not a transient failure — such technologies publish with no icon.
    """
    url = overlay_raw_url(commit_sha, f"src/images/icons/{icon_filename}")
    try:
        response = session.get(url, timeout=REQUEST_TIMEOUT_SECONDS)
    except HTTPError as exc:
        if exc.response is not None and exc.response.status_code == 404:
            return None
        raise
    return response.content


def _get_json(session: HttpSession, commit_sha: str, repo_path: str) -> dict[str, Any]:
    response = session.get(
        overlay_raw_url(commit_sha, repo_path), timeout=REQUEST_TIMEOUT_SECONDS
    )
    payload = response.json()
    if not isinstance(payload, dict):
        raise ValueError(f"{repo_path} at {commit_sha} is not a JSON object")
    return payload
