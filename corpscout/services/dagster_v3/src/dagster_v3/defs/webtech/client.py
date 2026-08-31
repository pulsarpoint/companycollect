from typing import Any

import dagster as dg
import requests
from pydantic import Field

from dagster_v3.defs.webtech.models import (
    CandidateManifestReference,
    RemoteScanPollResponse,
    RemoteScanSnapshot,
)


class UnknownRemoteScanError(RuntimeError):
    """The scanner restarted and no longer has the scan in memory."""


class WebtechApiUnavailableError(RuntimeError):
    """A transient transport failure prevented a scanner API request."""


class WebtechApiResource(dg.ConfigurableResource):
    """Authenticated client for the remote Webtech workstation."""

    base_url: str
    api_token: str = Field(repr=False)

    def submit(self, manifest: CandidateManifestReference) -> RemoteScanSnapshot:
        payload = {
            "schema_version": 1,
            "crawl_id": manifest.crawl_id,
            "partition_key": manifest.partition_key,
            "candidate_manifest_uri": manifest.uri,
            "candidate_manifest_sha256": manifest.sha256,
            "detector_version": manifest.detector_version,
        }
        response = self._request(
            "POST",
            "/v1/scans",
            json=payload,
            timeout=(10, 120),
        )
        return RemoteScanSnapshot.model_validate(response.json())

    def poll(
        self,
        scan_id: str,
        *,
        after_event: int,
        wait_seconds: int = 30,
    ) -> RemoteScanPollResponse:
        response = self._request(
            "GET",
            f"/v1/scans/{scan_id}",
            params={
                "after_event": after_event,
                "wait_seconds": wait_seconds,
            },
            timeout=(10, wait_seconds + 15),
            allow_not_found=True,
        )
        return RemoteScanPollResponse.model_validate(response.json())

    def cancel(self, scan_id: str) -> RemoteScanSnapshot:
        response = self._request(
            "POST",
            f"/v1/scans/{scan_id}/cancel",
            timeout=(10, 30),
            allow_not_found=True,
        )
        return RemoteScanSnapshot.model_validate(response.json())

    def _request(
        self,
        method: str,
        path: str,
        *,
        timeout: tuple[int, int],
        allow_not_found: bool = False,
        **kwargs: Any,
    ) -> requests.Response:
        try:
            response = requests.request(
                method,
                f"{self.base_url.rstrip('/')}{path}",
                headers={
                    "Authorization": f"Bearer {self.api_token}"
                },
                timeout=timeout,
                **kwargs,
            )
        except requests.RequestException as error:
            raise WebtechApiUnavailableError(
                f"Webtech API request failed: {method} {path}: {error}"
            ) from error
        if allow_not_found and response.status_code == 404:
            raise UnknownRemoteScanError(path)
        if not response.ok:
            detail = _response_detail(response)
            raise RuntimeError(
                f"Webtech API returned HTTP {response.status_code} for "
                f"{method} {path}: {detail}"
            )
        return response


def _response_detail(response: requests.Response) -> str:
    try:
        body = response.json()
    except requests.JSONDecodeError:
        return response.text[:500]
    if isinstance(body, dict) and isinstance(body.get("detail"), str):
        return body["detail"][:500]
    return str(body)[:500]
