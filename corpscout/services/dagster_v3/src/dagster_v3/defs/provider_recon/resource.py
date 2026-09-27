"""Dagster resource for the provider-recon HTTP service on companycollect."""

import time
from collections.abc import Callable, Sequence

import dagster as dg
import requests

# Full tailnet name: bare hostnames break on the dagster host (memory: short hostnames).
DEFAULT_API_URL = "http://companycollect.taileb086.ts.net:8095"
TERMINAL_STATUSES = frozenset({"succeeded", "failed"})


class ProviderReconError(Exception):
    """The service refused, failed or did not finish a run."""


class ProviderReconResource(dg.ConfigurableResource):
    """Starts provider-recon collects and polls them to completion."""

    api_url: str = DEFAULT_API_URL
    request_timeout_s: float = 30.0

    def start_collect(self, providers: Sequence[str] = ()) -> dict:
        body = {"providers": list(providers)} if providers else {}
        response = requests.post(
            f"{self.api_url}/v1/collect", json=body, timeout=self.request_timeout_s
        )
        if response.status_code == 409:
            raise ProviderReconError(f"provider-recon is busy with run {response.json().get('run_id')}")
        if response.status_code != 202:
            raise ProviderReconError(f"collect refused: HTTP {response.status_code} {response.text}")
        return response.json()

    def get_run(self, run_id: str) -> dict:
        response = requests.get(
            f"{self.api_url}/v1/runs/{run_id}", timeout=self.request_timeout_s
        )
        if response.status_code != 200:
            raise ProviderReconError(f"run {run_id}: HTTP {response.status_code} {response.text}")
        return response.json()

    def wait_for_run(
        self,
        run_id: str,
        *,
        timeout_s: float = 3600,
        poll_s: float = 10,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.monotonic,
    ) -> dict:
        deadline = clock() + timeout_s
        while True:
            run = self.get_run(run_id)
            if run["status"] in TERMINAL_STATUSES:
                return run
            if clock() >= deadline:
                raise ProviderReconError(f"run {run_id} still running after {timeout_s:.0f}s")
            sleep(poll_s)
