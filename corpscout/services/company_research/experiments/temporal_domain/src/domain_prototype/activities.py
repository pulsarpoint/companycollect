"""Simulated crawling plus real, retry-safe local result publication."""

import asyncio
import json
from dataclasses import asdict
from hashlib import sha256
from pathlib import Path
from tempfile import NamedTemporaryFile
from urllib.parse import urlencode

from temporalio import activity

from domain_prototype.models import Action, ActionInput, ActionResult, Publication


@activity.defn
async def simulate_action(request: ActionInput) -> ActionResult:
    """Discover further work at runtime; no website or search engine is contacted."""
    action = request.action
    if action.kind == "crawl":
        discovered = []
        if action.id == "homepage":
            discovered = [
                Action(
                    id="brave-search",
                    kind="search",
                    url="https://search.brave.com/search?"
                    + urlencode({"q": f"{request.domain} company relationships"}),
                ),
                Action(
                    id="contact",
                    kind="crawl",
                    url=f"https://{request.domain}/contact",
                ),
            ]
        return ActionResult(
            status="done",
            checkpoint=1,
            observations=[f"Simulated page collected: {action.url}"],
            discovered_actions=discovered,
        )

    observations = []
    cursor = action.checkpoint
    if action.verified_checkpoint == cursor:
        observations.append(f"Simulated search segment {cursor + 1} collected")
        cursor += 1
    if cursor < request.challenge_rounds:
        return ActionResult(
            status="needs_human",
            checkpoint=cursor,
            observations=observations,
            discovered_actions=[],
            reason=f"Simulated Brave verification at search segment {cursor + 1}",
        )

    return ActionResult(
        status="done",
        checkpoint=cursor,
        observations=observations + ["Simulated search discovered a partner page"],
        discovered_actions=[
            Action(
                id="discovered-partner",
                kind="crawl",
                url=f"https://{request.domain}/partners",
            )
        ],
    )


class LocalResults:
    """Keep publication outside the Workflow, as an S3 upload would be."""

    def __init__(self, directory: Path) -> None:
        self.directory = directory

    @activity.defn
    async def publish_result(self, publication: Publication) -> str:
        return await asyncio.to_thread(self._write, publication)

    def _write(self, publication: Publication) -> str:
        self.directory.mkdir(parents=True, exist_ok=True)
        key = sha256(publication.run_id.encode()).hexdigest()
        destination = self.directory / f"{key}.json"
        content = json.dumps(asdict(publication), indent=2, sort_keys=True) + "\n"
        # Retrying after a successful write returns the same artifact.
        if destination.exists():
            if destination.read_text(encoding="utf-8") != content:
                raise ValueError("Conflicting publication for the same workflow run")
            return str(destination.resolve())
        with NamedTemporaryFile(
            mode="w", encoding="utf-8", dir=self.directory, delete=False
        ) as file:
            temporary = Path(file.name)
            try:
                file.write(content)
                file.flush()
                temporary.replace(destination)
            finally:
                temporary.unlink(missing_ok=True)
        return str(destination.resolve())
