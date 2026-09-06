"""Locations of every artifact the lab reads or writes."""

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class DataDir:
    root: Path

    @property
    def sites_file(self) -> Path:
        return self.root / "sites.json"

    @property
    def candidates(self) -> Path:
        return self.root / "candidates"

    @property
    def gold(self) -> Path:
        return self.root / "gold"

    @property
    def prompts(self) -> Path:
        return self.root / "prompts"

    @property
    def runs(self) -> Path:
        return self.root / "runs"

    @property
    def results(self) -> Path:
        return self.root / "results"

    def candidate_file(self, domain: str) -> Path:
        return self.candidates / f"{domain}.json"

    def gold_file(self, domain: str) -> Path:
        return self.gold / f"{domain}.json"

    def run_dir(self, run_id: str) -> Path:
        return self.runs / run_id

    def result_file(
        self, run_id: str, domain: str, prompt_name: str, repeat: int
    ) -> Path:
        return self.run_dir(run_id) / domain / prompt_name / f"{repeat}.json"

    def scores_file(self, run_id: str) -> Path:
        return self.results / f"{run_id}.json"

    def report_file(self, run_id: str) -> Path:
        return self.results / f"{run_id}.md"
