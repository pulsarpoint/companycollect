"""Serializable inputs, checkpoints, and human requests for the experiment."""

from dataclasses import dataclass, field
from typing import Literal


@dataclass
class DomainInput:
    domain: str
    challenge_rounds: int
    human_wait_seconds: float
    verification_seconds: float


@dataclass
class Action:
    id: str
    kind: Literal["crawl", "search"]
    url: str
    checkpoint: int = 0
    verified_checkpoint: int | None = None


@dataclass
class ActionInput:
    domain: str
    action: Action
    challenge_rounds: int


@dataclass
class ActionResult:
    status: Literal["done", "needs_human"]
    checkpoint: int
    observations: list[str]
    discovered_actions: list[Action]
    reason: str | None = None


@dataclass
class HumanRequest:
    id: str
    action_id: str
    blocked_url: str
    reason: str
    deadline: float
    session_id: str | None = None
    session_deadline: float | None = None
    activation_count: int = 0


@dataclass
class Verification:
    request_id: str
    session_id: str
    success: bool


@dataclass
class DomainState:
    domain: str = ""
    status: str = "starting"
    pending_actions: list[Action] = field(default_factory=list)
    completed_action_ids: list[str] = field(default_factory=list)
    observations: list[str] = field(default_factory=list)
    pending_request: HumanRequest | None = None
    human_deadline: float | None = None
    wait_count: int = 0
    verified_count: int = 0
    ignored_verifications: int = 0
    reason: str | None = None
    artifact: str | None = None


@dataclass
class Publication:
    run_id: str
    outcome: Literal["complete", "incomplete", "error"]
    state: DomainState
