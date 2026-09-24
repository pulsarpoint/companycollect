"""One domain, dynamically discovered actions, and durable verification waits."""

from dataclasses import replace
from datetime import timedelta
from typing import Literal

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError, ApplicationError

with workflow.unsafe.imports_passed_through():
    from domain_prototype.activities import LocalResults, simulate_action
    from domain_prototype.models import (
        Action,
        ActionInput,
        DomainInput,
        DomainState,
        HumanRequest,
        Publication,
        Verification,
    )


@workflow.defn
class DomainWorkflow:
    def __init__(self) -> None:
        self.state = DomainState()
        self.verification_seconds = 0.0
        self.verified_session: str | None = None

    @workflow.run
    async def run(self, request: DomainInput) -> DomainState:
        if (
            not request.domain
            or request.challenge_rounds < 0
            or request.human_wait_seconds <= 0
            or request.verification_seconds <= 0
        ):
            raise ApplicationError("Invalid domain workflow input", non_retryable=True)
        self.verification_seconds = request.verification_seconds
        self.state.domain = request.domain
        self.state.status = "running"
        self.state.pending_actions = [
            Action("homepage", "crawl", f"https://{request.domain}/")
        ]
        seen_actions = {"homepage"}

        while self.state.pending_actions:
            action = self.state.pending_actions[0]
            try:
                result = await workflow.execute_activity(
                    simulate_action,
                    ActionInput(request.domain, action, request.challenge_rounds),
                    start_to_close_timeout=timedelta(seconds=30),
                    retry_policy=RetryPolicy(maximum_attempts=3),
                )
            except ActivityError:
                self.state.reason = f"Action failed: {action.id} (see Temporal history)"
                break

            action.checkpoint = result.checkpoint
            self.state.observations.extend(result.observations)
            for discovered in result.discovered_actions:
                if discovered.id not in seen_actions:
                    seen_actions.add(discovered.id)
                    self.state.pending_actions.append(discovered)

            if result.status == "needs_human":
                if self.state.human_deadline is None:
                    self.state.human_deadline = (
                        workflow.time() + request.human_wait_seconds
                    )
                self.state.wait_count += 1
                self.state.pending_request = HumanRequest(
                    id=f"{workflow.info().run_id}:{self.state.wait_count}",
                    action_id=action.id,
                    blocked_url=action.url,
                    reason=result.reason or "Human verification required",
                    deadline=self.state.human_deadline,
                )
                if not await self._wait_for_verification():
                    self.state.reason = "Human assistance deadline expired"
                    break
                action.verified_checkpoint = action.checkpoint
                self.state.pending_request = None
                self.state.status = "running"
                continue

            self.state.completed_action_ids.append(action.id)
            self.state.pending_actions.pop(0)

        outcome: Literal["complete", "incomplete", "error"] = "complete"
        if self.state.pending_actions:
            outcome = "incomplete" if self.state.observations else "error"
        self.state.pending_request = None
        self.state.status = "publishing"
        self.state.artifact = await workflow.execute_activity_method(
            LocalResults.publish_result,
            Publication(
                workflow.info().run_id, outcome, replace(self.state, status=outcome)
            ),
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=RetryPolicy(
                initial_interval=timedelta(seconds=1),
                maximum_interval=timedelta(seconds=5),
                non_retryable_error_types=["ValueError"],
            ),
        )
        self.state.status = outcome
        return self.state

    async def _wait_for_verification(self) -> bool:
        request = self.state.pending_request
        assert request is not None
        self.verified_session = None
        while workflow.time() < request.deadline:
            self.state.status = "waiting"
            try:
                await workflow.wait_condition(
                    lambda: request.session_id is not None,
                    timeout=timedelta(seconds=request.deadline - workflow.time()),
                )
            except TimeoutError:
                return False

            assert request.session_deadline is not None
            self.state.status = "verifying"
            remaining = (
                min(request.session_deadline, request.deadline) - workflow.time()
            )
            if remaining > 0:
                try:
                    await workflow.wait_condition(
                        lambda: self.verified_session == request.session_id,
                        timeout=timedelta(seconds=remaining),
                    )
                    self.state.verified_count += 1
                    return True
                except TimeoutError:
                    workflow.logger.info("Verification session expired: %s", request.id)
            request.session_id = None
            request.session_deadline = None
        return False

    @workflow.update()
    def activate_verification(self, request_id: str) -> HumanRequest:
        """Create a simulated browser session only after explicit activation."""
        request = self.state.pending_request
        if (
            request is None
            or request.id != request_id
            or self.state.status not in {"waiting", "verifying"}
            or workflow.time() >= request.deadline
        ):
            raise ApplicationError("Verification request is no longer waiting")
        if request.session_id is not None:
            if (
                request.session_deadline is None
                or workflow.time() >= request.session_deadline
            ):
                raise ApplicationError(
                    "Verification session expired; refresh before activating again"
                )
            return request
        request.activation_count += 1
        request.session_id = f"{request.id}:session:{request.activation_count}"
        request.session_deadline = min(
            workflow.time() + self.verification_seconds, request.deadline
        )
        self.state.status = "verifying"
        return request

    @workflow.signal
    def verification_completed(self, verification: Verification) -> None:
        """A real integration would accept this only from its browser service."""
        request = self.state.pending_request
        if (
            request is None
            or request.id != verification.request_id
            or request.session_id != verification.session_id
            or request.session_deadline is None
            or workflow.time() >= min(request.deadline, request.session_deadline)
            or not verification.success
        ):
            self.state.ignored_verifications += 1
            return
        self.verified_session = verification.session_id

    @workflow.query
    def status(self) -> DomainState:
        return self.state
