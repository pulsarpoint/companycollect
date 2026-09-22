"""Integration tests against a real private Temporal server, not a mocked loop."""

import asyncio
import json
from collections.abc import Callable
from pathlib import Path
from uuid import uuid4

import pytest
from temporalio import activity
from temporalio.client import WorkflowHandle, WorkflowUpdateFailedError
from temporalio.common import WorkflowIDConflictPolicy
from temporalio.exceptions import ApplicationError, WorkflowAlreadyStartedError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Replayer, Worker

from domain_prototype.activities import LocalResults, simulate_action
from domain_prototype.models import (
    ActionInput,
    ActionResult,
    DomainInput,
    DomainState,
    Publication,
    Verification,
)
from domain_prototype.workflow import DomainWorkflow


@pytest.fixture(scope="module")
async def env():
    async with await WorkflowEnvironment.start_local() as environment:
        yield environment


@pytest.fixture
def task_queue() -> str:
    return f"domain-test-{uuid4()}"


@pytest.fixture
def results(tmp_path: Path) -> LocalResults:
    return LocalResults(tmp_path / "results")


@pytest.fixture
async def worker(env, task_queue, results):
    async with Worker(
        env.client,
        task_queue=task_queue,
        workflows=[DomainWorkflow],
        activities=[simulate_action, results.publish_result],
    ) as running:
        yield running


async def start_domain(
    env: WorkflowEnvironment,
    task_queue: str,
    *,
    challenges: int = 2,
    wait: float = 30,
    session: float = 10,
) -> WorkflowHandle[DomainWorkflow, DomainState]:
    return await env.client.start_workflow(
        DomainWorkflow.run,
        DomainInput("example.com", challenges, wait, session),
        id=f"domain-test:{uuid4()}",
        task_queue=task_queue,
    )


async def wait_for_state(
    handle: WorkflowHandle[DomainWorkflow, DomainState],
    predicate: Callable[[DomainState], bool],
) -> DomainState:
    async with asyncio.timeout(10):
        while True:
            state = await handle.query(DomainWorkflow.status)
            if predicate(state):
                return state
            await asyncio.sleep(0.02)


async def verify_request(handle, request):
    session = await handle.execute_update(
        DomainWorkflow.activate_verification, request.id
    )
    await handle.signal(
        DomainWorkflow.verification_completed,
        Verification(request.id, session.session_id, True),
    )
    return session


async def test_discovers_actions_and_publishes_without_waiting(env, task_queue, worker):
    handle = await start_domain(env, task_queue, challenges=0)
    state = await asyncio.wait_for(handle.result(), timeout=10)
    assert state.status == "complete"
    assert state.wait_count == 0
    assert state.completed_action_ids == [
        "homepage",
        "brave-search",
        "contact",
        "discovered-partner",
    ]
    assert not state.pending_actions
    artifact = json.loads(Path(state.artifact).read_text(encoding="utf-8"))
    assert artifact["outcome"] == "complete"
    assert artifact["state"]["status"] == "complete"
    assert artifact["state"]["observations"] == state.observations
    await Replayer(workflows=[DomainWorkflow]).replay_workflow(
        await handle.fetch_history()
    )


async def test_repeated_dynamic_waits_and_stale_callbacks(env, task_queue, worker):
    handle = await start_domain(env, task_queue)
    first = await wait_for_state(handle, lambda state: state.status == "waiting")
    assert first.completed_action_ids == ["homepage"]
    assert [action.id for action in first.pending_actions] == [
        "brave-search",
        "contact",
    ]
    assert first.pending_request.session_id is None

    # Verification before explicit activation must not unblock processing.
    await handle.signal(
        DomainWorkflow.verification_completed,
        Verification(first.pending_request.id, "unactivated", True),
    )
    await wait_for_state(handle, lambda state: state.ignored_verifications == 1)
    with pytest.raises(WorkflowUpdateFailedError):
        await handle.execute_update(DomainWorkflow.activate_verification, "wrong-id")
    active = await handle.execute_update(
        DomainWorkflow.activate_verification, first.pending_request.id
    )
    repeated = await handle.execute_update(
        DomainWorkflow.activate_verification, first.pending_request.id
    )
    assert active == repeated
    assert active.deadline == first.human_deadline

    await handle.signal(
        DomainWorkflow.verification_completed,
        Verification(active.id, active.session_id, False),
    )
    failed = await wait_for_state(
        handle, lambda state: state.ignored_verifications == 2
    )
    assert failed.status == "verifying"
    await handle.signal(
        DomainWorkflow.verification_completed,
        Verification(active.id, active.session_id, True),
    )
    second = await wait_for_state(
        handle, lambda state: state.wait_count == 2 and state.status == "waiting"
    )
    assert second.pending_request.id != first.pending_request.id
    assert second.human_deadline == first.human_deadline
    assert second.pending_actions[0].checkpoint == 1
    assert second.completed_action_ids == ["homepage"]

    await handle.signal(
        DomainWorkflow.verification_completed,
        Verification(active.id, active.session_id, True),
    )
    unchanged = await wait_for_state(
        handle, lambda state: state.ignored_verifications == 3
    )
    assert unchanged.status == "waiting"
    assert unchanged.verified_count == 1
    await verify_request(handle, second.pending_request)
    final = await asyncio.wait_for(handle.result(), timeout=10)
    assert final.status == "complete"
    assert final.verified_count == 2
    assert final.observations.count("Simulated search segment 1 collected") == 1
    assert final.observations.count("Simulated search segment 2 collected") == 1
    await Replayer(workflows=[DomainWorkflow]).replay_workflow(
        await handle.fetch_history()
    )


async def test_timeout_publishes_partial_result(env, task_queue, worker):
    handle = await start_domain(env, task_queue, wait=0.5)
    state = await asyncio.wait_for(handle.result(), timeout=10)
    assert state.status == "incomplete"
    assert state.reason == "Human assistance deadline expired"
    assert state.pending_request is None
    assert state.completed_action_ids == ["homepage"]
    assert state.verified_count == 0
    artifact = json.loads(Path(state.artifact).read_text(encoding="utf-8"))
    assert artifact["outcome"] == "incomplete"
    assert len(artifact["state"]["pending_actions"]) == 2
    await Replayer(workflows=[DomainWorkflow]).replay_workflow(
        await handle.fetch_history()
    )


async def test_expired_session_can_be_reactivated_without_resetting_deadline(
    env, task_queue, worker
):
    handle = await start_domain(env, task_queue, challenges=1, session=0.4)
    waiting = await wait_for_state(handle, lambda state: state.status == "waiting")
    active = await handle.execute_update(
        DomainWorkflow.activate_verification, waiting.pending_request.id
    )
    expired = await wait_for_state(
        handle,
        lambda state: (
            state.status == "waiting" and state.pending_request.activation_count == 1
        ),
    )
    assert expired.pending_request.session_id is None
    new = await handle.execute_update(
        DomainWorkflow.activate_verification, waiting.pending_request.id
    )
    assert new.session_id != active.session_id
    assert new.deadline == active.deadline
    await handle.signal(
        DomainWorkflow.verification_completed,
        Verification(active.id, active.session_id, True),
    )
    rejected = await wait_for_state(
        handle, lambda state: state.ignored_verifications == 1
    )
    assert rejected.verified_count == 0
    await handle.signal(
        DomainWorkflow.verification_completed,
        Verification(new.id, new.session_id, True),
    )
    assert (await asyncio.wait_for(handle.result(), timeout=10)).status == "complete"


async def test_activation_cannot_extend_domain_deadline(env, task_queue, worker):
    handle = await start_domain(env, task_queue, wait=0.7, session=100)
    state = await wait_for_state(handle, lambda state: state.status == "waiting")
    active = await handle.execute_update(
        DomainWorkflow.activate_verification, state.pending_request.id
    )
    assert active.session_deadline == active.deadline
    assert (await asyncio.wait_for(handle.result(), timeout=10)).status == "incomplete"


async def test_waiting_domain_releases_worker_capacity(env, task_queue, results):
    async with Worker(
        env.client,
        task_queue=task_queue,
        workflows=[DomainWorkflow],
        activities=[simulate_action, results.publish_result],
        max_concurrent_activities=1,
    ):
        blocked = await start_domain(env, task_queue, challenges=1)
        state = await wait_for_state(blocked, lambda state: state.status == "waiting")
        other = await start_domain(env, task_queue, challenges=0)
        assert (await asyncio.wait_for(other.result(), timeout=10)).status == "complete"
        assert (await blocked.query(DomainWorkflow.status)).status == "waiting"
        await verify_request(blocked, state.pending_request)
        assert (
            await asyncio.wait_for(blocked.result(), timeout=10)
        ).status == "complete"


async def test_worker_restart_recovers_wait_and_accepts_queued_signal(
    env, task_queue, results
):
    async with Worker(
        env.client,
        task_queue=task_queue,
        workflows=[DomainWorkflow],
        activities=[simulate_action, results.publish_result],
    ):
        handle = await start_domain(env, task_queue, challenges=1)
        before = await wait_for_state(handle, lambda state: state.status == "waiting")
        active = await handle.execute_update(
            DomainWorkflow.activate_verification, before.pending_request.id
        )

    # Temporal accepts the signal even with no worker alive to handle it.
    await handle.signal(
        DomainWorkflow.verification_completed,
        Verification(active.id, active.session_id, True),
    )
    async with Worker(
        env.client,
        task_queue=task_queue,
        workflows=[DomainWorkflow],
        activities=[simulate_action, results.publish_result],
    ):
        final = await asyncio.wait_for(handle.result(), timeout=15)
        assert final.status == "complete"
        assert final.human_deadline == before.human_deadline
        assert (
            final.observations.count("Simulated page collected: https://example.com/")
            == 1
        )


async def test_duplicate_domain_start_uses_existing_execution(env, task_queue, worker):
    handle = await start_domain(env, task_queue, challenges=1)
    state = await wait_for_state(handle, lambda state: state.status == "waiting")
    duplicate = await env.client.start_workflow(
        DomainWorkflow.run,
        DomainInput("example.com", 0, 30, 10),
        id=handle.id,
        task_queue=task_queue,
        id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING,
    )
    assert duplicate.result_run_id == handle.result_run_id
    with pytest.raises(WorkflowAlreadyStartedError):
        await env.client.start_workflow(
            DomainWorkflow.run,
            DomainInput("example.com", 0, 30, 10),
            id=handle.id,
            task_queue=task_queue,
            id_conflict_policy=WorkflowIDConflictPolicy.FAIL,
        )
    await verify_request(handle, state.pending_request)
    assert (await asyncio.wait_for(handle.result(), timeout=10)).status == "complete"


async def test_failure_without_observations_publishes_error(env, task_queue, results):
    @activity.defn(name="simulate_action")
    async def failed_action(request: ActionInput) -> ActionResult:
        raise ApplicationError("Simulated permanent crawl failure", non_retryable=True)

    async with Worker(
        env.client,
        task_queue=task_queue,
        workflows=[DomainWorkflow],
        activities=[failed_action, results.publish_result],
    ):
        handle = await start_domain(env, task_queue)
        final = await asyncio.wait_for(handle.result(), timeout=10)
        assert final.status == "error"
        assert not final.observations
        assert (
            json.loads(Path(final.artifact).read_text(encoding="utf-8"))["outcome"]
            == "error"
        )


async def test_publication_retry_after_write_does_not_repeat_crawl(
    env, task_queue, results
):
    calls: list[str] = []
    uploads: list[int] = []

    @activity.defn(name="simulate_action")
    async def recorded_action(request: ActionInput) -> ActionResult:
        calls.append(request.action.id)
        return await simulate_action(request)

    @activity.defn(name="publish_result")
    async def interrupted_publication(publication: Publication) -> str:
        path = await results.publish_result(publication)
        uploads.append(activity.info().attempt)
        if activity.info().attempt == 1:
            raise OSError("Simulated lost acknowledgment after artifact write")
        return path

    async with Worker(
        env.client,
        task_queue=task_queue,
        workflows=[DomainWorkflow],
        activities=[recorded_action, interrupted_publication],
    ):
        handle = await start_domain(env, task_queue, challenges=0)
        final = await asyncio.wait_for(handle.result(), timeout=10)
        assert final.status == "complete"
        assert calls == ["homepage", "brave-search", "contact", "discovered-partner"]
        assert uploads == [1, 2]
        assert len(list(results.directory.glob("*.json"))) == 1


async def test_server_restart_restores_persisted_wait(tmp_path, task_queue):
    database = str(tmp_path / "temporal.db")
    results = LocalResults(tmp_path / "results")
    async with await WorkflowEnvironment.start_local(
        dev_server_database_filename=database
    ) as first:
        async with Worker(
            first.client,
            task_queue=task_queue,
            workflows=[DomainWorkflow],
            activities=[simulate_action, results.publish_result],
        ):
            handle = await start_domain(first, task_queue, challenges=1, wait=120)
            before = await wait_for_state(
                handle, lambda state: state.status == "waiting"
            )
            identifier = handle.id

    async with await WorkflowEnvironment.start_local(
        dev_server_database_filename=database
    ) as second:
        async with Worker(
            second.client,
            task_queue=task_queue,
            workflows=[DomainWorkflow],
            activities=[simulate_action, results.publish_result],
        ):
            restored = second.client.get_workflow_handle_for(
                DomainWorkflow.run, identifier
            )
            after = await wait_for_state(
                restored, lambda state: state.status == "waiting"
            )
            assert after == before
            await verify_request(restored, after.pending_request)
            assert (
                await asyncio.wait_for(restored.result(), timeout=10)
            ).status == "complete"


async def test_deadline_expires_while_worker_is_offline(env, task_queue, results):
    async with Worker(
        env.client,
        task_queue=task_queue,
        workflows=[DomainWorkflow],
        activities=[simulate_action, results.publish_result],
    ):
        handle = await start_domain(env, task_queue, challenges=1, wait=0.6)
        waiting = await wait_for_state(handle, lambda state: state.status == "waiting")
        active = await handle.execute_update(
            DomainWorkflow.activate_verification, waiting.pending_request.id
        )
    await asyncio.sleep(0.7)
    await handle.signal(
        DomainWorkflow.verification_completed,
        Verification(active.id, active.session_id, True),
    )
    async with Worker(
        env.client,
        task_queue=task_queue,
        workflows=[DomainWorkflow],
        activities=[simulate_action, results.publish_result],
    ):
        state = await asyncio.wait_for(handle.result(), timeout=15)
        assert state.status == "incomplete"
        assert state.verified_count == 0
        assert state.completed_action_ids == ["homepage"]


async def test_transient_activity_failure_retries_without_duplicating_results(
    env, task_queue, results
):
    attempts: list[int] = []

    @activity.defn(name="simulate_action")
    async def flaky_action(request: ActionInput) -> ActionResult:
        if request.action.id == "homepage":
            attempts.append(activity.info().attempt)
            if activity.info().attempt == 1:
                raise OSError("Simulated temporary connection failure")
        return await simulate_action(request)

    async with Worker(
        env.client,
        task_queue=task_queue,
        workflows=[DomainWorkflow],
        activities=[flaky_action, results.publish_result],
    ):
        handle = await start_domain(env, task_queue, challenges=0)
        state = await asyncio.wait_for(handle.result(), timeout=10)
        assert state.status == "complete"
        assert attempts == [1, 2]
        assert (
            state.observations.count("Simulated page collected: https://example.com/")
            == 1
        )
