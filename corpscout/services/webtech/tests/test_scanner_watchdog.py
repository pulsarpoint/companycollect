import asyncio
import logging

import scanner
from models import WebtechCandidate, WebtechDomainResult
from scanner import (
    ExtensionReportRouter,
    LaunchedContext,
    WebtechScannerSettings,
    _process_watchdog,
    _scan_pages,
)

PROFILE = "/tmp/webtech-profile-1-test"


def _install_fake_kill(monkeypatch, on_kill=None) -> list[str]:
    calls: list[str] = []

    def kill(profile_directory: str) -> tuple[int, ...]:
        calls.append(profile_directory)
        if on_kill is not None:
            on_kill()
        return (4242,)

    monkeypatch.setattr(scanner, "kill_profile_processes", kill)
    return calls


def test_process_watchdog_kills_the_profile_when_the_block_overruns(
    monkeypatch, caplog
) -> None:
    caplog.set_level(logging.WARNING, logger="scanner")
    calls = _install_fake_kill(monkeypatch)

    async def run() -> None:
        async with _process_watchdog(PROFILE, 0.01):
            await asyncio.sleep(0.1)

    asyncio.run(run())

    assert calls == [PROFILE]
    assert any(
        "Killed wedged CloakBrowser processes" in record.getMessage()
        and PROFILE in record.getMessage()
        and "4242" in record.getMessage()
        for record in caplog.records
    )


def test_process_watchdog_stands_down_when_the_block_finishes_in_time(
    monkeypatch,
) -> None:
    calls = _install_fake_kill(monkeypatch)

    async def run() -> None:
        async with _process_watchdog(PROFILE, 0.05):
            await asyncio.sleep(0)
        await asyncio.sleep(0.1)

    asyncio.run(run())

    assert calls == []


class _AbsorbingPage:
    url = "https://stuck.example"

    def is_closed(self) -> bool:
        return False

    async def close(self) -> None:
        return None


class _AbsorbingContext:
    """Behave like Playwright 1.62: a cancelled call waits for the driver's abort ack."""

    def __init__(self, released: asyncio.Event) -> None:
        self.released = released
        self.page = _AbsorbingPage()

    async def new_page(self) -> _AbsorbingPage:
        try:
            await asyncio.sleep(3600)
        except asyncio.CancelledError:
            await self.released.wait()
            raise
        return self.page


def test_page_worker_is_released_when_the_watchdog_kills_a_wedged_browser(
    monkeypatch,
) -> None:
    monkeypatch.setattr(scanner, "PAGE_CLOSE_TIMEOUT_SECONDS", 0.01)
    monkeypatch.setattr(scanner, "WATCHDOG_GRACE_SECONDS", 0.05)
    candidate = WebtechCandidate(root_domain="stuck.example", harmonic_rank=1)
    settings = WebtechScannerSettings(
        headless=True,
        browser_count=1,
        pages_per_browser=1,
        domain_timeout_seconds=0.05,
        domains_per_context=1,
        context_launch_interval_seconds=0,
    )
    stored: list[WebtechDomainResult] = []

    async def run() -> tuple[WebtechDomainResult, ...]:
        released = asyncio.Event()
        calls = _install_fake_kill(monkeypatch, released.set)
        context = _AbsorbingContext(released)
        results = await asyncio.wait_for(
            _scan_pages(
                (LaunchedContext(context=context, profile_directory=PROFILE),),
                (candidate,),
                router=ExtensionReportRouter(asyncio.get_running_loop()),
                settings=settings,
                progress_callback=stored.append,
            ),
            timeout=5,
        )
        assert calls == [PROFILE]
        return results

    results = asyncio.run(run())

    assert [result.outcome for result in results] == ["hard_timeout"]
    assert results[0].timeout_stage == "page_creation"
    assert stored == list(results)
