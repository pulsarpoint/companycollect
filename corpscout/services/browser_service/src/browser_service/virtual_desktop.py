"""Managed browser subprocesses, with an Xvfb/VNC desktop only when headed."""

import asyncio
import os
import signal
import socket
import sys
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from pathlib import Path
from uuid import uuid4

import httpx

from browser_service.capture import utc_now


@dataclass(frozen=True)
class VirtualDesktop:
    cdp_port: int
    vnc_port: int | None
    kind: str = "interactive"
    owner: str | None = None
    id: str = field(default_factory=lambda: uuid4().hex)
    started_at: str = field(default_factory=utc_now)


ACTIVE_DESKTOPS: dict[str, VirtualDesktop] = {}


@asynccontextmanager
async def open_virtual_browser(
    profile: Path | None = None,
    *,
    kind: str = "interactive",
    owner: str | None = None,
    headless: bool = False,
    proxy: str | None = None,
) -> AsyncIterator[VirtualDesktop]:
    with socket.socket() as cdp, socket.socket() as vnc:
        cdp.bind(("127.0.0.1", 0))
        vnc.bind(("127.0.0.1", 0))
        cdp_port = cdp.getsockname()[1]
        vnc_port = None if headless else vnc.getsockname()[1]
    process = await asyncio.create_subprocess_exec(
        *(
            []
            if headless
            else ["xvfb-run", "-a", "-s", "-screen 0 1440x1000x24 -nolisten tcp"]
        ),
        sys.executable,
        "-m",
        "browser_service.browser_process",
        "--cdp-port",
        str(cdp_port),
        *(["--headless"] if headless else ["--vnc-port", str(vnc_port)]),
        *(["--profile", str(profile)] if profile is not None else []),
        start_new_session=True,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL,
        env={**os.environ, "BROWSER_PROCESS_PROXY": proxy or ""},
    )
    desktop = VirtualDesktop(cdp_port, vnc_port, kind, owner)
    try:
        async with httpx.AsyncClient(timeout=1) as client:
            async with asyncio.timeout(30):
                while True:
                    if process.returncode is not None:
                        raise RuntimeError("The virtual browser could not start")
                    try:
                        response = await client.get(
                            f"http://127.0.0.1:{cdp_port}/json/version"
                        )
                        if response.status_code == 200:
                            if vnc_port is not None:
                                _, writer = await asyncio.open_connection(
                                    "127.0.0.1", vnc_port
                                )
                                writer.close()
                                await writer.wait_closed()
                            break
                    except (httpx.TransportError, OSError):
                        pass
                    await asyncio.sleep(0.1)
        if not headless:
            ACTIVE_DESKTOPS[desktop.id] = desktop
        yield desktop
    finally:
        ACTIVE_DESKTOPS.pop(desktop.id, None)
        try:
            os.killpg(process.pid, signal.SIGTERM)
            async with asyncio.timeout(5):
                await process.wait()
        except TimeoutError:
            os.killpg(process.pid, signal.SIGKILL)
            await process.wait()
        except ProcessLookupError:
            await process.wait()
