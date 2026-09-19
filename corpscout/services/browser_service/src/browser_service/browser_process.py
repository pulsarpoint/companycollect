"""A headed CloakBrowser and VNC server running inside xvfb-run."""

import argparse
import asyncio
import os
import signal
from pathlib import Path

from cloakbrowser import launch_async, launch_persistent_context_async


async def run(cdp_port: int, vnc_port: int, profile: Path | None = None) -> None:
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)
    processes = []
    try:
        processes.append(
            await asyncio.create_subprocess_exec(
                "openbox",
                "--sm-disable",
            )
        )
        processes.append(
            await asyncio.create_subprocess_exec(
                "x11vnc",
                "-display",
                os.environ["DISPLAY"],
                "-auth",
                os.environ["XAUTHORITY"],
                "-localhost",
                "-rfbport",
                str(vnc_port),
                "-forever",
                "-shared",
                "-nopw",
                "-noxdamage",
            )
        )
        args = [
            f"--remote-debugging-port={cdp_port}",
            "--remote-debugging-address=127.0.0.1",
            "--window-size=1440,900",
            "--fingerprint=1234"
        ]
        browser = (
            await launch_persistent_context_async(
                profile, headless=False, no_viewport=True, humanize=True, human_preset="careful", args=args
            )
            if profile is not None
            else await launch_async(headless=False, humanize=True, human_preset="careful", args=args)
        )
        try:
            await stop.wait()
        finally:
            await browser.close()
    finally:
        for process in processes:
            if process.returncode is None:
                process.terminate()
        await asyncio.gather(*(process.wait() for process in processes))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cdp-port", required=True, type=int)
    parser.add_argument("--vnc-port", required=True, type=int)
    parser.add_argument("--profile", type=Path)
    args = parser.parse_args()
    asyncio.run(run(args.cdp_port, args.vnc_port, args.profile))


if __name__ == "__main__":
    main()
