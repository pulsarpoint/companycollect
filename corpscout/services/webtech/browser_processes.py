"""Locate and kill the Chromium processes behind one CloakBrowser profile.

CloakBrowser does not expose the browser process it launches, but every
Chromium process of a persistent context carries ``--user-data-dir=<profile>``
on its command line and each scanner context owns a unique profile directory.
That argument is the handle used to tear a wedged browser down from outside.
"""

import os
import signal
from collections.abc import Callable
from pathlib import Path

PROC_ROOT = Path("/proc")

type KillFunction = Callable[[int, int], None]


def find_profile_pids(
    profile_directory: str,
    *,
    proc_root: Path = PROC_ROOT,
) -> tuple[int, ...]:
    """Return every PID whose command line names ``profile_directory`` exactly."""
    marker = f"--user-data-dir={profile_directory}".encode()
    try:
        entries = list(proc_root.iterdir())
    except OSError:
        return ()
    pids: list[int] = []
    for entry in entries:
        if not entry.name.isdigit():
            continue
        try:
            arguments = (entry / "cmdline").read_bytes().split(b"\0")
        except OSError:
            continue
        if marker in arguments:
            pids.append(int(entry.name))
    return tuple(sorted(pids))


def kill_profile_processes(
    profile_directory: str,
    *,
    proc_root: Path = PROC_ROOT,
    kill: KillFunction = os.kill,
) -> tuple[int, ...]:
    """SIGKILL every process of ``profile_directory`` and return the PIDs hit."""
    killed: list[int] = []
    for pid in find_profile_pids(profile_directory, proc_root=proc_root):
        try:
            kill(pid, signal.SIGKILL)
        except ProcessLookupError:
            continue
        killed.append(pid)
    return tuple(killed)
