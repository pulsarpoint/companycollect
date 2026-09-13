import signal
from pathlib import Path

from browser_processes import find_profile_pids, kill_profile_processes

PROFILE = "/tmp/webtech-profile-6-abc123"


def _write_process(proc_root: Path, pid: int, *args: str) -> None:
    process_dir = proc_root / str(pid)
    process_dir.mkdir()
    (process_dir / "cmdline").write_bytes("\0".join(args).encode() + b"\0")


def _fake_proc(tmp_path: Path) -> Path:
    proc_root = tmp_path / "proc"
    proc_root.mkdir()
    _write_process(proc_root, 100, "chrome", f"--user-data-dir={PROFILE}", "--headless")
    _write_process(
        proc_root,
        101,
        "chrome",
        "--type=renderer",
        f"--user-data-dir={PROFILE}",
    )
    _write_process(proc_root, 102, "chrome", f"--user-data-dir={PROFILE}-other")
    _write_process(proc_root, 103, "bash", "-c", f"echo --user-data-dir={PROFILE}")
    (proc_root / "104").mkdir()
    (proc_root / "self").mkdir()
    (proc_root / "self" / "cmdline").write_bytes(f"--user-data-dir={PROFILE}".encode())
    return proc_root


def test_find_profile_pids_matches_only_exact_user_data_dir_arguments(
    tmp_path: Path,
) -> None:
    proc_root = _fake_proc(tmp_path)

    assert find_profile_pids(PROFILE, proc_root=proc_root) == (100, 101)


def test_find_profile_pids_without_proc_filesystem_finds_nothing(
    tmp_path: Path,
) -> None:
    assert find_profile_pids(PROFILE, proc_root=tmp_path / "missing") == ()


def test_kill_profile_processes_sigkills_each_pid_and_skips_vanished_ones(
    tmp_path: Path,
) -> None:
    proc_root = _fake_proc(tmp_path)
    signals: list[tuple[int, int]] = []

    def kill(pid: int, signum: int) -> None:
        signals.append((pid, signum))
        if pid == 100:
            raise ProcessLookupError

    killed = kill_profile_processes(PROFILE, proc_root=proc_root, kill=kill)

    assert killed == (101,)
    assert signals == [(100, signal.SIGKILL), (101, signal.SIGKILL)]
