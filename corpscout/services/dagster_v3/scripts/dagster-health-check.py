#!/usr/bin/env python
"""Health check for the corpscout Dagster instance.

Surfaces the operational failure modes that silently wedge the pipeline at scale —
none of these raise an error or show up as a failed run, so you only notice when
something "just stops":

  1. LEAKED pool slots — an op-concurrency pool slot still claimed by a run that
     already finished (SUCCESS/FAILURE/CANCELED) or vanished. A limit-1 pool then
     blocks every later run needing that pool in QUEUED forever. This is the
     exact bug that wedged exchange_rates_v2 (a crashed partitioned backfill run
     never released its slot). Pass --fix to free them.
  2. STUCK QUEUED runs — queued past --queued-mins. Usually the visible symptom of
     (1), but also catches max_concurrent_runs saturation or a daemon problem.
  3. STALE STARTED runs — started but with no new event for --idle-mins (a hung
     step or a zombie run worker that may itself be holding a slot).

Usage (env is bootstrapped to match scripts/dagster-dev.sh):
    uv run python scripts/dagster-health-check.py            # report only
    uv run python scripts/dagster-health-check.py --fix      # free leaked slots
    uv run python scripts/dagster-health-check.py --queued-mins 30 --idle-mins 45

Exits non-zero when any problem is found, so it composes with cron/CI alerting:
    */10 * * * *  cd .../dagster_v3 && uv run python scripts/dagster-health-check.py || notify ...
"""

from __future__ import annotations

import argparse
import os
from datetime import datetime, timezone, timedelta
from pathlib import Path

# Bootstrap the same env dagster-dev.sh exports so the script "just works".
ROOT = Path(__file__).resolve().parents[1]
os.environ.setdefault("DAGSTER_HOME", str(ROOT))
if not os.environ.get("DAGSTER_PG_URL"):
    env_file = ROOT / ".env"
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            if line.startswith("DAGSTER_PG_URL="):
                os.environ["DAGSTER_PG_URL"] = line.split("=", 1)[1].strip()
                break

from dagster import DagsterInstance, DagsterRunStatus, RunsFilter  # noqa: E402

TERMINAL = {
    DagsterRunStatus.SUCCESS,
    DagsterRunStatus.FAILURE,
    DagsterRunStatus.CANCELED,
}


def _aware(value: datetime | float | None) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value, tz=timezone.utc)
    return value if value.tzinfo else value.replace(tzinfo=timezone.utc)


def _age(since: datetime | None, now: datetime) -> str:
    if since is None:
        return "?"
    mins = int((now - since).total_seconds() // 60)
    return f"{mins // 60}h{mins % 60:02d}m" if mins >= 60 else f"{mins}m"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--fix", action="store_true", help="free leaked pool slots")
    ap.add_argument("--queued-mins", type=int, default=15)
    ap.add_argument("--idle-mins", type=int, default=30)
    args = ap.parse_args()

    instance = DagsterInstance.get()
    els = instance.event_log_storage
    now = datetime.now(timezone.utc)
    problems = 0

    # 1. Leaked pool slots — the silent killer.
    print("== concurrency pools ==")
    for key in sorted(els.get_concurrency_keys()):
        info = els.get_concurrency_info(key)
        holders = {s.run_id for s in info.claimed_slots}
        holders |= {p.run_id for p in info.pending_steps}
        leaked: list[tuple[str, str]] = []
        for run_id in holders:
            run = instance.get_run_by_id(run_id)
            if run is None or run.status in TERMINAL:
                leaked.append((run_id, run.status.value if run else "MISSING"))
        used = len(info.claimed_slots)
        flag = f"  <<< LEAKED x{len(leaked)}" if leaked else ""
        print(f"  {key}: {used}/{info.slot_count} slots used{flag}")
        for run_id, status in leaked:
            problems += 1
            if args.fix:
                els.free_concurrency_slots_for_run(run_id)
                print(f"      freed slot held by finished run {run_id} ({status})")
            else:
                print(f"      slot held by finished run {run_id} ({status}) — rerun with --fix")

    # 2. Stuck QUEUED runs.
    print("== queued runs ==")
    queued_cut = now - timedelta(minutes=args.queued_mins)
    queued = instance.get_run_records(filters=RunsFilter(statuses=[DagsterRunStatus.QUEUED]))
    stuck = [r for r in queued if (_aware(r.create_timestamp) or now) < queued_cut]
    if not stuck:
        print(f"  ok — none queued > {args.queued_mins}m ({len(queued)} queued)")
    for rec in stuck:
        problems += 1
        run = rec.dagster_run
        job = run.job_name
        print(f"  STUCK {rec.dagster_run.run_id} [{job}] queued {_age(_aware(rec.create_timestamp), now)}")

    # 3. Stale STARTED runs (possible zombies / hung steps).
    print("== started runs ==")
    idle_cut = now - timedelta(minutes=args.idle_mins)
    started = instance.get_run_records(filters=RunsFilter(statuses=[DagsterRunStatus.STARTED]))
    stale = [r for r in started if (_aware(r.update_timestamp) or now) < idle_cut]
    if not stale:
        print(f"  ok — none idle > {args.idle_mins}m ({len(started)} started)")
    for rec in stale:
        problems += 1
        run = rec.dagster_run
        last_event = _aware(rec.update_timestamp)
        print(
            f"  STALE {run.run_id} [{run.job_name}] started "
            f"{_age(_aware(rec.start_time), now)} ago, no event for {_age(last_event, now)}"
        )

    print(f"\n{'PROBLEMS: ' + str(problems) if problems else 'OK — no problems found'}")
    return 1 if problems else 0


if __name__ == "__main__":
    raise SystemExit(main())
