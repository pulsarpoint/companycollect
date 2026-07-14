"""Clear stuck exchange_rates_v2 backfill runs.

The `dagster job backfill -m dagster_v3.definitions ...` CLI tagged its runs with
code-location `dagster_v3.definitions`, but `dg dev` serves the project as
`dagster_v3`, so the daemon can never launch them and loops on
DagsterCodeLocationNotFoundError.

Run this with `dg dev` STOPPED and the instance env set:

    export DAGSTER_HOME="$PWD"
    export DAGSTER_PG_URL="$(grep -m1 '^DAGSTER_PG_URL=' .env | cut -d= -f2-)"
    uv run python scripts/clear-stuck-exchange-rates-backfill.py          # dry run
    uv run python scripts/clear-stuck-exchange-rates-backfill.py --apply  # delete

Dry-run by default; pass --apply to cancel backfills and delete the queued runs.
"""

import sys

from dagster._core.instance import DagsterInstance
from dagster._core.storage.dagster_run import DagsterRunStatus, RunsFilter

JOB = "exchange_rates_v2_job"


def main() -> None:
    apply = "--apply" in sys.argv
    inst = DagsterInstance.get()

    # 1. Cancel in-progress backfills that target the job.
    try:
        from dagster._core.execution.backfill import BulkActionStatus

        for bf in inst.get_backfills():
            targets_job = JOB in (
                f"{getattr(bf, 'job_name', '')}|{getattr(bf, 'partition_set_name', '')}"
            )
            if bf.status == BulkActionStatus.REQUESTED and targets_job:
                print(f"backfill {bf.backfill_id} ({bf.status.value}) -> CANCELED")
                if apply:
                    inst.update_backfill(bf.with_status(BulkActionStatus.CANCELED))
    except Exception as exc:  # noqa: BLE001 - best-effort, API varies by version
        print(f"(skipped backfill cancel: {exc})")

    # 2. Delete the stuck QUEUED runs for the job.
    runs = inst.get_runs(
        filters=RunsFilter(job_name=JOB, statuses=[DagsterRunStatus.QUEUED])
    )
    print(f"{len(runs)} QUEUED {JOB} run(s)")
    for run in runs:
        print(f"  {run.run_id}")
        if apply:
            inst.delete_run(run.run_id)

    print("APPLIED" if apply else "DRY-RUN — pass --apply to cancel/delete")


if __name__ == "__main__":
    main()
