"""Schedule cron contract.

All jobs share one Postgres and one ClickHouse, so no two schedules may fire
at the same instant. Day-of-month and day-of-week fields can coincide (a
"monthly on the 5th" and a "weekly on Monday" collide whenever the 5th is a
Monday), so the enforced rule is conservative: every schedule must use a
unique ``(minute, hour)`` pair, and no wildcard/step minute fields.

One exemption, for schedules that run asset CHECKS and materialise nothing.
The rule exists because a register refresh landing on a financial pipeline is
a real collision; a check that runs two counting aggregates is not, and some
questions -- has the translator queue drained yet -- are only answerable by
asking repeatedly. The exemption is verified rather than trusted: a named
schedule that acquires a materialisable asset fails the test below.
"""

from collections import defaultdict


def _cron_schedules():
    from dagster_v3.definitions import defs as load_defs

    repo = load_defs().get_repository_def()
    for schedule in repo.schedule_defs:
        crons = schedule.cron_schedule
        if isinstance(crons, str):
            crons = [crons]
        for cron in crons:
            yield schedule.name, cron


# Schedules permitted a step/wildcard minute. Each must select only checks --
# asserted by test_check_only_schedules_materialise_nothing.
CHECK_ONLY_SCHEDULES = {"translation_coverage_schedule"}


def test_check_only_schedules_materialise_nothing() -> None:
    """The exemption is only sound while these stay check-only.

    A schedule that gained an asset would run it every ten minutes against the
    shared databases, which is exactly what the cron contract exists to stop.
    """
    from dagster_v3.definitions import defs as load_defs

    repo = load_defs().get_repository_def()
    for schedule in repo.schedule_defs:
        if schedule.name not in CHECK_ONLY_SCHEDULES:
            continue
        job = repo.get_job(schedule.job.name)
        assert set(job.asset_layer.executable_asset_keys) == set(), (
            f"{schedule.name} runs on a step-minute cron and may only select "
            f"checks, but its job materialises "
            f"{sorted(k.to_user_string() for k in job.asset_layer.executable_asset_keys)}"
        )


def test_every_schedule_fires_on_a_unique_minute_hour_pair() -> None:
    by_slot: dict[tuple[str, str], list[str]] = defaultdict(list)
    for name, cron in _cron_schedules():
        if name in CHECK_ONLY_SCHEDULES:
            continue
        minute, hour = cron.split()[:2]
        assert minute.isdigit(), (
            f"{name}: cron {cron!r} must pin an explicit minute (no */steps)"
        )
        by_slot[(minute, hour)].append(f"{name} ({cron})")

    collisions = {
        slot: names for slot, names in by_slot.items() if len(names) > 1
    }
    assert collisions == {}, f"Schedules sharing a (minute, hour) slot: {collisions}"
