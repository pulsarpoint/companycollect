"""Schedule cron contract.

All jobs share one Postgres and one ClickHouse, so no two schedules may fire
at the same instant. Day-of-month and day-of-week fields can coincide (a
"monthly on the 5th" and a "weekly on Monday" collide whenever the 5th is a
Monday), so the enforced rule is conservative: every schedule must use a
unique ``(minute, hour)`` pair, and no wildcard/step minute fields.
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


def test_every_schedule_fires_on_a_unique_minute_hour_pair() -> None:
    by_slot: dict[tuple[str, str], list[str]] = defaultdict(list)
    for name, cron in _cron_schedules():
        minute, hour = cron.split()[:2]
        assert minute.isdigit(), (
            f"{name}: cron {cron!r} must pin an explicit minute (no */steps)"
        )
        by_slot[(minute, hour)].append(f"{name} ({cron})")

    collisions = {
        slot: names for slot, names in by_slot.items() if len(names) > 1
    }
    assert collisions == {}, f"Schedules sharing a (minute, hour) slot: {collisions}"
