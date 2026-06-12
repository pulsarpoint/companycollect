"""Custom AutomationCondition factories for Corpscout data pipelines.

``dg.AutomationCondition.eager()`` is an ``AndAutomationCondition`` composed
of five gates:

1. ``InLatestTimeWindowCondition``  – only the *latest* time-window partition
   is eligible. This blocks cascades for historical backfill partitions.
2. ``SinceCondition``               – the asset has newly become missing *or*
   a dep was newly updated.
3. ``~any_deps_missing``            – ALL dep partitions must exist. For an
   unpartitioned asset whose deps are partitioned this gate almost always
   blocks because the other monthly partitions are absent.
4. ``~any_deps_in_progress``        – no dep is still running.
5. ``~in_progress``                 – the asset itself is not already running.

Gates 1 and 3 are the problem for our PRH-XBRL cascade:

* ``statement_tables`` is partitioned by registration month. When a backfill
  produces a 2025-01-01 partition of ``raw_xml_documents``, gate 1 prevents
  the downstream ``statement_tables/2025-01-01`` from being requested because
  June 2026 is the latest window.

* ``financial_metrics`` is *unpartitioned* but depends on the partitioned
  ``statement_tables``. Gate 3 never passes as long as even one monthly
  partition of ``statement_tables`` is absent.

``eager_partition_cascade()`` removes gate 1 so that any upstream partition
update – not just the latest window – propagates downstream.

``eager_rollup_cascade()`` removes both gate 1 and gate 3 so that an
unpartitioned rollup asset is re-derived whenever *any* upstream partition
changes, regardless of whether all other partitions exist.
"""

import dagster as dg


def eager_partition_cascade() -> dg.AutomationCondition:
    """Eager condition for a *partitioned* asset that cascades from any
    upstream partition, not only the latest time-window.

    Removes the ``InLatestTimeWindowCondition`` gate from the standard
    ``eager()`` composite so that backfilled or historical partitions
    propagate through the asset graph.

    Use on assets like ``statement_tables`` where the source data (e.g.
    ``raw_xml_documents``) can be materialised for any historical month.
    """
    return dg.AutomationCondition.eager().without(
        dg.AutomationCondition.in_latest_time_window()
    )


def eager_rollup_cascade() -> dg.AutomationCondition:
    """Eager condition for an *unpartitioned* rollup asset whose deps are
    partitioned and will rarely be fully present.

    Removes two gates from the standard ``eager()`` composite:

    1. ``InLatestTimeWindowCondition`` – not meaningful for an unpartitioned
       asset and would block updates driven by historical partitions.
    2. ``~any_deps_missing`` – the unpartitioned asset should re-derive
       whenever *any* upstream partition changes, even if the majority of
       other partitions are still absent.

    Use on assets like ``financial_metrics`` that aggregate over all
    available partitions of a partitioned upstream asset.
    """
    return (
        dg.AutomationCondition.eager()
        .without(dg.AutomationCondition.in_latest_time_window())
        .without(~dg.AutomationCondition.any_deps_missing())
    )
