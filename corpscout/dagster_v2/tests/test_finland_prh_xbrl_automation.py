"""Regression tests for automation-condition cascade behaviour.

These tests verify that:
1. ``statement_tables`` (partitioned, deps raw_xml_documents) fires when a
   *non-latest* monthly partition of its parent materialises.
2. ``financial_metrics`` (unpartitioned, deps statement_tables) fires even
   though most partitions of statement_tables are still unmaterialised.

With plain ``dg.AutomationCondition.eager()`` both tests should FAIL because:
- eager() has an ``InLatestTimeWindowCondition`` gate (test 1 fails for old
  partitions like 2025-01-01 when evaluated in 2026).
- eager() has a ``~any_deps_missing`` gate (test 2 never fires while most
  monthly statement_tables partitions are absent).

After the fix (``lib.automation.eager_partition_cascade()`` and
``eager_rollup_cascade()``) both tests should PASS.
"""

import dagster as dg
import pytest

from dagster_corpscout.definitions import defs

# The asset-key path prefix used for all PRH-XBRL assets.
_PREFIX = ["sources", "finland", "prh_xbrl"]


def _key(name: str) -> dg.AssetKey:
    return dg.AssetKey([*_PREFIX, name])


def _materialize(instance: dg.DagsterInstance, name: str, partition: str | None = None) -> None:
    instance.report_runless_asset_event(
        dg.AssetMaterialization(asset_key=_key(name), partition=partition)
    )


# ---------------------------------------------------------------------------
# Test 1 – statement_tables cascade for a non-latest partition
# ---------------------------------------------------------------------------

def test_statement_tables_cascade_fires_for_non_latest_partition():
    """Materialising raw_xml_documents/2025-01-01 should trigger
    statement_tables/2025-01-01, even though 2025-01-01 is not the *latest*
    monthly window when evaluated in June 2026.
    """
    with dg.instance_for_test() as instance:
        # --- baseline tick (nothing materialised yet) ---
        baseline = dg.evaluate_automation_conditions(defs=defs, instance=instance)

        # Simulate the upstream partition materialising (e.g. after a backfill).
        _materialize(instance, "raw_xml_documents", "2025-01-01")

        # --- second tick, using cursor so "already handled" events are excluded ---
        result = dg.evaluate_automation_conditions(
            defs=defs, instance=instance, cursor=baseline.cursor
        )

        num = result.get_num_requested(_key("statement_tables"))
        assert num > 0, (
            f"Expected statement_tables to be requested after raw_xml_documents/2025-01-01 "
            f"materialised, but get_num_requested returned {num}. "
            f"This indicates the in_latest_time_window gate is blocking the cascade."
        )

        requested = result.get_requested_partitions(_key("statement_tables"))
        assert "2025-01-01" in requested, (
            f"Expected partition '2025-01-01' to be requested for statement_tables, "
            f"got: {requested}"
        )


# ---------------------------------------------------------------------------
# Test 2 – financial_metrics cascade despite missing sibling partitions
# ---------------------------------------------------------------------------

def test_financial_metrics_cascade_fires_despite_missing_sibling_partitions():
    """After statement_tables/2025-01-01 materialises, financial_metrics
    (unpartitioned) should be requested, even though all *other* monthly
    partitions of statement_tables are still unmaterialised.
    """
    with dg.instance_for_test() as instance:
        # --- baseline tick ---
        baseline = dg.evaluate_automation_conditions(defs=defs, instance=instance)

        # Simulate both parent and grandparent materialising for one partition.
        _materialize(instance, "raw_xml_documents", "2025-01-01")
        _materialize(instance, "statement_tables", "2025-01-01")

        # --- second tick ---
        result = dg.evaluate_automation_conditions(
            defs=defs, instance=instance, cursor=baseline.cursor
        )

        num = result.get_num_requested(_key("financial_metrics"))
        assert num > 0, (
            f"Expected financial_metrics to be requested after statement_tables/2025-01-01 "
            f"materialised, but get_num_requested returned {num}. "
            f"This indicates the ~any_deps_missing gate is blocking the cascade because "
            f"other monthly partitions of statement_tables are absent."
        )
