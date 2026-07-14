"""Backfill-policy contract.

Every partitioned asset must declare
``BackfillPolicy.multi_run(max_partitions_per_run=1)``: without a policy a
UI backfill defaults to launching runs unthrottled, and ``single_run`` puts
the whole backfill in one giant run (event-log connection storm + the
leaked-pool-slot wedge documented in CLAUDE.md Troubleshooting).
"""


def test_every_partitioned_asset_uses_multi_run_backfill_policy() -> None:
    from dagster_v3.definitions import defs as load_defs

    asset_graph = load_defs().get_repository_def().asset_graph

    offenders: list[str] = []
    for key in sorted(asset_graph.get_all_asset_keys()):
        node = asset_graph.get(key)
        if node.partitions_def is None:
            continue
        policy = node.backfill_policy
        if policy is None or policy.max_partitions_per_run != 1:
            offenders.append(key.to_user_string())

    assert offenders == [], (
        "Partitioned assets without BackfillPolicy.multi_run(max_partitions_per_run=1): "
        + ", ".join(offenders)
    )
