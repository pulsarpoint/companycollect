"""Merge a monthly Socios snapshot into the connection history.

One row per SPELL of a connection: company X to entity Y in role R, from a
start date to an end date, NULL meaning current.

Two things about this model are easy to reverse by accident and must not be:

`relation_code` is part of a spell's identity, so a partner becoming an
administrator closes one spell and opens another. That control shift is the
change the table exists to show; holding the role in a mutable column hides it.

`relation_since_key` is part of a spell's identity because RFB publishes no
departures but DOES publish re-entries -- a partner who rejoins carries a new
data_entrada_sociedade. That makes a second spell detectable from a single
snapshot rather than depending on us having observed the gap.

And `end_at` means "gone by this snapshot", never "left on this date": the
source never says when a relationship ended, so its precision is exactly the
run cadence. See brazil_rfb_socios_history-design.md sections 1, 2 and 4.
"""

from collections.abc import Sequence

# What makes two observations the same spell.
SPELL_KEY: tuple[str, ...] = (
    "cnpj_basico",
    "related_entity_kind",
    "related_tax_id",
    "relation_code",
    "relation_since_key",
)

# Refreshed from the newest observation while a spell is open.
_ATTRIBUTES: tuple[str, ...] = (
    "related_name",
    "related_country",
    "age_band",
    "representative_tax_id",
    "representative_name",
    "representative_code",
    "relation_since",
)


def assert_snapshot_is_newer(
    snapshot_year_month: str, merged_months: Sequence[str]
) -> None:
    """Refuse an out-of-order or repeated snapshot, loudly.

    Manual runs mean months will arrive out of order eventually. Absorbing a
    late one silently would reopen spells a later month had closed and stamp
    dates that contradict the timeline -- and would hide that the cadence had
    slipped, which is what end_at's precision depends on.
    """
    if not merged_months:
        return
    if snapshot_year_month in merged_months:
        raise ValueError(
            f"Brazil RFB snapshot {snapshot_year_month} is already merged into "
            "the connection history"
        )
    newest = max(merged_months)
    if snapshot_year_month < newest:
        raise ValueError(
            f"Brazil RFB snapshot {snapshot_year_month} is older than the newest "
            f"merged snapshot {newest}; merging it would corrupt the timeline. "
            "Rebuild from the S3 archives in ascending order instead -- see "
            "brazil_rfb_socios_history-design.md section 8."
        )


def build_merge_select_sql(
    *,
    state_table: str,
    snapshot_table: str,
    snapshot_year_month: str,
    snapshot_date: str,
) -> str:
    """Full outer join of the history against the new snapshot.

    Returns a SELECT, not a statement, because the same merge runs in DuckDB
    (tests) and ClickHouse (export). Only the wrapper differs; keeping the
    logic in one string means what is tested is what ships.

    Three cases: in both (extend), state only (close), snapshot only (open).
    An already-closed spell is left untouched -- a re-entry arrives as a
    different key because its relation_since_key differs.
    """
    join = " and ".join(f"st.{c} = sn.{c}" for c in SPELL_KEY)
    key_out = ",\n        ".join(f"coalesce(st.{c}, sn.{c}) as {c}" for c in SPELL_KEY)
    attrs_out = ",\n        ".join(
        f"case when sn.cnpj_basico is not null then sn.{c} else st.{c} end as {c}"
        for c in _ATTRIBUTES
    )
    return f"""
    select
        coalesce(st.country_iso2, sn.country_iso2) as country_iso2,
        coalesce(st.source_slug, sn.source_slug) as source_slug,
        {key_out},
        {attrs_out},
        coalesce(st.first_seen_snapshot, '{snapshot_year_month}')
            as first_seen_snapshot,
        case
            when sn.cnpj_basico is not null then '{snapshot_year_month}'
            else st.last_seen_snapshot
        end as last_seen_snapshot,
        coalesce(st.start_at, sn.relation_since) as start_at,
        case
            -- already closed: leave it exactly as it was
            when st.is_current = 0 then st.end_at
            -- present now: still open
            when sn.cnpj_basico is not null then null
            -- was open, absent now: gone BY this snapshot
            else date '{snapshot_date}'
        end as end_at,
        case
            when st.is_current = 0 then 0
            when sn.cnpj_basico is not null then 1
            else 0
        end as is_current,
        coalesce(st.observations, 0)
            + case when sn.cnpj_basico is not null then 1 else 0 end
            as observations,
        now() as resolved_at
    from {state_table} as st
    full outer join {snapshot_table} as sn on {join}
    """
