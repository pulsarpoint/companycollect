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

from dagster_v3.defs.brazil_companies.rfb import source, tables

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

# Everything a snapshot row supplies that the merge reads. Order matches the
# test/production snapshot table shape: SPELL_KEY plus the refreshed
# attributes, prefixed with the two constant columns.
_SNAPSHOT_COLUMNS: tuple[str, ...] = ("country_iso2", "source_slug", *SPELL_KEY, *_ATTRIBUTES)


def assert_snapshot_is_newer(
    snapshot_year_month: str, merged_months: Sequence[str]
) -> None:
    """Refuse an out-of-order or repeated snapshot, loudly.

    Manual runs mean months will arrive out of order eventually. Absorbing a
    late one silently would reopen spells a later month had closed and stamp
    dates that contradict the timeline -- and would hide that the cadence had
    slipped, which is what end_at's precision depends on.

    This is the sole guard against that corruption class, so it must not trust
    its own input: a lexicographic comparison alone would accept '2026-6'
    (unpadded) or 'garbage' as if they were valid year-months. Validate first.
    """
    snapshot_year_month = source.validate_snapshot_year_month(snapshot_year_month)
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
    """Full outer join of the OPEN history against the new snapshot.

    Returns a SELECT, not a statement, because the same merge runs in DuckDB
    (tests) and ClickHouse (export). The wrapper differs between engines, but
    one setting must travel with it and does NOT live in this string: ClickHouse
    defaults to join_use_nulls=0, which fills the unmatched side of a FULL JOIN
    with type defaults instead of NULL -- so every sentinel below would take the
    wrong arm silently (sn.cnpj_basico would read '' instead of NULL, so nothing
    would ever close; an unmatched state side would read is_current=0, so every
    new spell would be born closed). The export task MUST run this SQL with
    settings={"join_use_nulls": 1}. This module only generates the SQL and
    cannot enforce that setting itself, but it is where a maintainer will look
    for it, so it is written here.

    Four cases:
      - in both OPEN state and snapshot -> extend (refresh attributes, keep
        first_seen/start_at, bump observations, end_at stays NULL)
      - OPEN state only (absent now)    -> close (is_current=0, end_at stamped)
      - snapshot only, no OPEN match    -> open a new spell. It is legitimate
        for this to share a key with an older CLOSED spell: ORDER BY is not a
        unique key, and "seen, gone, seen again" as two rows is the record.
      - already CLOSED state            -> untouched, column for column. A
        closed spell must never re-enter the join -- if it did, a reappearing
        key would mutate the closed row instead of opening the new spell
        above, and the edge would be silently lost from current state.
    """
    join = " and ".join(f"st.{c} = sn.{c}" for c in SPELL_KEY)
    key_out = ",\n            ".join(f"coalesce(st.{c}, sn.{c}) as {c}" for c in SPELL_KEY)
    attrs_out = ",\n            ".join(
        f"case when sn.cnpj_basico is not null then sn.{c} else st.{c} end as {c}"
        for c in _ATTRIBUTES
    )
    snapshot_cols = ",\n                ".join(_SNAPSHOT_COLUMNS)
    # Deterministic tie-break for a duplicate SPELL_KEY within one snapshot.
    # SPELL_KEY is coarser than the pipeline's own record identity -- e.g. two
    # partners of one company whose MASKED CPFs collide on the 6 of 11 visible
    # digits, same role, same entry date, are two source rows but one spell
    # key. Picking "lexicographically smallest across every attribute column"
    # (rather than "first row DuckDB/ClickHouse happens to hand back") means
    # the choice does not depend on file part order, dlt's load order, or
    # engine-specific scan order, and is reproducible on a replay from S3.
    dedup_order = ",\n                    ".join(
        f"coalesce(cast({c} as varchar), '')" for c in _ATTRIBUTES
    )
    passthrough_cols = ",\n        ".join(tables.BR_COMPANY_RELATIONS_COLUMNS)
    return f"""
    with snapshot_unique as (
        select
            {snapshot_cols}
        from (
            select
                {snapshot_cols},
                row_number() over (
                    partition by {", ".join(SPELL_KEY)}
                    order by {dedup_order}
                ) as _rn
            from {snapshot_table}
        ) as ranked
        where _rn = 1
    ),
    open_state as (
        -- Only an OPEN spell may be matched and mutated. A closed spell must
        -- never enter this join.
        select * from {state_table} where is_current = 1
    ),
    closed_state as (
        select {passthrough_cols} from {state_table} where is_current = 0
    ),
    merged_open as (
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
                when sn.cnpj_basico is not null then null
                else date '{snapshot_date}'
            end as end_at,
            case when sn.cnpj_basico is not null then 1 else 0 end as is_current,
            coalesce(st.observations, 0)
                + case when sn.cnpj_basico is not null then 1 else 0 end
                as observations,
            now() as resolved_at
        from open_state as st
        full outer join snapshot_unique as sn on {join}
    )
    select {passthrough_cols} from merged_open
    union all
    select {passthrough_cols} from closed_state
    """
