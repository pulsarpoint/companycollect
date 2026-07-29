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
from datetime import date

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
            "the connection history -- but 'merged' here means only that the "
            "ledger (br_company_relations_snapshots) has a row for it. The "
            "ledger is written BEFORE EXCHANGE TABLES publishes the merge "
            "(see clickhouse.py), so this can also mean the month was "
            "RECORDED WITHOUT BEING PUBLISHED, if a previous run's EXCHANGE "
            "failed after its ledger write. This is not reassurance that "
            "br_company_relations reflects this month -- verify that "
            "separately. If it does not, the recovery is: delete this "
            "month's row from br_company_relations_snapshots, then re-run "
            "the merge for this snapshot_year_month."
        )
    newest = max(merged_months)
    if snapshot_year_month < newest:
        raise ValueError(
            f"Brazil RFB snapshot {snapshot_year_month} is older than the newest "
            f"merged snapshot {newest}; merging it would corrupt the timeline. "
            "Rebuild from the S3 archives in ascending order instead -- see "
            "brazil_rfb_socios_history-design.md section 8."
        )


# RFB ships socios as ~10 roughly equal-sized ZIP parts. This ratio is a
# COARSE FLOOR against a large-scale event -- several parts missing from one
# failed transfer, or an implausible month-over-month collapse -- not a
# defence against a single missing part, and it was checked and found NOT to
# catch that case (a previous version of this comment claimed it did; that
# claim was wrong).
#
# What it does NOT catch, and why:
#   - A mid-stream truncation of ONE zip part is not silent in the first
#     place. zipfile fails loudly opening/extracting a corrupt archive (see
#     source.py's _extract_single_csv), so that failure was never the shape
#     this guard exists for.
#   - The genuinely silent shape is a part that simply never downloaded: the
#     CSV parses fine, the run looks clean, and the snapshot is just short.
#     Losing 1 of ~10 parts is only a ~10% edge-count drop -- comfortably
#     inside this 50% floor, so it sails through here undetected.
#   - There is no upstream backstop either: _download's Content-Length check
#     only LOGS a byte-count mismatch, it never verifies it (see
#     source.py's _download), and the partial-sync LookupError in
#     resolve_snapshot_remote_files_from_object_store checks only that each
#     FAMILY is present, not that every socios part within the family is.
#
# The single-missing-part case is instead caught EXACTLY, with no threshold,
# by assert_snapshot_part_count_is_not_decreasing below, which compares this
# month's manifest socios-part count against the previous merged month's.
# This ratio stays as an additional coarse floor on top of that -- it still
# catches the multi-part/implausible-collapse shape the part-count guard
# alone would not (e.g. a shrinking register is a legitimate part-count-equal
# but edge-count-lower month) -- not as a replacement for it.
#
# 0.5 remains deliberately loose, not tight: it tolerates the real
# month-to-month register churn described in
# brazil_rfb_socios_history-design.md (partners leaving, companies being
# deregistered) without false-positiving on a legitimate shrink. A tighter
# threshold would need real production month-over-month variance data to
# justify without risking false positives that block a legitimate (if
# unusually small) run; this is the conservative choice until that data
# exists.
MIN_SNAPSHOT_EDGE_RATIO = 0.5


def assert_snapshot_edge_count_is_plausible(
    edges_in_snapshot: int, previous_edges_in_snapshot: int | None
) -> None:
    """Refuse a short snapshot before it ever reaches the merge.

    Only compares against the immediately preceding MERGED month -- the
    caller passes None when there is no previous month (first-ever merge),
    and this is a no-op in that case: there is nothing to compare against,
    and refusing the first snapshot on principle would be its own bug.

    This is deliberately a size check, not a content check: it cannot tell a
    truncated download from a genuine 50%+ month-over-month change in
    Brazil's company register, and does not try to. It exists to stop a
    large-scale, mechanical failure (several parts missing, or a genuinely
    implausible collapse) -- it is NOT the defence against a single missing
    part; see the MIN_SNAPSHOT_EDGE_RATIO comment above and
    assert_snapshot_part_count_is_not_decreasing below for that.
    """
    if previous_edges_in_snapshot is None or previous_edges_in_snapshot <= 0:
        return
    threshold = previous_edges_in_snapshot * MIN_SNAPSHOT_EDGE_RATIO
    if edges_in_snapshot < threshold:
        raise ValueError(
            f"Brazil RFB snapshot has {edges_in_snapshot} edges, fewer than "
            f"{MIN_SNAPSHOT_EDGE_RATIO:.0%} of the {previous_edges_in_snapshot} "
            "edges in the previous merged month. This looks like a truncated "
            "download -- RFB ships socios as ~10 ZIP parts and a partial "
            "transfer still produces a valid-looking but incomplete snapshot "
            "-- not a real change in the register. This guard runs BEFORE "
            "anything is written to the history or the ledger, so no "
            "recovery procedure is needed here (section 8 is for repairing "
            "history already written, not for this): verify all archive "
            "parts were fetched for this snapshot_year_month, re-run the "
            "download for any that are missing, rebuild the relations "
            "stage, and merge again."
        )


# RFB ships socios as exactly 10 ZIP parts, Socios0.zip through Socios9.zip.
# MEASURED against a real RFB archive on 2026-07-29, not assumed -- this is
# not the same "~10" the rest of this module hedges with elsewhere, it is a
# counted fact about today's archive layout.
#
# This is an ABSOLUTE floor, checked regardless of whether a previous merged
# month exists. It has to be: the comparison in
# assert_snapshot_part_count_is_not_decreasing below is a no-op when
# previous_socios_part_count is None, which is exactly production's current
# state -- 0 rows, an empty ledger, so the next run IS the first-ever merge.
# Without this floor, a first run carrying only 1 of 10 parts is accepted
# silently (verified on real ClickHouse), and that short count becomes the
# permanent baseline every later month is judged against -- partners present
# since 2018 would get recorded as spells first seen in whatever month the
# short first run happened to be, with nothing flagging either run.
#
# If RFB legitimately repartitions socios into a different number of files,
# THIS CONSTANT IS WHAT TO CHANGE, and this guard firing is how you find out.
# A repartition is not a transfer failure -- telling the operator to
# "re-run the download" when the real cause is RFB changing its layout sends
# them in a circle: the re-run will always come back with the new (correct,
# but different-from-10) part count and fail here again.
EXPECTED_SOCIOS_PART_COUNT = 10


def assert_snapshot_part_count_is_not_decreasing(
    socios_part_count: int, previous_socios_part_count: int | None
) -> None:
    """Refuse a snapshot whose manifest recorded FEWER socios ZIP parts than
    the previous merged month's -- exactly, not by ratio. Also refuse one
    below EXPECTED_SOCIOS_PART_COUNT outright, with no previous month needed.

    This is the actual defence against the failure
    MIN_SNAPSHOT_EDGE_RATIO's own comment concedes it cannot catch: RFB ships
    socios as ~10 parts, and a single missing part is only a ~10% edge-count
    drop -- comfortably inside that 50% floor, so it would sail through there
    undetected. The snapshot manifest (tables.SNAPSHOT_FILE_COLUMNS) records
    one row per downloaded file with its family, so the socios part count for
    a month is knowable EXACTLY, not estimated -- comparing it to the
    previous merged month needs no threshold and cannot false-positive on
    legitimate register churn: a shrinking register does not change how many
    ZIP files RFB split socios into.

    The EXPECTED_SOCIOS_PART_COUNT check runs FIRST and unconditionally,
    before the previous-month comparison, because that comparison alone
    cannot guard the first-ever merge: previous_socios_part_count is None
    with nothing to compare against, and production is at exactly that state
    today (0 rows, empty ledger). See EXPECTED_SOCIOS_PART_COUNT's own
    comment for what a first run with too few parts does if unguarded.

    Same no-baseline handling as assert_snapshot_edge_count_is_plausible for
    the previous-month comparison specifically: previous_socios_part_count is
    None on the first-ever merge, and <= 0 also covers a ledger row written
    before this column existed (migration 000211's `ADD COLUMN ... DEFAULT
    0`) -- in both cases there is no real baseline for a RELATIVE comparison,
    so the relative check is skipped. The absolute floor above is not
    skipped by either of those conditions.
    """
    if socios_part_count < EXPECTED_SOCIOS_PART_COUNT:
        raise ValueError(
            f"Brazil RFB snapshot has {socios_part_count} socios ZIP parts, "
            f"fewer than the {EXPECTED_SOCIOS_PART_COUNT} RFB is measured to "
            "ship (Socios0.zip-Socios9.zip). This floor applies regardless "
            "of whether a previous merged month exists, so it also guards "
            "the first-ever merge -- there is no previous month's part "
            "count to compare against yet, and a short first run would "
            "otherwise become the permanent, silently-accepted baseline "
            "every later month is judged against. If RFB has legitimately "
            "repartitioned socios into a different number of files, update "
            "EXPECTED_SOCIOS_PART_COUNT in history.py to match -- do not "
            "assume this is a transfer failure and re-run the download "
            "before checking that."
        )
    if previous_socios_part_count is None or previous_socios_part_count <= 0:
        return
    if socios_part_count < previous_socios_part_count:
        raise ValueError(
            f"Brazil RFB snapshot has {socios_part_count} socios ZIP parts, "
            f"fewer than the {previous_socios_part_count} parts the previous "
            "merged month's manifest recorded. RFB ships socios as multiple "
            "ZIP parts and a missing one parses fine -- it does not fail "
            "extraction and is not reliably caught by the edge-count ratio "
            "guard (MIN_SNAPSHOT_EDGE_RATIO). Re-run the download for this "
            "snapshot_year_month, verify every socios archive part was "
            "fetched, and rebuild the relations stage before merging again."
        )


def _validate_snapshot_stamp(snapshot_year_month: str, snapshot_date: str) -> tuple[str, str]:
    """Validate and normalize the two values `build_merge_select_sql` stamps
    into every row of a self-referential, permanent history table.

    `assert_snapshot_is_newer` was hardened to reject a malformed
    `snapshot_year_month` before this function existed to interpolate one
    into SQL text. It was not enough on its own: nothing called it on the
    value that actually gets written, and nothing validated `snapshot_date`
    at all. Left unchecked, both arguments are interpolated raw into the
    returned SELECT (see the f-string below), so an unvalidated caller could:

      - pass snapshot_year_month="2026-06'" and emit the broken/injectable
        literal '2026-06''  (a stray quote closes the string early)
      - pass snapshot_date="garbage" and emit the invalid literal
        date 'garbage', which only fails at execution time, deep inside the
        merge, in whichever engine happens to run it
      - pass "2026-6" (unpadded) and have it silently accepted and written
        into first_seen_snapshot forever
      - pass a snapshot_date that doesn't agree with snapshot_year_month
        (e.g. month 2026-07 with date 2026-06-01) and silently write a row
        where end_at contradicts last_seen_snapshot -- nothing else checks
        that these two stay in sync, and once written it is permanent: the
        merge folds its own output back into itself every run.

    Returns the two values re-serialized from their parsed form (not the
    caller's raw strings), so only clean 'YYYY-MM' / 'YYYY-MM-DD' digit-and-
    hyphen text ever reaches the SQL text below.
    """
    clean_year_month = source.validate_snapshot_year_month(snapshot_year_month)
    try:
        parsed_date = date.fromisoformat(snapshot_date.strip())
    except ValueError as exc:
        raise ValueError(
            "snapshot_date must be an ISO date (YYYY-MM-DD), got "
            f"{snapshot_date!r}"
        ) from exc
    year_str, month_str = clean_year_month.split("-")
    try:
        expected_date = date(int(year_str), int(month_str), 1)
    except ValueError as exc:
        raise ValueError(
            f"snapshot_year_month {clean_year_month!r} is not a valid "
            "calendar month"
        ) from exc
    if parsed_date != expected_date:
        raise ValueError(
            f"snapshot_date {snapshot_date!r} must be the first day of "
            f"snapshot_year_month {clean_year_month!r} (expected "
            f"{expected_date.isoformat()}); a mismatch would write history "
            "where end_at contradicts last_seen_snapshot"
        )
    return clean_year_month, parsed_date.isoformat()


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
    one setting must travel with it and does NOT live in this string:
    ClickHouse defaults to join_use_nulls=0, which fills the unmatched side of
    a FULL JOIN with type defaults instead of NULL. That is not a two-column
    cosmetic gap -- running this SQL with join_use_nulls=0 on ClickHouse does
    NOT error, and it blanks the entire spell identity, not just
    first_seen_snapshot:
      - key_out's coalesce(st.col, sn.col) never sees a NULL to fall through
        to, because the unmatched side reads '' instead of NULL. So on the
        first merge (an empty state table, everything unmatched on the state
        side), country_iso2, source_slug, cnpj_basico, related_entity_kind,
        related_tax_id, relation_code and relation_since_key are ALL written
        as '' -- every one of them, the complete identity a spell is keyed
        by, not a couple of incidental fields.
      - every row produced by that unmatched arm then collapses onto the
        SAME all-blank sort key, so instead of one row per spell the table
        ends up with every snapshot row fighting over a single identity.
        This has been produced and observed on a real ClickHouse 26.5 run.
      - sn.cnpj_basico also reads '' instead of NULL, so "is not null" never
        fires and nothing ever closes; an unmatched state row's
        first_seen_snapshot reads '' instead of NULL, so
        coalesce(st.first_seen_snapshot, '{snapshot_year_month}') keeps the
        '' instead of stamping the new month -- every new spell is also born
        with a blank first_seen_snapshot.
    The export task MUST run this SQL with settings={"join_use_nulls": 1}.
    This module only generates the SQL and cannot enforce that setting
    itself -- a bare SELECT cannot carry its own SETTINGS clause -- so this
    paragraph is the entire defence against that failure mode. It is where a
    maintainer will look for it, so it is written here in full.

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
    clean_year_month, clean_date = _validate_snapshot_stamp(
        snapshot_year_month, snapshot_date
    )
    join = " and ".join(f"st.{c} = sn.{c}" for c in SPELL_KEY)
    key_out = ",\n            ".join(f"coalesce(st.{c}, sn.{c}) as {c}" for c in SPELL_KEY)
    attrs_out = ",\n            ".join(
        f"case when sn.cnpj_basico is not null then sn.{c} else st.{c} end as {c}"
        for c in _ATTRIBUTES
    )
    passthrough_cols = ",\n        ".join(tables.BR_COMPANY_RELATIONS_COLUMNS)

    # --- Dedup: GROUP BY + argMax, not row_number() over a window sort ---
    #
    # All three dedups below used to be row_number() over a full partition
    # sort, then `where _rn = 1`. That sort is what makes the merge unable to
    # run at socios scale (~20-25M rows/month): measured on real ClickHouse
    # 26.5 in a 6GB container, it dies in MergeSortingTransform (the window
    # sort, not the join) at 12M rows -- OOM-killed, exit 137 -- and none of
    # max_bytes_before_external_sort, join_algorithm=grace_hash,
    # partial_merge or full_sorting_merge changes that; a window sort simply
    # does not honour those spill settings. A plain aggregate GROUP BY does
    # honour max_bytes_before_external_group_by (ClickHouse 26.5 also spills
    # it automatically as memory approaches the query limit, with no setting
    # needed) and can complete by spilling to disk instead of dying, so "pick
    # one row per group, deterministically" is rewritten below as `GROUP BY
    # key, argMax(col, tie_break) AS col` per output column, in place of a
    # window sort + row_number() filter.
    #
    # Re-measured end to end (this rewritten SELECT, real ClickHouse 26.5, 6GB
    # container, first-merge shape -- empty state, snapshot at N rows, same
    # methodology as the row_number() figures above): 2M -> 1.45 GiB, 4M ->
    # 2.57 GiB, 6M -> 2.71 GiB, 12M -> 2.63 GiB (survives -- row_number() dies
    # here), 18M -> 2.82 GiB, 25M -> 3.2 GiB (the top of the design doc's
    # per-month estimate). Below ~6M the rewrite uses MORE memory than
    # row_number(), not less -- GROUP BY's hash table costs more per group
    # than a sort costs per row when almost every group has exactly one
    # member, which is the common case here (SPELL_KEY is close to unique per
    # row; real duplicate-collapsing is the rare case the tie-break exists
    # for). What the rewrite buys is not a lower peak at small N, it's a
    # peak that stops climbing with N once GROUP BY starts spilling, instead
    # of climbing until MergeSortingTransform is OOM-killed. Do not set
    # max_bytes_before_external_group_by manually here -- an explicit low
    # threshold (500MB) was tried at 25M and made things WORSE, moving the
    # failure into the join's FillingRightJoinSide step instead of avoiding
    # it; ClickHouse 26.5's own default handling of this outperformed a
    # manually-tuned one in every run measured for this change.
    #
    # Two portability facts make the SQL text below identical in both
    # engines -- verified against ClickHouse 26.5 and this repo's pinned
    # DuckDB, not assumed:
    #   - `argMax` (that exact spelling) resolves in both. ClickHouse's
    #     aggregate function name is case-sensitive and only ever resolves as
    #     `argMax`; DuckDB is case-insensitive, so the identical literal text
    #     resolves to DuckDB's own `arg_max`. There is no third spelling that
    #     is native to both, so this uses ClickHouse's exact spelling.
    #   - A bare parenthesised list `(a, b, c)` builds a tuple/row value
    #     identically in both engines and both compare it lexicographically,
    #     so it stands in for the multi-column `ORDER BY a desc, b desc, c
    #     desc` this replaces. As before, nothing here is CAST -- see the
    #     comment below on why that would break ClickHouse specifically.
    #
    # WHERE THIS IS *NOT* EQUIVALENT TO row_number(), AND WHY IT IS STILL SAFE.
    #
    # argMax SKIPS ROWS WHOSE ARGUMENT IS NULL -- in both engines, identically.
    # So if the winning row held NULL in an argMax'd column, argMax returns the
    # *runner-up's* value for that column while other columns come from the
    # winner: a mixed row assembled from two source rows. row_number() returns
    # the winning row whole and has no such hazard. Verified by execution:
    # adversarial NULL inputs diverge from row_number() in 299/300 DuckDB and
    # 18/20 ClickHouse cases.
    #
    # It is safe here only because of an invariant that lives in ANOTHER FILE:
    # relations.py `_blank()`s all six string attributes (so they are never
    # NULL), and derives `relation_since` and `relation_since_key` from the
    # same `nullif(trim(data_entrada_sociedade), '')` expression -- which makes
    # relation_since constant within every SPELL_KEY group, so the one
    # NULL-capable column can never be the odd one out. Production-shaped
    # inputs diverge in 0/300 and 0/60 cases respectively.
    #
    # test_relations_never_emit_null_in_the_dedup_tiebreak_columns pins that
    # invariant. If it ever fails, this dedup starts writing mixed rows
    # silently -- and the merge is self-referential, so they are permanent.
    # DuckDB has arg_max_null for this, ClickHouse has no same-spelled
    # equivalent, so there is no portable one-word fix.
    #
    # What is NOT portable, and is why this stops at one argMax call per
    # output column instead of bundling a whole row into a single call:
    # extracting a field back out of a returned tuple/struct has no shared
    # syntax. ClickHouse accepts `t.1`; DuckDB's parser rejects that exact
    # text (it reads `.1` as a numeric literal, not field access) and DuckDB
    # has no `tupleElement`. So every output column below gets its own
    # `argMax(col, tie_break)` call, all sharing the identical tie_break
    # expression, rather than one call returning a whole row that gets
    # destructured afterwards.
    #
    # That leaves a question portability alone can't answer: can independent
    # argMax(colA, v) and argMax(colB, v) calls sharing the same v disagree
    # on which row won a genuine tie in v, and hand back a Frankenstein row
    # -- colA from one candidate, colB from another? Verified empirically,
    # not assumed: a 2M-row ClickHouse 26.5 stress test with deliberately
    # tied groups (two candidates per group, forced to tie on the argMax
    # value, differing on a marker column and on a second output column)
    # showed every group's two argMax-derived columns staying paired to the
    # SAME winning candidate -- both with in-memory aggregation and with
    # max_bytes_before_external_group_by forced low enough to force a spill.
    # DuckDB showed the same pairing at 1M rows. Neither engine assembled a
    # mixed-candidate row in that test.
    #
    # Deterministic tie-break for a duplicate SPELL_KEY within one snapshot.
    # SPELL_KEY is coarser than the pipeline's own record identity -- e.g. two
    # partners of one company whose MASKED CPFs collide on the 6 of 11 visible
    # digits, same role, same entry date, are two source rows but one spell
    # key. Picking "the populated attributes win over the blank ones,
    # deterministically tie-broken column by column" (rather than "first row
    # DuckDB/ClickHouse happens to hand back") means the choice does not
    # depend on file part order, dlt's load order, or engine-specific scan
    # order, is reproducible on a replay from S3, and does not throw away a
    # real name/country/etc in favor of an equally-valid-looking blank one.
    #
    # relation_since is deliberately excluded from this tie-break tuple, not
    # just left uncast: it is functionally determined by relation_since_key,
    # which is already part of the partition (GROUP BY) above, so it is
    # constant within every group and a tie-break on it can never fire -- it
    # would only add a CAST. It is still an output column, produced by its
    # own argMax below, just not part of what decides the winning row.
    #
    # NEVER add cast(col as varchar) to any column in this tuple. ClickHouse's
    # CAST does not propagate Nullable: cast(relation_since as varchar) raises
    # CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN the moment a row has a NULL
    # relation_since -- which is ordinary RFB data (see relations.py's
    # try_strptime over data_entrada_sociedade) -- before coalesce ever gets a
    # chance to paper over it. DuckDB's CAST instead returns NULL from that
    # same expression and lets coalesce paper over it silently, so the DuckDB
    # tests below CANNOT see a re-introduced CAST break production; only a
    # real ClickHouse run catches it. Every column in this tuple must already
    # be a String/varchar so plain coalesce(col, '') is sufficient on its own.
    #
    # Every reference below is qualified with the `sn` alias for the same
    # reason state's `s` alias exists above: an unqualified
    # `coalesce(related_name, '')` inside the tie-break tuple, sitting next
    # to `argMax(related_name, tie_break) AS related_name`, made ClickHouse
    # resolve the bare `related_name` inside the tuple against the SELECT
    # alias instead of the source column -- `Code: 184 ILLEGAL_AGGREGATION --
    # ... AS related_name is found inside another aggregate function`, an
    # aggregate appearing to reference itself. Qualifying with `sn.`
    # resolves every reference against the table, not the alias, in both
    # engines.
    snapshot_tiebreak = (
        "("
        + ", ".join(f"coalesce(sn.{c}, '')" for c in _ATTRIBUTES if c != "relation_since")
        + ")"
    )
    snapshot_argmax_cols = tuple(c for c in _SNAPSHOT_COLUMNS if c not in SPELL_KEY)
    snapshot_spell_key_csv = ", ".join(f"sn.{c}" for c in SPELL_KEY)
    snapshot_out = ",\n            ".join(
        f"argMax(sn.{c}, {snapshot_tiebreak}) as {c}" for c in snapshot_argmax_cols
    )

    # Tie-break for pre-existing duplicate STATE rows (see open_state and
    # closed_state below). This is a different problem from the snapshot
    # dedup above: state duplicates are not "two source rows collapsed to one
    # key", they are the merge's own output already fanned out (e.g. a
    # retried `INSERT INTO stage` in the export). Any deterministic order
    # converges them to one row; favor the row with more observations and,
    # failing that, the most recently resolved one -- unchanged from before,
    # just the MAX of a tuple instead of the head of a DESC, DESC sort.
    #
    # Every reference to the state table below is qualified with the `s`
    # alias, including inside the tie-break tuple and the WHERE clause. That
    # is not stylistic: verified against a real ClickHouse 26.5, an
    # unqualified `where is_current = 1` alongside an unqualified
    # `argMax(is_current, ...) as is_current` raises
    # `Code: 184 ILLEGAL_AGGREGATION -- argMax(is_current, ...) AS is_current
    # is found in WHERE`, because ClickHouse resolves the bare WHERE
    # reference against the SELECT-list alias (the aggregate) instead of the
    # source column, since the two share a name. Qualifying every reference
    # with `s.` resolves it against the table instead and sidesteps the
    # clash entirely; DuckDB accepts the same qualified form.
    state_tiebreak = "(s.observations, s.resolved_at)"
    state_argmax_cols = tuple(
        c for c in tables.BR_COMPANY_RELATIONS_COLUMNS if c not in SPELL_KEY
    )
    state_spell_key_csv = ", ".join(f"s.{c}" for c in SPELL_KEY)
    open_state_out = ",\n            ".join(
        f"argMax(s.{c}, {state_tiebreak}) as {c}" for c in state_argmax_cols
    )
    # first_seen_snapshot and end_at are part of closed_state's own
    # partition (below), not tie-break output, so they are dropped from the
    # argMax list here and selected as bare group-by columns instead.
    closed_state_argmax_cols = tuple(
        c for c in state_argmax_cols if c not in ("first_seen_snapshot", "end_at")
    )
    closed_state_out = ",\n            ".join(
        f"argMax(s.{c}, {state_tiebreak}) as {c}" for c in closed_state_argmax_cols
    )

    return f"""
    with snapshot_unique as (
        select
            {snapshot_spell_key_csv},
            {snapshot_out}
        from {snapshot_table} as sn
        group by {snapshot_spell_key_csv}
    ),
    open_state as (
        -- Only an OPEN spell may be matched and mutated. A closed spell must
        -- never enter this join. Only one open spell per key can legitimately
        -- exist, so pre-existing duplicates here (never produced by a clean
        -- run, but reachable via a retried export) are collapsed on SPELL_KEY
        -- alone -- otherwise the merge is self-referential with no self-heal
        -- path and the duplicate would persist forever.
        select
            {state_spell_key_csv},
            {open_state_out}
        from {state_table} as s
        where s.is_current = 1
        group by {state_spell_key_csv}
    ),
    closed_state as (
        -- Multiple CLOSED spells per key are legitimate history (seen, gone,
        -- seen again, gone again), so collapsing on SPELL_KEY alone would
        -- wrongly merge distinct spells. Partition on the key plus
        -- first_seen_snapshot and end_at -- the columns that actually
        -- identify one closed spell -- so only true duplicates (identical on
        -- all three) collapse.
        select
            {state_spell_key_csv},
            s.first_seen_snapshot,
            s.end_at,
            {closed_state_out}
        from {state_table} as s
        where s.is_current = 0
        group by {state_spell_key_csv}, s.first_seen_snapshot, s.end_at
    ),
    merged_open as (
        select
            coalesce(st.country_iso2, sn.country_iso2) as country_iso2,
            coalesce(st.source_slug, sn.source_slug) as source_slug,
            {key_out},
            {attrs_out},
            coalesce(st.first_seen_snapshot, '{clean_year_month}')
                as first_seen_snapshot,
            case
                when sn.cnpj_basico is not null then '{clean_year_month}'
                else st.last_seen_snapshot
            end as last_seen_snapshot,
            coalesce(st.start_at, sn.relation_since) as start_at,
            case
                when sn.cnpj_basico is not null then null
                else date '{clean_date}'
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
