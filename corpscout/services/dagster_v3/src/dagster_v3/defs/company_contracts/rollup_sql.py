"""The contracts list's aggregation, moved off the request path.

This SQL used to live in `contracts.server.ts` and ran on every page load. It
is a two-level aggregation over every winner row in a country -- GROUP BY
(contract_ref, source_slug) under GROUP BY contract_ref -- and all of it
completes before the page's LIMIT applies. For Brazil it collapses nothing
(4,605,018 contracts from 4,605,018 winner rows), so it built 4.6M groups
holding a dozen aggregate states over wide strings in order to emit 4.6M rows:
31s and 13.2 GiB, which the app abandons as a 500.

Every comment below records a decision that was got wrong once, and the
expressions are what the page computed -- the rollup has to order and display
identically to what it replaces.

What was dropped, deliberately:

- The filter clause. Filters are all contract-level (agreement, CPV prefix,
  amount range, date range), so they now apply to the rollup on the read side.
  That is the whole point: filtering one row per contract instead of
  aggregating every winner row and then filtering.
- The ORDER BY and LIMIT. The asset writes every row.

Three expressions are NOT verbatim, each because the page's OUTER projection
did the work and there is no outer projection any more:

- `nullIf(max(priority), -1.0) AS amount_usd`. The page unwrapped the sentinel
  one level up. Stored, -1.0 stops being a sentinel and starts being a number:
  `amount_usd <= X` is true for it, so every contract with no USD figure would
  join every max-amount filter, and `ORDER BY amount_usd ASC` would lead with
  them instead of trailing. The sentinel stays in the inner query, where it is
  still doing its job of keeping "no figure" apart from a genuine zero while
  max() collapses the per-source values.
- `toUInt32(...) AS supplier_count`. The page cast it; uniqExact returns UInt64
  and the column is UInt32.
- `argMax(publication_date_in, priority) AS publication_date` -- see below.

`publication_date_in` is an addition: the page only ever needed the string
form, and the from/to filters need a real Date.

**argMax, not max, for that date.** `contract_date` is
`argMax(source_date, priority)`, so it comes from the one source carrying the
largest USD amount. A plain max() over every source would let a multi-source
contract be FILTERED on one date and DISPLAY another -- the same class of bug
as filtering the raw agreement_type while displaying the extracted one.
Sharing `priority` makes the filtered date and the displayed date the same date
by construction.

The projection is in ROLLUP_COLUMNS order, because the asset's INSERT names its
columns positionally: a SELECT in a different order writes source_url into
publication_date, and ClickHouse accepts it.
"""

# PNCP (Brazil) publishes agreement_type as a raw {"id":N,"nome":"..."} blob
# while every other loaded source publishes plain text. The read side must
# filter on the identical expression -- filtering the bare column would match
# nothing for Brazil while looking perfectly correct.
AGREEMENT_EXPR = (
    "multiIf(startsWith(agreement_type, '{'), "
    "JSONExtractString(agreement_type, 'nome'), agreement_type)"
)


def build_rollup_select(*, source_table: str, has_supplier_detail: bool) -> str:
    """One row per contract, from a winner-level facts table.

    `has_supplier_detail` says whether the source carries the award shape's
    three supplier columns. The plain shape does not have
    winner_registered_id/winner_match_status at all, so selecting them would
    fail at query time for six of the eight countries -- the outer projection
    is identical either way and only the inner source expression differs, which
    is exactly what the page did.

    The status falls back to the literal 'exact', not a blank: a source with no
    supplier detail has nothing to disagree with, and that is what the page has
    always displayed for those six countries.
    """
    supplier_id = "winner_registered_id" if has_supplier_detail else "''"
    supplier_status = "winner_match_status" if has_supplier_detail else "'exact'"
    return f"""
SELECT
    contract_ref,
    coalesce(toString(argMax(source_date, priority)), '') AS contract_date,
    argMax(buyer_name_in, priority) AS buyer_name,
    argMax(title_in, priority) AS title,
    argMax(agreement_type_in, priority) AS agreement_type,
    argMax(cpv_code_in, priority) AS cpv_code,
    argMax(winner_name_in, priority) AS winner_name,
    argMax(winner_registered_id_in, priority) AS winner_registered_id,
    argMax(winner_match_status_in, priority) AS winner_match_status,
    toUInt32(argMax(winner_count, priority)) AS supplier_count,
    argMax(amount_original_in, priority) AS amount_original,
    argMax(currency_in, priority) AS currency,
    nullIf(max(priority), -1.0) AS amount_usd,
    argMax(source_url_in, priority) AS source_url,
    argMax(publication_date_in, priority) AS publication_date
FROM (
    SELECT
        contract_ref,
        source_slug AS source,
        max(publication_date) AS source_date,
        max(publication_date) AS publication_date_in,
        any(buyer_name) AS buyer_name_in,
        any(title) AS title_in,
        -- PNCP (Brazil) publishes agreement_type as a raw {{"id":N,"nome":"..."}}
        -- blob -- every other loaded source publishes plain text.
        any({AGREEMENT_EXPR}) AS agreement_type_in,
        -- max(), not any(): a contract's rows within one source can carry CPV
        -- on some lots and '' on others, and any() would pick the blank often
        -- enough to look like the register publishes nothing.
        max(cpv_code) AS cpv_code_in,
        any(source_url) AS source_url_in,
        -- Alphabetically first, not source_winner_ordinal: that ordinal is just
        -- the order our parser happened to iterate the notice XML, so it encodes
        -- no rank -- arbitrary but stable, which looks meaningful and is not.
        -- argMin over the same expression min() uses, so the name, its id and
        -- its status all describe ONE supplier rather than three different ones.
        min(if(winner_name != '', winner_name, company_id)) AS winner_name_in,
        argMin({supplier_id}, if(winner_name != '', winner_name, company_id))
            AS winner_registered_id_in,
        argMin({supplier_status}, if(winner_name != '', winner_name, company_id))
            AS winner_match_status_in,
        uniqExact(if(company_id != '', company_id, winner_name)) AS winner_count,
        sum(value_amount_original) AS amount_original_in,
        any(value_currency) AS currency_in,
        -- -1 sentinel distinguishes "no source reported a USD figure" from a
        -- genuine zero once max() below collapses the per-source values.
        coalesce(toFloat64(sum(value_amount_usd)), -1.0) AS priority
    FROM {source_table}
    GROUP BY contract_ref, source
)
GROUP BY contract_ref
"""
