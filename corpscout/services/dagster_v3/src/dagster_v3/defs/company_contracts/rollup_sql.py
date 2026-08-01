"""The contracts list's aggregation, moved off the request path.

This SQL used to live in `contracts.server.ts` and ran on every page load. It
is a two-level aggregation over every winner row in a country -- GROUP BY
(contract_ref, source_slug) under GROUP BY contract_ref -- and all of it
completes before the page's LIMIT applies. For Brazil it collapses nothing
(4,605,018 contracts from 4,605,018 winner rows), so it built 4.6M groups
holding a dozen aggregate states over wide strings in order to emit 4.6M rows:
31s and 13.2 GiB, which the app abandons as a 500.

Nothing about the shape changed in the move. Every comment below records a
decision that was got wrong once, and the expressions are character-for-
character what the page computed -- the rollup has to order and display
identically to what it replaces.

What was dropped, deliberately:

- The filter clause. Filters are all contract-level (agreement, CPV prefix,
  amount range, date range), so they now apply to the rollup on the read side.
  That is the whole point: filtering one row per contract instead of
  aggregating every winner row and then filtering.
- The ORDER BY and LIMIT. The asset writes every row.

What was added: `publication_date_in`, so the rollup can carry a real Date for
the from/to filters. The page only ever needed the string form.
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

    An empty match status reads the same as 'exact' on the page
    (`supplierStatusLabel` returns null for both), so a plain-shape country
    still renders no badge.
    """
    supplier_id = "winner_registered_id" if has_supplier_detail else "''"
    supplier_status = "winner_match_status" if has_supplier_detail else "''"
    return f"""
SELECT
    contract_ref,
    coalesce(toString(argMax(source_date, priority)), '') AS contract_date,
    argMax(buyer_name_in, priority) AS buyer_name,
    argMax(title_in, priority) AS title,
    argMax(agreement_type_in, priority) AS agreement_type,
    argMax(cpv_code_in, priority) AS cpv_code,
    argMax(source_url_in, priority) AS source_url,
    argMax(winner_name_in, priority) AS winner_name,
    argMax(winner_registered_id_in, priority) AS winner_registered_id,
    argMax(winner_match_status_in, priority) AS winner_match_status,
    argMax(winner_count, priority) AS winner_count_primary,
    argMax(amount_original_in, priority) AS amount_original,
    argMax(currency_in, priority) AS currency,
    max(priority) AS amount_usd,
    max(publication_date_in) AS publication_date
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
