"""Procurement sources, each projected into one canonical evidence shape.

Every source -- national register or TED -- emits the same eleven columns::

    country_code, company_id, evidence_id, source_slug, source_reference,
    source_url, publication_date, buyer_name, title, agreement_type,
    source_updated_at, dedup_key

That contract already existed implicitly: the UHM and TED CTEs were written to
union cleanly. Naming it means a source is defined by *how it reaches those
columns*, not by the shape of its tables -- so a flat awards table (UHM) and a
winners/notices pair (TED, Hilma) are the same kind of thing here.

Adding a source is a projection. Adding a country is a list of them.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dagster_v3.defs.company_signals.rules import CountryProcurementRule

UHM_SOURCE_SLUG = "sweden_uhm_procurement"
TED_SOURCE_SLUG = "ted_procurement"
HILMA_SOURCE_SLUG = "finland_hilma_procurement"


@dataclass(frozen=True)
class ProcurementSource:
    """One source of government-contract evidence for a country.

    build_cte returns a CTE emitting the canonical columns above. The source
    owns its own joins, so a pair-shaped register needs no special handling
    from the caller.
    """

    slug: str
    upstream_asset_key: str
    required_tables: tuple[str, ...]
    build_cte: Callable[["CountryProcurementRule", str], str]

    @property
    def cte_name(self) -> str:
        return f"{self.slug}_base"


def _uhm_cte(rule: "CountryProcurementRule", cte_name: str) -> str:
    """Sweden's UHM: one flat awards row per supplier, already company-matched.

    The table is named here rather than on the rule: a source knows its own
    joins, which is what lets a flat table and a winners/notices pair sit
    behind the same interface.
    """
    source_slug = UHM_SOURCE_SLUG
    return f"""    {cte_name} AS
    (
        SELECT
            '{rule.country_code}' AS country_code,
            u.company_id AS company_id,
            concat(
                'uhm:',
                lower(hex(MD5(concat(
                    u.company_id, '|', u.source_procurement_id, '|', u.source_lot_id,
                    '|', ifNull(toString(u.publication_date), ''), '|',
                    lowerUTF8(replaceRegexpAll(trim(u.title), '\\\\s+', ' '))
                ))))
            ) AS evidence_id,
            '{source_slug}' AS source_slug,
            concat(
                u.source_procurement_id,
                if(u.source_lot_id = '', '', concat(':', u.source_lot_id))
            ) AS source_reference,
            -- UHM publishes no per-award address, only which advertising
            -- database carried the notice. Empty is the honest answer.
            '' AS source_url,
            u.publication_date AS publication_date,
            u.buyer_name AS buyer_name,
            u.title AS title,
            any(u.agreement_type) AS agreement_type,
            max(u.source_retrieved_at) AS source_updated_at,
            if(
                u.publication_date IS NULL OR u.buyer_name = '' OR u.title = '',
                '',
                lower(hex(MD5(concat(
                    u.company_id, '|',
                    lowerUTF8(replaceRegexpAll(trim(u.buyer_name), '\\\\s+', ' ')), '|',
                    toString(u.publication_date), '|',
                    lowerUTF8(replaceRegexpAll(trim(u.title), '\\\\s+', ' '))
                ))))
            ) AS dedup_key
        FROM corpscout.se_uhm_procurement_awards AS u
        WHERE u.company_match_status = 'exact'
          AND u.company_id != ''
        GROUP BY
            u.company_id,
            u.source_procurement_id,
            u.source_lot_id,
            u.publication_date,
            u.buyer_name,
            u.title
    ),
"""


def _ted_cte(rule: "CountryProcurementRule", cte_name: str) -> str:
    """TED: winners joined to notices for buyer and title."""
    source_slug = TED_SOURCE_SLUG
    ted_countries = ", ".join(f"'{c}'" for c in rule.ted_winner_countries)
    return f"""    {cte_name} AS
    (
        SELECT
            '{rule.country_code}' AS country_code,
            c.{rule.company_id_column} AS company_id,
            concat(
                'ted:', w.publication_number, ':', w.lot_id, ':',
                w.tender_id, ':', toString(w.winner_ordinal)
            ) AS evidence_id,
            '{source_slug}' AS source_slug,
            concat(
                w.publication_number, ':', w.lot_id, ':',
                w.tender_id, ':', toString(w.winner_ordinal)
            ) AS source_reference,
            -- Verified live: this endpoint returns the notice XML (HTTP 200,
            -- parsed for 350545-2025 and 351526-2025).
            concat('https://ted.europa.eu/en/notice/', w.publication_number, '/xml')
                AS source_url,
            w.publication_date AS publication_date,
            any(n.buyer_name) AS buyer_name,
            any(n.notice_title) AS title,
            '' AS agreement_type,
            max(greatest(w.resolved_at, n.resolved_at)) AS source_updated_at,
            if(
                w.publication_date IS NULL
                    OR any(n.buyer_name) = ''
                    OR any(n.notice_title) = '',
                '',
                lower(hex(MD5(concat(
                    c.{rule.company_id_column}, '|',
                    lowerUTF8(replaceRegexpAll(trim(any(n.buyer_name)), '\\\\s+', ' ')), '|',
                    toString(w.publication_date), '|',
                    lowerUTF8(replaceRegexpAll(trim(any(n.notice_title)), '\\\\s+', ' '))
                ))))
            ) AS dedup_key
        FROM corpscout.ted_notice_winners AS w
        INNER JOIN corpscout.ted_notices AS n
            ON n.country_iso2 = w.country_iso2
           AND n.publication_number = w.publication_number
        INNER JOIN corpscout.{rule.companies_table} AS c
            ON c.{rule.company_id_column} = w.winner_national_id
        WHERE w.country_iso2 = '{rule.country_code}'
          AND upper(w.winner_country) IN ({ted_countries})
          AND length(w.winner_national_id) = {rule.identifier_length}
          AND length(c.{rule.company_id_column}) = {rule.identifier_length}
        GROUP BY
            c.{rule.company_id_column},
            w.publication_number,
            w.lot_id,
            w.tender_id,
            w.winner_ordinal,
            w.publication_date
    )
"""


def _hilma_cte(rule: "CountryProcurementRule", cte_name: str) -> str:
    """Finland's Hilma: a winners/notices pair, like TED rather than like UHM.

    This is the source that proves the interface. Hilma's shape has nothing in
    common with UHM's flat awards table, yet it needs no accommodation from the
    caller -- it joins its own two tables and emits the same eleven columns.

    Titles and buyer names are multilingual; Finnish is preferred with English
    as the fallback, matching how the Finnish detail page already reads them.
    """
    source_slug = HILMA_SOURCE_SLUG
    return f"""    {cte_name} AS
    (
        SELECT
            '{rule.country_code}' AS country_code,
            c.{rule.company_id_column} AS company_id,
            concat(
                'hilma:', w.notice_number, ':', w.lot_id, ':',
                toString(w.winner_ordinal)
            ) AS evidence_id,
            '{source_slug}' AS source_slug,
            concat(w.notice_number, ':', w.lot_id) AS source_reference,
            -- hankintailmoitukset.fi is a single-page app, so it answers 200
            -- for any path and the pattern could not be verified the way TED's
            -- was. Kept because the notice number is the portal's own key, but
            -- treat a dead link here as unsurprising.
            concat(
                'https://www.hankintailmoitukset.fi/fi/public/procurement/',
                w.notice_number, '/overview'
            ) AS source_url,
            toDate(w.published_at) AS publication_date,
            any(coalesce(nullIf(n.buyer_name_fi, ''), n.buyer_name_en)) AS buyer_name,
            any(coalesce(
                nullIf(n.lot_name_fi, ''),
                nullIf(n.notice_name_fi, ''),
                nullIf(n.lot_name_en, ''),
                n.notice_name_en
            )) AS title,
            any(coalesce(n.procedure_type, '')) AS agreement_type,
            max(greatest(w.resolved_at, n.resolved_at)) AS source_updated_at,
            if(
                w.published_at IS NULL
                    OR any(coalesce(nullIf(n.buyer_name_fi, ''), n.buyer_name_en)) = ''
                    OR any(coalesce(
                        nullIf(n.lot_name_fi, ''), nullIf(n.notice_name_fi, ''),
                        nullIf(n.lot_name_en, ''), n.notice_name_en
                    )) = '',
                '',
                lower(hex(MD5(concat(
                    c.{rule.company_id_column}, '|',
                    lowerUTF8(replaceRegexpAll(
                        trim(any(coalesce(nullIf(n.buyer_name_fi, ''), n.buyer_name_en))),
                        '\\\\s+', ' '
                    )), '|',
                    toString(toDate(w.published_at)), '|',
                    lowerUTF8(replaceRegexpAll(trim(any(coalesce(
                        nullIf(n.lot_name_fi, ''), nullIf(n.notice_name_fi, ''),
                        nullIf(n.lot_name_en, ''), n.notice_name_en
                    ))), '\\\\s+', ' '))
                ))))
            ) AS dedup_key
        FROM corpscout.fi_hilma_notice_winners AS w
        INNER JOIN corpscout.fi_hilma_notices AS n
            ON n.notice_number = w.notice_number
           AND n.lot_id = w.lot_id
        INNER JOIN corpscout.{rule.companies_table} AS c
            ON c.{rule.company_id_column} = w.winner_business_id
        WHERE w.is_award = 1
          AND w.winner_business_id != ''
        GROUP BY
            c.{rule.company_id_column},
            w.notice_number,
            w.lot_id,
            w.winner_ordinal,
            w.published_at
    )"""


UHM = ProcurementSource(
    slug=UHM_SOURCE_SLUG,
    upstream_asset_key="sweden_uhm_procurement_awards_clickhouse",
    required_tables=("se_uhm_procurement_awards",),
    build_cte=_uhm_cte,
)

TED = ProcurementSource(
    slug=TED_SOURCE_SLUG,
    upstream_asset_key="ted_publish_clickhouse",
    required_tables=("ted_notice_winners", "ted_notices"),
    build_cte=_ted_cte,
)

HILMA = ProcurementSource(
    slug=HILMA_SOURCE_SLUG,
    upstream_asset_key="finland_hilma_clickhouse",
    required_tables=("fi_hilma_notice_winners", "fi_hilma_notices"),
    build_cte=_hilma_cte,
)
