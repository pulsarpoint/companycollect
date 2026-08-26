r"""v7 exploration driver: measure candidate matcher loosenings against the LOCAL
workbench, on BOTH the unmatched (yield) pool and the matched-control (regression) pool.

Runs the REAL matcher engine (search_documents + address_resolution.resolution) with the
production reference-index construction reused verbatim from
``address_resolution_shadow``. The expensive OSM reference index (~6.8M rows, independent
of the suffix map and the policy knobs varied here) is built ONCE and cached in a real
table, so each candidate only re-runs street-variants + candidate-gen + results.

Every candidate is a ``Candidate`` = (suffix map, optional replacement expansion function,
policy). Production code is never edited: the suffix map and policy are injected by
rebinding the shadow module globals, and a replacement ``expanded_street_suffix_variants``
(for separate-word definite forms) is injected by monkeypatching the search_documents
module for the duration of the candidate run.

Not a Dagster run, no store writes, SELECT-only against ClickHouse (only the control pull,
which lives in geocode_workbench_experiment / the driver's --refresh-control path).
"""

from __future__ import annotations

import argparse
import sys
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

_SERVICE_ROOT = Path(__file__).resolve().parents[1]
if str(_SERVICE_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(_SERVICE_ROOT / "src"))

import duckdb
from dotenv import load_dotenv

from dagster_v3.defs.address_resolution import resolution as res
from dagster_v3.defs.address_resolution import search_documents as sd
from dagster_v3.defs.address_resolution.resolution import (
    replace_address_resolution_candidates,
    replace_address_resolution_results,
)
from dagster_v3.defs.address_resolution.search_documents import (
    replace_address_search_documents,
    replace_address_street_variants,
)
from dagster_v3.defs.sweden_company import address_resolution_shadow as sm
from dagster_v3.defs.sweden_company.address_canonicalization import ENRICHMENT_SCHEMA
from dagster_v3.defs.sweden_company.address_resolution_policy import (
    SWEDEN_ADDRESS_RESOLUTION_POLICY,
    SWEDEN_STREET_SUFFIX_EXPANSIONS,
    SWEDEN_STREET_VARIANT_LANGUAGES,
)
from dagster_v3.defs.sweden_company.geocode_store import GEOCODED_STATUSES

WORKBENCH_PATH = _SERVICE_ROOT / "data" / "geocode_workbench_local.duckdb"
_GEO = ", ".join(f"'{s}'" for s in GEOCODED_STATUSES)

UNMATCHED_TABLE = f"{ENRICHMENT_SCHEMA}.se_addresses_current"
CONTROL_TABLE = f"{ENRICHMENT_SCHEMA}.se_control_addresses"
REF_TABLE = f"{ENRICHMENT_SCHEMA}.v7_reference_index"
BASE_SUFFIX = SWEDEN_STREET_SUFFIX_EXPANSIONS


def _log(msg: str) -> None:
    print(f"[{datetime.now():%H:%M:%S}] {msg}", flush=True)


# --------------------------------------------------------------------------- #
# Candidate definition                                                         #
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class Candidate:
    name: str
    suffix_map: Mapping[str, Mapping[str, str]]
    policy: Any = SWEDEN_ADDRESS_RESOLUTION_POLICY
    # Optional replacement for search_documents.expanded_street_suffix_variants,
    # letting a candidate expand separate-word indefinite last tokens ("stora väg" ->
    # "stora vägen") which the glued-only production function cannot.
    expansion_fn: Callable[..., tuple[str, ...]] | None = None
    # When True, suffix-expanded street variants may only match a reference EXACTLY --
    # they are kept out of the fuzzy edit-distance path. This blocks the double-guess
    # (expand a guessed suffix, THEN fuzzy-match it to a near-namesake) that turns a
    # decisive match into a spurious near-tie (Strandbergsgatan vs Strindbergsgatan).
    exact_expanded_only: bool = False
    # Strictly-additive v7 mode: v6 glued suffixes keep fuzzy; these NEW glued abbreviations
    # (and their punctuated forms) plus the separate-word definite map are added exact-only.
    additive_new_glued: Mapping[str, str] | None = None
    additive_separate: Mapping[str, str] | None = None


_FUZZY_POSTINGS_SRC = res._replace_fuzzy_street_postings


def _fuzzy_postings_exact_expanded(
    connection: Any,
    *,
    source_table: str,
    postings_table: str,
    policy: Any,
    reference_documents: bool,
) -> None:
    """Patched _replace_fuzzy_street_postings that drops suffix_expansion variants from
    the QUERY fuzzy postings, so suffix-expanded variants never enter fuzzy_pairs."""
    reference_filter = (
        "and reference_precision = 'building'" if reference_documents else ""
    )
    street_variant_kind = (
        "'parsed'::varchar" if reference_documents else "street_variant_kind"
    )
    expanded_filter = (
        "" if reference_documents else "and street_variant_kind != 'suffix_exact'"
    )
    connection.execute(
        f"""
        create or replace temporary table {postings_table} as
        select distinct
            document_id, index_scope, country_code,
            normalized_street, normalized_house_number,
            normalized_postal_code, normalized_locality,
            {street_variant_kind} as street_variant_kind,
            signature.value as street_signature
        from {source_table}
        cross join unnest(
            list_concat([normalized_street], street_deletion_signatures)
        ) signature(value)
        where address_kind = 'physical'
          and normalized_house_number != ''
          and length(normalized_street) >= {policy.minimum_fuzzy_street_length}
          and signature.value != ''
          {reference_filter}
          {expanded_filter}
        """
    )


def _punctuated(glued: Mapping[str, Mapping[str, str]]) -> dict[str, dict[str, str]]:
    return {
        country: {**m, **{f"{a}.": e for a, e in m.items()}}
        for country, m in glued.items()
    }


# --------------------------------------------------------------------------- #
# Separate-word definite expansion (superset of the glued production function)  #
# --------------------------------------------------------------------------- #

# Indefinite last-word -> definite full word. Additive: the parsed (unchanged) street is
# always kept as a variant, so this only ever ADDS a definite candidate.
SEPARATE_DEFINITE_MAP: dict[str, str] = {
    "väg": "vägen",
    "gata": "gatan",
    "torg": "torget",
    "allé": "allén",
    "backe": "backen",
    "gränd": "gränden",
    "plan": "planen",
    "stig": "stigen",
    "led": "leden",
    "gång": "gången",
    "park": "parken",
}


def make_expansion_fn(
    separate_map: Mapping[str, str],
) -> Callable[..., tuple[str, ...]]:
    """A drop-in replacement for expanded_street_suffix_variants that ALSO expands a
    separate-word indefinite last token to its definite form, on top of the production
    glued/punctuated behaviour."""
    glued_fn = sd.expanded_street_suffix_variants

    def expand(street_name: str, suffix_expansions: Mapping[str, str]) -> tuple[str, ...]:
        out: list[str] = list(glued_fn(street_name, suffix_expansions))
        tokens = street_name.split()
        if tokens:
            last = tokens[-1]
            definite = separate_map.get(last.lower())
            if definite is not None:
                repl = definite.upper() if last.isupper() else definite
                out.append(" ".join([*tokens[:-1], repl]))
        # de-dup, preserve order
        seen: set[str] = set()
        result: list[str] = []
        for v in out:
            if v not in seen:
                seen.add(v)
                result.append(v)
        return tuple(result)

    return expand


# --------------------------------------------------------------------------- #
# Reference index (built once, cached)                                          #
# --------------------------------------------------------------------------- #


def refresh_control_pool(con: Any, *, sample_permille: int) -> int:
    """Pull a random sample of currently-GEOCODED identities from prod ClickHouse, with
    each one's baseline truth (servable status + matched OSM record ids), into
    ``se_control_addresses``. SELECT-only. The sample is a stable cityHash64 cut so
    re-pulls are reproducible."""
    import os

    import clickhouse_connect

    from dagster_v3.defs.sweden_company.geocode_store import build_current_geocodes_sql

    ch = clickhouse_connect.get_client(
        host=os.environ["CLICKHOUSE_HOST"],
        port=int(os.environ.get("CLICKHOUSE_HTTP_PORT", "8123")),
        username=os.environ["CLICKHOUSE_USER"],
        password=os.environ["CLICKHOUSE_PASSWORD"],
        database=os.environ.get("CLICKHOUSE_DATABASE", "corpscout"),
    )
    servable = build_current_geocodes_sql(
        columns=["address_id", "match_status", "candidate_record_ids", "policy_version"]
    )
    sql = f"""
        with g as (
            select address_id, match_status, candidate_record_ids, policy_version
            from ({servable}) where match_status in ({_GEO})
        )
        select
            toString(a.address_id) address_id,
            a.canonical_display_address canonical_display_address,
            a.street_address street_address,
            a.street_name street_name,
            a.house_number house_number,
            a.unit unit,
            a.postal_code postal_code,
            a.post_town post_town,
            toString(a.country_code) country_code,
            toString(a.address_kind) address_kind,
            g.match_status base_status,
            arrayStringConcat(arraySort(g.candidate_record_ids), '|') base_record_ids,
            g.policy_version base_policy_version
        from g inner join corpscout.se_addresses_current a on a.address_id = g.address_id
        where cityHash64(a.address_id) % 1000 < {int(sample_permille)}
    """
    at = ch.query_arrow(sql)
    con.execute(f"create schema if not exists {ENRICHMENT_SCHEMA}")
    con.register("_ctrl_pull", at)
    con.execute(f"create or replace table {CONTROL_TABLE} as select * from _ctrl_pull")
    con.unregister("_ctrl_pull")
    [(n,)] = con.execute(f"select count(*) from {CONTROL_TABLE}").fetchall()
    _log(f"control pool refreshed: {int(n)} geocoded identities with baseline truth")
    return int(n)


def build_reference_index(con: Any, *, rebuild: bool) -> None:
    con.execute(f"create schema if not exists {ENRICHMENT_SCHEMA}")
    exists = con.execute(
        "select count(*) from information_schema.tables "
        "where table_schema=? and table_name=?",
        [ENRICHMENT_SCHEMA, "v7_reference_index"],
    ).fetchone()[0]
    if exists and not rebuild:
        [(n,)] = con.execute(f"select count(*) from {REF_TABLE}").fetchall()
        _log(f"reference index cached ({int(n)} rows); reuse (use --rebuild-ref to redo)")
        return
    _log("building OSM reference index (once)")
    t = time.monotonic()
    sm._replace_building_reference_documents(con)
    sm._replace_street_reference_documents(con)
    con.execute(
        f"""
        create or replace table {REF_TABLE} as
        select * from _sweden_shadow_building_reference_documents
        union all
        select * from _sweden_shadow_street_reference_documents
        """
    )
    [(n,)] = con.execute(f"select count(*) from {REF_TABLE}").fetchall()
    _log(f"reference index built: {int(n)} rows in {time.monotonic()-t:.1f}s")


# --------------------------------------------------------------------------- #
# Query documents (suffix-independent; built once per pool)                     #
# --------------------------------------------------------------------------- #


def build_query_docs(
    con: Any, *, source_table: str, target: str, where: str = ""
) -> int:
    where_sql = f"and {where}" if where else ""
    replace_address_search_documents(
        con,
        table_name=target,
        source_sql=f"""
            select
                '{sm.INDEX_SCOPE}'::varchar as index_scope,
                cast(address_id as varchar) as document_id,
                country_code,
                canonical_display_address as raw_address,
                canonical_display_address as search_text,
                street_name,
                house_number,
                unit,
                postal_code,
                post_town as locality,
                case
                    when address_kind = 'physical'
                     and regexp_matches(
                        street_address,
                        '(?i)(^|[[:space:]])[0-9]+:[0-9]+($|[[:space:],])'
                     ) then 'property_identifier'
                    else address_kind
                end as address_kind,
                ''::varchar as reference_precision,
                null::double as latitude,
                null::double as longitude,
                null::double as coordinate_spread_meters,
                0::uinteger as supporting_record_count,
                cast(address_id as varchar) as source_record_id,
                ''::varchar as source_record_url
            from {source_table}
            where coalesce(street_name,'') != '' {where_sql}
        """,
    )
    [(n,)] = con.execute(f"select count(*) from {target}").fetchall()
    return int(n)


# --------------------------------------------------------------------------- #
# Run one candidate: variants + candidates + results                            #
# --------------------------------------------------------------------------- #


def build_additive_variants(
    con: Any,
    *,
    query_table: str,
    variant_table: str,
    new_glued_map: Mapping[str, str],
    separate_map: Mapping[str, str],
) -> None:
    """Variant table where v6-glued suffix variants keep the fuzzy-eligible
    'suffix_expansion' kind (unchanged behaviour) and every NEW suffix/definite variant is
    tagged 'suffix_exact' (exact-only). v7 is therefore a strict superset of v6: it never
    removes a v6 match, and its own additions can only match a reference exactly.
    """
    import pyarrow as pa

    # 1. v6 base variants (parsed + libpostal + v6 glued suffix_expansion), unchanged.
    base = "_v7_add_base"
    replace_address_street_variants(
        con,
        document_table=query_table,
        variant_table=base,
        languages_by_country=SWEDEN_STREET_VARIANT_LANGUAGES,
        suffix_expansions_by_country=BASE_SUFFIX,
    )
    # 2. NEW variants per distinct street = (new glued + punctuated + separate) MINUS v6.
    v6_fn = sd.expanded_street_suffix_variants
    full_fn = make_expansion_fn(separate_map)
    full_map = {"SE": {**new_glued_map, **{f"{a}.": e for a, e in new_glued_map.items()}}}
    rows = con.execute(
        f"select distinct country_code, street_name from {query_table} "
        "where street_name != ''"
    ).fetchall()
    countries: list[str] = []
    streets: list[str] = []
    expanded: list[str] = []
    for cc, street in rows:
        cc = str(cc)
        street = str(street)
        v6 = set(v6_fn(street, BASE_SUFFIX.get(cc, {})))
        for variant in full_fn(street, full_map.get(cc, {})):
            if variant not in v6 and variant != street:
                countries.append(cc)
                streets.append(street)
                expanded.append(variant)
    reg = "_v7_add_new_rows"
    con.register(reg, pa.table(
        {"country_code": countries, "street_name": streets, "expanded_street": expanded}
    ))
    compact = sd._compact_text_sql("n.expanded_street")
    try:
        con.execute(
            f"""
            create or replace temporary table _v7_add_new as
            select
                d.document_id, d.index_scope, d.country_code,
                n.expanded_street as street_variant,
                {compact} as normalized_street_variant,
                'suffix_exact'::varchar as variant_kind,
                3::utinyint as variant_rank
            from {query_table} d
            inner join {reg} n
                on n.country_code = d.country_code and n.street_name = d.street_name
            """
        )
    finally:
        con.unregister(reg)
    # 3. Union, dedup per (document, normalized variant) preferring the lower rank so a
    #    v6/parsed/libpostal form always wins over a duplicate suffix_exact, then rebuild
    #    deletion signatures.
    sig = sd._deletion_signatures_sql("normalized_street_variant")
    con.execute(
        f"""
        create or replace table {variant_table} as
        with unioned as (
            select document_id, index_scope, country_code, street_variant,
                   normalized_street_variant, variant_kind, variant_rank
            from {base}
            union all
            select document_id, index_scope, country_code, street_variant,
                   normalized_street_variant, variant_kind, variant_rank
            from _v7_add_new
        ), dedup as (
            select *
            from unioned
            where normalized_street_variant != ''
            qualify row_number() over (
                partition by document_id, normalized_street_variant
                order by variant_rank, street_variant
            ) = 1
        )
        select *, {sig} as street_deletion_signatures
        from dedup
        """
    )


def run_candidate(
    con: Any,
    *,
    query_table: str,
    label: str,
    candidate: Candidate,
) -> str:
    variant_table = f"{ENRICHMENT_SCHEMA}._v7_variants_{label}"
    candidate_table = f"{ENRICHMENT_SCHEMA}._v7_candidates_{label}"
    result_table = f"{ENRICHMENT_SCHEMA}.v7_results_{label}"
    additive = candidate.additive_new_glued is not None
    if additive:
        build_additive_variants(
            con,
            query_table=query_table,
            variant_table=variant_table,
            new_glued_map=candidate.additive_new_glued or {},
            separate_map=candidate.additive_separate or {},
        )
    else:
        original_fn = sd.expanded_street_suffix_variants
        if candidate.expansion_fn is not None:
            sd.expanded_street_suffix_variants = candidate.expansion_fn
        try:
            replace_address_street_variants(
                con,
                document_table=query_table,
                variant_table=variant_table,
                languages_by_country=SWEDEN_STREET_VARIANT_LANGUAGES,
                suffix_expansions_by_country=candidate.suffix_map,
            )
        finally:
            sd.expanded_street_suffix_variants = original_fn
    if candidate.exact_expanded_only or additive:
        res._replace_fuzzy_street_postings = _fuzzy_postings_exact_expanded
    try:
        replace_address_resolution_candidates(
            con,
            query_table=query_table,
            query_street_variant_table=variant_table,
            reference_table=REF_TABLE,
            candidate_table=candidate_table,
            policy=candidate.policy,
        )
    finally:
        res._replace_fuzzy_street_postings = _FUZZY_POSTINGS_SRC
    replace_address_resolution_results(
        con,
        query_table=query_table,
        candidate_table=candidate_table,
        result_table=result_table,
        policy=candidate.policy,
    )
    return result_table


# --------------------------------------------------------------------------- #
# Metrics                                                                       #
# --------------------------------------------------------------------------- #


def yield_vs_baseline(con: Any, *, baseline: str, cand: str) -> dict[str, int]:
    [(pending,)] = con.execute(f"select count(*) from {baseline}").fetchall()
    [(base_m,)] = con.execute(
        f"select count(*) from {baseline} where resolution_status in ({_GEO})"
    ).fetchall()
    [(cand_m,)] = con.execute(
        f"select count(*) from {cand} where resolution_status in ({_GEO})"
    ).fetchall()
    [(newly,)] = con.execute(
        f"""select count(*) from {cand} c join {baseline} b
            on b.query_document_id=c.query_document_id
            where c.resolution_status in ({_GEO})
              and b.resolution_status not in ({_GEO})"""
    ).fetchall()
    [(lost,)] = con.execute(
        f"""select count(*) from {cand} c join {baseline} b
            on b.query_document_id=c.query_document_id
            where b.resolution_status in ({_GEO})
              and c.resolution_status not in ({_GEO})"""
    ).fetchall()
    return {
        "pending": int(pending),
        "baseline_matched": int(base_m),
        "candidate_matched": int(cand_m),
        "newly": int(newly),
        "lost": int(lost),
        "net": int(newly) - int(lost),
    }


def regressions(con: Any, *, baseline: str, cand: str) -> dict[str, Any]:
    """On the control pool: baseline-v6 geocoded rows that the candidate breaks.

    Two regression kinds, both disqualifying:
      flip   -- baseline geocoded, candidate not geocoded (unmatched/ambiguous).
      record -- both geocoded, but the matched OSM record set changed.
    The candidate is additive, so a legitimately-better record still counts as a change:
    the bar rejects ANY change to a currently-correct match.
    """
    rows = con.execute(
        f"""
        with j as (
            select
                b.query_document_id id,
                b.resolution_status bs, c.resolution_status cs,
                list_aggregate(b.candidate_record_ids,'string_agg','|') br,
                list_aggregate(c.candidate_record_ids,'string_agg','|') cr
            from {baseline} b join {cand} c
              on b.query_document_id=c.query_document_id
            where b.resolution_status in ({_GEO})
        )
        select
            count(*) filter (where cs not in ({_GEO})) as flip,
            count(*) filter (where cs in ({_GEO}) and coalesce(br,'')!=coalesce(cr,'')) as record,
            count(*) as base_geocoded
        from j
        """
    ).fetchall()
    flip, record, base_geo = int(rows[0][0]), int(rows[0][1]), int(rows[0][2])
    return {"flip": flip, "record": record, "base_geocoded": base_geo,
            "regressions": flip + record}


def regression_samples(con: Any, *, baseline: str, cand: str, limit: int = 12) -> list:
    return con.execute(
        f"""
        with j as (
            select b.query_document_id id, b.resolution_status bs, c.resolution_status cs,
                   list_aggregate(b.candidate_record_ids,'string_agg','|') br,
                   list_aggregate(c.candidate_record_ids,'string_agg','|') cr
            from {baseline} b join {cand} c on b.query_document_id=c.query_document_id
            where b.resolution_status in ({_GEO})
              and (c.resolution_status not in ({_GEO})
                   or coalesce(list_aggregate(b.candidate_record_ids,'string_agg','|'),'')
                      != coalesce(list_aggregate(c.candidate_record_ids,'string_agg','|'),''))
        )
        select j.id, a.street_name, a.postal_code, j.bs, j.cs, j.br, j.cr
        from j join {CONTROL_TABLE} a on cast(a.address_id as varchar)=j.id
        limit {limit}
        """
    ).fetchall()


# --------------------------------------------------------------------------- #
# Candidate matrix                                                             #
# --------------------------------------------------------------------------- #

# Yield-scope regex: an address can only be newly matched by a suffix/definite candidate
# if its street's last token carries one of these abbreviations/indefinite forms.
YIELD_SCOPE_REGEX = (
    r"(?i)("
    r"[a-zåäö]{3}[a-zåäö]*(v|g|gr|gg|tg|t|pl|st|str|vg|gt|stg|ba|all|li|le|r|ga)\.\s*$"
    r"|[a-zåäö]{3}[a-zåäö]*(gg|all|stg|vg|pl|ba|li|gt|str)$"
    r"|(^|[ ])(väg|gata|torg|allé|backe|gränd|plan|stig|led|gång|park)$"
    r")"
)


def _sm(extra: dict[str, str]) -> dict[str, dict[str, str]]:
    """A SE suffix map = v6 glued base plus `extra` glued abbreviations."""
    return {"SE": {**BASE_SUFFIX["SE"], **extra}}


def build_candidates() -> list[Candidate]:
    pol = SWEDEN_ADDRESS_RESOLUTION_POLICY
    cands: list[Candidate] = []
    # G2 punctuated forms of v6 (proven ~1970)
    cands.append(Candidate("g2_punct_v6", _punctuated(BASE_SUFFIX), pol))
    # G3 additional glued+punctuated abbreviations, each on top of g2
    extra_all = {
        "gg": "gången", "all": "allén", "stg": "stigen", "pl": "plan",
        "tg": "torget", "ba": "backen", "li": "liden", "str": "stråket",
        "vg": "vägen", "gt": "gatan",
    }
    for ab, exp in extra_all.items():
        m = _punctuated(_sm({ab: exp}))
        cands.append(Candidate(f"g3_{ab}", m, pol))
    # G3 combined: all extra abbreviations, glued+punctuated
    cands.append(Candidate("g3_all_extra", _punctuated(_sm(extra_all)), pol))
    # G4 separate-word definite (needs replacement expansion fn), on top of g2 punct
    fn_full = make_expansion_fn(SEPARATE_DEFINITE_MAP)
    cands.append(
        Candidate("g4_separate_definite", _punctuated(BASE_SUFFIX), pol, fn_full)
    )
    # G4 per-word isolation for the big ones
    for w in ("väg", "gata", "torg", "allé", "gränd", "backe"):
        fn = make_expansion_fn({w: SEPARATE_DEFINITE_MAP[w]})
        cands.append(
            Candidate(f"g4_{w}", _punctuated(BASE_SUFFIX), pol, fn)
        )
    # G5 edit-distance loosening (global dist 2)
    cands.append(
        Candidate("g5_editdist2", _punctuated(BASE_SUFFIX),
                  replace(pol, maximum_street_edit_distance=2))
    )
    # G5b edit-distance 2 with a higher fuzzy floor (long streets only)
    cands.append(
        Candidate("g5_editdist2_len9", _punctuated(BASE_SUFFIX),
                  replace(pol, maximum_street_edit_distance=2,
                          minimum_fuzzy_street_length=9))
    )
    # G6 EXACT-ONLY refinements: suffix-expanded variants kept out of fuzzy_pairs, which
    # removes the double-guess near-tie regression while keeping the exact-expanded yield.
    cands.append(
        Candidate("g6_punct_exact", _punctuated(BASE_SUFFIX), pol,
                  exact_expanded_only=True)
    )
    cands.append(
        Candidate("g6_all_extra_exact", _punctuated(_sm(extra_all)), pol,
                  exact_expanded_only=True)
    )
    cands.append(
        Candidate("g6_separate_exact", _punctuated(BASE_SUFFIX), pol,
                  make_expansion_fn(SEPARATE_DEFINITE_MAP), exact_expanded_only=True)
    )
    # G7 the full accumulated v7 candidate: all extra abbreviations (glued+punctuated) AND
    # separate-word definite forms, all exact-only.
    cands.append(
        Candidate("g7_v7_full", _punctuated(_sm(extra_all)), pol,
                  make_expansion_fn(SEPARATE_DEFINITE_MAP), exact_expanded_only=True)
    )
    # G8 STRICTLY-ADDITIVE v7: v6 glued stays fuzzy-eligible (never loses a v6 match);
    # NEW variants are exact-only. This is the recommended shape.
    v6 = dict(BASE_SUFFIX["SE"])
    cands.append(  # punctuated v6 (v./g./gr.) only, additive exact-only
        Candidate("g8_punct_additive", BASE_SUFFIX, pol,
                  additive_new_glued=v6, additive_separate={})
    )
    cands.append(  # punctuated v6 + separate-word definite, additive  (RECOMMENDED v7)
        Candidate("g8_v7_recommended", BASE_SUFFIX, pol,
                  additive_new_glued=v6, additive_separate=SEPARATE_DEFINITE_MAP)
    )
    cands.append(  # + extra abbreviations too, to confirm they are still harmful here
        Candidate("g8_v7_plus_extra", BASE_SUFFIX, pol,
                  additive_new_glued={**v6, **extra_all},
                  additive_separate=SEPARATE_DEFINITE_MAP)
    )
    return cands


BASELINE = Candidate("baseline_v6", BASE_SUFFIX, SWEDEN_ADDRESS_RESOLUTION_POLICY)


def explore(con: Any, *, only: str | None) -> None:
    build_reference_index(con, rebuild=False)

    # Control query docs (suffix-independent) -- build once.
    _log("building control query documents")
    ctrl_q = f"{ENRICHMENT_SCHEMA}._v7_ctrl_query"
    nctrl = build_query_docs(con, source_table=CONTROL_TABLE, target=ctrl_q)
    _log(f"control query docs: {nctrl}")

    # Attach baseline truth (record ids) to control baseline run for regression compare.
    # Yield query docs -- scoped to abbreviation-affected streets (suffix candidates can
    # only newly-match these), built once.
    _log("building unmatched (yield) query documents, scoped to abbreviation streets")
    esc = YIELD_SCOPE_REGEX.replace("'", "''")
    yq = f"{ENRICHMENT_SCHEMA}._v7_yield_query"
    nyield = build_query_docs(
        con, source_table=UNMATCHED_TABLE, target=yq,
        where=f"regexp_matches(street_name, '{esc}')",
    )
    _log(f"scoped yield query docs: {nyield}")

    # Baseline runs on both pools.
    _log("running baseline v6 (control)")
    base_ctrl = run_candidate(con, query_table=ctrl_q, label="baseline_ctrl", candidate=BASELINE)
    _log("running baseline v6 (yield)")
    base_yield = run_candidate(con, query_table=yq, label="baseline_yield", candidate=BASELINE)

    cands = build_candidates()
    if only:
        wanted = set(only.split(","))
        cands = [c for c in cands if c.name in wanted]

    print("\n" + "=" * 112)
    print(f"{'candidate':24} {'+new':>7} {'-lost':>7} {'net':>7} "
          f"{'reg_flip':>9} {'reg_rec':>8} {'reg_tot':>8}  verdict")
    print("=" * 112)
    results: dict[str, dict] = {}
    for c in cands:
        t = time.monotonic()
        rc = run_candidate(con, query_table=ctrl_q, label=f"{c.name}_ctrl", candidate=c)
        ry = run_candidate(con, query_table=yq, label=f"{c.name}_yield", candidate=c)
        y = yield_vs_baseline(con, baseline=base_yield, cand=ry)
        r = regressions(con, baseline=base_ctrl, cand=rc)
        # SAFE = adds net yield, breaks no control match, and loses no v6 unmatched match.
        safe = y["net"] > 0 and r["regressions"] == 0 and y["lost"] == 0
        verdict = "ACCEPT" if safe else (
            "reject:regress" if r["regressions"] > 0 else
            "reject:v6-loss" if y["lost"] > 0 else "reject:no-yield")
        results[c.name] = {**y, **r, "verdict": verdict, "secs": time.monotonic()-t}
        print(f"{c.name:24} {y['newly']:>7} {y['lost']:>7} {y['net']:>7} "
              f"{r['flip']:>9} {r['record']:>8} {r['regressions']:>8}  {verdict}  "
              f"({results[c.name]['secs']:.0f}s)")
    print("=" * 112)
    # regression detail for any regressing candidate
    for c in cands:
        if results[c.name]["regressions"] > 0:
            print(f"\n-- regression samples for {c.name} --")
            for row in regression_samples(con, baseline=base_ctrl, cand=f"{ENRICHMENT_SCHEMA}.v7_results_{c.name}_ctrl"):
                print("  ", row)


def main(argv: Sequence[str] | None = None) -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--rebuild-ref", action="store_true")
    p.add_argument("--refresh-control", action="store_true",
                   help="Re-pull the matched-control sample from ClickHouse (SELECT-only).")
    p.add_argument("--control-permille", type=int, default=44,
                   help="Control sample size as parts-per-1000 of geocoded identities.")
    p.add_argument("--only", type=str, default=None, help="comma-separated candidate names")
    args = p.parse_args(argv)
    load_dotenv(_SERVICE_ROOT / ".env")
    load_dotenv(_SERVICE_ROOT.parent / "backoffice" / ".env")
    con = duckdb.connect(str(WORKBENCH_PATH))
    try:
        if args.refresh_control:
            refresh_control_pool(con, sample_permille=args.control_permille)
        if args.rebuild_ref:
            build_reference_index(con, rebuild=True)
        explore(con, only=args.only)
    finally:
        con.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
