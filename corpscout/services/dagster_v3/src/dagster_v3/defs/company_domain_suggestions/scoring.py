from __future__ import annotations

import re
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from dagster_v3.defs.company_domain_suggestions import tables

IDENTIFIER_SCORE = 70.0
WEBSITE_NAME_SCORE = 45.0
DOMAIN_NAME_SCORE = 35.0
DOMAIN_FIRST_TOKEN_SCORE = 20.0
DOMAIN_ACRONYM_SCORE = 15.0
PEOPLE_SCORE = 25.0
INDUSTRY_SCORE = 10.0
COUNTRY_SCORE = 5.0
WEB_PRESENCE_SCORE = 5.0
CONFLICT_PENALTY = 100.0

MAX_COMPANIES_PER_NAME_FEATURE = 20
MAX_COMPANIES_PER_PERSON_FEATURE = 5

_WORD_RE = re.compile(r"[^\W_]+", re.UNICODE)
_LEGAL_FORM_SUFFIXES = (
    ("ab", "publ"),
    ("aktiebolag", "publ"),
    ("ekonomisk", "förening"),
    ("ideell", "förening"),
    ("kommanditbolag",),
    ("handelsbolag",),
    ("aktiebolag",),
    ("stiftelse",),
    ("ab",),
    ("hb",),
    ("kb",),
)


@dataclass(frozen=True)
class CompanyFeature:
    feature_type: str
    normalized_value: str
    raw_value: str
    source_field: str
    trigger_score: float


def normalize_match_value(value: str) -> str:
    return "".join(character for character in value.lower() if character.isalnum())


def company_name_features(legal_name: str) -> tuple[CompanyFeature, ...]:
    tokens = tuple(_WORD_RE.findall(legal_name.lower()))
    if not tokens:
        return ()

    core_tokens = _without_legal_form(tokens)
    full_value = normalize_match_value(legal_name)
    core_value = "".join(core_tokens)
    features: list[CompanyFeature] = []
    _append_feature(
        features,
        feature_type="organization_name",
        normalized_value=full_value,
        raw_value=legal_name,
        source_field="legal_name_full",
        trigger_score=WEBSITE_NAME_SCORE,
    )
    _append_feature(
        features,
        feature_type="organization_name",
        normalized_value=core_value,
        raw_value=legal_name,
        source_field="legal_name_core",
        trigger_score=WEBSITE_NAME_SCORE,
    )
    _append_feature(
        features,
        feature_type="domain_label",
        normalized_value=core_value,
        raw_value=legal_name,
        source_field="legal_name_domain_core",
        trigger_score=DOMAIN_NAME_SCORE,
    )

    if len(core_tokens) > 1 and len(core_tokens[0]) >= 5:
        _append_feature(
            features,
            feature_type="domain_label",
            normalized_value=core_tokens[0],
            raw_value=legal_name,
            source_field="legal_name_domain_first_token",
            trigger_score=DOMAIN_FIRST_TOKEN_SCORE,
        )
    acronym = "".join(token[0] for token in core_tokens if token)
    if len(core_tokens) > 1 and len(acronym) >= 3:
        _append_feature(
            features,
            feature_type="domain_label",
            normalized_value=acronym,
            raw_value=legal_name,
            source_field="legal_name_domain_acronym",
            trigger_score=DOMAIN_ACRONYM_SCORE,
        )
    return tuple(features)


def company_identifier_features(
    company_id: str,
    *,
    lei: str | None = None,
) -> tuple[CompanyFeature, ...]:
    normalized_company_id = normalize_match_value(company_id)
    if not normalized_company_id:
        return ()
    values = [
        CompanyFeature(
            feature_type="identifier",
            normalized_value=normalized_company_id,
            raw_value=company_id,
            source_field="registration_number",
            trigger_score=IDENTIFIER_SCORE,
        ),
        CompanyFeature(
            feature_type="identifier",
            normalized_value=f"se{normalized_company_id}01",
            raw_value=f"SE{company_id}01",
            source_field="vat",
            trigger_score=IDENTIFIER_SCORE,
        ),
    ]
    normalized_lei = normalize_match_value(lei or "")
    if normalized_lei:
        values.append(
            CompanyFeature(
                feature_type="identifier",
                normalized_value=normalized_lei,
                raw_value=str(lei),
                source_field="lei",
                trigger_score=IDENTIFIER_SCORE,
            )
        )
    return tuple(values)


def prepare_staging_tables(connection: Any) -> None:
    schema = tables.DUCKDB_SCHEMA
    connection.execute(f"create schema if not exists {schema}")
    connection.execute(
        f"""
        create or replace table {schema}.companies (
            company_id varchar,
            legal_name varchar
        )
        """
    )
    connection.execute(
        f"""
        create or replace table {schema}.company_features (
            company_id varchar,
            feature_type varchar,
            normalized_value varchar,
            raw_value varchar,
            source_field varchar,
            trigger_score float
        )
        """
    )
    connection.execute(
        f"""
        create or replace table {schema}.company_people_raw (
            company_id varchar,
            normalized_value varchar,
            raw_value varchar,
            source_field varchar
        )
        """
    )
    connection.execute(
        f"""
        create or replace table {schema}.domain_features (
            feature_type varchar,
            normalized_value varchar,
            root_domain varchar,
            raw_value varchar,
            source_field varchar,
            source_url varchar,
            crawl_id varchar,
            source_resolved_at timestamptz
        )
        """
    )
    connection.execute(
        f"""
        create or replace table {schema}.company_industries (
            company_id varchar,
            nace_code varchar
        )
        """
    )
    connection.execute(
        f"""
        create or replace table {schema}.domain_industries (
            root_domain varchar,
            nace_code varchar,
            crawl_id varchar,
            source_url varchar
        )
        """
    )
    connection.execute(
        f"""
        create or replace table {schema}.domain_support (
            root_domain varchar,
            country_match boolean,
            country_value varchar,
            crawl_id varchar,
            source_url varchar
        )
        """
    )
    connection.execute(
        f"""
        create or replace table {schema}.domain_identifiers (
            root_domain varchar,
            normalized_value varchar,
            raw_value varchar,
            source_field varchar,
            crawl_id varchar,
            source_url varchar
        )
        """
    )


def add_distinctive_people_features(connection: Any) -> int:
    schema = tables.DUCKDB_SCHEMA
    connection.execute(
        f"""
        insert into {schema}.company_features
        select
            company_id,
            'person_name' as feature_type,
            normalized_value,
            any_value(raw_value) as raw_value,
            any_value(source_field) as source_field,
            {PEOPLE_SCORE}::float as trigger_score
        from {schema}.company_people_raw
        where length(normalized_value) >= 8
        group by company_id, normalized_value
        qualify count(distinct company_id) over (partition by normalized_value)
            <= {MAX_COMPANIES_PER_PERSON_FEATURE}
        """
    )
    return _table_count(
        connection, f"{schema}.company_features", "feature_type = 'person_name'"
    )


def remove_ambiguous_name_features(connection: Any) -> int:
    schema = tables.DUCKDB_SCHEMA
    before = _table_count(connection, f"{schema}.company_features")
    connection.execute(
        f"""
        create or replace temporary table filtered_company_features as
        select * exclude (company_count, duplicate_rank)
        from (
            select
                *,
                count(distinct company_id) over (
                    partition by feature_type, normalized_value
                ) as company_count,
                row_number() over (
                    partition by company_id, feature_type, normalized_value
                    order by trigger_score desc, source_field
                ) as duplicate_rank
            from {schema}.company_features
            where normalized_value <> ''
        )
        where duplicate_rank = 1
          and (
              feature_type = 'identifier'
              or (
                  feature_type = 'person_name'
                  and company_count <= {MAX_COMPANIES_PER_PERSON_FEATURE}
              )
              or (
                  feature_type in ('organization_name', 'domain_label')
                  and company_count <= {MAX_COMPANIES_PER_NAME_FEATURE}
              )
          )
        """
    )
    connection.execute(f"delete from {schema}.company_features")
    connection.execute(
        f"insert into {schema}.company_features select * from filtered_company_features"
    )
    return before - _table_count(connection, f"{schema}.company_features")


def replace_scored_suggestions(
    connection: Any,
    *,
    discovery_run_id: str,
    suggested_at: datetime,
    log: Callable[..., object] | None = None,
) -> dict[str, int | float]:
    schema = tables.DUCKDB_SCHEMA
    scoring_started_at = time.monotonic()
    _log(log, "Sweden domain suggestion scoring started")
    _execute_scoring_sql(
        connection,
        phase="run_context",
        log=log,
        sql="""
        create or replace temporary table suggestion_run_context as
        select ?::varchar as discovery_run_id, ?::timestamptz as suggested_at
        """,
        parameters=[discovery_run_id, suggested_at],
    )
    _execute_scoring_sql(
        connection,
        phase="trigger_evidence",
        output_table="trigger_evidence",
        log=log,
        sql=f"""
        create or replace temporary table trigger_evidence as
        select * exclude (evidence_rank)
        from (
            select
                cf.company_id,
                c.legal_name as company_name,
                df.root_domain,
                cf.feature_type,
                case cf.feature_type
                    when 'identifier' then 'identifier'
                    when 'organization_name' then 'website_name'
                    when 'domain_label' then 'domain_name'
                    when 'person_name' then 'person'
                end as signal_type,
                cf.source_field,
                cf.raw_value as company_value,
                df.raw_value as domain_value,
                cf.trigger_score as score_contribution,
                df.source_url,
                df.crawl_id,
                row_number() over (
                    partition by cf.company_id, df.root_domain, cf.feature_type
                    order by
                        cf.trigger_score desc,
                        df.source_resolved_at desc,
                        df.source_url
                ) as evidence_rank
            from {schema}.company_features cf
            inner join {schema}.companies c using (company_id)
            inner join {schema}.domain_features df
                on df.feature_type = cf.feature_type
               and df.normalized_value = cf.normalized_value
        )
        where evidence_rank = 1
        """,
    )
    _execute_scoring_sql(
        connection,
        phase="candidate_components",
        output_table="candidate_components",
        log=log,
        sql="""
        create or replace temporary table candidate_components as
        select
            company_id,
            any_value(company_name) as company_name,
            root_domain,
            list_sort(list(distinct feature_type)) as candidate_sources,
            coalesce(max(score_contribution) filter (
                where feature_type = 'identifier'
            ), 0)::float as identifier_score,
            coalesce(max(score_contribution) filter (
                where feature_type = 'organization_name'
            ), 0)::float as website_name_score,
            coalesce(max(score_contribution) filter (
                where feature_type = 'domain_label'
            ), 0)::float as domain_name_score,
            coalesce(max(score_contribution) filter (
                where feature_type = 'person_name'
            ), 0)::float as people_score
        from trigger_evidence
        group by company_id, root_domain
        """,
    )
    _execute_scoring_sql(
        connection,
        phase="industry_evidence",
        output_table="industry_evidence",
        log=log,
        sql=f"""
        create or replace temporary table industry_evidence as
        select * exclude (industry_rank)
        from (
            select
                cc.company_id,
                cc.root_domain,
                ci.nace_code,
                di.source_url,
                di.crawl_id,
                row_number() over (
                    partition by cc.company_id, cc.root_domain
                    order by ci.nace_code, di.crawl_id desc
                ) as industry_rank
            from candidate_components cc
            inner join {schema}.company_industries ci using (company_id)
            inner join {schema}.domain_industries di
                on di.root_domain = cc.root_domain
               and di.nace_code = ci.nace_code
        )
        where industry_rank = 1
        """,
    )
    _execute_scoring_sql(
        connection,
        phase="candidate_scoring",
        output_table="candidate_scored",
        log=log,
        sql=f"""
        create or replace temporary table candidate_scored as
        select
            cc.*,
            case when ie.company_id is not null then {INDUSTRY_SCORE} else 0 end::float
                as industry_score,
            case when coalesce(ds.country_match, false) then {COUNTRY_SCORE} else 0 end::float
                as country_score,
            case when ds.root_domain is not null then {WEB_PRESENCE_SCORE} else 0 end::float
                as web_presence_score,
            case when exists (
                select 1
                from {schema}.domain_identifiers di
                where di.root_domain = cc.root_domain
                  and (
                      regexp_matches(di.normalized_value, '^[0-9]{{10}}$')
                      or regexp_matches(di.normalized_value, '^se[0-9]{{12}}$')
                      or regexp_matches(di.normalized_value, '^[a-z0-9]{{20}}$')
                  )
                  and not exists (
                      select 1
                      from {schema}.company_features cf
                      where cf.company_id = cc.company_id
                        and cf.feature_type = 'identifier'
                        and cf.normalized_value = di.normalized_value
                  )
            ) then {CONFLICT_PENALTY} else 0 end::float as conflict_penalty,
            ds.country_value,
            ds.crawl_id as support_crawl_id,
            ds.source_url as support_source_url
        from candidate_components cc
        left join industry_evidence ie
            on ie.company_id = cc.company_id
           and ie.root_domain = cc.root_domain
        left join {schema}.domain_support ds
           on ds.root_domain = cc.root_domain
        """,
    )
    _execute_scoring_sql(
        connection,
        phase="suggestions",
        output_table=f"{schema}.{tables.SUGGESTIONS_TABLE}",
        log=log,
        sql=f"""
        create or replace table {schema}.{tables.SUGGESTIONS_TABLE} as
        select
            '{tables.COUNTRY_ISO2}'::varchar as country_iso2,
            company_id,
            root_domain,
            row_number() over (
                partition by company_id
                order by total_score desc, root_domain
            )::usmallint as rank,
            company_name,
            candidate_sources,
            identifier_score,
            website_name_score,
            domain_name_score,
            people_score,
            industry_score,
            country_score,
            web_presence_score,
            conflict_penalty,
            total_score,
            '{tables.SCORING_VERSION}'::varchar as scoring_version,
            rc.discovery_run_id,
            rc.suggested_at
        from (
            select
                *,
                least(
                    100,
                    greatest(
                        0,
                        identifier_score
                        + website_name_score
                        + domain_name_score
                        + people_score
                        + industry_score
                        + country_score
                        + web_presence_score
                        - conflict_penalty
                    )
                )::float as total_score
            from candidate_scored
        ) scored
        cross join suggestion_run_context rc
        where conflict_penalty < {CONFLICT_PENALTY}
          and total_score > 0
        """,
    )
    _execute_scoring_sql(
        connection,
        phase="evidence",
        output_table=f"{schema}.{tables.EVIDENCE_TABLE}",
        log=log,
        sql=f"""
        create or replace table {schema}.{tables.EVIDENCE_TABLE} as
        select
            '{tables.COUNTRY_ISO2}'::varchar as country_iso2,
            te.company_id,
            te.root_domain,
            te.signal_type,
            te.source_field,
            te.company_value,
            te.domain_value,
            te.score_contribution::float as score_contribution,
            te.source_url,
            te.crawl_id,
            rc.discovery_run_id,
            rc.suggested_at
        from trigger_evidence te
        inner join {schema}.{tables.SUGGESTIONS_TABLE} s
            on s.company_id = te.company_id
           and s.root_domain = te.root_domain
        cross join suggestion_run_context rc

        union all

        select
            '{tables.COUNTRY_ISO2}',
            cs.company_id,
            cs.root_domain,
            'industry',
            'nace_code',
            ie.nace_code,
            ie.nace_code,
            {INDUSTRY_SCORE}::float,
            ie.source_url,
            ie.crawl_id,
            rc.discovery_run_id,
            rc.suggested_at
        from candidate_scored cs
        inner join industry_evidence ie
            on ie.company_id = cs.company_id
           and ie.root_domain = cs.root_domain
        inner join {schema}.{tables.SUGGESTIONS_TABLE} s
            on s.company_id = cs.company_id
           and s.root_domain = cs.root_domain
        cross join suggestion_run_context rc

        union all

        select
            '{tables.COUNTRY_ISO2}',
            cs.company_id,
            cs.root_domain,
            'country',
            'country',
            '{tables.COUNTRY_ISO2}',
            coalesce(cs.country_value, ''),
            {COUNTRY_SCORE}::float,
            coalesce(cs.support_source_url, ''),
            coalesce(cs.support_crawl_id, ''),
            rc.discovery_run_id,
            rc.suggested_at
        from candidate_scored cs
        inner join {schema}.{tables.SUGGESTIONS_TABLE} s
            on s.company_id = cs.company_id
           and s.root_domain = cs.root_domain
        cross join suggestion_run_context rc
        where cs.country_score > 0

        union all

        select
            '{tables.COUNTRY_ISO2}',
            cs.company_id,
            cs.root_domain,
            'web_presence',
            'commoncrawl',
            '',
            cs.root_domain,
            {WEB_PRESENCE_SCORE}::float,
            coalesce(cs.support_source_url, ''),
            coalesce(cs.support_crawl_id, ''),
            rc.discovery_run_id,
            rc.suggested_at
        from candidate_scored cs
        inner join {schema}.{tables.SUGGESTIONS_TABLE} s
            on s.company_id = cs.company_id
           and s.root_domain = cs.root_domain
        cross join suggestion_run_context rc
        where cs.web_presence_score > 0
        """,
    )
    candidate_pairs = _table_count(connection, "candidate_components")
    disqualified_candidates = _table_count(
        connection,
        "candidate_scored",
        f"conflict_penalty >= {CONFLICT_PENALTY}",
    )
    counts: dict[str, int | float] = {
        "candidate_pairs": candidate_pairs,
        "disqualified_candidates": disqualified_candidates,
        "suggestions": _table_count(connection, f"{schema}.{tables.SUGGESTIONS_TABLE}"),
        "evidence": _table_count(connection, f"{schema}.{tables.EVIDENCE_TABLE}"),
        "scoring_elapsed_seconds": round(time.monotonic() - scoring_started_at, 3),
    }
    _execute_scoring_sql(
        connection,
        phase="run_metrics",
        output_table=f"{schema}.run_metrics",
        log=log,
        sql=f"""
        create or replace table {schema}.run_metrics as
        select
            ?::ubigint as candidate_pairs,
            ?::ubigint as disqualified_candidates,
            ?::ubigint as suggestions,
            ?::ubigint as evidence
        """,
        parameters=[
            counts["candidate_pairs"],
            counts["disqualified_candidates"],
            counts["suggestions"],
            counts["evidence"],
        ],
    )
    _log(log, "Sweden domain suggestion scoring completed: counts=%s", counts)
    return counts


def _execute_scoring_sql(
    connection: Any,
    *,
    phase: str,
    sql: str,
    log: Callable[..., object] | None,
    parameters: Sequence[object] | None = None,
    output_table: str | None = None,
) -> None:
    phase_started_at = time.monotonic()
    _log(log, "Sweden domain suggestion scoring phase started: phase=%s", phase)
    if parameters is None:
        connection.execute(sql)
    else:
        connection.execute(sql, parameters)

    elapsed_seconds = time.monotonic() - phase_started_at
    if output_table is None:
        _log(
            log,
            "Sweden domain suggestion scoring phase completed: phase=%s "
            "elapsed_seconds=%.1f",
            phase,
            elapsed_seconds,
        )
        return

    _log(
        log,
        "Sweden domain suggestion scoring phase completed: phase=%s rows=%d "
        "elapsed_seconds=%.1f",
        phase,
        _table_count(connection, output_table),
        elapsed_seconds,
    )


def _without_legal_form(tokens: tuple[str, ...]) -> tuple[str, ...]:
    core = tokens
    changed = True
    while core and changed:
        changed = False
        for suffix in _LEGAL_FORM_SUFFIXES:
            if len(core) >= len(suffix) and core[-len(suffix) :] == suffix:
                core = core[: -len(suffix)]
                changed = True
                break
    return core or tokens


def _append_feature(
    features: list[CompanyFeature],
    *,
    feature_type: str,
    normalized_value: str,
    raw_value: str,
    source_field: str,
    trigger_score: float,
) -> None:
    if len(normalized_value) < 3:
        return
    candidate = CompanyFeature(
        feature_type=feature_type,
        normalized_value=normalized_value,
        raw_value=raw_value,
        source_field=source_field,
        trigger_score=trigger_score,
    )
    if candidate not in features:
        features.append(candidate)


def _table_count(connection: Any, table: str, where: str = "") -> int:
    predicate = f" where {where}" if where else ""
    row = connection.execute(f"select count(*) from {table}{predicate}").fetchone()
    return int(row[0]) if row is not None else 0


def _log(
    log: Callable[..., object] | None,
    message: str,
    *args: object,
) -> None:
    if log is not None:
        log(message, *args)
