"""Incremental Brave answer extraction with durable per-company response checkpoints."""

import re
from collections import defaultdict
from collections.abc import Callable
from contextlib import closing
from datetime import UTC, datetime, timedelta
from html import unescape
from urllib.parse import urlsplit

import dagster as dg
from dagster_clickhouse import ClickhouseResource
from pydantic import Field

from dagster_v3.defs.clickhouse.resolved import assert_clickhouse_tables_exist
from dagster_v3.defs.esef_filings.website_candidates import registrable_domain_for_host
from dagster_v3.defs.se_company.basic_info.extract import ExtractConfig, scope_pages
from dagster_v3.defs.se_company.domain import tables
from dagster_v3.defs.se_company.domain.batch import read_rows
from dagster_v3.defs.se_company.domain.evidence import digest, json_text

EXTRACTOR_VERSION = "brave-domains-v1"
CHECKPOINT_TABLE = "se_company_domain_brave_extraction"
CHECKPOINT_COLUMNS = (
    "company_id",
    "brave_result_id",
    "answer_hash",
    "domains_json",
    "extractor_version",
    "processed_at",
    "source_run_id",
)
QUERY_SETTINGS = {"max_execution_time": 1800, "use_query_condition_cache": 0}
SOURCE_COLUMNS = (
    "company_id",
    "result_id",
    "answer_text",
    "answer_hash",
    "query",
    "source_url",
    "completed_at",
)
SOURCE_SQL = """SELECT company_id,result_id,answer_text,lower(hex(SHA256(answer_text))) AS answer_hash,
    query,source_url,completed_at
FROM corpscout.se_company_brave_domains FINAL
WHERE country_code='SE' AND query_type='official_website' AND status='success'"""
CHANGED_SQL = f"""SELECT answer.company_id FROM ({SOURCE_SQL}) AS answer
LEFT ANTI JOIN (SELECT * FROM corpscout.{CHECKPOINT_TABLE} FINAL) AS processed
ON answer.company_id=processed.company_id AND answer.result_id=processed.brave_result_id
    AND answer.answer_hash=processed.answer_hash AND processed.extractor_version=%(extractor_version)s"""

# Consume full URLs so domain-like text inside paths cannot become another host.
# The bare-host branch also handles Markdown link labels and international names.
WEBSITE_PATTERN = re.compile(
    r"[a-z][a-z0-9+.-]*://[^\s<>\"'`*]+"
    r"|(?<![\w@.-])(?:[^\W_][\w-]*\.)+[^\W\d_][\w-]*(?![\w@-]|\.[\w-])"
    r"(?:/[^\s<>\"'`*]*)?",
    re.IGNORECASE,
)


class BraveExtractConfig(ExtractConfig):
    execute: bool = Field(
        default=True,
        description="Save Brave domain suggestions and extraction checkpoints. Set false to preview without saving.",
    )


def extract_domains(answer: str) -> list[str]:
    """Unique PSL-validated registrable domains in first-mention order, without I/O."""
    domains = []
    for match in WEBSITE_PATTERN.finditer(unescape(answer)):
        candidate = match[0].rstrip(".,;:!?)]}")
        try:
            parsed = urlsplit(
                candidate if "://" in candidate else "https://" + candidate
            )
            if (
                parsed.scheme.lower() not in ("http", "https")
                or parsed.username is not None
                or parsed.password is not None
            ):
                continue
            if parsed.hostname is None:
                continue
            domain = registrable_domain_for_host(parsed.hostname)
        except ValueError:
            continue
        if domain is not None and domain not in domains:
            domains.append(domain)
    return domains


def suggestion_rows(
    answer: dict,
    domains: list[str],
    previous: list[dict],
    *,
    stamp: datetime,
    run_id: str,
) -> list[dict]:
    """Replace this source's domain slots, retaining the response as verification evidence."""
    evidence = json_text(
        {
            "query": answer["query"],
            "answer_text": answer["answer_text"],
            "domains": domains,
        }
    )
    observed_at = answer["completed_at"].replace(
        microsecond=answer["completed_at"].microsecond // 1000 * 1000
    )
    current = {row["slot"]: row for row in previous}
    rows = []
    for index, domain in enumerate(domains):
        rows.append(
            {
                "company_id": answer["company_id"],
                "source": "brave",
                "slot": domain,
                "root_domain": domain,
                "website_url": f"https://{domain}",
                "website_host": domain,
                "association": "uncertain",
                "is_primary": int(index == 0),
                "confidence": 0.5,
                "confidence_basis": "brave_answer_candidate",
                "source_record_id": answer["result_id"],
                "source_url": answer["source_url"],
                "evidence": evidence,
                "observed_at": observed_at,
                "removed": 0,
                "decided_by": "",
                "note": "",
            }
        )
    for old in previous:
        if not old["removed"] and old["root_domain"] not in domains:
            rows.append(
                {
                    **old,
                    "removed": 1,
                    "association": "uncertain",
                    "is_primary": 0,
                    "confidence": 0.0,
                    "source_record_id": answer["result_id"],
                    "source_url": answer["source_url"],
                    "evidence": evidence,
                    "observed_at": observed_at,
                }
            )
    changed = []
    ignored = {"suggestion_id", "suggested_at", "source_run_id"}
    for row in rows:
        row.update(extractor_version=EXTRACTOR_VERSION)
        before = current.get(row["slot"])
        if before is not None and all(
            before[column] == row[column]
            for column in tables.SUGGESTION_COLUMNS
            if column not in ignored
        ):
            continue
        # A replay after a checkpoint failure retains the same logical observation.
        row.update(
            suggestion_id=digest(
                json_text(
                    [
                        answer["company_id"],
                        answer["result_id"],
                        answer["answer_hash"],
                        row["slot"],
                        row["removed"],
                        EXTRACTOR_VERSION,
                    ]
                )
            ),
            suggested_at=stamp,
            source_run_id=run_id,
        )
        changed.append(row)
    return changed


def process_brave_answers(
    client, *, config: BraveExtractConfig, run_id: str, log: Callable
) -> dict:
    params = {"extractor_version": EXTRACTOR_VERSION}
    scope_sql = CHANGED_SQL
    if config.since:
        scope_sql = f"SELECT company_id FROM ({SOURCE_SQL}) WHERE completed_at>parseDateTime64BestEffort(%(since)s,6,'UTC')"
        params["since"] = config.since
    pages = (
        (
            config.company_ids[i : i + config.page_size]
            for i in range(0, len(config.company_ids), config.page_size)
        )
        if config.company_ids
        else scope_pages(
            client,
            scope_sql=scope_sql,
            params=params,
            page_size=config.page_size,
            settings=QUERY_SETTINGS,
            prefix="corpscout._tmp_brave_domain_scope_",
        )
    )
    counts = dict(
        companies=0,
        domains=0,
        empty_answers=0,
        suggestions_written=0,
        checkpoints_written=0,
        execute=config.execute,
        stopped_at_cap=False,
    )
    with closing(pages) as scope:
        for ids in scope:
            remaining = config.max_companies - counts["companies"]
            if len(ids) > remaining:
                counts["stopped_at_cap"] = True
                ids = ids[:remaining]
            if not ids:
                break
            answers = [
                dict(zip(SOURCE_COLUMNS, values, strict=True))
                for values in client.execute(
                    f"SELECT {','.join(SOURCE_COLUMNS)} FROM ({SOURCE_SQL}) WHERE company_id IN %(company_ids)s ORDER BY company_id",
                    {"company_ids": tuple(ids)},
                    settings=QUERY_SETTINGS,
                )
            ]
            previous = defaultdict(list)
            for row in read_rows(
                client, tables.SUGGESTION_TABLE, tables.SUGGESTION_COLUMNS, ids
            ):
                if row["source"] == "brave":
                    previous[row["company_id"]].append(row)
            stamp = max(
                [
                    datetime.now(UTC),
                    *(
                        row["suggested_at"].replace(tzinfo=UTC)
                        + timedelta(milliseconds=1)
                        for rows in previous.values()
                        for row in rows
                    ),
                ]
            )
            suggestions, checkpoints = [], []
            for answer in answers:
                domains = extract_domains(answer["answer_text"])
                suggestions.extend(
                    suggestion_rows(
                        answer,
                        domains,
                        previous[answer["company_id"]],
                        stamp=stamp,
                        run_id=run_id,
                    )
                )
                checkpoints.append(
                    dict(
                        zip(
                            CHECKPOINT_COLUMNS,
                            (
                                answer["company_id"],
                                answer["result_id"],
                                answer["answer_hash"],
                                json_text(domains),
                                EXTRACTOR_VERSION,
                                stamp,
                                run_id,
                            ),
                            strict=True,
                        )
                    )
                )
                counts["companies"] += 1
                counts["domains"] += len(domains)
                counts["empty_answers"] += int(not domains)
            if config.execute:
                # Acknowledged suggestions first, checkpoint second. A failure between
                # them is replayable, including a response that withdraws every domain.
                for table, columns, rows, counter in (
                    (
                        tables.SUGGESTION_TABLE,
                        tables.SUGGESTION_COLUMNS,
                        suggestions,
                        "suggestions_written",
                    ),
                    (
                        CHECKPOINT_TABLE,
                        CHECKPOINT_COLUMNS,
                        checkpoints,
                        "checkpoints_written",
                    ),
                ):
                    if rows:
                        client.execute(
                            f"INSERT INTO corpscout.{table} ({','.join(columns)}) VALUES",
                            [tuple(row[column] for column in columns) for row in rows],
                            settings={"async_insert": 0},
                        )
                        counts[counter] += len(rows)
            log(
                "Brave domain extraction: companies=%d domains=%d empty=%d execute=%s",
                counts["companies"],
                counts["domains"],
                counts["empty_answers"],
                config.execute,
            )
            if counts["stopped_at_cap"]:
                break
    return counts


@dg.asset(
    group_name=tables.GROUP_NAME,
    kinds={"clickhouse", "python"},
    pool="se_company_domain_brave",
    deps=["se_company_brave_domains"],
    metadata={"table": "corpscout.se_company_domain_suggestion", "source": "brave"},
    description="Extract and save domain suggestions from each new official-website Brave response. Save response-ID/hash checkpoints with JSON domain lists after source suggestions, including empty lists. Candidates retain the original answer for domain verification. Writes are enabled by default; execute=false previews without saving.",
)
def se_company_domain_suggestions_brave(
    context: dg.AssetExecutionContext,
    config: BraveExtractConfig,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    assert_clickhouse_tables_exist(
        clickhouse,
        database=tables.DATABASE,
        tables=("se_company_brave_domains", CHECKPOINT_TABLE, tables.SUGGESTION_TABLE),
    )
    with clickhouse.get_connection() as client:
        counts = process_brave_answers(
            client, config=config, run_id=context.run.run_id, log=context.log.info
        )
    return dg.MaterializeResult(
        metadata={**counts, "source": "brave", "checkpoint_table": CHECKPOINT_TABLE}
    )


se_company_domain_brave_job = dg.define_asset_job(
    "se_company_domain_brave_job",
    selection=dg.AssetSelection.assets(
        se_company_domain_suggestions_brave, "se_company_domain_precedence_clickhouse"
    ),
)
