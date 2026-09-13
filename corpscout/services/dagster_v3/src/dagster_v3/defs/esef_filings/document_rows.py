"""Replace an extractor table's rows for a set of documents atomically.

The stage + EXCHANGE TABLES recipe of llm_enrichment_assets' writer with a
document-only predicate, shared by the deterministic extractors (esef_domains
first). The LLM writer keeps its provider/model/prompt predicate for now.
"""

import uuid
from collections.abc import Mapping, Sequence

from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.esef_filings import tables


def replace_document_rows(
    clickhouse: ClickhouseResource,
    *,
    table: str,
    columns: Sequence[str],
    source_document_ids: Sequence[str],
    rows: Sequence[Mapping[str, object]],
) -> None:
    """Copy every row of `table` for documents NOT in `source_document_ids`
    into a fresh stage table, append `rows`, EXCHANGE the stage with the
    target, drop the old one. The documents' previous rows vanish even when
    `rows` holds nothing for them. No-op without `source_document_ids`.
    """
    if not source_document_ids:
        return
    target = f"`{tables.ESEF_DATABASE}`.`{table}`"
    stage_name = f"_tmp_{table}_{uuid.uuid4().hex}"
    stage = f"`{tables.ESEF_DATABASE}`.`{stage_name}`"
    parameters = {"source_document_ids": tuple(sorted(set(source_document_ids)))}
    with clickhouse.get_connection() as client:
        client.execute(f"CREATE TABLE {stage} AS {target}")
        try:
            client.execute(
                f"INSERT INTO {stage} SELECT * FROM {target} "
                "WHERE source_document_id NOT IN %(source_document_ids)s",
                parameters,
            )
            if rows:
                client.execute(
                    f"INSERT INTO {stage} ({', '.join(columns)}) VALUES",
                    [tuple(row[column] for column in columns) for row in rows],
                )
            client.execute(f"EXCHANGE TABLES {stage} AND {target}")
        finally:
            client.execute(f"DROP TABLE IF EXISTS {stage}")
