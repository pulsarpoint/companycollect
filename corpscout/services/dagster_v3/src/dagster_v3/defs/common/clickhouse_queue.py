"""Read bounded pages from a prepared, immutable ClickHouse input selection."""

import json
import re

from dagster_clickhouse import ClickhouseResource


def validate_relation(relation: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*", relation):
        raise ValueError("input_relation must be a database.table name")
    return relation


class ClickHouseInputQueue:
    def __init__(self, resource: ClickhouseResource, relation: str):
        self.resource = resource
        self.relation = validate_relation(relation)

    def identity(self, client) -> str:
        database, table = self.relation.split(".")
        rows = client.execute(
            "SELECT toString(uuid),engine,sorting_key FROM system.tables "
            "WHERE database=%(database)s AND name=%(table)s",
            {"database": database, "table": table},
        )
        if len(rows) != 1:
            raise ValueError("input queue table does not exist")
        identity, engine, sorting_key = rows[0]
        if engine not in ("MergeTree", "ReplicatedMergeTree", "SharedMergeTree"):
            raise ValueError(
                "input queue must be a physical MergeTree table, prepared before the task"
            )
        if sorting_key not in ("input_id", "tuple(input_id)"):
            raise ValueError("input queue must be sorted by input_id")
        if identity == "00000000-0000-0000-0000-000000000000":
            raise ValueError("input queue requires a table UUID (an Atomic database)")
        return identity

    def inspect(self) -> dict:
        """Only scalar metadata crosses into PostgreSQL at registration."""
        with self.resource.get_connection() as client:
            identity = self.identity(client)
            columns = client.execute(f"DESCRIBE TABLE {self.relation}")
            if dict((row[0], row[1]) for row in columns).get("input_id") != "String":
                raise ValueError("input_id must be a non-nullable String")
            [(total, unique_ids, invalid_ids, upper_id)] = client.execute(
                f"SELECT count(),uniqExact(input_id),"
                f"countIf(empty(trimBoth(input_id)) OR position(input_id,char(0))>0),"
                f"max(input_id) FROM {self.relation}"
            )
            if total != unique_ids or invalid_ids:
                raise ValueError("input_id must be unique, nonempty and contain no NUL")
            if self.identity(client) != identity:
                raise ValueError("input queue was replaced during registration")
            return {
                "relation": self.relation,
                "table_uuid": identity,
                "total": total,
                "upper_id": upper_id,
            }

    def read(
        self,
        source_info: dict,
        *,
        after: str | None = None,
        limit: int = 1,
        input_id: str | None = None,
    ) -> list[dict]:
        if not 1 <= limit <= 10_000:
            raise ValueError("input page limit must be between 1 and 10000")
        params = {"upper": source_info["upper_id"], "limit": limit}
        where = "input_id <= %(upper)s"
        if input_id is not None:
            where += " AND input_id = %(input_id)s"
            params["input_id"] = input_id
            # Detect an invalid duplicate even during a retry point lookup.
            params["limit"] = 2
        elif after is not None:
            where += " AND input_id > %(after)s"
            params["after"] = after
        with self.resource.get_connection() as client:
            if self.identity(client) != source_info["table_uuid"]:
                raise ValueError(
                    "input queue was replaced; resume requires the original fixed selection"
                )
            rows, columns = client.execute(
                f"SELECT * FROM {self.relation} WHERE {where} ORDER BY input_id LIMIT %(limit)s",
                params,
                with_column_types=True,
            )
        names = [name for name, _ in columns]
        values = [
            json.loads(json.dumps(dict(zip(names, row, strict=True)), default=str))
            for row in rows
        ]
        if input_id is not None and len(values) != 1:
            raise ValueError(
                "unfinished input is missing or duplicated in its fixed queue"
            )
        return values
