"""Read bounded pages from a prepared, immutable ClickHouse input selection."""

import json
import re

from dagster_clickhouse import ClickhouseResource


def validate_relation(relation: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*", relation):
        raise ValueError("input_relation must be a database.table name")
    return relation


class ClickHouseInputQueue:
    def __init__(
        self,
        resource: ClickhouseResource,
        relation: str,
        *,
        selection_task_id: str | None = None,
    ):
        self.resource = resource
        self.relation = validate_relation(relation)
        self.selection_task_id = selection_task_id

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
        expected = (
            ("input_id", "tuple(input_id)")
            if self.selection_task_id is None
            # "task_id, input_id" is the task-partitioned queue contract
            # (PARTITION BY task_id, ORDER BY (task_id, input_id)); the other
            # two forms are the older single-table convention.
            else ("input_id, task_id", "task_id, input_id", "tuple(input_id, task_id)")
        )
        if sorting_key not in expected:
            raise ValueError("input queue sorting key requires its selection task_id")
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
            where = (
                ""
                if self.selection_task_id is None
                else " WHERE task_id=%(selection_task_id)s"
            )
            [(total, unique_ids, invalid_ids, upper_id)] = client.execute(
                f"SELECT count(),uniqExact(input_id),"
                f"countIf(empty(trimBoth(input_id)) OR position(input_id,char(0))>0),"
                f"max(input_id) FROM {self.relation}{where}",
                {"selection_task_id": self.selection_task_id},
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
                **(
                    {"selection_task_id": self.selection_task_id}
                    if self.selection_task_id is not None
                    else {}
                ),
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
        if source_info.get("selection_task_id") != self.selection_task_id:
            raise ValueError(
                "input queue selection task_id differs from the saved task"
            )
        params = {"upper": source_info["upper_id"], "limit": limit}
        where = "input_id <= %(upper)s"
        if self.selection_task_id is not None:
            where += " AND task_id = %(selection_task_id)s"
            params["selection_task_id"] = self.selection_task_id
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
        if self.selection_task_id is not None:
            for value in values:
                value.pop("task_id")
        if input_id is not None and len(values) != 1:
            raise ValueError(
                "unfinished input is missing or duplicated in its fixed queue"
            )
        return values
