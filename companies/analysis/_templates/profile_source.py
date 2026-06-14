#!/usr/bin/env python3
"""Profile a raw source sample for the onboarding dossier (§4 evidence).

Reads CSV / JSON(L) / Parquet (globs ok) via DuckDB and emits, per input file:
  - profile.md   human-readable schema + null/cardinality/sample report
  - profile.json machine-readable, for later extras->core promotion queries

DuckDB is the whole engine here: it reads every format natively, `SUMMARIZE`
gives null %, approx-distinct, min/max/quantiles in one pass, and it streams so
multi-GB snapshots don't need to fit in RAM. Polars is great too, but eager
dataframes are a liability on the big country dumps — DuckDB is the safe default.

Usage:
    uv run python profile_source.py 'finland/prh_ytj/samples/*.parquet' --out finland/prh_ytj
    uv run python profile_source.py companies.csv addresses.csv --out ./out

Requires: duckdb  (uv run --with duckdb python profile_source.py ... if not pinned)
"""

from __future__ import annotations

import argparse
import glob
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb

# A column is a candidate join/primary key when it is (nearly) unique and (nearly)
# always populated. Tunable; surfaced in the report, never auto-decided.
KEY_UNIQUENESS_MIN = 0.98
KEY_FILL_MIN = 0.99
# Sample values to show per column (helps spot codes/enums and formats).
SAMPLE_VALUES = 5


def _read_relation(con: duckdb.DuckDBPyConnection, path: str) -> str:
    """Return a DuckDB table-function call that reads `path` by extension."""
    suffix = Path(path).suffix.lower()
    if suffix in {".parquet", ".pq"}:
        return f"read_parquet('{path}')"
    if suffix in {".json", ".jsonl", ".ndjson"}:
        return f"read_json_auto('{path}')"
    if suffix in {".csv", ".tsv", ".txt"}:
        return f"read_csv_auto('{path}', sample_size=-1)"
    # Let DuckDB sniff anything else.
    return f"read_csv_auto('{path}', sample_size=-1)"


def profile_file(con: duckdb.DuckDBPyConnection, path: str) -> dict[str, Any]:
    rel = _read_relation(con, path)
    total = con.sql(f"SELECT count(*) FROM {rel}").fetchone()[0]

    # SUMMARIZE does the heavy lifting in a single pass.
    summary_rows = con.sql(f"SUMMARIZE SELECT * FROM {rel}").fetchall()
    summary_cols = [d[0] for d in con.sql(f"SUMMARIZE SELECT * FROM {rel}").description]

    columns: list[dict[str, Any]] = []
    for row in summary_rows:
        rec = dict(zip(summary_cols, row))
        name = rec["column_name"]
        approx_unique = int(rec.get("approx_unique") or 0)
        null_pct = float(rec.get("null_percentage") or 0.0)
        fill = 1.0 - null_pct / 100.0
        uniqueness = (approx_unique / total) if total else 0.0
        columns.append(
            {
                "column": name,
                "type": rec.get("column_type"),
                "null_pct": round(null_pct, 2),
                "approx_distinct": approx_unique,
                "min": rec.get("min"),
                "max": rec.get("max"),
                "samples": _sample_values(con, rel, name),
                "candidate_key": uniqueness >= KEY_UNIQUENESS_MIN and fill >= KEY_FILL_MIN,
            }
        )

    return {
        "path": path,
        "rows": total,
        "column_count": len(columns),
        "candidate_keys": [c["column"] for c in columns if c["candidate_key"]],
        "columns": columns,
    }


def _sample_values(con: duckdb.DuckDBPyConnection, rel: str, column: str) -> list[str]:
    try:
        rows = con.sql(
            f'SELECT DISTINCT "{column}" AS v FROM {rel} '
            f"WHERE v IS NOT NULL LIMIT {SAMPLE_VALUES}"
        ).fetchall()
    except duckdb.Error:
        return []  # nested/complex columns DISTINCT may reject — skip gracefully
    return [str(r[0])[:80] for r in rows]


def render_markdown(profiles: list[dict[str, Any]]) -> str:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    out: list[str] = [f"# Source profile\n", f"Generated: {now}\n"]
    for p in profiles:
        out.append(f"## `{p['path']}`\n")
        out.append(f"- Rows: **{p['rows']:,}**")
        out.append(f"- Columns: **{p['column_count']}**")
        keys = ", ".join(f"`{k}`" for k in p["candidate_keys"]) or "_none detected_"
        out.append(f"- Candidate keys: {keys}\n")
        out.append("| Column | Type | Null % | Distinct | Key? | Samples |")
        out.append("|---|---|---:|---:|:--:|---|")
        for c in p["columns"]:
            key = "✓" if c["candidate_key"] else ""
            samples = ", ".join(c["samples"])
            samples = samples.replace("|", "\\|")
            out.append(
                f"| `{c['column']}` | {c['type']} | {c['null_pct']} | "
                f"{c['approx_distinct']:,} | {key} | {samples} |"
            )
        out.append("")
    out.append("> Promote `extras` → core later using the null %/distinct columns above.")
    return "\n".join(out)


def expand_inputs(patterns: list[str]) -> list[str]:
    paths: list[str] = []
    for pattern in patterns:
        matched = sorted(glob.glob(pattern))
        if not matched:
            raise SystemExit(f"no files matched: {pattern}")
        paths.extend(matched)
    return paths


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("inputs", nargs="+", help="file(s) or glob(s) to profile")
    parser.add_argument("--out", type=Path, default=Path("."), help="output dir")
    args = parser.parse_args()

    args.out.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()

    profiles = [profile_file(con, path) for path in expand_inputs(args.inputs)]

    (args.out / "profile.json").write_text(json.dumps(profiles, indent=2, default=str))
    (args.out / "profile.md").write_text(render_markdown(profiles))
    print(f"wrote {args.out / 'profile.md'} and {args.out / 'profile.json'}")


if __name__ == "__main__":
    main()
