"""Upload a manually downloaded Hilma search-results CSV export to S3.

The Hilma portal (hankintailmoitukset.fi) has no keyless machine interface, so
an authenticated user exports the search results CSV (FULL column set) and
uploads it here. The finland_hilma Dagster assets then read every uploaded
export and dedup by notice + lot.

Usage (from services/dagster_v3, so .env is picked up):

    uv run python scripts/upload_hilma_export.py ~/Downloads/"Hilma search results.csv"

After uploading, launch the pipeline from the Dagster UI (finland_hilma_job).
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from datetime import UTC, datetime
from pathlib import Path

from dotenv import load_dotenv

REPO_DAGSTER_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_DAGSTER_ROOT / "src"))
load_dotenv(REPO_DAGSTER_ROOT / ".env")

from dagster_v3.defs.common.resources import ObjectStoreResource  # noqa: E402
from dagster_v3.defs.finland_hilma import tables  # noqa: E402
from dagster_v3.defs.finland_hilma.parsing import (  # noqa: E402
    CSV_SOURCE_ENCODING,
    validate_export_header,
)


def _slug(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-") or "export"


def upload(paths: list[Path]) -> int:
    store = ObjectStoreResource(bucket=tables.S3_BUCKET)
    store.ensure_bucket()
    for path in paths:
        raw = path.read_bytes()
        # Fail before uploading if the export was made with a partial column set.
        validate_export_header(raw.decode(CSV_SOURCE_ENCODING))
        stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        key = f"{tables.S3_EXPORTS_PREFIX}{stamp}_{_slug(path.stem)}.csv"
        store.write_bytes(key, raw)
        meta = {
            "source_filename": path.name,
            "uploaded_at": datetime.now(UTC).isoformat(timespec="seconds"),
            "size_bytes": len(raw),
            "sha256": hashlib.sha256(raw).hexdigest(),
            "encoding": CSV_SOURCE_ENCODING,
            "note": "Manual Hilma portal export (authenticated user).",
        }
        store.write_json(f"{key}.metadata.json", json.dumps(meta, indent=2))
        print(f"uploaded s3://{tables.S3_BUCKET}/{key} ({len(raw):,} bytes)")
    print("Now launch finland_hilma_job from the Dagster UI.")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("files", nargs="+", type=Path, help="Hilma export CSV file(s)")
    args = parser.parse_args()
    missing = [p for p in args.files if not p.is_file()]
    if missing:
        parser.error(f"not a file: {', '.join(map(str, missing))}")
    return upload(args.files)


if __name__ == "__main__":
    raise SystemExit(main())
