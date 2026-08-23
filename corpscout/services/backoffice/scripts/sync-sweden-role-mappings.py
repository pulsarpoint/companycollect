from collections.abc import Mapping, Set
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
import sqlite3
from types import ModuleType

BACKOFFICE_ROOT = Path(__file__).resolve().parents[1]
DAGSTER_SOURCE_ROOT = BACKOFFICE_ROOT.parent / "dagster_v3" / "src"
OUTPUT_PATH = BACKOFFICE_ROOT / "content" / "sweden" / "people" / "role_mappings.sqlite"
BACKOFFICE_MAPPING_MODULE = "backoffice.admin"


def load_mapping_module(relative_path: str) -> ModuleType:
    module_path = DAGSTER_SOURCE_ROOT / relative_path
    spec = spec_from_file_location(module_path.stem, module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load Dagster mapping module: {module_path}")
    module = module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


bolagsverket_roles = load_mapping_module("dagster_v3/defs/sweden_financial/roles.py")
esef_roles = load_mapping_module("dagster_v3/defs/esef_filings/roles.py")
wikidata_roles = load_mapping_module("dagster_v3/defs/wikidata/roles.py")

SOURCE_MAPPINGS: tuple[tuple[str, str, Mapping[str, str], Set[str]], ...] = (
    (
        "bolagsverket",
        "dagster_v3.defs.sweden_financial.roles",
        bolagsverket_roles.BOLAGSVERKET_ROLE_KIND_TO_CANONICAL_ROLE,
        bolagsverket_roles.BOLAGSVERKET_ROLELESS_ROLE_KINDS,
    ),
    (
        "esef",
        "dagster_v3.defs.esef_filings.roles",
        esef_roles.ESEF_ROLE_CATEGORY_TO_CANONICAL_ROLE,
        esef_roles.ESEF_ROLELESS_ROLE_CATEGORIES,
    ),
    (
        "wikidata",
        "dagster_v3.defs.wikidata.roles",
        wikidata_roles.WIKIDATA_ROLE_PROPERTY_TO_CANONICAL_ROLE,
        wikidata_roles.WIKIDATA_ROLELESS_PROPERTIES,
    ),
)

SOURCE_ORIGINAL_ROLE_MAPPINGS: tuple[tuple[str, str, str, Mapping[str, str]], ...] = (
    (
        "bolagsverket",
        "dagster_v3.defs.sweden_financial.roles",
        "other",
        bolagsverket_roles.BOLAGSVERKET_ORIGINAL_ROLE_TO_CANONICAL_ROLE,
    ),
)


def mapping_rows() -> list[tuple[str, str, str, str | None, str, str]]:
    rows: list[tuple[str, str, str, str | None, str, str]] = []
    for source, module, mappings, roleless_codes in SOURCE_MAPPINGS:
        rows.extend(
            (
                source,
                source_role_code,
                "",
                canonical_role_code,
                "mapped",
                module,
            )
            for source_role_code, canonical_role_code in mappings.items()
        )
        rows.extend(
            (source, source_role_code, "", None, "roleless", module)
            for source_role_code in roleless_codes
        )
    for source, module, source_role_code, mappings in SOURCE_ORIGINAL_ROLE_MAPPINGS:
        rows.extend(
            (
                source,
                source_role_code,
                source_role_name,
                canonical_role_code,
                "mapped",
                module,
            )
            for source_role_name, canonical_role_code in mappings.items()
        )
    return sorted(rows)


def preserved_backoffice_rows(
    output_path: Path,
) -> list[tuple[str, str, str, str | None, str, str]]:
    if not output_path.exists():
        return []

    with sqlite3.connect(output_path) as connection:
        columns = {
            str(row[1]) for row in connection.execute("PRAGMA table_info(role_mapping)")
        }
        if "source_role_name" not in columns:
            return []
        return [
            (
                str(source),
                str(source_role_code),
                str(source_role_name),
                None if canonical_role_code is None else str(canonical_role_code),
                str(mapping_status),
                str(dagster_module),
            )
            for (
                source,
                source_role_code,
                source_role_name,
                canonical_role_code,
                mapping_status,
                dagster_module,
            ) in connection.execute(
                """SELECT
                    source,
                    source_role_code,
                    source_role_name,
                    canonical_role_code,
                    mapping_status,
                    dagster_module
                FROM role_mapping
                WHERE dagster_module = ?""",
                (BACKOFFICE_MAPPING_MODULE,),
            )
        ]


def write_database(output_path: Path = OUTPUT_PATH) -> int:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = output_path.with_suffix(".sqlite.tmp")
    temporary_path.unlink(missing_ok=True)
    rows_by_key = {
        (row[0], row[1], row[2]): row for row in preserved_backoffice_rows(output_path)
    }
    rows_by_key.update({(row[0], row[1], row[2]): row for row in mapping_rows()})
    rows = sorted(rows_by_key.values())

    with sqlite3.connect(temporary_path) as connection:
        connection.executescript(
            """
            PRAGMA application_id = 0x4343524d;
            PRAGMA user_version = 2;

            CREATE TABLE metadata (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            ) WITHOUT ROWID;

            CREATE TABLE role_mapping (
                source TEXT NOT NULL,
                source_role_code TEXT NOT NULL,
                source_role_name TEXT NOT NULL,
                canonical_role_code TEXT,
                mapping_status TEXT NOT NULL CHECK (
                    mapping_status IN ('mapped', 'roleless')
                ),
                dagster_module TEXT NOT NULL,
                PRIMARY KEY (source, source_role_code, source_role_name)
            ) WITHOUT ROWID;
            """
        )
        connection.executemany(
            "INSERT INTO metadata (key, value) VALUES (?, ?)",
            (
                ("country_code", "SE"),
                ("schema_version", "2"),
                (
                    "source_of_truth",
                    "dagster_v3 source role modules and backoffice admin curation",
                ),
            ),
        )
        connection.executemany(
            """
            INSERT INTO role_mapping (
                source,
                source_role_code,
                source_role_name,
                canonical_role_code,
                mapping_status,
                dagster_module
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
    temporary_path.replace(output_path)
    return len(rows)


if __name__ == "__main__":
    row_count = write_database()
    print(f"Wrote {row_count} mappings to {OUTPUT_PATH}")
