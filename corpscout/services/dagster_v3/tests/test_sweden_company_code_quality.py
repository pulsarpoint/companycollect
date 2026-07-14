import ast
from pathlib import Path


PACKAGE_DIR = (
    Path(__file__).resolve().parents[1]
    / "src"
    / "dagster_v3"
    / "defs"
    / "sweden_company"
)


def _module_tree(module_name: str) -> ast.Module:
    return ast.parse((PACKAGE_DIR / f"{module_name}.py").read_text(encoding="utf-8"))


def test_sweden_company_assets_do_not_wrap_local_asset_names_in_constants() -> None:
    tree = _module_tree("assets")

    assigned_names = {
        target.id
        for node in tree.body
        if isinstance(node, ast.Assign)
        for target in node.targets
        if isinstance(target, ast.Name)
    }

    assert not {
        name
        for name in assigned_names
        if name.startswith("SWEDEN_COMPANY_") and name.endswith("_ASSET_KEY")
    }


def test_sweden_company_normalized_sql_uses_direct_table_names() -> None:
    source = (PACKAGE_DIR / "normalized_duckdb.py").read_text(encoding="utf-8")

    assert "dataset = tables.DLT_DATASET_NAME" not in source
    for temporary_table_constant in (
        "tables.COMPANIES_TABLE",
        "tables.COMPANY_ADDRESSES_TABLE",
        "tables.COMPANY_INDUSTRY_CODES_TABLE",
        "tables.BOLAGSVERKET_RAW_TABLE",
        "tables.SCB_RAW_TABLE",
    ):
        assert temporary_table_constant not in source


def test_sweden_company_raw_loader_keeps_source_specific_sql_explicit() -> None:
    defined_functions = {
        node.name
        for node in _module_tree("raw_duckdb").body
        if isinstance(node, ast.FunctionDef)
    }

    assert "_replace_source_table" not in defined_functions


def test_sweden_company_clickhouse_export_has_no_local_forwarding_helper() -> None:
    defined_functions = {
        node.name
        for node in _module_tree("clickhouse").body
        if isinstance(node, ast.FunctionDef)
    }

    assert "_export_table" not in defined_functions
    assert "export_sweden_company_clickhouse" not in defined_functions


def test_sweden_company_tables_keeps_only_external_table_contracts() -> None:
    assigned_names = {
        target.id
        for node in _module_tree("tables").body
        if isinstance(node, ast.Assign)
        for target in node.targets
        if isinstance(target, ast.Name)
    }

    assert "RAW_FILES_TABLE" not in assigned_names
    assert "BOLAGSVERKET_RAW_TABLE" not in assigned_names
    assert "SCB_RAW_TABLE" not in assigned_names
    assert "COMPANIES_TABLE" not in assigned_names
    assert "COMPANY_ADDRESSES_TABLE" not in assigned_names
    assert "COMPANY_INDUSTRY_CODES_TABLE" not in assigned_names
