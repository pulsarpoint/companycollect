import builtins
import importlib
from pathlib import Path


def test_bootstrap_modules_import_without_dagster_project(monkeypatch) -> None:
    original_import = builtins.__import__

    def import_without_dagster(name, globals=None, locals=None, fromlist=(), level=0):
        if name == "dagster_v3" or name.startswith("dagster_v3."):
            raise AssertionError(f"standalone bootstrap imported {name}")
        return original_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", import_without_dagster)

    for module_name in [
        "norway_financial_bootstrap.activities",
        "norway_financial_bootstrap.brreg_client",
        "norway_financial_bootstrap.candidates",
        "norway_financial_bootstrap.clickhouse",
        "norway_financial_bootstrap.cli",
        "norway_financial_bootstrap.storage",
        "norway_financial_bootstrap.worker",
        "norway_financial_bootstrap.workflows",
    ]:
        importlib.import_module(module_name)


def test_standalone_pyproject_declares_bootstrap_scripts() -> None:
    pyproject = Path(__file__).resolve().parents[1] / "pyproject.toml"

    text = pyproject.read_text()

    assert 'name = "norway-financial-bootstrap"' in text
    assert (
        'norway-financial-bootstrap = "norway_financial_bootstrap.cli:main"'
        in text
    )
    assert (
        'norway-financial-bootstrap-worker = '
        '"norway_financial_bootstrap.worker:worker_main"'
        in text
    )
    assert "dagster" not in text.lower()
