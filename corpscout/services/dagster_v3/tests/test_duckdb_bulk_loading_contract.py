import ast
from collections import Counter
from pathlib import Path


PRODUCTION_DEFS = (
    Path(__file__).resolve().parents[1] / "src" / "dagster_v3" / "defs"
)
TED_EXECUTEMANY_DEBT = Counter(
    {
        Path("ted_procurement/assets.py"): 4,
        Path("ted_procurement/publish.py"): 1,
    }
)


def test_production_has_only_the_explicit_ted_executemany_debt() -> None:
    matches = Counter(
        source_path.relative_to(PRODUCTION_DEFS)
        for source_path in sorted(PRODUCTION_DEFS.rglob("*.py"))
        for node in ast.walk(
            ast.parse(source_path.read_text(encoding="utf-8"), filename=source_path)
        )
        if isinstance(node, ast.Attribute) and node.attr == "executemany"
    )

    assert matches == TED_EXECUTEMANY_DEBT
