"""The company-source-record uid is rendered in two languages -- Python for the
basic-info extractors and the serving builder, Jinja for the company_serving dbt models --
and the two must never drift: a uid that differs by one byte links nothing."""

from pathlib import Path

from dagster_v3.defs.se_company.common import bolagsverket_record_uid_sql, register_record_uid_sql

MACRO = (
    Path(__file__).resolve().parents[1]
    / "src/dagster_v3/defs/company_serving/dbt/macros/se_register_record_uid.sql"
)


def _macro_render(source_slug: str, alias: str) -> str:
    body = MACRO.read_text(encoding="utf-8")
    start = body.index("lower(hex(SHA256(")
    end = body.index("{%- endmacro %}")
    return body[start:end].strip().replace("{{ source_slug }}", source_slug).replace("{{ alias }}", alias)


def test_python_and_dbt_render_the_same_uid_expression() -> None:
    for slug, alias in (("sweden_bolagsverket", "b"), ("sweden_scb", "s")):
        assert _macro_render(slug, alias) == register_record_uid_sql(slug, alias)


def test_bolagsverket_helper_is_the_general_one_with_its_slug() -> None:
    assert bolagsverket_record_uid_sql("register") == register_record_uid_sql("sweden_bolagsverket", "register")
