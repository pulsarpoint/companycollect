"""Register descriptions: the metadata the UI should read instead of holding.

These are prose, so most of what can be tested is that they are present,
consistent with the rest of the pipeline, and not quietly stale.
"""

from pathlib import Path

from dagster_v3.defs.company_signals.register_assets import (
    REGISTER_COLUMNS,
    REGISTERS_TABLE,
    procurement_registers_clickhouse,
)
from dagster_v3.defs.company_signals.registers import (
    PROCUREMENT_REGISTERS,
    register_for,
)
from dagster_v3.defs.company_signals.rules import COUNTRY_PROCUREMENT_RULES
from dagster_v3.defs.ted_procurement.tables import COUNTRIES as TED_COUNTRIES


def _migrations_dir() -> Path:
    return Path(__file__).resolve().parents[3] / "clickhouse" / "migrations"


def _migration_sql() -> str:
    return (
        _migrations_dir() / "000199_corpscout_procurement_registers.up.sql"
    ).read_text()


def _repair_sql() -> str:
    return (
        _migrations_dir() / "000204_corpscout_procurement_registers_repair.up.sql"
    ).read_text()


def test_every_source_a_country_reads_has_a_register_description() -> None:
    """A source in a country's rules with no register row would leave the UI
    with a slug and nothing to say about it."""
    declared = {register.source_slug for register in PROCUREMENT_REGISTERS}
    used = {
        source.slug
        for rule in COUNTRY_PROCUREMENT_RULES.values()
        for source in rule.sources
    }

    assert used <= declared, used - declared


def test_register_countries_cover_the_rules_that_read_each_source() -> None:
    """A register may be ingested before a country can expose company signals,
    but it must include every country whose rule already consumes it."""
    expected: dict[str, set[str]] = {}
    for rule in COUNTRY_PROCUREMENT_RULES.values():
        for source in rule.sources:
            expected.setdefault(source.slug, set()).add(rule.country_code)

    for register in PROCUREMENT_REGISTERS:
        assert expected[register.source_slug] <= set(register.country_codes), (
            register.source_slug
        )


def test_ted_countries_are_derived_from_the_ingestion_config_not_restated() -> None:
    """TED's country list has exactly one owner: COUNTRIES in
    ted_procurement.tables, where adding a country is what makes us ingest it.

    Restating it here made the two drift the moment four countries were added --
    the register kept saying FI, NO, SE, and the source page said so too,
    faithfully, because a page can only be as current as the row behind it. A
    test that the two match catches that a day late and only if someone runs it.
    Deriving means there is nothing to catch.
    """
    import inspect

    from dagster_v3.defs.company_signals import registers as registers_module

    source = inspect.getsource(registers_module)
    assert '"DK", "FI", "FR", "LV", "NO", "SE", "SK"' not in source, (
        "TED's countries are restated here instead of derived from COUNTRIES"
    )

    ted = register_for("ted_procurement")
    assert set(ted.country_codes) == {country.country_iso2 for country in TED_COUNTRIES}


def test_ted_is_one_row_serving_eight_countries() -> None:
    """The case the grain exists for. Per-country rows would hold its licence
    eight times and let them drift."""
    ted = register_for("ted_procurement")

    assert ted.country_codes == ("DK", "EE", "FI", "FR", "LV", "NO", "SE", "SK")
    assert (
        len([r for r in PROCUREMENT_REGISTERS if r.source_slug == "ted_procurement"])
        == 1
    )


def test_every_register_either_says_where_open_tenders_are_or_why_it_cannot() -> None:
    """The question nothing else in the product answers. Everything built so far
    describes what was awarded, which is no use to a supplier looking for work.

    An empty URL is allowed but must be explained, because for Sweden the
    absence IS the answer: there is no single national portal, and our own data
    shows the notices spread across five competing ad databases. A guessed URL
    would send a supplier to a fraction of the market -- the first one tried
    here 404'd, which is how this was noticed.
    """
    for register in PROCUREMENT_REGISTERS:
        if register.open_tenders_url:
            assert register.open_tenders_url.startswith("https://"), (
                register.source_slug
            )
            # ...and it must not just be the homepage again.
            assert register.open_tenders_url != register.homepage_url, (
                register.source_slug
            )
        else:
            assert "no single national tender portal" in register.notes, (
                register.source_slug
            )


def test_every_register_carries_a_licence_and_an_operator() -> None:
    for register in PROCUREMENT_REGISTERS:
        assert register.licence, register.source_slug
        assert register.operator, register.source_slug
        assert register.homepage_url.startswith("https://"), register.source_slug


def test_every_register_names_the_artifact_it_is_actually_read_from() -> None:
    """The provenance question: where did the rows in this table come from. A
    publisher's landing page that merely links to the data is not an answer, so
    the URL must be more specific than the homepage."""
    for register in PROCUREMENT_REGISTERS:
        assert register.api_or_download_url, register.source_slug
        assert register.api_or_download_url != register.homepage_url, (
            register.source_slug
        )
        assert register.retrieval_method, register.source_slug


def test_a_manually_uploaded_source_says_so() -> None:
    """Hilma is not fetched. A human exports a CSV from the portal and uploads
    it, so its freshness is whenever someone last did that -- and a page that
    implied an API would misrepresent how current the data is."""
    hilma = register_for("finland_hilma_procurement")

    assert hilma.retrieval_method.startswith("MANUAL")
    assert "upload_hilma_export.py" in hilma.retrieval_method
    # ...and it must not claim an API it does not use.
    assert "api.hankintailmoitukset.fi" not in hilma.api_or_download_url


def test_the_swedish_source_is_the_csv_resource_not_the_catalogue_page() -> None:
    """The whole Swedish register is one bulk CSV; there is no API behind it,
    and the statistics landing page is not the file."""
    uhm = register_for("sweden_uhm_procurement")

    assert uhm.api_or_download_url.startswith(
        "https://catalog.upphandlingsmyndigheten.se"
    )
    assert uhm.retrieval_method.startswith("Downloaded")


def test_the_source_tables_are_the_ones_the_country_rules_require() -> None:
    """A source page reads these directly, so a typo would show an empty page
    rather than fail."""
    for rule in COUNTRY_PROCUREMENT_RULES.values():
        for source in rule.sources:
            register = register_for(source.slug)
            assert set(source.required_tables) <= set(register.source_tables), (
                source.slug
            )


def test_the_notice_lookup_names_a_table_it_actually_lists() -> None:
    for register in PROCUREMENT_REGISTERS:
        assert register.notice_table in register.source_tables, register.source_slug
        assert register.notice_key_column, register.source_slug


def test_coverage_description_describes_the_register_not_our_ingest() -> None:
    """The distinction the two tables exist to keep. 'Below-threshold contracts
    are absent' is a fact about TED; 'we have not loaded 2019' is a fact about
    us and belongs in company_signal_coverage."""
    for register in PROCUREMENT_REGISTERS:
        text = register.coverage_description.lower()
        assert "not ingested" not in text, register.source_slug
        assert "we have" not in text, register.source_slug
        assert len(register.coverage_description) > 80, register.source_slug


def test_the_migration_covers_every_written_column() -> None:
    sql = _migration_sql()

    assert f"CREATE TABLE IF NOT EXISTS corpscout.{REGISTERS_TABLE}" in sql
    for column in REGISTER_COLUMNS:
        assert f"    {column} " in sql, column
    assert "ORDER BY source_slug" in sql


def test_retrieval_method_arrives_by_alter_not_only_by_the_create() -> None:
    """000199 was edited after it had already been applied -- b285033e added
    retrieval_method to its CREATE TABLE.

    golang-migrate records applied versions, so a database whose ledger already
    reached 199 never re-runs that file, and CREATE TABLE IF NOT EXISTS would be
    inert even if it did. `migrate up` therefore exits clean and the column is
    still missing, which is the worst shape a defect can take: the remedy that
    looks obvious reports success and changes nothing.

    The ledger is forward-only, so the column has to arrive by a later ALTER.
    This pins that repair in place -- reading the CREATE, seeing the column and
    concluding it is covered is exactly the reasoning that leaves prod without
    it.
    """
    assert "ADD COLUMN IF NOT EXISTS retrieval_method String" in _repair_sql()


def test_the_table_is_replaced_atomically() -> None:
    """A reader must never see it mid-replacement, which a truncate-then-insert
    would allow."""
    import inspect

    source = inspect.getsource(
        procurement_registers_clickhouse.op.compute_fn.decorated_fn
    )

    assert "EXCHANGE TABLES" in source
    assert "TRUNCATE" not in source
    # And an empty declaration must not silently blank the table.
    assert "refusing to blank" in source
