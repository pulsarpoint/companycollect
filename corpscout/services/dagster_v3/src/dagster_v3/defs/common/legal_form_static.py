"""Shared body for a country's curated legal-form translation asset.

Three registers publish the legal form as a label on the company row, and each
already carries a reviewed English map in its ingest module. Those maps used to
be applied at load time, stamping an English column into the country table --
which is why Latvia's 118,008 rows kept June's wrong wording for six weeks: the
map was corrected, but nothing re-read it until the whole register was
downloaded again.

Applying the same map as a STATIC translation instead makes a correction cost
one asset run rather than a full re-ingest. The map itself does not move: it
stays in the source module where it is reviewed in diffs, and this only changes
where its output is written.

Deliberately no translator dependency. A curated map is exact by construction
and must not be blocked on a service being reachable -- the machine loader is a
separate asset per source, as it is for France.
"""

from __future__ import annotations

from typing import Any, Mapping

from dagster_v3.defs.translator_load.loader import (
    build_static_scan_sql,
    insert_static_translations,
)


def load_curated_legal_forms(
    client: Any,
    *,
    table: str,
    label_column: str,
    key_column: str,
    source_lang: str,
    mapping: Mapping[str, str],
    target_lang: str = "en",
) -> int:
    """Insert a register's curated English into text_translations.

    `key_column` is what the map is keyed by, which is not always a code:
    Estonia's register publishes no code, so its label is its own key and
    `key_column` is the label column itself.

    Unknown keys are skipped by insert_static_translations rather than
    defaulted, so a form nobody has curated stays untranslated. That is the
    point -- a wrong legal form reads exactly like a right one.
    """
    rows = client.execute(build_static_scan_sql(table, label_column, key_column))
    return insert_static_translations(
        client,
        table,
        label_column,
        source_lang,
        target_lang,
        rows,
        mapping,
    )
