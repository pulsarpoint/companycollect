# Brazil RFB Socios Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ingest RFB's `Socios` file into `corpscout.br_company_relations` — one row per company-to-partner edge, where the partner is a company, a natural person, or a foreign entity.

**Architecture:** A tenth *family* inside the existing `brazil_companies/rfb` module, not a new module. The RFB module is `MonthlyPartitionsDefinition` and every family already gets its own DuckDB stage file and its own concurrency pool, so `socios` is isolated at storage while sharing the partition's snapshot. The generic loader `staging.load_raw_family_from_manifest` is driven entirely by two dicts in `tables.py`, so raw loading needs no new staging code — only registration.

**Tech Stack:** Python 3.14, Dagster, DuckDB, ClickHouse, `uv` for all commands, pytest.

Design doc: `src/dagster_v3/defs/brazil_companies/docs/brazil_rfb_socios-design.md`

## Global Constraints

- **Every new asset MUST be added to the `defs = dg.Definitions(assets=[...])`
  list at the bottom of `assets.py` (~line 730).** `load_from_defs_folder`
  merges each module's explicit `Definitions` object, so a function decorated
  `@dg.asset` but omitted from that list is invisible to Dagster: absent from
  the asset graph, unselectable in the UI, and excluded from
  `brazil_comp_rfb_resolve_job`. **Neither `pytest` nor `dg check defs` catches
  this** — `dg check defs` only validates what is already wired. Verify with:

  ```bash
  uv run python -c "
  from dagster_v3.definitions import defs as load_defs
  keys = {k.path[-1] for k in load_defs().get_repository_def().asset_graph.get_all_asset_keys()}
  print('registered:', '<your_asset_name>' in keys)
  "
  ```

  Then extend `RAW_STAGE_ASSET_KEYS` in `tests/test_brazil_comp_rfb_assets.py`
  so the module's existing registration/pool contract test covers the new asset.

- All commands run from `corpscout/services/dagster_v3/` and are prefixed **`uv run`**.
- **No `from __future__ import annotations`** in any module defining a `@dg.asset` — it stringizes the context hint and breaks Dagster's op context-type validation.
- **Every asset that opens a DuckDB file declares a `pool=`**, including read-only ones.
- **Non-nullable ClickHouse `String`/`LowCardinality(String)` columns must receive `''`, never `NULL`** — the native driver calls `.encode()` per value and dies on `None`.
- **`ORDER BY` cannot contain `Nullable` columns** (`allow_nullable_key` is off).
- **No `;` inside a `--` comment in a migration** — the driver splits on `;` without stripping comments and the chunk fails as an empty query.
- **Never load a wide CSV row-by-row in Python.** Use DuckDB's native reader.
- **Migration number is NOT fixed by this plan.** Another session is actively renumbering (`000201` → `000207` at time of writing). Run `ls corpscout/clickhouse/migrations/ | tail -2` and take the next free number. This plan writes `000208` — verify before creating the file.
- Validate before finishing any task: `uv run dg check defs` and the task's tests.
- **Commit by explicit path.** The working tree carries other sessions' in-flight work; never `git add -A`.

---

## File Structure

| File | Responsibility | Change |
|---|---|---|
| `src/dagster_v3/defs/brazil_companies/rfb/tables.py` | family registry, column layouts, CH table constants | modify |
| `src/dagster_v3/defs/brazil_companies/rfb/source.py` | archive discovery patterns | modify |
| `src/dagster_v3/defs/brazil_companies/rfb/assets.py` | stage paths, pools, asset definitions | modify |
| `src/dagster_v3/defs/brazil_companies/rfb/relations.py` | **NEW** — the raw→relations transform | create |
| `src/dagster_v3/defs/brazil_companies/rfb/clickhouse.py` | ClickHouse export functions | modify |
| `corpscout/clickhouse/migrations/000208_*.up.sql` / `.down.sql` | `br_company_relations` schema | create |
| `tests/test_brazil_comp_rfb_source.py` | archive pattern tests | modify |
| `tests/test_brazil_comp_rfb_staging.py` | family layout contract test | modify |
| `tests/test_brazil_comp_rfb_relations.py` | **NEW** — transform tests | create |
| `tests/test_brazil_comp_rfb_clickhouse.py` | export tests | modify |
| `tests/test_clickhouse_migrations.py` | ledger + column contract | modify |

The transform gets its own `relations.py` rather than joining `transforms.py`: that file owns the companies/establishments join and is already large, and the relations build shares nothing with it.

---

## Task 1: Verify the published column layout — DEPLOYMENT GATE, NOT A LOCAL TASK

> **Do not run this locally.** The archive is multi-GB and heavy runs belong on
> the prod server. This task is **deferred to deployment** (see Deployment
> below) and is NOT dispatched to an implementer subagent.
>
> Tasks 2–4 are written against the documented 11-column layout. **The risk this
> gate exists to catch is live until it runs**: the CSV is headerless, so a
> wrong column tuple shifts every value one place left and nothing errors —
> `related_name` would silently hold tax ids, `relation_code` would hold dates.
>
> **Run it on prod before the first `brazil_comp_rfb_socios_duckdb`
> materialization.** If the column count is not 11, stop: Task 2's tuple and
> Task 3's transform both need revising.
>
> A cheap way to do it without the full download: the first member's local file
> header sits at the *start* of a ZIP, so an HTTP range request for the first
> few MB can be stream-decompressed far enough to read several rows. Worth
> trying before pulling the whole archive.

The steps below are the procedure to run **on prod**, kept here so it is not
re-derived.

**Files:**
- Modify: `src/dagster_v3/defs/brazil_companies/docs/brazil_rfb_socios-design.md`

- [ ] **Step 1: Find the current snapshot's Socios archive URL**

```bash
cd corpscout/services/dagster_v3
uv run python - <<'PY'
import urllib.request, re
base = "https://dados-abertos-rf-cnpj.casadosdados.com.br/arquivos/"
html = urllib.request.urlopen(base, timeout=60).read().decode("utf-8", "replace")
dirs = sorted(set(re.findall(r'href="(\d{4}-\d{2}[^"/]*)/?"', html)))
print("snapshot dirs:", dirs[-3:])
PY
```

Expected: a list of `YYYY-MM` directories. Take the newest.

- [ ] **Step 2: Download one Socios archive and read its first rows**

Replace `<SNAPSHOT>` with the directory from Step 1.

```bash
uv run python - <<'PY'
import urllib.request, re, zipfile, io, csv
SNAP = "<SNAPSHOT>"
base = f"https://dados-abertos-rf-cnpj.casadosdados.com.br/arquivos/{SNAP}/"
html = urllib.request.urlopen(base, timeout=60).read().decode("utf-8", "replace")
names = re.findall(r'href="([^"]*[Ss]ocio[^"]*\.zip)"', html)
print("socios archives:", names[:5])
url = base + names[0].split("/")[-1]
print("downloading", url)
data = urllib.request.urlopen(url, timeout=1800).read()
with zipfile.ZipFile(io.BytesIO(data)) as z:
    member = z.namelist()[0]
    print("member:", member)
    with z.open(member) as fh:
        text = io.TextIOWrapper(fh, encoding="latin-1", newline="")
        for i, row in enumerate(csv.reader(text, delimiter=";", quotechar='"')):
            print(len(row), row)
            if i >= 4:
                break
PY
```

Expected: each printed row has the **same column count**. Record that count.

- [ ] **Step 3: Record the verified layout in the design doc**

In `brazil_rfb_socios-design.md` §1, replace the "NOT yet verified" note with the measured facts: the column count, the archive naming pattern observed (e.g. `Socios0.zip` vs `K3241.K03200Y0.D30612.SOCIOCSV.zip`), and a sample row with personal values redacted.

**If the column count is not 11, stop and report.** Every later task's column tuple is wrong and the plan needs revising before continuing.

- [ ] **Step 4: Commit**

```bash
git add src/dagster_v3/defs/brazil_companies/docs/brazil_rfb_socios-design.md
git commit -m "docs(corpscout): verify RFB Socios column layout against the real archive"
```

---

## Task 2: Register socios as a tenth family

**Files:**
- Modify: `src/dagster_v3/defs/brazil_companies/rfb/tables.py`
- Modify: `src/dagster_v3/defs/brazil_companies/rfb/source.py`
- Modify: `src/dagster_v3/defs/brazil_companies/rfb/assets.py`
- Test: `tests/test_brazil_comp_rfb_source.py`, `tests/test_brazil_comp_rfb_staging.py`

**Interfaces:**
- Consumes: the verified layout from Task 1.
- Produces: `tables.RAW_TABLE_BY_FAMILY["socios"] == "socios_raw"`; `tables.RAW_COLUMNS_BY_FAMILY["socios"]` (the 11-tuple below); `assets.SOCIOS_ASSET_KEY == "brazil_comp_rfb_socios_duckdb"`; `BrazilCompRfbStagePaths.socios`.

- [ ] **Step 1: Write the failing tests**

In `tests/test_brazil_comp_rfb_source.py`, add:

```python
def test_family_from_archive_name_matches_socios_patterns() -> None:
    assert (
        source.family_from_archive_name("F.K03200$W.SIMPLES.CSV.D30612.SOCIOCSV.zip")
        == "socios"
    )
    assert source.family_from_archive_name("Socios0.zip") == "socios"
    assert source.family_from_archive_name("Socios.zip") == "socios"
```

In `tests/test_brazil_comp_rfb_staging.py`, extend the existing
`test_rfb_raw_column_layouts_match_published_file_families` dict assertion with
`"socios": "socios_raw",` and add:

```python
def test_socios_raw_layout_is_the_published_column_order() -> None:
    """Headerless CSV: the tuple IS the layout. A wrong order silently shifts
    every value one column left and nothing errors."""
    assert tables.RAW_COLUMNS_BY_FAMILY["socios"] == (
        "cnpj_basico",
        "identificador_socio",
        "nome_socio_razao_social",
        "cnpj_cpf_socio",
        "qualificacao_socio",
        "data_entrada_sociedade",
        "pais",
        "representante_legal",
        "nome_representante",
        "qualificacao_representante",
        "faixa_etaria",
    )
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
uv run pytest tests/test_brazil_comp_rfb_source.py::test_family_from_archive_name_matches_socios_patterns tests/test_brazil_comp_rfb_staging.py -q
```

Expected: FAIL — `family_from_archive_name` returns `""` for the Socios names, and `RAW_COLUMNS_BY_FAMILY` has no `"socios"` key (`KeyError`).

- [ ] **Step 3: Register the family**

In `tables.py`, add to `RAW_TABLE_BY_FAMILY` after `"estabelecimentos"`:

```python
    "socios": "socios_raw",
```

and to `RAW_COLUMNS_BY_FAMILY`:

```python
    "socios": (
        "cnpj_basico",
        "identificador_socio",
        "nome_socio_razao_social",
        "cnpj_cpf_socio",
        "qualificacao_socio",
        "data_entrada_sociedade",
        "pais",
        "representante_legal",
        "nome_representante",
        "qualificacao_representante",
        "faixa_etaria",
    ),
```

In `source.py`, add to `_FAMILY_PATTERNS` after the `estabelecimentos` entry:

```python
    ("socios", re.compile(r"(SOCIOCSV|Socios\d*)\.zip$", re.IGNORECASE)),
```

`DEFAULT_FAMILIES` derives from `RAW_TABLE_BY_FAMILY`, so the downloader picks the archive up with no further change, and the existing missing-families guard now *requires* it.

- [ ] **Step 4: Run the tests to verify they pass**

```bash
uv run pytest tests/test_brazil_comp_rfb_source.py tests/test_brazil_comp_rfb_staging.py -q
```

Expected: PASS.

- [ ] **Step 5: Add the stage path and pool**

In `assets.py`, add the pool constant beside its siblings (near line 35):

```python
BRAZIL_COMP_RFB_SOCIOS_DUCKDB_POOL = "brazil_comp_rfb_socios_duckdb"
```

Add the asset key constant beside `EMPRESAS_ASSET_KEY`:

```python
SOCIOS_ASSET_KEY = "brazil_comp_rfb_socios_duckdb"
```

Add `socios: Path` to the `BrazilCompRfbStagePaths` dataclass after `estabelecimentos`, and in `brazil_comp_rfb_stage_paths` add:

```python
        socios=root / "socios.duckdb",
```

Note: `cleanup_previous_partition_files` removes the whole partition directory, so no cleanup change is needed — but the stage path must exist for the file to be created inside that directory in the first place.

- [ ] **Step 6: Add the raw-load asset**

In `assets.py`, after `brazil_comp_rfb_estabelecimentos_duckdb`:

```python
@dg.asset(
    name=SOCIOS_ASSET_KEY,
    deps=[dg.AssetKey(SNAPSHOT_FILES_ASSET_KEY)],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb"},
    partitions_def=BRAZIL_COMP_RFB_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    pool=BRAZIL_COMP_RFB_SOCIOS_DUCKDB_POOL,
    description="Brazil RFB Socios raw CSV files loaded into a stage DuckDB file.",
)
def brazil_comp_rfb_socios_duckdb(
    context: dg.AssetExecutionContext,
) -> dg.MaterializeResult:
    stage_paths = _stage_paths_for_context(context)
    stage_paths.ensure_root()
    table_name = tables.RAW_TABLE_BY_FAMILY["socios"]
    existing_counts = resume.stage_table_counts(stage_paths.socios, (table_name,))
    if existing_counts is not None:
        counts = {"socios": existing_counts[table_name]}
        _log_reused_stage(context, "Socios", counts)
        return dg.MaterializeResult(metadata=_metadata_reused(counts))

    with duckdb_resource(stage_paths.socios).get_connection() as connection:
        rows = staging.load_raw_family_from_manifest(
            connection=connection,
            manifest_database_path=stage_paths.manifest,
            family="socios",
            source_run_id=context.run_id,
        )
    context.log.info("Loaded Brazil RFB Socios raw CSV files: rows=%s", rows)
    return dg.MaterializeResult(metadata={"socios": rows})
```

- [ ] **Step 7: Verify definitions load**

```bash
uv run dg check defs
```

Expected: `All definitions loaded successfully.`

- [ ] **Step 8: Commit**

```bash
git add src/dagster_v3/defs/brazil_companies/rfb/tables.py \
        src/dagster_v3/defs/brazil_companies/rfb/source.py \
        src/dagster_v3/defs/brazil_companies/rfb/assets.py \
        tests/test_brazil_comp_rfb_source.py \
        tests/test_brazil_comp_rfb_staging.py
git commit -m "feat(corpscout): fetch and stage RFB Socios as a tenth family"
```

---

## Task 3: Transform raw socios into the relations shape

**Files:**
- Create: `src/dagster_v3/defs/brazil_companies/rfb/relations.py`
- Create: `tests/test_brazil_comp_rfb_relations.py`
- Modify: `src/dagster_v3/defs/brazil_companies/rfb/tables.py`
- Modify: `src/dagster_v3/defs/brazil_companies/rfb/assets.py`

**Interfaces:**
- Consumes: `tables.RAW_TABLE_BY_FAMILY["socios"]`, `BrazilCompRfbStagePaths.socios`, `attached_read_only_database` from `duckdb_attach`.
- Produces: `relations.build_brazil_rfb_company_relations(connection, source_run_id, snapshot_year_month, socios_database_path) -> dict[str, int]`; `tables.COMPANY_RELATIONS_TABLE = "company_relations"`; `tables.BR_COMPANY_RELATIONS_COLUMNS`; asset `brazil_comp_rfb_company_relations_duckdb`.

- [ ] **Step 1: Write the failing test**

Create `tests/test_brazil_comp_rfb_relations.py`:

```python
from pathlib import Path

import duckdb
import pytest

from dagster_v3.defs.brazil_companies.rfb import relations, tables

DATASET = tables.DLT_DATASET_NAME


def _socios_stage(path: Path) -> None:
    """A raw socios stage with all three partner kinds plus a legal
    representative -- the two-edges-per-row case."""
    connection = duckdb.connect(str(path))
    connection.execute(f"create schema if not exists {DATASET}")
    connection.execute(
        f"""
        create table {DATASET}.socios_raw (
            cnpj_basico varchar, identificador_socio varchar,
            nome_socio_razao_social varchar, cnpj_cpf_socio varchar,
            qualificacao_socio varchar, data_entrada_sociedade varchar,
            pais varchar, representante_legal varchar,
            nome_representante varchar, qualificacao_representante varchar,
            faixa_etaria varchar
        )
        """
    )
    connection.executemany(
        f"insert into {DATASET}.socios_raw values (?,?,?,?,?,?,?,?,?,?,?)",
        [
            # corporate partner
            ("11111111", "1", "HOLDING ALFA LTDA", "22222222000199",
             "22", "20180314", "", "", "", "", "0"),
            # natural person, masked CPF
            ("11111111", "2", "MARIA SOUZA", "***456789**",
             "49", "20190701", "", "", "", "", "5"),
            # foreign partner WITH a legal representative -- two edges, one row
            ("33333333", "3", "ALFA HOLDINGS BV", "",
             "37", "20200115", "NETHERLANDS", "***111222**", "JOAO LIMA", "10", "4"),
            # empty optionals: must land '' and a NULL date, never None strings
            ("44444444", "2", "ANA COSTA", "***999888**", "22", "", "", "", "", "", ""),
        ],
    )
    connection.close()


def test_relations_keep_every_partner_kind_verbatim(tmp_path: Path) -> None:
    """One edge model: the discriminator distinguishes company, person and
    foreign partners rather than three tables doing it."""
    socios_path = tmp_path / "socios.duckdb"
    _socios_stage(socios_path)
    connection = duckdb.connect(":memory:")

    counts = relations.build_brazil_rfb_company_relations(
        connection=connection,
        source_run_id="run-1",
        snapshot_year_month="2026-07",
        socios_database_path=socios_path,
    )

    assert counts["company_relations"] == 4
    rows = connection.execute(
        f"""
        select cnpj_basico, related_entity_kind, related_name, related_tax_id,
               relation_code, relation_since, related_country
        from {DATASET}.{tables.COMPANY_RELATIONS_TABLE}
        order by cnpj_basico, related_tax_id
        """
    ).fetchall()
    assert rows[0][:5] == (
        "11111111", "1", "HOLDING ALFA LTDA", "22222222000199", "22",
    )
    assert rows[0][5].isoformat() == "2018-03-14"
    assert rows[1][:5] == ("11111111", "2", "MARIA SOUZA", "***456789**", "49")
    assert rows[2][1] == "3"
    assert rows[2][6] == "NETHERLANDS"


def test_legal_representative_is_carried_as_a_second_edge(tmp_path: Path) -> None:
    """A foreign partner's representative is another named person. It rides on
    the same row because that is how RFB publishes it."""
    socios_path = tmp_path / "socios.duckdb"
    _socios_stage(socios_path)
    connection = duckdb.connect(":memory:")

    relations.build_brazil_rfb_company_relations(
        connection=connection,
        source_run_id="run-1",
        snapshot_year_month="2026-07",
        socios_database_path=socios_path,
    )

    assert connection.execute(
        f"""
        select representative_tax_id, representative_name, representative_code
        from {DATASET}.{tables.COMPANY_RELATIONS_TABLE}
        where cnpj_basico = '33333333'
        """
    ).fetchone() == ("***111222**", "JOAO LIMA", "10")


def test_absent_values_land_as_empty_string_not_null(tmp_path: Path) -> None:
    """Non-nullable ClickHouse Strings: the native driver calls .encode() per
    value and dies on None. Only real data with blanks triggers it."""
    socios_path = tmp_path / "socios.duckdb"
    _socios_stage(socios_path)
    connection = duckdb.connect(":memory:")

    relations.build_brazil_rfb_company_relations(
        connection=connection,
        source_run_id="run-1",
        snapshot_year_month="2026-07",
        socios_database_path=socios_path,
    )

    string_columns = [
        column
        for column in tables.BR_COMPANY_RELATIONS_COLUMNS
        if column not in ("relation_since", "resolved_at")
    ]
    nulls = " + ".join(
        f"count(*) filter (where {column} is null)" for column in string_columns
    )
    assert connection.execute(
        f"select {nulls} from {DATASET}.{tables.COMPANY_RELATIONS_TABLE}"
    ).fetchone() == (0,)
    assert connection.execute(
        f"""
        select relation_since, related_country
        from {DATASET}.{tables.COMPANY_RELATIONS_TABLE}
        where cnpj_basico = '44444444'
        """
    ).fetchone() == (None, "")


def test_relations_refuse_an_empty_source(tmp_path: Path) -> None:
    socios_path = tmp_path / "socios.duckdb"
    connection = duckdb.connect(str(socios_path))
    connection.execute(f"create schema if not exists {DATASET}")
    connection.execute(
        f"create table {DATASET}.socios_raw (cnpj_basico varchar, "
        f"identificador_socio varchar, nome_socio_razao_social varchar, "
        f"cnpj_cpf_socio varchar, qualificacao_socio varchar, "
        f"data_entrada_sociedade varchar, pais varchar, representante_legal varchar, "
        f"nome_representante varchar, qualificacao_representante varchar, "
        f"faixa_etaria varchar)"
    )
    connection.close()

    with pytest.raises(ValueError, match="no company relations"):
        relations.build_brazil_rfb_company_relations(
            connection=duckdb.connect(":memory:"),
            source_run_id="run-1",
            snapshot_year_month="2026-07",
            socios_database_path=socios_path,
        )
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
uv run pytest tests/test_brazil_comp_rfb_relations.py -q
```

Expected: FAIL — `ModuleNotFoundError: No module named 'dagster_v3.defs.brazil_companies.rfb.relations'`.

- [ ] **Step 3: Add the table constants**

In `tables.py`, after `COMPANIES_TABLE = "companies"`:

```python
COMPANY_RELATIONS_TABLE = "company_relations"

BR_COMPANY_RELATIONS_TABLE_CH = "br_company_relations"
QUALIFIED_BR_COMPANY_RELATIONS_TABLE = (
    f"{BRAZIL_COMP_RFB_DATABASE}.{BR_COMPANY_RELATIONS_TABLE_CH}"
)

BR_COMPANY_RELATIONS_COLUMNS = (
    "country_iso2",
    "source_slug",
    "source_run_id",
    "source_record_id",
    "snapshot_year_month",
    "cnpj_basico",
    "related_entity_kind",
    "related_name",
    "related_tax_id",
    "relation_code",
    "relation_since",
    "related_country",
    "representative_tax_id",
    "representative_name",
    "representative_code",
    "age_band",
    "resolved_at",
)
BR_COMPANY_RELATIONS_EXPORT_COLUMNS = BR_COMPANY_RELATIONS_COLUMNS
```

`resolved_at` is last in every other Brazil RFB column tuple and the transform
emits it with `now()`, matching `transforms.py`. It is excluded from the
`''`-coalescing test below because it is a timestamp, not a String.

- [ ] **Step 4: Write the transform**

Create `src/dagster_v3/defs/brazil_companies/rfb/relations.py`:

```python
"""RFB Socios -> company relation edges.

One row per company-to-partner edge. `related_entity_kind` discriminates the
far end: '1' company, '2' natural person, '3' foreign. Person names and masked
CPFs are stored exactly as RFB publishes them -- the publisher performs the
masking, we add nothing and never attempt to reverse it. See
docs/brazil_rfb_socios-design.md section 6.

Verbatim: no joins, no resolution, no vocabulary mapping. `related_tax_id` is
NOT resolved against br_companies here, so a partner pointing at a company we
have not ingested stays visible instead of silently becoming empty.
"""

from pathlib import Path
from typing import Any

import duckdb

from dagster_v3.defs.brazil_companies.rfb import tables
from dagster_v3.defs.brazil_companies.rfb.duckdb_attach import (
    attached_read_only_database,
)


def _blank(column: str) -> str:
    """Coalesce to '' -- a non-nullable ClickHouse String must never see NULL."""
    return f"coalesce(nullif(trim({column}), ''), '')"


def build_brazil_rfb_company_relations(
    *,
    connection: duckdb.DuckDBPyConnection,
    source_run_id: str,
    snapshot_year_month: str,
    socios_database_path: str | Path,
) -> dict[str, int]:
    dataset = tables.DLT_DATASET_NAME
    raw_table = tables.RAW_TABLE_BY_FAMILY["socios"]
    target = f"{dataset}.{tables.COMPANY_RELATIONS_TABLE}"

    connection.execute(f"create schema if not exists {dataset}")
    with attached_read_only_database(
        connection,
        database_path=socios_database_path,
        alias="socios_db",
    ) as socios_alias:
        connection.execute(
            f"""
            create or replace table {target} as
            select
                'BR' as country_iso2,
                'brazil_rfb' as source_slug,
                cast(? as varchar) as source_run_id,
                lower(sha256(concat_ws(
                    '|',
                    {_blank('s.cnpj_basico')},
                    {_blank('s.identificador_socio')},
                    {_blank('s.cnpj_cpf_socio')},
                    {_blank('s.nome_socio_razao_social')},
                    {_blank('s.qualificacao_socio')}
                ))) as source_record_id,
                cast(? as varchar) as snapshot_year_month,
                {_blank('s.cnpj_basico')} as cnpj_basico,
                {_blank('s.identificador_socio')} as related_entity_kind,
                {_blank('s.nome_socio_razao_social')} as related_name,
                {_blank('s.cnpj_cpf_socio')} as related_tax_id,
                {_blank('s.qualificacao_socio')} as relation_code,
                try_strptime(nullif(trim(s.data_entrada_sociedade), ''), '%Y%m%d')::date
                    as relation_since,
                {_blank('s.pais')} as related_country,
                {_blank('s.representante_legal')} as representative_tax_id,
                {_blank('s.nome_representante')} as representative_name,
                {_blank('s.qualificacao_representante')} as representative_code,
                {_blank('s.faixa_etaria')} as age_band,
                now() as resolved_at
            from {socios_alias}.{dataset}.{raw_table} as s
            """,
            [source_run_id, snapshot_year_month],
        )

    row_count = int(
        connection.execute(f"select count(*) from {target}").fetchone()[0]
    )
    if row_count == 0:
        raise ValueError(
            "Brazil RFB Socios produced no company relations; "
            "refusing to publish an empty edge table"
        )
    return {"company_relations": row_count}
```

- [ ] **Step 5: Run the tests to verify they pass**

```bash
uv run pytest tests/test_brazil_comp_rfb_relations.py -q
```

Expected: PASS (4 tests).

- [ ] **Step 6: Add the DuckDB build asset**

In `assets.py`, add the pool and key constants:

```python
BRAZIL_COMP_RFB_RELATIONS_DUCKDB_POOL = "brazil_comp_rfb_relations_duckdb"
COMPANY_RELATIONS_ASSET_KEY = "brazil_comp_rfb_company_relations_duckdb"
```

Add `relations: Path` to `BrazilCompRfbStagePaths` and
`relations=root / "relations.duckdb",` to `brazil_comp_rfb_stage_paths`.

Add the asset (import `relations` at the top of `assets.py`):

```python
@dg.asset(
    name=COMPANY_RELATIONS_ASSET_KEY,
    deps=[dg.AssetKey(SOCIOS_ASSET_KEY)],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb"},
    partitions_def=BRAZIL_COMP_RFB_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    pool=BRAZIL_COMP_RFB_RELATIONS_DUCKDB_POOL,
    description=(
        "Brazil RFB company relation edges: one row per company-to-partner "
        "link, partner being a company, a natural person or a foreign entity."
    ),
)
def brazil_comp_rfb_company_relations_duckdb(
    context: dg.AssetExecutionContext,
) -> dg.MaterializeResult:
    stage_paths = _stage_paths_for_context(context)
    stage_paths.ensure_root()
    with duckdb_resource(stage_paths.relations).get_connection() as connection:
        counts = relations.build_brazil_rfb_company_relations(
            connection=connection,
            source_run_id=context.run_id,
            snapshot_year_month=brazil_comp_rfb_snapshot_year_month(
                context.partition_key
            ),
            socios_database_path=stage_paths.socios,
        )
    context.log.info("Built Brazil RFB company relations: counts=%s", counts)
    return dg.MaterializeResult(metadata=dict(counts))
```

- [ ] **Step 7: Verify definitions load**

```bash
uv run dg check defs
```

Expected: `All definitions loaded successfully.`

- [ ] **Step 8: Commit**

```bash
git add src/dagster_v3/defs/brazil_companies/rfb/relations.py \
        src/dagster_v3/defs/brazil_companies/rfb/tables.py \
        src/dagster_v3/defs/brazil_companies/rfb/assets.py \
        tests/test_brazil_comp_rfb_relations.py
git commit -m "feat(corpscout): build RFB Socios into company relation edges"
```

---

## Task 4: Migration and ClickHouse export

**Files:**
- Create: `corpscout/clickhouse/migrations/000208_corpscout_br_company_relations.up.sql` / `.down.sql`
- Modify: `src/dagster_v3/defs/brazil_companies/rfb/clickhouse.py`
- Modify: `src/dagster_v3/defs/brazil_companies/rfb/assets.py`
- Modify: `tests/test_clickhouse_migrations.py`, `tests/test_brazil_comp_rfb_clickhouse.py`

**Interfaces:**
- Consumes: `tables.BR_COMPANY_RELATIONS_EXPORT_COLUMNS`, `tables.COMPANY_RELATIONS_TABLE`, `BrazilCompRfbStagePaths.relations`.
- Produces: `clickhouse.export_brazil_comp_rfb_clickhouse_company_relations(...) -> int`; asset `brazil_comp_rfb_company_relations_clickhouse`.

- [ ] **Step 1: Confirm the migration number is still free**

```bash
ls corpscout/clickhouse/migrations/ | tail -3
```

If `000208` is taken, use the next free number and substitute it everywhere below.

- [ ] **Step 2: Write the failing contract test**

In `tests/test_clickhouse_migrations.py`, add `"000208_corpscout_br_company_relations",` to the end of `EXPECTED_MIGRATIONS`, and add:

```python
def test_br_company_relations_migration_covers_export_columns() -> None:
    """The edge table: one row per company-to-partner link. A newly exported
    column with no migration behind it fails here rather than mid-export."""
    sql = _migration_sql("000208_corpscout_br_company_relations.up.sql")
    down_sql = _migration_sql("000208_corpscout_br_company_relations.down.sql")

    assert "CREATE TABLE IF NOT EXISTS corpscout.br_company_relations" in sql
    for column in brazil_rfb_tables.BR_COMPANY_RELATIONS_EXPORT_COLUMNS:
        assert f"    {column} " in sql, column
    assert (
        "ORDER BY (cnpj_basico, related_entity_kind, related_tax_id, relation_code)"
        in sql
    )
    assert "DROP TABLE IF EXISTS corpscout.br_company_relations" in down_sql
```

Add the import at the top of the file if absent:

```python
from dagster_v3.defs.brazil_companies.rfb import tables as brazil_rfb_tables
```

- [ ] **Step 3: Run it to verify it fails**

```bash
uv run pytest tests/test_clickhouse_migrations.py -q -k br_company_relations
```

Expected: FAIL — `FileNotFoundError` for the migration file.

- [ ] **Step 4: Write the migration**

Create `corpscout/clickhouse/migrations/000208_corpscout_br_company_relations.up.sql`:

```sql
CREATE DATABASE IF NOT EXISTS corpscout;

-- How a Brazilian company is connected to something else.
--
-- One row per partner edge from RFB's Socios file. The far end is a company, a
-- natural person, or a foreign entity, and `related_entity_kind` says which --
-- one edge model rather than separate people and ownership tables, because a
-- connection is a connection whichever kind sits at the other end.
--
-- This is the only Brazilian source that answers who controls a company and
-- what else they control. CVM's shareholder data covers 1,230 companies; this
-- covers the register.
--
-- Person names and masked CPFs are stored exactly as RFB publishes them. RFB
-- performs the masking itself as part of an open-transparency dataset. We add
-- nothing and never attempt to reverse it. Redaction is a view concern.
--
-- `related_tax_id` holds a CNPJ when kind is 1 and a MASKED CPF when kind is 2,
-- discriminated by related_entity_kind. Any join to br_companies must carry
-- that predicate or it silently matches nothing.
CREATE TABLE IF NOT EXISTS corpscout.br_company_relations
(
    country_iso2 LowCardinality(String),
    source_slug LowCardinality(String),
    source_run_id String,
    source_record_id String,
    snapshot_year_month LowCardinality(String),
    cnpj_basico String,
    related_entity_kind LowCardinality(String),
    related_name String,
    related_tax_id String,
    relation_code LowCardinality(String),
    relation_since Nullable(Date32),
    related_country String,
    representative_tax_id String,
    representative_name String,
    representative_code LowCardinality(String),
    age_band LowCardinality(String),
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY (cnpj_basico, related_entity_kind, related_tax_id, relation_code);
```

Create the `.down.sql`:

```sql
DROP TABLE IF EXISTS corpscout.br_company_relations;
```

- [ ] **Step 5: Run the contract test to verify it passes**

```bash
uv run pytest tests/test_clickhouse_migrations.py -q
```

Expected: PASS (whole file — the ledger and comment-semicolon checks run too).

- [ ] **Step 6: Write the export function**

In `clickhouse.py`, after `export_brazil_comp_rfb_clickhouse_establishments`:

```python
def export_brazil_comp_rfb_clickhouse_company_relations(
    *,
    duckdb_connection: Any,
    clickhouse: ClickhouseResource,
    log: Callable[..., object] | None = None,
) -> int:
    """Replace corpscout.br_company_relations with the DuckDB relations table."""
    assert_clickhouse_tables_exist(
        clickhouse,
        database=tables.BRAZIL_COMP_RFB_DATABASE,
        tables=(tables.BR_COMPANY_RELATIONS_TABLE_CH,),
    )
    if log is not None:
        log(
            "Exporting Brazil RFB company relations to ClickHouse: table=%s",
            tables.QUALIFIED_BR_COMPANY_RELATIONS_TABLE,
        )
    with clickhouse.get_connection() as client:
        rows = export_duckdb_connection_table_to_clickhouse(
            duckdb_connection=duckdb_connection,
            clickhouse_client=client,
            duckdb_schema=DLT_DATASET_NAME,
            duckdb_table=tables.COMPANY_RELATIONS_TABLE,
            clickhouse_database=tables.BRAZIL_COMP_RFB_DATABASE,
            clickhouse_table=tables.BR_COMPANY_RELATIONS_TABLE_CH,
            columns=tables.BR_COMPANY_RELATIONS_EXPORT_COLUMNS,
            truncate=True,
        )
    if log is not None:
        log("Finished Brazil RFB company relations export: rows=%s", rows)
    return rows
```

- [ ] **Step 7: Add the export asset**

In `assets.py`, first add the function to the **existing** import block (it
imports export functions by name, not as a module — match that exactly):

```python
from dagster_v3.defs.brazil_companies.rfb.clickhouse import (
    export_brazil_comp_rfb_clickhouse_companies,
    export_brazil_comp_rfb_clickhouse_company_contacts,
    export_brazil_comp_rfb_clickhouse_company_domains,
    export_brazil_comp_rfb_clickhouse_company_relations,
    export_brazil_comp_rfb_clickhouse_establishments,
    export_brazil_comp_rfb_clickhouse_websites,
)
```

Then the asset:

```python
CLICKHOUSE_COMPANY_RELATIONS_ASSET_KEY = "brazil_comp_rfb_company_relations_clickhouse"


@dg.asset(
    name=CLICKHOUSE_COMPANY_RELATIONS_ASSET_KEY,
    deps=[dg.AssetKey(COMPANY_RELATIONS_ASSET_KEY)],
    group_name=GROUP_NAME,
    kinds={"clickhouse", "sql"},
    partitions_def=BRAZIL_COMP_RFB_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    pool=BRAZIL_COMP_RFB_RELATIONS_DUCKDB_POOL,
    description="Publish Brazil RFB company relation edges to ClickHouse.",
)
def brazil_comp_rfb_company_relations_clickhouse(
    context: dg.AssetExecutionContext,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    stage_paths = _stage_paths_for_context(context)
    with duckdb_resource(stage_paths.relations).get_connection() as connection:
        rows = export_brazil_comp_rfb_clickhouse_company_relations(
            duckdb_connection=connection,
            clickhouse=clickhouse,
            log=context.log.info,
        )
    return dg.MaterializeResult(metadata={"rows": rows})
```

- [ ] **Step 8: Add the export contract test**

In `tests/test_brazil_comp_rfb_clickhouse.py`. That file does **not** currently
import the RFB tables module, so add this import alongside the existing ones:

```python
from dagster_v3.defs.brazil_companies.rfb import tables as brazil_rfb_tables
```

Then the test:

```python
def test_company_relations_export_uses_the_declared_column_contract() -> None:
    """Column order is the contract: the exporter ships this tuple positionally
    into the migrated table."""
    assert brazil_rfb_tables.BR_COMPANY_RELATIONS_EXPORT_COLUMNS[0] == "country_iso2"
    assert "cnpj_basico" in brazil_rfb_tables.BR_COMPANY_RELATIONS_EXPORT_COLUMNS
    assert "related_entity_kind" in brazil_rfb_tables.BR_COMPANY_RELATIONS_EXPORT_COLUMNS
    assert brazil_rfb_tables.BR_COMPANY_RELATIONS_EXPORT_COLUMNS == (
        brazil_rfb_tables.BR_COMPANY_RELATIONS_COLUMNS
    )
```

- [ ] **Step 9: Run everything**

```bash
uv run pytest tests/test_brazil_comp_rfb_relations.py \
               tests/test_brazil_comp_rfb_clickhouse.py \
               tests/test_brazil_comp_rfb_source.py \
               tests/test_brazil_comp_rfb_staging.py \
               tests/test_clickhouse_migrations.py -q
uv run dg check defs
```

Expected: all PASS, `All definitions loaded successfully.`

- [ ] **Step 10: Commit**

```bash
git add corpscout/clickhouse/migrations/000208_corpscout_br_company_relations.up.sql \
        corpscout/clickhouse/migrations/000208_corpscout_br_company_relations.down.sql \
        src/dagster_v3/defs/brazil_companies/rfb/clickhouse.py \
        src/dagster_v3/defs/brazil_companies/rfb/assets.py \
        tests/test_clickhouse_migrations.py \
        tests/test_brazil_comp_rfb_clickhouse.py
git commit -m "feat(corpscout): publish Brazil company relation edges to ClickHouse"
```

---

## Deployment

**Gate first: run Task 1's layout verification on prod** before materializing
anything. Tasks 2–4 ship code written against an unverified layout; this is the
step that confirms it. A wrong column order produces no error, only wrong data.

Migrations run **before** code deploy (the export asserts the table exists).
Then materialize the chain for one partition on the **prod Dagster UI**, not
locally — the snapshot download is multi-GB and CLAUDE.md forbids pointing a
local daemon at the deployed metadata database.

Asset order within the partition:
`brazil_comp_rfb_snapshot_files_duckdb` → `brazil_comp_rfb_socios_duckdb` →
`brazil_comp_rfb_company_relations_duckdb` →
`brazil_comp_rfb_company_relations_clickhouse`.

## First measurements to take after the first run

The design doc scopes this to the edge table precisely because the graph's shape
is unknown. Record these in the design doc's §11 once known — they decide
whether the group views are a two-hop join or a guarded traversal:

```sql
-- how many edges, and the kind split
SELECT related_entity_kind, count() AS edges, uniqExact(cnpj_basico) AS companies
FROM corpscout.br_company_relations GROUP BY related_entity_kind;

-- do corporate partners resolve to the register we hold?
-- related_tax_id is the full 14-digit CNPJ; br_companies.cnpj_basico is only
-- the 8-digit root, so the join must take the first 8 digits of related_tax_id.
SELECT countIf(c.cnpj_basico != '') AS resolves, count() AS corporate_edges
FROM corpscout.br_company_relations AS r
LEFT ANY JOIN corpscout.br_companies AS c ON c.cnpj_basico = substr(r.related_tax_id, 1, 8)
WHERE r.related_entity_kind = '1';

-- graph density: the most connected partners
SELECT related_tax_id, any(related_name), count() AS companies_linked
FROM corpscout.br_company_relations
GROUP BY related_tax_id ORDER BY companies_linked DESC LIMIT 20;
```

---

## Task 5: Snapshot RFB archives to S3 before parsing

**Added mid-flight (user, 2026-07-28).** Not part of the original socios scope —
this fixes the RFB module's download path for **all ten families**.

**Why.** `brazil_comp_rfb_snapshot_files_duckdb` downloads archives straight to
`data/brazil_rfb_downloads/<YYYY-MM>/`, and `cleanup_previous_partition_files`
deletes `download_root / previous_partition`. So re-running an older month
re-downloads ~10 GB from RFB — and once RFB's mirror stops publishing a
snapshot, that partition becomes **unrebuildable**. Its sibling modules already
solve this: `brazil_companies/cgu` and `brazil_companies/pgfn` both snapshot raw
archives to S3 first. `rfb` has no `ObjectStoreResource` at all and is the
largest source we ingest.

**Shape (mirrors PGFN exactly):**

```
brazil_comp_rfb_raw_archives_s3        NEW: origin -> s3://source-brazil-rfb, skip if present
        |
brazil_comp_rfb_snapshot_files_duckdb  CHANGED: s3 -> local -> extract -> delete local archive
```

**Files:**
- Modify: `src/dagster_v3/defs/brazil_companies/rfb/source.py`
- Modify: `src/dagster_v3/defs/brazil_companies/rfb/assets.py`
- Test: `tests/test_brazil_comp_rfb_source.py`, `tests/test_brazil_comp_rfb_assets.py`

**Interfaces:**
- Consumes: `ObjectStoreResource` from `dagster_v3.defs.common.resources`
  (`.ensure_bucket(bucket)`, `.exists(key, bucket=)`, `.upload_file(...)`,
  `.download_file(...)` — read `pgfn/source.py:_sync_source_archive` and
  `pgfn/parsing.py:parse_brazil_comp_pgfn_archives_from_object_store` for the
  exact call signatures, and mirror them).
- Produces: `source.BRAZIL_RFB_RAW_BUCKET = "source-brazil-rfb"`;
  `source.rfb_archive_object_key(snapshot_year_month, family, archive_name) -> str`;
  `source.sync_snapshot_archives_to_object_store(...)`;
  asset key `brazil_comp_rfb_raw_archives_s3`.

**Key layout**, following PGFN's `brazil_pgfn/raw_archives/snapshot=…/…`:

```
brazil_rfb/raw_archives/snapshot=<YYYY-MM>/family=<family>/<archive_name>
```

- [ ] **Step 1: Write the failing test for the object key**

In `tests/test_brazil_comp_rfb_source.py`:

```python
def test_rfb_archive_object_key_is_partitioned_by_snapshot_and_family() -> None:
    """The key layout IS the reuse contract: a re-run must resolve to the same
    key so `exists` short-circuits the download."""
    assert source.rfb_archive_object_key(
        "2026-07", "socios", "Socios0.zip"
    ) == "brazil_rfb/raw_archives/snapshot=2026-07/family=socios/Socios0.zip"
```

- [ ] **Step 2: Run it, confirm it fails**

```bash
uv run pytest tests/test_brazil_comp_rfb_source.py::test_rfb_archive_object_key_is_partitioned_by_snapshot_and_family -q
```

Expected: FAIL, `AttributeError: module ... has no attribute 'rfb_archive_object_key'`.

- [ ] **Step 3: Implement the key + bucket + sync in `source.py`**

Add near the other module constants:

```python
BRAZIL_RFB_RAW_BUCKET = "source-brazil-rfb"


def rfb_archive_object_key(
    snapshot_year_month: str, family: str, archive_name: str
) -> str:
    return (
        f"brazil_rfb/raw_archives/snapshot={validate_snapshot_year_month(snapshot_year_month)}"
        f"/family={family}/{archive_name}"
    )
```

Then a sync function that, for each discovered remote file, skips when the key
already exists and otherwise streams origin -> local temp -> S3. Reuse the
existing `_download` helper (it already has retry/verification) and
`fetch_snapshot_remote_files` for discovery. Return, per archive, at minimum:
`family`, `archive_name`, `archive_url`, `object_key`, `size_bytes`, `sha256`,
`reused_existing` — the asset reports these as metadata.

- [ ] **Step 4: Run the key test, confirm it passes**

```bash
uv run pytest tests/test_brazil_comp_rfb_source.py -q
```

- [ ] **Step 5: Read archives back from S3 when building the manifest**

Change `download_extract_snapshot_files` so that when an `object_store` is
supplied it fetches each archive with `object_store.download_file(...)` instead
of `_download(...)` from the origin, then extracts as today, then **deletes the
local `.zip` once extraction succeeds**. The extracted CSV must remain — the
DuckDB load reads it, and the partition's own cleanup removes it later.

Thread `object_store` through `brazil_rfb_source(...)` to that call.

- [ ] **Step 6: Add the S3 asset and make the manifest asset depend on it**

In `assets.py`, add `RAW_ARCHIVES_ASSET_KEY = "brazil_comp_rfb_raw_archives_s3"`
and an asset taking `object_store: ObjectStoreResource`, partitioned on
`BRAZIL_COMP_RFB_PARTITIONS`, with `BackfillPolicy.multi_run(max_partitions_per_run=1)`,
`kinds={"python", "s3", "zip"}`. It calls the Step 3 sync and returns the
per-archive metadata.

Then add `deps=[dg.AssetKey(RAW_ARCHIVES_ASSET_KEY)]` to
`brazil_comp_rfb_snapshot_files_duckdb` and pass `object_store` into its
`brazil_rfb_source(...)` call.

**Register the new asset in `defs = dg.Definitions(assets=[...])`** — first in
the list, since it now precedes the manifest asset — and add a membership +
partition assertion in `tests/test_brazil_comp_rfb_assets.py`.

- [ ] **Step 7: Verify**

```bash
uv run pytest tests/test_brazil_comp_rfb_source.py tests/test_brazil_comp_rfb_assets.py -q
uv run python -c "
from dagster_v3.definitions import defs as load_defs
keys = {k.path[-1] for k in load_defs().get_repository_def().asset_graph.get_all_asset_keys()}
print('registered:', 'brazil_comp_rfb_raw_archives_s3' in keys)
"
uv run dg check defs
```

- [ ] **Step 8: Commit**

```bash
git add src/dagster_v3/defs/brazil_companies/rfb/source.py \
        src/dagster_v3/defs/brazil_companies/rfb/assets.py \
        tests/test_brazil_comp_rfb_source.py \
        tests/test_brazil_comp_rfb_assets.py
git commit -m "feat(corpscout): snapshot RFB archives to S3 before parsing"
```

**Not in scope:** backfilling S3 with archives from partitions already
materialized. RFB's mirror may no longer carry them; whether any are still
retrievable is a question for after this ships.
