# Unified XBRL/iXBRL Extraction Layer — Design

**Date:** 2026-08-31
**Status:** Approved design (owner-reviewed in session)
**Scope:** National company-filing sources — Finland PRH, Sweden Bolagsverket, UK Companies House. **ESEF is explicitly out of scope** and stays on its Arelle pipeline unchanged.

## Problem

The repo has four independent XBRL fact-extraction implementations:

| Module | Format | Parser | What it stores |
|---|---|---|---|
| `esef_filings` | ESEF iXBRL packages | Arelle (full DTS) | full facts + artifacts (out of scope here) |
| `finland_xbrl/parser.py` | plain XBRL | private lxml | documents / contexts / units / facts |
| `sweden_financial/parsing.py` | iXBRL | private lxml | documents / contexts / units / facts (~290M rows) + Arelle-built taxonomy dictionary |
| `uk_companies_house` + `xbrl_common/parser.py` | iXBRL | shared light lxml | ~12 fixed metric concepts only; no stored facts |

Each private parser has its own scale/sign handling, number cleaning, context model, and quirks. None implements the iXBRL transformation registry (`format=` attribute); each approximates it with regex cleaning. Cross-country fact semantics are therefore not guaranteed to match, and UK stores no facts at all.

## Decisions (owner-confirmed)

1. **Unify code AND storage shape.** One shared extractor and one canonical set of per-source table shapes. UK is upgraded from metrics-only to full fact storage.
2. **Re-extract with parity gate.** Each migrating source re-parses its raw filings from S3 into new tables built alongside the old ones; downstream cuts over only when a per-filing diff is explained; old tables are dropped after cutover. S3 raw data is never modified (additive-only, standing rule).
3. **Taxonomy dictionaries for all three countries.** Generalize Sweden's Arelle-once pattern into a shared builder; UK FRC and Finland PRH get dictionaries for the first time; Sweden's migrates to the shared shape.
4. **Rollout order: Finland → Sweden → UK.** Finland is closest to the target shape and smallest; Sweden is the hard parity test; UK is a new capability (largest volume).
5. **Packaging: grow `xbrl_common` into the subsystem; sources stay thin adapters.** A Dagster Resolved component (`XbrlSourceComponent`) is a possible later evolution once three sources use the library, not part of this project.

## Architecture

```
per-source (unchanged): acquisition → raw filings on S3
                                          │
xbrl_common (new/extended):               ▼
  extractor.py    ── parse plain XBRL + iXBRL → 4 row groups
  transforms.py   ── ixt transformation registry
  tables.py       ── canonical column tuples + contract-test helpers
  taxonomy.py     ── Arelle-once concept-metadata dictionary builder
  parity.py       ── old-vs-new diff harness (migration only)
                                          │
per-source (thin): DuckDB staging → ClickHouse export (migration-owned DDL)
  {src}_xbrl_documents / _contexts / _units / _facts / {src}_taxonomy_concepts
```

### 1. Extractor — `xbrl_common/extractor.py`

One full-fidelity extraction path for both plain XBRL and iXBRL. Replaces (eventually) `finland_xbrl/parser.py`, the fact-extraction half of `sweden_financial/parsing.py`, and extends the existing light `xbrl_common/parser.py` (which remains temporarily for UK metrics until UK migrates).

Per filing, emits four row groups (the Finland shape, extended):

- **documents** — source identity (per-source key columns provided by the adapter), xml sha256/size, root element, `schema_refs` JSON + `taxonomy_entrypoint`, reported entity id / company name / period start / period end (per-source reported-concept mapping), context/unit/fact counts, `validation_warnings` JSON, `parser_version`, `parsed_at`.
- **contexts** — context id, entity identifier + scheme, period type (`instant`/`duration`/`none`), instant/start/end dates, dimensions JSON (`[dimension_qname, member_qname, typed_value]` triples — explicit AND typed members), comparative flag, raw XML.
- **units** — unit id, measures JSON, numerator/denominator measures (divide units), `is_divide`, resolved ISO-4217 currency.
- **facts** — concept qname (canonical prefix) + namespace + local name, context/unit refs, `value_kind ∈ {numeric, date, text, empty}`, `raw_value`, typed `numeric_value`/`date_value`/`text_value`, decimals/precision, `is_nil`, `xml_lang`, dimensions + comparative flag denormalized from context, fact ordinal, `parser_version`, `parsed_at`.

iXBRL correctness requirements (the reason regex cleaning is retired):

- `ix:nonFraction` / `ix:nonNumeric` / `ix:fraction` facts, including facts inside `ix:hidden`.
- `ix:continuation` chains concatenated in order for text facts; `ix:exclude` subtrees removed.
- `sign` and `scale` attributes applied after transformation.
- `format=` resolved via the transformation registry (below); an unknown transform records a warning and stores the raw value with `value_kind='text'` — it never guesses.
- Nested facts (a fact element inside another fact's content) are extracted independently.
- Entity/period resolution identical to plain XBRL (contexts live in `ix:resources`).

Parser hardening carried over from Sweden's implementation: `etree.XMLParser(recover=True, huge_tree=True, resolve_entities=False)`.

Adapter contract (what a source supplies):

```python
@dataclass(frozen=True)
class SourceProfile:
    source_slug: str                       # "finland_prh", "sweden_bolagsverket", "uk_companies_house"
    canonical_prefixes: dict[str, str]     # namespace URI -> canonical prefix
    reported_concepts: dict[str, str]      # concept qname -> documents column
                                           # (reported_entity_id, reported_company_name,
                                           #  reported_period_start, reported_period_end)

def extract_filing(body: bytes, *, profile: SourceProfile,
                   document_identity: Mapping[str, str]) -> ExtractedFiling
    # ExtractedFiling: .documents / .contexts / .units / .facts row lists + .warnings
```

`document_identity` carries the per-source key columns (e.g. Finland `business_id`+`financial_date`, Sweden org number + document id, UK company number + archive member) that prefix every row.

### 2. Transformation registry — `xbrl_common/transforms.py`

Implements the ixt transforms actually used by the three sources' filings, covering registries v1–v4 numeric and date families:

- numeric: `num-dot-decimal`, `num-comma-decimal`, `num-unit-decimal`, `zero-dash` (and `fixed-zero`), `fixed-empty`, `fixed-false`, `fixed-true`, `num-comma-dot`, thousands-separator variants.
- date: `date-day-month-year`, `date-month-day-year`, `date-year-month-day`, dotted/slashed variants, `date-month-year`, month-name forms (`date-day-monthname-year` in en/sv/fi at minimum).

API: `apply_transform(format_qname, raw_text) -> TransformResult(value, kind)`; unknown registries/transforms raise `UnknownTransform`, which the extractor converts to a warning + raw text value. A coverage script (dev-time) scans a corpus of raw filings and reports `format=` values not implemented, so the registry is grown from evidence, not speculation.

### 3. Storage shape — `xbrl_common/tables.py`

- Canonical column tuples: `XBRL_DOCUMENT_COLUMNS`, `XBRL_CONTEXT_COLUMNS`, `XBRL_UNIT_COLUMNS`, `XBRL_FACT_COLUMNS`, `TAXONOMY_CONCEPT_COLUMNS` — each defined once here. Per-source key columns are prepended by the adapter (`document_identity` keys), so a source table = identity columns + canonical columns.
- Tables are **per-source** (`fi_xbrl_facts`, `se_xbrl_facts`, `gb_xbrl_facts`, …), per the per-country principle: countries differ, merging is a view concern.
- ClickHouse DDL remains **migration-owned** (repo rule). A shared contract-test helper asserts each source's migration column order matches identity + canonical tuples, mirroring the existing export-contract-test pattern.
- DuckDB staging and ClickHouse export reuse the existing shared exporters (`defs/clickhouse/resolved.py` conventions: stage + `EXCHANGE TABLES`, empty-input refusal, per-source DuckDB file + pool).
- String columns coalesce to `''` (native driver NULL rule); numeric/date NULLs only in `Nullable(...)` columns.
- `raw_xml` columns (Finland has them on contexts/units today) are **dropped from ClickHouse** in the new shape (biggest columns, nothing queries them) — raw filings on S3 remain the evidence of record.

### 4. Taxonomy dictionaries — `xbrl_common/taxonomy.py`

Generalizes `sweden_financial/concepts.py`: load one taxonomy entrypoint once via Arelle (offline path, slowness acceptable, persistent cache dir honored), walk the DTS, and emit rows keyed by `(taxonomy_version, concept_qname)`:

- identity: namespace, local name, canonical qname, substitution group, abstract flag
- typing: item type, balance (debit/credit), period type (instant/duration)
- labels: one row per (language, label role) in a companion `{src}_taxonomy_labels` table — standard, terse, documentation roles at minimum
- structure: presentation parent + order per ELR, calculation parent + weight per ELR, dimension/domain/member relationships

Per source: a small config listing taxonomy entrypoint URLs per version (Finland PRH versions, Swedish Bolagsverket K2/K3 versions, UK FRC yearly releases). Dictionary assets are unpartitioned, insert-only by `(taxonomy_version, concept_qname)` (merge semantics, never replace — Sweden's existing rule), scheduled rarely (manual or yearly; new versions are a config change).

Sweden's existing `se_financial_taxonomy_concepts` + translation flow keeps working during migration; it moves to the shared shape as part of Sweden's phase, and the translation cache (`text_translations`) plugs in on top unchanged.

### 5. Parity harness — `xbrl_common/parity.py`

Per migrating source, during its migration window only:

1. New-shape tables are built **alongside** old tables from the same raw S3 filings (separate asset chain, separate DuckDB file, same source pool).
2. A parity asset diffs per filing: fact count old vs new; joined value comparison on (concept local name, context period, dimensions) for numeric facts; document counts. Results land in a `{src}_xbrl_parity_report` table with per-filing status (`match` / `explained` / `mismatch`) and diff details JSON.
3. Known-good differences (e.g. transformation registry fixing a value the old regex mangled, hidden facts the old parser missed) are recorded as explained-diff rules in code, not waved through by hand.
4. Cutover: downstream (metrics, serving views, translations) repoints to new tables only when mismatch count is zero-or-explained; old tables are dropped in a follow-up migration after cutover is verified.

### 6. Rollout

**Phase 1 — Finland.** Closest to target shape, smallest volume, plain XBRL only. Deliverables: extractor + transforms + tables + Finland adapter, new `fi_xbrl_*` tables, parity vs existing Finland tables, Finland taxonomy dictionary, cutover, drop old tables. Proves the whole recipe.

**Phase 2 — Sweden.** The hard parity test (~290M facts, iXBRL, years of quirk handling in the old parser). Sweden's parser also does non-fact work (catalog parsing, reported-name heuristics, signatories); only fact/context/unit extraction moves to the shared extractor — the rest stays in `sweden_financial`. Taxonomy dictionary migrates to shared shape; translation flow unchanged on top.

**Phase 3 — UK.** New capability: full `gb_xbrl_*` fact storage parsed from the stored bulk archives (billions-of-rows commitment; backfill reads existing S3 archives, no re-download). No old fact tables exist, so the "parity" gate is instead: the existing ~12-metric extraction is recomputed from the new fact tables and diffed against the current metrics table. UK FRC taxonomy dictionary. The light `xbrl_common/parser.py` is retired at the end of this phase.

Each phase is its own plan + implementation cycle (separate implementation plan documents).

### Non-goals

- ESEF: untouched (extension-taxonomy resolution stays Arelle's job there).
- Cross-country unified facts tables or views: explicitly out (unification of serving is frozen until 15 countries — standing rule; this project unifies extraction code and shapes only).
- Norway: no XBRL (Regnskapsregisteret JSON) — not affected.
- New downstream metrics: metric mappings stay per-source as they are.

## Error handling

- Extractor never raises on malformed filing content: recover-mode parse; a filing that yields zero facts produces a documents row with a warning (Finland's existing behavior, kept).
- Unknown transform / unparseable value → warning + raw text value; warnings aggregate into `validation_warnings` on the document row.
- Export refuses to replace populated tables with empty input (existing shared-exporter rule).
- Parity mismatches block cutover; they never block the old pipeline, which keeps running until cutover.

## Testing

- Golden-fixture tests: real filings per source (reuse existing test fixtures; add iXBRL fixtures exercising hidden/continuation/exclude/transform paths), asserting exact row output of the extractor.
- Transform registry: table-driven unit tests per transform, plus the corpus coverage script run once per source during its phase.
- Contract tests: canonical column tuples vs each source's ClickHouse migration (existing pattern).
- Parity harness has its own unit tests on synthetic old/new row sets.
- `uv run dg check defs` + affected `uv run pytest` suites green per phase.
