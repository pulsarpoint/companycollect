# Source onboarding templates

Standard intake artifacts for adding a new company/financial data source. The
goal at 300+ sources / 150 countries is that **every source is onboarded the
same way and produces the same dossier**, so a reviewer can sign off and a
second person can implement the Dagster pipeline without re-discovering anything.

## The three phases (only phase 3 touches Dagster)

```
1. DISCOVERY   land a raw sample in S3 → format-normalize to Parquet →
   (notebook)   profile with profile_source.py. Throwaway, exploratory.

2. MAPPING     fill in source_dossier.md: file inventory, schema profile,
   (dossier)    join keys, source-to-target mapping, archetype, cadence.
                ← REVIEW GATE. Nothing gets built before this is approved.

3. PRODUCTION  implement in dagster_v2, one asset layer at a time:
   (dagster)    raw → structured-parquet → canonical load → automation.
```

Do **not** discover structure inside Dagster assets. Dagster receives the
*conclusion* of the dossier, never performs the analysis.

## How to use

```bash
# 1. Copy the template into the country dir, one folder per source.
mkdir -p <country>/<source>
cp _templates/source_dossier.md <country>/<source>/dossier.md

# 2. Land a representative raw sample, normalize FORMAT only (no meaning yet)
#    to Parquet/CSV/JSON, then profile it. Writes profile.md + profile.json
#    next to the dossier.
uv run python _templates/profile_source.py \
    '<country>/<source>/samples/*.parquet' \
    --out <country>/<source>

# 3. Fill in dossier.md using profile.md as evidence. Get it reviewed.
# 4. Only then implement the source in ../corpscout/dagster_v2.
```

`profile_source.py` reads CSV / JSON(L) / Parquet (and globs) via DuckDB —
no need to pre-convert if DuckDB can already read the format.

## Why a dossier, not ad-hoc notes

- **Reviewable gate** — the source-to-target mapping is the real intellectual
  work; it must be reviewed before any code exists.
- **Promotion input** — the coverage/fill-rate section is what later decides
  which `extras` fields graduate to typed canonical columns (see
  `<country>/data_model/`).
- **Archetype evidence** — recurring fetch/cadence shapes across ~50 dossiers
  are what justify adding a new Dagster archetype/scaffold, instead of guessing
  archetypes upfront.

## Relationship to existing country artifacts

| Artifact | Scope | Owns |
|---|---|---|
| `<country>/source_inventory.md` | country | which sources exist, license, status |
| `<country>/data_model/country_company_profile.schema.json` | country | the canonical **companies** target schema |
| `<country>/<source>/dossier.md` (this template) | **source** | how THIS source maps to the canonical target + how to ingest it |
