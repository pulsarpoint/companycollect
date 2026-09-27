# Industry evidence limitation found during the lookup experiment

The crawler reads `corpscout.se_company_industry_display_current`. Its builder is
`services/dagster_v3/src/dagster_v3/defs/company_serving/dbt/models/se_company_industry_display_current_build.sql`.
The builder reports `NACE_REV2` for every row, but chooses labels from currently
active `nace_categories` records grouped only by normalized code. Some supplied
codes/labels belong to NACE Rev. 2.1. Consequently, a code's number alone does
not establish its meaning.

The upstream normalizer
`services/dagster_v3/src/dagster_v3/defs/sweden_company/normalized_duckdb.py`
derives `nace_rev2_class_code` with `left(sni_code, 4)` without attaching a source
classification revision. The four-digit mapping rule alone cannot distinguish
SNI 2007/NACE Rev. 2 from SNI 2025/NACE Rev. 2.1. Changing only the display label
would therefore leave source-version ambiguity unresolved.

Observed examples in the frozen Swedish cohort:

- JM: code `4100` does not exist in the declared Rev. 2 reference.
- Instalco: `6421` does not exist in the declared Rev. 2 reference.
- Arise: `3512` has a label inconsistent with the declared Rev. 2 reference.
- Brinova: `6832` has a label inconsistent with the declared Rev. 2 reference.

The experiment joins labels by both declared version and normalized code, and
excludes absent codes or conflicting labels from model evidence. It retains
these rows in the result and debug trace. Missing evidence remains neutral.
Matching a code and label to the declared reference does not independently
prove that the source assigned the correct version.

Before relying on industry data more strongly, preserve the source SNI version
and its corresponding NACE revision, use version-specific mappings and labels,
and rebuild the serving table. Do not infer the revision solely from which
reference happens to contain a code. This experiment does not change or
rematerialize that upstream pipeline.
