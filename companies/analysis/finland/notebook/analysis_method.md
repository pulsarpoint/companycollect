# Finland Source Analysis Method

Reasoning a developer follows before writing the Phase 5 transforms. Read this
before touching `conformance/transforms.py`.

---

## 1. Goal and layers

```
S3 (raw XML / NDJSON)
  ↓  download.py (Phase 3)
structured Parquet  ← persisted; future Dagster asset boundary
  ↓  transforms.py (Phase 5)
canonical Parquet   ← 8-table contract; loaded into ClickHouse
```

**Why the structured Parquet layer?** Each source has its own shape; the
structured files contain the reshaped, validated, per-source rows. When the
pipeline becomes a Dagster asset graph, `structured/<source>/` is one asset and
`canonical/<table>/` is a downstream asset. Rebuilding the canonical tables does
not require re-downloading raw files.

Finland has **two sources**:

| Source slug | Raw format | Structured datasets |
|---|---|---|
| `prh_ytj` | NDJSON (one JSON object per company per line) | `statuses`, `names`, `websites`, `addresses`, `business_lines` |
| `prh_xbrl` | XML (XBRL instance documents, one file per statement) | `documents`, `contexts`, `units`, `facts` |

---

## 2. Per-source raw shape

### prh_ytj

Source: PRH Open Data YTJ API v3 — CC-BY-4.0, daily, no auth.

Each line in the NDJSON snapshot is a JSON object for one company. Key fields
(see `companies/analysis/finland/data_model/country_company_profile_mapping.md`
for the full mapping):

| JSON path | Type | Notes |
|---|---|---|
| `businessId.value` | string | Y-tunnus; primary key |
| `businessId.registrationDate` | string | ISO date |
| `tradeRegisterStatus` | string | `"1"`=active, `"4"`=ceased, `"3"`=intermediate |
| `status` | string | Constant `"2"` — **not a liveness indicator** |
| `endDate` | string\|null | null = active |
| `names[]` | array | `{name, type, registrationDate, endDate}` |
| `companyForms[]` | array | `{name, type, registrationDate, endDate, descriptions[]}` |
| `mainBusinessLine` | object\|null | `{code, name, descriptions[]}` — TOL/NACE |
| `website.url` | string\|null | raw URL, needs normalisation |
| `addresses[]` | array | `{type, postOffices[], street, postCode}` |
| `registeredEntries[]` | array | `{register, type, registrationDate, endDate, ...}` |

### prh_xbrl

Source: PRH Digital Financial Statement API v3 — XBRL instance documents (XML).

Facts are encoded as `fi_met:*` elements. **The line item is not the element
name** — it is the `fi_dim:MCY` dimension member on the referenced context.
See `companies/analysis/finland/prh_xbrl_schema_spike/schema_analysis.md` for
the full parser analysis.

Key fact concepts observed across the 10-statement sample:

| Concept | Kind | Meaning |
|---|---|---|
| `fi_met:mi53` | numeric EUR | Book value / balance |
| `fi_met:md103` | numeric EUR | Monetary accumulation (P&L) |
| `fi_met:di120` | date text | Period start |
| `fi_met:di121` | date text | Period end |
| `fi_met:si168` | text | Company name |
| `fi_met:si289` | text | Business ID |

Dimension used for line-item identity: `fi_dim:MCY` (e.g. `fi_MC:x673` =
Liikevaihto / revenue). Comparative context distinguished by `fi_dim:REF`
(`fi_RF:x4` = prior balance date, `fi_RF:x53` = prior accounting period).

---

## 3. Converting NDJSON with Polars (prh_ytj)

**Rule: native Polars for JSONL/CSV. Never a Python row loop.**

### Load

```python
import polars as pl

# small sample
df = pl.read_ndjson("prh_ytj_sample.jsonl")

# full snapshot (lazy, streams from S3)
df = pl.scan_ndjson("s3://corpscout/raw/fi/prh_ytj/latest.jsonl").collect()
```

`pl.read_ndjson` infers nested struct and list columns automatically. The
resulting frame has struct columns (`businessId`, `website`) and list-of-struct
columns (`names`, `addresses`, `registeredEntries`, etc.).

### Vectorized reshape patterns

```python
# Unnest a struct column
df = df.with_columns(
    pl.col("businessId").struct.field("value").alias("business_id"),
    pl.col("businessId").struct.field("registrationDate").alias("incorporation_date"),
)

# Derive liveness — DO NOT USE status field
df = df.with_columns(
    (
        (pl.col("tradeRegisterStatus") == "1") & pl.col("endDate").is_null()
    ).alias("is_active"),
    pl.col("tradeRegisterStatus").alias("trade_register_status_code"),
)
# trade_register_status_code: "1"=active, "4"=ceased, "3"=intermediate

# Extract website URL and normalise
df = df.with_columns(
    pl.col("website").struct.field("url").alias("website_url_raw"),
).with_columns(
    pl.col("website_url_raw")
    .str.strip_chars()
    .str.to_lowercase()
    .alias("website_url"),
)

# Current primary name: type==1, endDate null
names_df = (
    df.select("business_id", pl.col("names").alias("n"))
    .explode("n")
    .unnest("n")
    .filter((pl.col("type") == 1) & pl.col("endDate").is_null())
    .sort("registrationDate", descending=True)
    .unique(subset=["business_id"], keep="first")
    .rename({"name": "legal_name"})
)
```

### Profiling

```python
# Polars null audit
print(df.null_count())

# DuckDB SUMMARIZE (for Parquet output or large samples)
# uv run python companies/analysis/_templates/profile_source.py \
#     'finland/prh_ytj/samples/*.parquet' --out finland/prh_ytj
```

Run `profile_source.py` (at `companies/analysis/_templates/profile_source.py`)
on the raw NDJSON or structured Parquet to get per-column null %, cardinality,
and sample values before writing transforms.

### Domain rules for prh_ytj

| Rule | Implementation |
|---|---|
| Liveness | `tradeRegisterStatus == "1" AND endDate IS NULL`; never use `status` (constant `"2"`) |
| Current primary name | `names[type=1 AND endDate=null]`, tie-break by latest `registrationDate` |
| Auxiliary names | `names[type IN (2,3) AND endDate=null]` |
| Current company form | `companyForms[endDate=null]`, tie-break by latest `registrationDate` |
| VAT flag | `registeredEntries[register="6" AND endDate=null]` |
| Employer flag | `registeredEntries[register="5" AND endDate=null]` |
| Prepayment-register flag | `registeredEntries[register="7" AND endDate=null]` |
| VAT number | `"FI" + businessId.value` (dash removed) |
| Address language | `postOffices[langCode="1"]` for Finnish city, `langCode="2"` for Swedish |
| Website URL | strip whitespace, lowercase; prepend `https://` if scheme absent |

### Structured Parquet datasets produced

| File | Key column(s) | Rows |
|---|---|---|
| `statuses.parquet` | `business_id` | 1 per company |
| `names.parquet` | `business_id`, `name`, `type` | 1 per name record |
| `websites.parquet` | `business_id` | 1 per company (nullable url) |
| `addresses.parquet` | `business_id`, `type` | 1 per address record |
| `business_lines.parquet` | `business_id` | 1 per company (nullable) |

---

## 4. When to reuse a parser instead (prh_xbrl)

**Rule: native Polars for JSONL/CSV; reuse the parser only where parsing is
non-trivial (XML, binary formats, deeply nested taxonomies). Polars wraps the
output rows.**

Polars cannot parse XBRL XML. The `lxml`-based parser developed in
`companies/analysis/finland/prh_xbrl_schema_spike/` is **copied and reused**
in Phase 5. Its row output (Python dicts) is then wrapped into Polars DataFrames
for structured Parquet output.

```python
from lxml import etree
import polars as pl

# parser produces list-of-dicts
facts = parse_xbrl_statement(xml_bytes)          # returns List[dict]

# wrap into Polars — vectorized from here
facts_df = pl.DataFrame(facts)
facts_df.write_parquet("structured/prh_xbrl/facts.parquet")
```

Parser steps follow the rules from the schema spike:
1. Parse contexts first; preserve all dimensions.
2. Parse units (all EUR in the sample).
3. Parse every fact with `contextRef` — do not filter to known metrics in raw.
4. Join facts to contexts; denormalize `mcy_member_code` and `ref_member_code`.
5. Extract statement-level metadata from `si289` (business ID), `si168` (name),
   `di120`/`di121` (period start/end).

### Structured Parquet datasets produced

| File | Key column(s) | Rows |
|---|---|---|
| `documents.parquet` | `statement_key` | 1 per XML file |
| `contexts.parquet` | `statement_key`, `context_id` | 1 per XBRL context |
| `units.parquet` | `statement_key`, `unit_id` | 1 per XBRL unit |
| `facts.parquet` | `statement_key`, `fact_ordinal` | 1 per XBRL fact element |

---

## 5. Mapping structured rows to canonical tables

Finland fills **4 of 8** canonical tables.

**Known-absent tables** — Finland open data does not publish officers, beneficial
owners, or contact details (email, phone). The following 4 tables remain empty
for Finland:

| Table | Reason absent |
|---|---|
| `persons` | PRH does not publish officer/owner persons in open data |
| `company_people` | No person-to-registration roles in open data |
| `company_contacts` | No email or phone in PRH YTJ or XBRL data |
| `company_relationships` | No corporate ownership graph in open data |

The canonical schema is defined in
`companies/analysis/_canonical/canonical_schema.md`.

### `registrations` (from `prh_ytj`)

One row per company. Primary source: `statuses.parquet` joined with
`names.parquet` (current name), `business_lines.parquet` (NACE), and
`addresses.parquet` (first visiting-type address). Key mapping:

| Canonical column | Source field | Notes |
|---|---|---|
| `registration_uid` | `"FI:" + business_id` | |
| `country` | `"FI"` | constant |
| `registration_number` | `businessId.value` | Y-tunnus |
| `registry_source` | `"prh_ytj"` | |
| `legal_name` | `names[type=1 AND endDate=null].name` | |
| `legal_form_code` | `companyForms[endDate=null].type` | Finnish code; ELF crosswalk pending |
| `legal_form_label` | `companyForms[endDate=null].name` | |
| `lifecycle_status` | derived | `"1"`→`active`, `"4"`→`ceased`, `"3"`→`intermediate` |
| `is_active` | derived | `tradeRegisterStatus=="1" AND endDate IS NULL` |
| `incorporation_date` | `businessId.registrationDate` | |
| `dissolution_date` | `endDate` | null = active |
| `addr_*` | `addresses[type=1]` | visiting address; city from `postOffices[langCode=1]` |
| `addr_municipality_code` | `addresses[].postOffices[].municipalityCode` | Statistics Finland geo key |
| `activity_code_national` | `mainBusinessLine.code` | TOL2008 |
| `activity_scheme` | `"TOL2008"` | constant |
| `vat_number` | `"FI" + business_id` (dash removed) | confirm via `registeredEntries[register=6]` |
| `eu_id` | `euId.value` | present on ~18% of sample |
| `primary_website` | `website.url` (normalised) | |
| `extras` | `companySituations`, raw register entries | country-specific fields |

### `company` (from `prh_ytj`)

One row per resolved entity. For single-registration Finland companies,
`company_uid = "c:" + sha1("FI:" + business_id)`. Populated from the same
`statuses` + `names` join used for `registrations`; the key fields resolved
here are `primary_name`, `status`, `incorporation_date`, `dissolution_date`,
`home_country = "FI"`, and `primary_website` (denormalised from
`company_websites`). `field_provenance` maps every resolved field to `"prh_ytj"`.

### `company_websites` (from `prh_ytj`)

One row per company where `website.url` is non-null (~6% of the sample). All
registry-sourced websites are `source_kind="registry"`,
`discovery_method="registry_field"`, `confidence=1.0`. URL normalisation
(scheme, lowercase, trailing-slash strip) runs before writing. Scope is
`"registration"` (tied to the Finnish registration); `"entity_main"` is set
only after the cross-country entity zone runs and selects the canonical
flagship URL.

### `financials` (from `prh_xbrl`)

Tall format: one row per metric per statement. Populated from `facts.parquet`
filtered to the curated metric map (concept + MCY member):

| `metric_code` | `concept_qname` | `mcy_member_code` |
|---|---|---|
| `revenue` | `fi_met:md103` | `fi_MC:x673` |
| `operating_profit_loss` | `fi_met:md103` | `fi_MC:x689` |
| `profit_loss` | `fi_met:md103` | `fi_MC:x740` |
| `total_assets` | `fi_met:mi53` | `fi_MC:x360` |
| `equity` | `fi_met:mi53` | `fi_MC:x376` |
| `liabilities` | `fi_met:mi53` | `fi_MC:x424` |
| `cash_and_bank` | `fi_met:mi53` | `fi_MC:x399` |
| `current_liabilities` | `fi_met:mi53` | `fi_MC:x1811` |
| `personnel_expenses` | `fi_met:md103` | `fi_MC:x5` |
| `wages_and_salaries` | `fi_met:md103` | `fi_MC:x6` |

`period_reference` comes from `fi_dim:REF`: absent → `"current"`;
`fi_RF:x4` → `"prior_balance"`; `fi_RF:x53` → `"prior_period"`.
`source_metric_id = concept_qname + "/" + mcy_member_code` (e.g.
`"fi_met:md103/fi_MC:x673"`). `mapping_version` is a bumped string whenever
the metric map changes so the derived table can be rebuilt from
`facts.parquet` without re-downloading XML.

---

## 6. Validation before writing

Every canonical DataFrame is checked against the Phase 6 schemas before writing
Parquet:

```python
# conceptual — actual API in Phase 6 schemas.py / validate.py
from conformance.validate import validate_canonical

errors = validate_canonical("registrations", registrations_df)
assert not errors, errors

errors = validate_canonical("company", company_df)
assert not errors, errors

errors = validate_canonical("company_websites", websites_df)
assert not errors, errors

errors = validate_canonical("financials", financials_df)
assert not errors, errors
```

Checks enforced:
- Required columns present and non-null where the schema marks them mandatory.
- `country == "FI"` on all rows in the Finland partition.
- `is_active` values are `0` or `1` (no boolean drift).
- `metric_code` values are in the declared vocabulary.
- `registration_uid` format matches `"FI:<digits>"`.
- No duplicate `(country, registration_number)` in `registrations`.
- No duplicate `(company_uid, statement_id, metric_code, period_reference)` in
  `financials`.

Validation failures abort the write; they are surfaced as Dagster asset check
failures when the pipeline runs in the asset graph.
