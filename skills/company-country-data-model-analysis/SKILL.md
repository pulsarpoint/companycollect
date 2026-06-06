---
name: company-country-data-model-analysis
description: Use when analyzing a completed country company open-data investigation to document source-specific company fields, field meanings, per-source JSON catalogs, and a country-specific combined company data object.
---

# Company Country Data Model Analysis

## Purpose

Use this skill after `company-open-data-discovery` has produced a country folder
under:

```text
/Users/graovic/pulsarpoint/ppoint/companycollect/companies/{country_slug}
```

The goal is to analyze what company information can actually be obtained for
that country, source by source, then propose a country-specific combined JSON
object that represents the richest practical company profile for that country.

Do not force countries into one shared global schema. Country registers differ
too much in identifiers, legal concepts, filings, ownership, financial data,
addresses, language, and source access. Preserve the country-specific model
first. Add common-field mapping only as a later suggestion layer.

## When To Use

Use this skill when the task asks for any of the following:

- Analyze fields available from known country company data sources.
- Explain what each field/value from a company source means.
- Produce one `source_field_catalog.json` per source.
- Design a country-specific JSON object for company data.
- Combine multiple sources for one country into a proposed company profile.
- Document join keys, source precedence, freshness, missing fields, and license
  constraints for country company data.

Do not use this skill to discover sources from scratch. Use
`company-open-data-discovery` first when the country folder or source inventory
does not exist.

## Required Input

The user should provide at least one of:

```text
country_slug: serbia
country_folder: /Users/graovic/pulsarpoint/ppoint/companycollect/companies/serbia
```

If only `country_slug` is provided, resolve it under:

```text
/Users/graovic/pulsarpoint/ppoint/companycollect/companies/{country_slug}
```

Required files or directories:

```text
source_inventory.json
schema_notes.md
license_notes.md
raw/
normalized/
```

Useful optional files:

```text
source_inventory.md
investigation.md
search_attempts.md
raw/api/*
raw/bulk/*
raw/samples/*
normalized/companies.sample.jsonl
normalized/companies.sample.csv
```

If required files are missing, report exactly what is missing and stop. Do not
invent source fields.

## Output Directory Structure

Create all output under the country folder:

```text
analysis/
  company_data_analysis.md
  sources/
    {source_slug}/
      source_field_catalog.json
      source_field_catalog.md
      sample_record.json
  country_company_profile.schema.json
  country_company_profile.example.json
  country_company_profile_mapping.md
  common_field_mapping_suggestions.md
```

Each relevant source gets its own folder under `analysis/sources/{source_slug}`.
Use a safe ASCII slug derived from the source name, for example:

```text
apr_companies
apr_financial_statements
apr_ngo
offeneregister_companies_jsonl
```

## Source Selection

Read `source_inventory.json` first. Analyze sources with statuses such as:

```text
recommended
useful_secondary_source
sample_only
blocked_by_license_uncertainty
blocked_by_payment
blocked_by_authentication
```

Include blocked or restricted sources only when they document important values
that are unavailable from open sources. Keep them clearly marked as restricted
or planning-only.

Skip sources classified as:

```text
not_company_data
not_relevant
unavailable
```

unless the user explicitly asks to include them.

## Data Inspection Rules

For each selected source:

1. Read the source entry in `source_inventory.json`.
2. Read relevant sections in `schema_notes.md`, `license_notes.md`, and
   `investigation.md` if present.
3. Inspect downloaded files listed in `downloaded_files`.
4. Prefer small files, metadata files, headers, existing samples, and bounded
   samples.
5. If a raw file is large, inspect only a safe sample. Do not parse an entire
   huge file just to infer fields.
6. Preserve original field paths and original language.
7. Use translated names only as metadata.

Safe sampling examples:

```bash
head -n 5 raw/samples/example.jsonl
python3 -m json.tool raw/api/example_sample.json | sed -n '1,160p'
bzcat raw/bulk/example.jsonl.bz2 | head -n 3
```

Do not run unbounded reads over large compressed files.

## Per-Source Field Catalog JSON

For every selected source, write:

```text
analysis/sources/{source_slug}/source_field_catalog.json
```

Top-level object shape:

```json
{
  "country": "Serbia",
  "country_slug": "serbia",
  "source_slug": "apr_companies",
  "source_name": "APR Registar privrednih drustava",
  "source_type": "official_registry",
  "organization": "Agencija za privredne registre",
  "source_url": "https://openapi.apr.gov.rs/api/opendata/companies",
  "license": "public_domain",
  "access": "public",
  "data_freshness": "monthly",
  "record_shape": "envelope_with_records_keyed_by_maticni_broj",
  "primary_keys": ["maticni_broj"],
  "join_keys": ["maticni_broj"],
  "field_count": 1,
  "fields": [
    {
      "path": "Podaci.<maticni_broj>.PoslovnoIme",
      "source_field": "PoslovnoIme",
      "english_name": "business_name",
      "meaning": "Full registered business name of the company.",
      "data_type": "string",
      "semantic_type": "legal_name",
      "example_values": [
        "GRAFICKO PREDUZECE GRAFOPRINT D O O GORNJI MILANOVAC"
      ],
      "nullable": false,
      "repeatable": false,
      "code_list": null,
      "language": "Serbian Latin",
      "limitations": "Does not include historical names.",
      "source_confidence": "high",
      "analysis_notes": "Use as the main display name unless a newer source overrides it."
    }
  ]
}
```

Field entry requirements:

- `path`: full path in the original record shape.
- `source_field`: exact original field name.
- `english_name`: concise English helper name.
- `meaning`: what the value means in company-data terms.
- `data_type`: observed type such as `string`, `integer`, `decimal`,
  `boolean`, `date`, `datetime`, `array`, `object`, or `unknown`.
- `semantic_type`: one practical category from the list below, or
  `raw_extension` when no category fits.
- `example_values`: observed non-secret examples, preferably from raw samples.
- `nullable`: whether missing/null values are observed or documented.
- `repeatable`: true for arrays or repeated concepts.
- `code_list`: code list name or URL if the field is coded; otherwise `null`.
- `language`: source language/script when relevant.
- `limitations`: missing context, stale data, partial coverage, or ambiguity.
- `source_confidence`: `high`, `medium`, or `low`.
- `analysis_notes`: ingestion or interpretation notes.

Use these `semantic_type` values by default:

```text
identifier
legal_name
status
legal_form
activity
address
geography
date
financial
employment
ownership
person
relationship
document
filing
license_or_terms
metadata
raw_extension
```

Add a local semantic type only when a country has a real concept that does not
fit the list. Explain it in `source_field_catalog.md`.

## Per-Source Markdown Catalog

For every selected source, write:

```text
analysis/sources/{source_slug}/source_field_catalog.md
```

Use this structure:

```markdown
# {Source Name} Field Catalog

## Source Summary

- Country:
- Source type:
- Organization:
- URL:
- License:
- Access:
- Freshness:
- Record shape:
- Primary keys:
- Join keys:

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|

## Interpretation Notes

Explain source-specific concepts, language/script issues, code lists, field
limitations, and any uncertainty.
```

## Sample Record

When safe, save one representative raw or lightly shortened record:

```text
analysis/sources/{source_slug}/sample_record.json
```

Rules:

- Preserve original field names.
- Keep the example small enough to review.
- Do not include secrets or credential-protected data.
- If the real record is huge, keep only one company/entity and note truncation in
  `source_field_catalog.md`.

## Country-Specific Combined Profile

Write:

```text
analysis/country_company_profile.schema.json
analysis/country_company_profile.example.json
```

The combined profile is country-specific. It should represent what can be built
for that country from the analyzed sources.

Rules:

- Use country-specific identifiers and names where helpful.
- Group fields by real country/source concepts.
- Preserve source provenance on each major section.
- Use arrays for repeatable values such as officers, owners, filings, documents,
  activities, previous names, and yearly financials.
- Do not add fake universal fields only because they exist in another country.
- Keep important but unavailable concepts in mapping notes instead of inventing
  empty fields.
- Include restricted/paid/license-uncertain data only as clearly marked
  planning-only sections unless the user explicitly asks otherwise.

Example shape only:

```json
{
  "country": "Serbia",
  "country_slug": "serbia",
  "country_profile_version": "2026-06-06",
  "registration": {
    "maticni_broj": "string",
    "source": "apr_companies"
  },
  "legal_identity": {},
  "status": {},
  "activity": {},
  "registered_location": {},
  "financial_statements": [],
  "documents": [],
  "source_provenance": []
}
```

Adapt the actual object to the country. Germany, Serbia, France, Norway, and the
United States should not be expected to share the same top-level sections.

## Mapping Report

Write:

```text
analysis/country_company_profile_mapping.md
```

Include:

- Combined profile section or field.
- Source field path.
- Source slug.
- Join key.
- Freshness.
- License/access status.
- Conflict or precedence rule.
- Missing-data notes.

Use a table where possible:

```markdown
| Profile path | Source | Source path | Join key | Precedence | Notes |
|---|---|---|---|---|---|
```

Explain source precedence explicitly. Prefer official current sources over
aggregators, stale mirrors, or restricted planning-only sources.

## Common Mapping Suggestions

Write:

```text
analysis/common_field_mapping_suggestions.md
```

This file is only a suggestion for a future cross-country mapper. It must state
that it does not constrain the country-specific profile.

Suggest mappings when possible for:

```text
company_id
registration_number
tax_id
vat_id
legal_name
status
legal_form
incorporation_date
dissolution_date
registered_address
activity_code
financials
officers
owners
source_provenance
```

Use `not_available_in_open_sources` when a common concept is not present in the
country's public/open data.

## Executive Analysis

Write:

```text
analysis/company_data_analysis.md
```

Use this structure:

```markdown
# Company Data Analysis For {Country}

## Summary

Explain what company profile can be built for this country.

## Sources Analyzed

Table of source slug, source name, status, access, license, and role.

## What Each Source Contributes

Short source-by-source explanation.

## Proposed Country Company Profile

Explain the country-specific combined JSON object.

## Join And Precedence Rules

Explain identifiers, joins, conflicts, and freshness.

## Missing Or Restricted Data

List important fields unavailable from open/public sources and fields available
only from paid/restricted/uncertain sources.

## Common Mapper Notes

Summarize later cross-country mapping opportunities.
```

## Safety And Accuracy Rules

- Do not invent fields that were not observed or documented.
- Do not claim a field meaning with high confidence when it is inferred only
  from the field name.
- Do not remove original source names, field names, paths, language, or code
  values.
- Do not treat public access as permission for redistribution.
- Do not bypass authentication, payment, CAPTCHA, robots restrictions, or access
  controls.
- Do not parse huge raw files without bounded sampling.
- Mark license uncertainty and paid/restricted access clearly.
- Mark stale sources clearly.
- Keep per-source catalogs separate even when multiple sources share fields.
- Log important uncertainty in both JSON `limitations` and Markdown notes.

## Final Answer Format

When this skill is used, the final response to the user must include:

```markdown
## Summary

Short country-specific conclusion.

## Source catalogs created

List each `analysis/sources/{source_slug}/source_field_catalog.json`.

## Combined profile files

List:

- `analysis/country_company_profile.schema.json`
- `analysis/country_company_profile.example.json`
- `analysis/country_company_profile_mapping.md`
- `analysis/common_field_mapping_suggestions.md`

## Key modeling notes

Mention join keys, source precedence, and unavailable/restricted fields.

## Risks

Mention license uncertainty, stale data, unclear field meanings, or missing raw
samples.
```
