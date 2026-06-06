# Company Country Data Model Analysis Skill Design

## Goal

Create a new Codex skill that takes the output of `company-open-data-discovery`
for a single country and produces a detailed country-specific analysis of what
company data can be obtained from each public source.

The skill must not force each country into a shared global company schema. Each
country can have different registers, identifiers, legal concepts, financial
fields, ownership data, and source-specific constraints. A later common mapper
can be suggested, but the primary output is a per-country representation of what
is actually available.

## Input

The skill works on an existing country investigation folder, for example:

```text
/Users/graovic/pulsarpoint/ppoint/companycollect/companies/serbia
```

Required files:

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

If raw files are too large, the skill should inspect bounded samples, metadata
files, headers, schema notes, and existing normalized examples. It should not
perform unbounded parsing of large source files.

## Output Layout

For each country, the skill creates an `analysis/` folder:

```text
companycollect/companies/{country_slug}/analysis/
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

Each source gets its own field catalog. This keeps official registry fields,
financial statement fields, beneficial ownership fields, procurement fields, and
aggregator fields separate before combining them.

## Per-Source Field Catalog

Each `analysis/sources/{source_slug}/source_field_catalog.json` describes one
source and every meaningful company-related value that source can provide.

Required top-level shape:

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
  "fields": []
}
```

Required field entry shape:

```json
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
```

`semantic_type` should use practical categories, such as:

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

The catalog should preserve original field names and paths. Translated or
normalized names are additional metadata, not replacements.

## Country-Specific Combined Profile

The skill proposes a country-specific company JSON object in:

```text
country_company_profile.schema.json
country_company_profile.example.json
```

This object represents the richest company profile that can reasonably be built
for that country from the discovered public sources.

The object should:

- Use country-specific identifiers and terms where needed.
- Group fields by real source concepts, not by a forced global schema.
- Preserve source provenance for each major section.
- Keep unavailable-but-important concepts explicit in mapping notes, not as fake
  nullable fields unless they are useful for downstream planning.
- Include arrays for repeatable values such as officers, owners, filings,
  activities, previous names, documents, or yearly financials.
- Include `raw_sources` or equivalent references so the combined object remains
  auditable.

Example high-level shape:

```json
{
  "country": "Serbia",
  "country_profile_version": "2026-06-06",
  "registration": {},
  "status": {},
  "legal_form": {},
  "activity": {},
  "registered_location": {},
  "financial_statements": [],
  "related_entities": [],
  "documents": [],
  "source_provenance": []
}
```

This is only an example. The skill should adapt the actual structure to each
country and its sources.

## Mapping Report

`country_company_profile_mapping.md` explains how the country-specific combined
object is assembled:

- Which source populates each combined section.
- Which keys join sources together.
- Which source wins when fields conflict.
- How freshness differs between sources.
- Which fields are only available from restricted, paid, or uncertain-license
  sources.
- Which important company concepts are missing from open/public data.

## Common Mapping Suggestions

`common_field_mapping_suggestions.md` is a non-binding bridge toward a future
cross-country model. It should suggest how country-specific fields could map to
common ideas such as:

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

This file must clearly state that it is a suggestion layer. It must not constrain
the primary country-specific object.

## Workflow

1. Locate the country folder and read `source_inventory.json`.
2. Select sources with statuses such as `recommended`, `useful_secondary_source`,
   or source types that may contain useful company data.
3. For each source, inspect schema notes, raw samples, metadata, and bounded data
   samples.
4. Create one `source_field_catalog.json` and one human-readable
   `source_field_catalog.md` per source.
5. Save a representative `sample_record.json` per source when a safe sample is
   available.
6. Propose `country_company_profile.schema.json`.
7. Create `country_company_profile.example.json` using realistic but non-secret
   sample values from the available public data.
8. Write `country_company_profile_mapping.md`.
9. Write `common_field_mapping_suggestions.md`.
10. Write `company_data_analysis.md` as the executive analysis of what can be
    built for that country.

## Safety And Quality Rules

- Do not invent fields that were not observed or documented.
- Do not collapse source-specific meanings into generic names without preserving
  the original source field.
- Do not treat public access as permission for redistribution.
- Do not parse huge raw files without bounded sampling.
- Do not bypass authentication, payment, CAPTCHA, robots restrictions, or access
  controls.
- Mark uncertain field meanings with lower confidence and explain the reason.
- Keep restricted, paid, and license-uncertain sources separate from open/public-source
  combined-profile fields unless the user explicitly wants a planning-only model.

## Success Criteria

For a country such as Serbia, the skill should produce:

- Separate catalogs for APR companies, APR financial statements, APR NGOs, and
  any other relevant sources.
- A country-specific JSON profile that represents the full practical Serbia
  company data shape.
- Clear join logic, such as joining APR company and financial statement data on
  `maticni_broj`.
- Clear explanation of missing open fields, such as tax ID, VAT ID, street
  address, directors, shareholders, or beneficial ownership when not available
  in the open feed.
- A later-mapper suggestion file without forcing the country profile into that
  common model.
