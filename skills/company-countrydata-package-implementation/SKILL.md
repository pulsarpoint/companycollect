---
name: company-countrydata-package-implementation
description: Use when creating a standalone Go countrydata package under companycollect/companies/{country_slug} from companies/analysis artifacts, including per-country go.mod, source packages, country CLI, source/final parquet exports, manifests, and runtime data layout.
---

# Company Countrydata Package Implementation

## Purpose

Use this skill to create or extend a complete standalone Go country package for
company data collection and export.

The country package lives under:

```text
companycollect/companies/{country_slug}
```

The required input comes from discovery and data-model analysis artifacts under:

```text
companycollect/companies/analysis/{country_slug}
```

The package produces a country binary that downloads source data, writes source
parquet exports, writes final country parquet exports, and reports status
without importing Corpscout scheduler code.

## Required Detailed Guide

After loading this skill, read:

```text
skills/company-countrydata-package-implementation/references/implementation-guide.md
```

That guide contains the exact preflight gates, package layout, public API,
runtime data layout, CLI contract, source/final export rules, tests, verification
commands, and Finland execution lessons.

## Non-Negotiable Rules

- Verify upstream `company-open-data-discovery` and
  `company-country-data-model-analysis` outputs before planning or coding.
- If required artifacts are missing, stop and tell the user which upstream skill
  to run. Do not invent source fields, licenses, pagination, mapping rules, or
  sample records.
- Each country has its own Go module: `companies/{country_slug}/go.mod`.
- Do not create `companies/go.mod`; it would include investigation scripts under
  `companies/analysis/*/scripts`.
- Shared source-agnostic helpers belong in `companies/common`.
- Country packages must not import `corpscout`, `scheduler`, sqlc, or DB types.
- Runtime data must live under `companies/data/{country_slug}/countrydata`, not
  inside `companies/{country_slug}`.
- Generated runtime files and country binaries must be ignored by git.

## Reference Implementation

Use Finland as the structural example:

```text
companies/common/countryimport/
companies/finland/
companies/finland/prhytj/
companies/finland/cmd/finland-countrydata/
companies/data/finland/countrydata/
```

Copy structure and behavior, not Finland-specific field names or PRH-specific
pagination assumptions.

## Quick Verification

Run after implementation:

```sh
cd companycollect/companies/common
GOWORK=off go test ./... -count=1

cd companycollect/companies/{country_slug}
GOWORK=off go test ./... -count=1
GOWORK=off go build -o ./bin/{country_slug}-countrydata ./cmd/{country_slug}-countrydata
rm -f ./bin/{country_slug}-countrydata
rmdir ./bin 2>/dev/null || true
GOWORK=off go run ./cmd/{country_slug}-countrydata status-source --source {source_package}
```

Before committing, confirm generated runtime data is ignored and not staged:

```sh
git status --short
git diff --check
```
