# Corpscout Resolved Entity Tables Design

## Goal

Restructure Corpscout's central identity model so it stores only resolved, trusted entities of the three types we care about:

- companies
- organizations
- open-source projects

Corpscout should not promote every imported vendor string, CPE token, GitHub name, or external catalog vendor into a resolved entity. Uncertain inputs should stay in source-specific candidate tables until they are confidently resolved or manually reviewed.

Corpscout will own the resolved identity data and approved CPE/CVE-to-entity mappings. External consumer workflows are out of scope for this phase. A later phase will expose a CPE-based lookup API that consumers can use to pull resolved vendor/entity information.

## Current Context

Corpscout currently has a `companies` table and company-specific child tables:

- `company_locations`
- `company_phones`
- `company_emails`
- `company_industries`
- `company_markets`
- `company_services`
- `company_domains`
- `company_aliases`
- `company_sources`
- `company_relationships`

The current schema is company-centric. That works for registry and GLEIF data, but it is not enough for catalog identity resolution from security/product sources. CPE vendor tokens can represent companies, foundations, open-source projects, brands, product families, or ambiguous labels.

The new model must support precise storage without creating fake companies.

## Design Decision

Do not add a generic `entries` parent table.

Use three resolved root tables instead:

- `companies`
- `organizations`
- `open_source_projects`

Each root type owns its own child tables. There will not be shared generic child tables for contacts, links, locations, forums, CPE tokens, or sources. The schema should stay explicit because each entity type has different profile data and different enrichment rules.

When consumers need a unified surface, Corpscout should provide a resolver API or SQL view over the three tables. The database does not need a shared root row just to make polymorphism convenient.

## Entity Types

### Company

A company is a commercial or legal business entity.

Examples:

- F5
- Microsoft
- NOVELIC
- Cloudflare

Companies can have legal identifiers, registration numbers, LEI values, ownership data, headquarters, financial estimates, markets, industries, and commercial services.

### Organization

An organization is a non-company institution, foundation, standards body, nonprofit, university, government agency, or similar entity.

Examples:

- Apache Software Foundation
- CNCF
- OWASP
- IETF
- MIT

Organizations can have governance data, foundation/nonprofit metadata, official sites, project relationships, forums, contacts, and maintained CPE vendor tokens.

### Open-Source Project

An open-source project is a project/community/repository/product-like OSS identity that is not itself the legal owner.

Examples:

- Nmap
- OpenSSL
- Kubernetes
- Apache HTTP Server

Open-source projects can have repositories, package names, maintainers, licenses, documentation, security contacts, forums, release metadata, and related organizations or companies.

## Non-Goals

- Do not create a universal identity graph in this phase.
- Do not model products in Corpscout.
- Do not import every CPE vendor token as a resolved entity.
- Do not store product names as companies.
- Do not add a generic `entries` table unless a future phase introduces shared child tables or strong cross-type FK requirements.
- Do not force ambiguous inputs into one of the three resolved tables.
- Do not design or implement a separate field-update review queue in this phase.
- Do not design or implement external consumer integration flows in this phase.

## Resolved Tables

### `companies`

Keep the current `companies` table as the root table for company entities.

Existing company enrichment columns and child tables remain company-specific. Future company-specific additions should continue to use `company_*` tables.

Add these company columns before creating the unified read view:

```sql
ALTER TABLE companies
    ADD COLUMN IF NOT EXISTS canonical_slug TEXT,
    ADD COLUMN IF NOT EXISTS display_name TEXT,
    ADD COLUMN IF NOT EXISTS resolution_status TEXT NOT NULL DEFAULT 'resolved',
    ADD COLUMN IF NOT EXISTS evidence JSONB NOT NULL DEFAULT '{}'::jsonb;
```

`name` can remain the legal or registry name where existing imports depend on it. `display_name` can become the product-facing name used by consumers.

For existing company rows, add `canonical_slug` as nullable first, backfill it with the rules below, resolve collisions, then enforce `NOT NULL` and uniqueness. New company writes must provide `canonical_slug`.

After backfill:

```sql
ALTER TABLE companies
    ALTER COLUMN canonical_slug SET NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS uq_companies_canonical_slug
    ON companies(canonical_slug);
```

### Canonical Slug Rules

`canonical_slug` is a stable handle for URLs, search, and operator-facing references. It is unique inside each resolved root table, not globally unique across all entity types. Consumers must treat `(entity_type, canonical_slug)` as the public lookup key when using the unified resolver surface.

Generate slugs with this algorithm:

1. Choose the base label from `display_name`; for companies, fall back to `name` when `display_name` is empty.
2. Trim whitespace, normalize Unicode with NFKD, transliterate when supported, drop remaining non-ASCII characters, lowercase the result, and replace `&` with `and`.
3. Replace every run of non-alphanumeric characters with one hyphen.
4. Collapse repeated hyphens and trim leading or trailing hyphens.
5. If the slug is empty, use `{entity_type}-{uuid8}`.
6. If the slug collides inside the same root table, append `-{uuid8}` from the entity ID instead of using an order-dependent numeric suffix.

Do not automatically rewrite `canonical_slug` when `display_name` changes. Slug changes should be explicit administrative operations so external references remain stable. Resolver matching must not treat slug equality alone as strong identity evidence.

### `company_relationships`

Add a company-specific relationship table for parent, ownership, acquisition, and group relationships between resolved companies.

This is not a generic entity relationship table. It only models company-to-company relationships where both sides are resolved company rows. Organization and open-source project relationships should use their own explicit tables later if needed.

```sql
CREATE TABLE company_relationships (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    subject_company_id UUID NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
    related_company_id UUID NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
    relationship_type TEXT NOT NULL,
    ownership_percentage NUMERIC(5,2),
    valid_from DATE,
    valid_to DATE,
    source TEXT NOT NULL DEFAULT 'manual',
    confidence REAL,
    evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
    status TEXT NOT NULL DEFAULT 'active',
    removed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT chk_company_relationships_type CHECK (
        relationship_type IN (
            'direct_parent',
            'ultimate_parent',
            'subsidiary_of',
            'owned_by',
            'acquired_by',
            'merged_into',
            'same_group',
            'other'
        )
    ),
    CONSTRAINT chk_company_relationships_distinct CHECK (
        subject_company_id <> related_company_id
    ),
    CONSTRAINT chk_company_relationships_ownership_percentage CHECK (
        ownership_percentage IS NULL OR ownership_percentage BETWEEN 0 AND 100
    ),
    CONSTRAINT chk_company_relationships_confidence CHECK (
        confidence IS NULL OR confidence BETWEEN 0 AND 1
    ),
    CONSTRAINT chk_company_relationships_status CHECK (
        status IN ('active', 'needs_review', 'rejected', 'superseded')
    ),
    CONSTRAINT chk_company_relationships_evidence_object CHECK (
        jsonb_typeof(evidence) = 'object'
    ),
    CONSTRAINT chk_company_relationships_valid_range CHECK (
        valid_to IS NULL OR valid_from IS NULL OR valid_to >= valid_from
    )
);

CREATE UNIQUE INDEX uq_company_relationships_current
    ON company_relationships(subject_company_id, related_company_id, relationship_type)
    WHERE removed_at IS NULL
      AND status IN ('active', 'needs_review');

CREATE INDEX idx_company_relationships_subject
    ON company_relationships(subject_company_id, status, relationship_type)
    WHERE removed_at IS NULL;

CREATE INDEX idx_company_relationships_related
    ON company_relationships(related_company_id, status, relationship_type)
    WHERE removed_at IS NULL;
```

Direction rule:

- `subject_company_id` is the company being described.
- `related_company_id` is the parent, owner, acquirer, merged target, or group company.

Relationship type rule:

- `direct_parent` and `ultimate_parent` are registry-style parent-chain relationships, especially for GLEIF or corporate registry enrichment.
- `subsidiary_of` and `owned_by` are evidence-backed business relationships when the exact registry parent chain is not available.
- Only one current relationship of the same type between the same companies is allowed. Rejected, superseded, or removed rows can remain as history without blocking a corrected active row.

Source and precedence rule:

- `source` must identify the origin of the relationship, such as `gleif`, `company_registry`, `manual_review`, `ai_research`, or a named importer.
- `direct_parent` is authoritative over `subsidiary_of` for parent-chain consumers when both rows exist for the same subject and related company.
- `subsidiary_of` can coexist with `direct_parent` as business-research evidence, but operational parent lookup should prefer `direct_parent`.
- `ultimate_parent` rows are denormalized cache rows derived from the `direct_parent` chain. They exist to make queries fast, not as independently asserted facts. When the direct-parent chain changes, matching `ultimate_parent` rows must be rebuilt or superseded.

Examples:

```text
NOVELIC subsidiary_of Sona BLW Precision Forgings Ltd.
subject_company_id = NOVELIC
related_company_id = Sona BLW Precision Forgings Ltd.
relationship_type = subsidiary_of
```

```text
NOVELIC direct_parent Sona BLW Precision Forgings Ltd.
subject_company_id = NOVELIC
related_company_id = Sona BLW Precision Forgings Ltd.
relationship_type = direct_parent
```

`companies.parent_lei` and `companies.ultimate_parent_lei` can remain as imported GLEIF fields for compatibility, but `company_relationships` should become the reviewable relationship surface. When the parent company is resolved locally, GLEIF enrichment should upsert a relationship row in addition to storing the LEI values.

### `organizations`

Create a separate root table for resolved non-company organizations.

```sql
CREATE TABLE organizations (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    canonical_slug TEXT NOT NULL UNIQUE,
    display_name TEXT NOT NULL,
    organization_type TEXT NOT NULL,
    website TEXT,
    short_description TEXT,
    description TEXT,
    country_code TEXT,
    governance JSONB NOT NULL DEFAULT '{}'::jsonb,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
    status TEXT NOT NULL DEFAULT 'active',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT chk_organizations_type CHECK (
        organization_type IN ('foundation', 'standards_body', 'nonprofit', 'government', 'university', 'community', 'other')
    ),
    CONSTRAINT chk_organizations_status CHECK (
        status IN ('active', 'inactive', 'unknown')
    ),
    CONSTRAINT chk_organizations_metadata_object CHECK (jsonb_typeof(metadata) = 'object'),
    CONSTRAINT chk_organizations_governance_object CHECK (jsonb_typeof(governance) = 'object'),
    CONSTRAINT chk_organizations_evidence_object CHECK (jsonb_typeof(evidence) = 'object')
);
```

Organization child tables should be explicit:

- `organization_websites`
- `organization_contacts`
- `organization_locations`
- `organization_social_links`
- `organization_forums`
- `organization_sources`
- `organization_projects`

### `open_source_projects`

Create a separate root table for resolved open-source projects.

```sql
CREATE TABLE open_source_projects (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    canonical_slug TEXT NOT NULL UNIQUE,
    display_name TEXT NOT NULL,
    website TEXT,
    repository_url TEXT,
    license TEXT,
    short_description TEXT,
    description TEXT,
    lifecycle_status TEXT NOT NULL DEFAULT 'active',
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
    status TEXT NOT NULL DEFAULT 'active',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT chk_open_source_projects_lifecycle_status CHECK (
        lifecycle_status IN ('active', 'maintenance', 'deprecated', 'unknown')
    ),
    CONSTRAINT chk_open_source_projects_status CHECK (
        status IN ('active', 'inactive', 'unknown')
    ),
    CONSTRAINT chk_open_source_projects_metadata_object CHECK (jsonb_typeof(metadata) = 'object'),
    CONSTRAINT chk_open_source_projects_evidence_object CHECK (jsonb_typeof(evidence) = 'object')
);
```

Open-source project child tables should be explicit:

- `open_source_project_repositories`
- `open_source_project_package_names`
- `open_source_project_maintainers`
- `open_source_project_security_contacts`
- `open_source_project_forums`
- `open_source_project_social_links`
- `open_source_project_sources`
- `open_source_project_related_organizations`

## CPE And CVE Entity Links

CPE vendor tokens and CVE IDs are associations, not aliases.

For example, `nginx` should not become a company alias for F5. If evidence shows the CPE vendor token belongs under F5 for catalog purposes, store it as a CPE token associated with F5.

Use a two-step model:

- suggestion tables store review workflow data, evidence, confidence, proposed target, and approval state
- approved link tables store only the final mapping and a reference back to the approved suggestion

Do not store `source`, `evidence`, `confidence`, reviewer notes, or proposed entity creation payloads on approved links. Those belong on suggestions. Approved links should stay small because they are operational lookup tables.

These link tables are the narrow exception to the "no shared child tables" rule. They are not profile child tables; they are resolver mapping tables from external security identifiers to one resolved entity type.

### CPE Entity Link Suggestions

`cpe_entity_link_suggestions` stores candidate mappings from a CPE vendor token to either an existing resolved entity or a proposed entity that should be created first.

```sql
CREATE TABLE cpe_entity_link_suggestions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    cpe_vendor_token TEXT NOT NULL,
    target_entity_type TEXT NOT NULL,
    target_company_id UUID REFERENCES companies(id) ON DELETE SET NULL,
    target_organization_id UUID REFERENCES organizations(id) ON DELETE SET NULL,
    target_open_source_project_id UUID REFERENCES open_source_projects(id) ON DELETE SET NULL,
    proposed_entity_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    suggested_by TEXT NOT NULL,
    confidence REAL,
    evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
    status TEXT NOT NULL DEFAULT 'pending',
    reviewed_by TEXT,
    reviewed_at TIMESTAMPTZ,
    review_note TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT chk_cpe_entity_link_suggestions_token CHECK (btrim(cpe_vendor_token) <> ''),
    CONSTRAINT chk_cpe_entity_link_suggestions_target_type CHECK (
        target_entity_type IN ('company', 'organization', 'open_source_project')
    ),
    CONSTRAINT chk_cpe_entity_link_suggestions_target_matches_type CHECK (
        (
            target_entity_type = 'company'
            AND target_organization_id IS NULL
            AND target_open_source_project_id IS NULL
        )
        OR (
            target_entity_type = 'organization'
            AND target_company_id IS NULL
            AND target_open_source_project_id IS NULL
        )
        OR (
            target_entity_type = 'open_source_project'
            AND target_company_id IS NULL
            AND target_organization_id IS NULL
        )
    ),
    CONSTRAINT chk_cpe_entity_link_suggestions_target_or_payload CHECK (
        (
            num_nonnulls(target_company_id, target_organization_id, target_open_source_project_id) = 1
            AND proposed_entity_payload = '{}'::jsonb
        )
        OR (
            num_nonnulls(target_company_id, target_organization_id, target_open_source_project_id) = 0
            AND proposed_entity_payload <> '{}'::jsonb
        )
    ),
    CONSTRAINT chk_cpe_entity_link_suggestions_status CHECK (
        status IN ('pending', 'approved', 'rejected', 'superseded')
    ),
    CONSTRAINT chk_cpe_entity_link_suggestions_confidence CHECK (
        confidence IS NULL OR confidence BETWEEN 0 AND 1
    ),
    CONSTRAINT chk_cpe_entity_link_suggestions_evidence_object CHECK (jsonb_typeof(evidence) = 'object'),
    CONSTRAINT chk_cpe_entity_link_suggestions_payload_object CHECK (jsonb_typeof(proposed_entity_payload) = 'object')
);

CREATE INDEX idx_cpe_entity_link_suggestions_review
    ON cpe_entity_link_suggestions(status, cpe_vendor_token, target_entity_type);
```

If the target entity does not exist, `proposed_entity_payload` should contain the proposed `display_name`, `canonical_slug` input, `website` when known, and enough evidence for a reviewer to decide whether to create a `company`, `organization`, or `open_source_project`.

When a suggestion is approved for a missing entity, the approval transaction should create the resolved entity first, update the suggestion with the new target ID, then insert the approved link.

### CPE Entity Links

`cpe_entity_links` stores approved CPE vendor-token mappings only. There should be one active approved link per CPE vendor token. Ambiguous tokens should stay as pending or rejected suggestions until one target is chosen.

```sql
CREATE TABLE cpe_entity_links (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    cpe_vendor_token TEXT NOT NULL,
    entity_type TEXT NOT NULL,
    company_id UUID REFERENCES companies(id) ON DELETE CASCADE,
    organization_id UUID REFERENCES organizations(id) ON DELETE CASCADE,
    open_source_project_id UUID REFERENCES open_source_projects(id) ON DELETE CASCADE,
    approved_suggestion_id UUID NOT NULL REFERENCES cpe_entity_link_suggestions(id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    removed_at TIMESTAMPTZ,
    CONSTRAINT chk_cpe_entity_links_token CHECK (btrim(cpe_vendor_token) <> ''),
    CONSTRAINT chk_cpe_entity_links_entity_type CHECK (
        entity_type IN ('company', 'organization', 'open_source_project')
    ),
    CONSTRAINT chk_cpe_entity_links_one_target CHECK (
        num_nonnulls(company_id, organization_id, open_source_project_id) = 1
    ),
    CONSTRAINT chk_cpe_entity_links_type_matches_target CHECK (
        (entity_type = 'company' AND company_id IS NOT NULL)
        OR (entity_type = 'organization' AND organization_id IS NOT NULL)
        OR (entity_type = 'open_source_project' AND open_source_project_id IS NOT NULL)
    )
);

CREATE UNIQUE INDEX uq_cpe_entity_links_active_token
    ON cpe_entity_links(cpe_vendor_token)
    WHERE removed_at IS NULL;

CREATE UNIQUE INDEX uq_cpe_entity_links_active_suggestion
    ON cpe_entity_links(approved_suggestion_id)
    WHERE removed_at IS NULL;
```

The service layer must verify that `approved_suggestion_id` points to a suggestion with `status = 'approved'` and that the approved link target matches the suggestion target. A database trigger can enforce that later, but the first migration can keep this in service code.

### CVE Entity Link Suggestions

Use the same suggestion pattern for CVE-to-entity mappings. Corpscout should store CVE entity relevance only; product-level CVE mappings are outside Corpscout.

```sql
CREATE TABLE cve_entity_link_suggestions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    cve_id TEXT NOT NULL,
    target_entity_type TEXT NOT NULL,
    target_company_id UUID REFERENCES companies(id) ON DELETE SET NULL,
    target_organization_id UUID REFERENCES organizations(id) ON DELETE SET NULL,
    target_open_source_project_id UUID REFERENCES open_source_projects(id) ON DELETE SET NULL,
    proposed_entity_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    suggested_by TEXT NOT NULL,
    confidence REAL,
    evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
    status TEXT NOT NULL DEFAULT 'pending',
    reviewed_by TEXT,
    reviewed_at TIMESTAMPTZ,
    review_note TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT chk_cve_entity_link_suggestions_cve_id CHECK (
        cve_id ~ '^CVE-[0-9]{4}-[0-9]{4,}$'
    ),
    CONSTRAINT chk_cve_entity_link_suggestions_target_type CHECK (
        target_entity_type IN ('company', 'organization', 'open_source_project')
    ),
    CONSTRAINT chk_cve_entity_link_suggestions_target_matches_type CHECK (
        (
            target_entity_type = 'company'
            AND target_organization_id IS NULL
            AND target_open_source_project_id IS NULL
        )
        OR (
            target_entity_type = 'organization'
            AND target_company_id IS NULL
            AND target_open_source_project_id IS NULL
        )
        OR (
            target_entity_type = 'open_source_project'
            AND target_company_id IS NULL
            AND target_organization_id IS NULL
        )
    ),
    CONSTRAINT chk_cve_entity_link_suggestions_target_or_payload CHECK (
        (
            num_nonnulls(target_company_id, target_organization_id, target_open_source_project_id) = 1
            AND proposed_entity_payload = '{}'::jsonb
        )
        OR (
            num_nonnulls(target_company_id, target_organization_id, target_open_source_project_id) = 0
            AND proposed_entity_payload <> '{}'::jsonb
        )
    ),
    CONSTRAINT chk_cve_entity_link_suggestions_status CHECK (
        status IN ('pending', 'approved', 'rejected', 'superseded')
    ),
    CONSTRAINT chk_cve_entity_link_suggestions_confidence CHECK (
        confidence IS NULL OR confidence BETWEEN 0 AND 1
    ),
    CONSTRAINT chk_cve_entity_link_suggestions_evidence_object CHECK (jsonb_typeof(evidence) = 'object'),
    CONSTRAINT chk_cve_entity_link_suggestions_payload_object CHECK (jsonb_typeof(proposed_entity_payload) = 'object')
);

CREATE INDEX idx_cve_entity_link_suggestions_review
    ON cve_entity_link_suggestions(status, cve_id, target_entity_type);
```

### CVE Entity Links

Unlike CPE vendor tokens, one CVE can be linked to multiple resolved entities. Enforce uniqueness only for the same CVE and same target entity.

```sql
CREATE TABLE cve_entity_links (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    cve_id TEXT NOT NULL,
    entity_type TEXT NOT NULL,
    company_id UUID REFERENCES companies(id) ON DELETE CASCADE,
    organization_id UUID REFERENCES organizations(id) ON DELETE CASCADE,
    open_source_project_id UUID REFERENCES open_source_projects(id) ON DELETE CASCADE,
    approved_suggestion_id UUID NOT NULL REFERENCES cve_entity_link_suggestions(id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    removed_at TIMESTAMPTZ,
    CONSTRAINT chk_cve_entity_links_cve_id CHECK (
        cve_id ~ '^CVE-[0-9]{4}-[0-9]{4,}$'
    ),
    CONSTRAINT chk_cve_entity_links_entity_type CHECK (
        entity_type IN ('company', 'organization', 'open_source_project')
    ),
    CONSTRAINT chk_cve_entity_links_one_target CHECK (
        num_nonnulls(company_id, organization_id, open_source_project_id) = 1
    ),
    CONSTRAINT chk_cve_entity_links_type_matches_target CHECK (
        (entity_type = 'company' AND company_id IS NOT NULL)
        OR (entity_type = 'organization' AND organization_id IS NOT NULL)
        OR (entity_type = 'open_source_project' AND open_source_project_id IS NOT NULL)
    )
);

CREATE UNIQUE INDEX uq_cve_entity_links_active_company
    ON cve_entity_links(cve_id, company_id)
    WHERE removed_at IS NULL AND company_id IS NOT NULL;

CREATE UNIQUE INDEX uq_cve_entity_links_active_organization
    ON cve_entity_links(cve_id, organization_id)
    WHERE removed_at IS NULL AND organization_id IS NOT NULL;

CREATE UNIQUE INDEX uq_cve_entity_links_active_open_source_project
    ON cve_entity_links(cve_id, open_source_project_id)
    WHERE removed_at IS NULL AND open_source_project_id IS NOT NULL;

CREATE UNIQUE INDEX uq_cve_entity_links_active_suggestion
    ON cve_entity_links(approved_suggestion_id)
    WHERE removed_at IS NULL;
```

The service layer must verify that `approved_suggestion_id` points to a suggestion with `status = 'approved'` and that the approved CVE link target matches the suggestion target.

Examples:

- `nginx` CPE vendor token can have a pending suggestion to link to company `F5`; after approval, `cpe_entity_links` stores `cpe_vendor_token = 'nginx'` and `company_id = F5`.
- `apache` CPE vendor token can link to organization `Apache Software Foundation`.
- `nmap` CPE vendor token can link to open-source project `Nmap`.
- `CVE-2024-0001` can link to both company `F5` and open-source project `NGINX` only when reviewers decide both entity-level links are useful. Product-level applicability remains outside Corpscout.

## Source-Specific Input And Suggestion Tables

Do not add a generic `identity_observations` table.

Corpscout receives inputs from many different sources, and each source carries different native identifiers, evidence, confidence semantics, and review needs. Candidate storage should therefore be source-specific and named by the source or identifier being processed.

Examples:

- `cpe_entity_link_suggestions`
- `cve_entity_link_suggestions`
- `domain_entity_link_suggestions`
- `website_entity_link_suggestions`
- `github_owner_entity_link_suggestions`
- `gleif_company_imports`
- `company_registry_imports`
- `ai_company_profile_suggestions`
- `manual_entity_suggestions`

CPE and CVE are only two source families. They are not the primary or universal candidate model.

Each source-specific candidate table should keep the columns that are natural for that source. Common lifecycle fields can be reused where useful:

- source-native identifier or payload
- proposed target entity type
- proposed target entity ID, when the entity already exists
- proposed entity payload, when the entity does not exist yet
- confidence, if the source produces one
- evidence or raw source payload
- status: `pending`, `approved`, `rejected`, `superseded`
- reviewer metadata when human review is involved

Avoid flattening every source into the same generic columns. For example, a GitHub owner candidate should preserve GitHub-specific fields, while a GLEIF import should preserve LEI and registry-specific fields.

Promotion rules:

- Promote to `companies`, `organizations`, or `open_source_projects` only when source evidence is strong enough or a human approves the resolution.
- Use `cpe_entity_link_suggestions` and `cve_entity_link_suggestions` for proposed CPE/CVE mappings to resolved entities.
- Keep ambiguous inputs in their source-specific candidate table.
- Rejected source-specific candidates remain as durable negative evidence in the source table that produced them.

## Source And Candidate Responsibilities

Keep `company_sources`; it is not replaced by source-specific candidate tables.

`company_sources` remains the company-specific audit/source table for accepted profile facts on resolved company rows. It should be used for sources that support company fields such as legal name, website, headquarters, market, services, ownership notes, and registry data.

Source-specific candidate tables are for raw inputs, candidate matches, ambiguous inputs, and negative evidence before or during resolution. When a candidate is promoted into a resolved company fact, keep the original source-specific row for traceability and copy or link the accepted evidence into the resolved entity's evidence fields or `company_sources`.

Organizations and open-source projects should get equivalent explicit source tables, such as `organization_sources` and `open_source_project_sources`, instead of sharing `company_sources`.

## Deferred Field Update Review Queue

This phase intentionally does not add a separate queue for proposed field updates to already-resolved entities.

Trusted imports and manual review can update resolved profile fields directly while recording source/evidence metadata in the appropriate resolved source table. Lower-confidence field changes, such as a newly discovered website or address for an existing company, should not be auto-applied unless the importer can meet the normal confidence rules. A future phase can introduce a dedicated field-update review workflow, but this design should not block the resolved entity table migration on that queue.

## Resolver API

Corpscout should expose a unified resolver API over the three precise root tables.

Example request:

```json
{
  "name": "Apache Software Foundation",
  "website": "https://www.apache.org",
  "cpe_vendor_tokens": ["apache", "apache_software_foundation"],
  "domains": ["apache.org"]
}
```

Example response:

```json
{
  "matched": true,
  "entity_type": "organization",
  "entity_id": "00000000-0000-0000-0000-000000000000",
  "display_name": "Apache Software Foundation",
  "confidence": 0.97,
  "resolution_reason": "matched official domain and CPE vendor tokens"
}
```

If no high-confidence match exists:

```json
{
  "matched": false,
  "status": "no_match"
}
```

The resolver should never create resolved rows from weak input automatically. Source-specific ingestion endpoints can create source-specific candidates when a workflow requires review.

## Unified Read View

Provide a read-only SQL view for browsing/searching resolved entities:

```sql
CREATE VIEW v_resolved_entities AS
SELECT
    'company'::text AS entity_type,
    id AS entity_id,
    COALESCE(display_name, name) AS display_name,
    canonical_slug,
    website,
    status,
    updated_at
FROM companies
UNION ALL
SELECT
    'organization',
    id,
    display_name,
    canonical_slug,
    website,
    status,
    updated_at
FROM organizations
UNION ALL
SELECT
    'open_source_project',
    id,
    display_name,
    canonical_slug,
    website,
    status,
    updated_at
FROM open_source_projects;
```

This gives internal UIs and services one search surface without requiring a root table.

## Phase 2 CPE Lookup API Boundary

External consumer integration is deferred to a second phase.

That phase should expose a read API that accepts a CPE vendor token and returns the approved resolved entity profile from Corpscout. The API should read from `cpe_entity_links` and the resolved root tables. It should not create suggestions, approve links, create products, or model consumer-side workflow.

Expected high-level behavior:

1. Caller submits a CPE vendor token.
2. Corpscout normalizes the token and looks up an active `cpe_entity_links` row.
3. If a link exists, Corpscout returns the resolved entity type, entity ID, display name, slug, website, short description, and relevant profile fields.
4. If no approved link exists, Corpscout returns no match.

Detailed API request/response contracts, authentication, pagination, and consumer-side storage are second-phase design work.

## CPE Import Flow

1. Import CPE vendor tokens and group them by normalized token and supporting context.
2. Try to match each token to existing companies, organizations, and open-source projects.
3. If the target entity exists, create a `cpe_entity_link_suggestions` row pointing to that entity.
4. If the target entity does not exist, create a `cpe_entity_link_suggestions` row with `proposed_entity_payload`.
5. Reviewers approve or reject the suggestion.
6. On approval, insert one active `cpe_entity_links` row. If the entity did not exist, create it before inserting the approved link.
7. On rejection, keep the suggestion as durable negative evidence.

Example:

```text
CPE token: nginx
Suggestion: cpe_entity_link_suggestions(cpe_vendor_token='nginx', target_company_id=F5)
Approved link: cpe_entity_links(cpe_vendor_token='nginx', company_id=F5)
```

```text
CPE token: apache
Suggestion: cpe_entity_link_suggestions(cpe_vendor_token='apache', target_organization_id=ASF)
Approved link: cpe_entity_links(cpe_vendor_token='apache', organization_id=ASF)
```

```text
CPE token: nmap
Suggestion: cpe_entity_link_suggestions(cpe_vendor_token='nmap', target_open_source_project_id=Nmap)
Approved link: cpe_entity_links(cpe_vendor_token='nmap', open_source_project_id=Nmap)
```

## CVE Import Flow

1. Import CVE IDs and supporting context from vulnerability feeds.
2. Match each CVE to known companies, organizations, or open-source projects only when there is entity-level relevance.
3. Create `cve_entity_link_suggestions` rows for candidate entity links.
4. Reviewers approve or reject the suggestions.
5. On approval, insert `cve_entity_links` rows. A single CVE may have multiple approved entity links.
6. Product-level CVE applicability remains outside Corpscout.

## Review Rules

Automatic suggestion creation requires strong evidence. Examples:

- official website/domain match
- registry or trusted source confirms identity
- CPE token appears consistently with known product ownership
- GitHub/repository/project site confirms the OSS project identity
- manual review confirms ambiguous cases

Do not create an approved link when:

- only a raw CPE vendor token is available
- the token is a product or brand name with unclear owner
- the same token appears in unrelated contexts
- the suggestion confidence is low
- the proposed entity type is uncertain

## Migration Strategy

1. Keep existing `companies` tables, including `company_sources`.
2. Add `canonical_slug`, `display_name`, `resolution_status`, and `evidence` to `companies`.
3. Backfill company slugs with the canonical slug algorithm, resolve collisions, then enforce `canonical_slug` uniqueness and `NOT NULL`.
4. Add `company_relationships`.
5. Add `organizations` and `open_source_projects`.
6. Add `cpe_entity_link_suggestions`, `cpe_entity_links`, `cve_entity_link_suggestions`, and `cve_entity_links`.
7. Add `v_resolved_entities`.
8. Add resolver queries and API endpoints.
9. Defer additional source-specific candidate tables until their importers are implemented.
10. Defer the external CPE lookup API and all consumer integration work to phase 2.

The first implementation should not remove existing company endpoints. It should add the new tables and resolver surface alongside them.

## Testing Plan

Database tests:

- migrations create all three resolved root tables
- no generic `entries` table is introduced
- `company_relationships` enforces resolved company parents on both sides
- company relationships reject self-references
- company relationships enforce current active/review uniqueness by subject, related company, and type
- company relationship consumers prefer `direct_parent` over `subsidiary_of` for parent-chain lookups
- `ultimate_parent` rows can be rebuilt or superseded when the direct-parent chain changes
- canonical slug generation is deterministic and handles collisions with the entity ID suffix
- existing `company_sources` data remains available after the migration
- CPE link suggestions require exactly one of: an existing target entity FK (with empty payload) or a non-empty proposed entity payload (with no target FK); both together are rejected
- CPE approved links enforce one active entity link per CPE vendor token
- CVE approved links allow multiple entity links per CVE but reject duplicate active links to the same target
- approved CPE/CVE links require exactly one target among company, organization, and open-source project
- source-specific candidate rows can exist without resolved entity IDs when they include a proposed entity payload
- `v_resolved_entities` returns companies, organizations, and open-source projects

Service tests:

- high-confidence inputs resolve to existing entities
- low-confidence identity inputs create source-specific candidates only when a matching source-specific ingestion table exists
- CPE/CVE candidates create pending suggestions instead of approved links
- approved CPE/CVE suggestions create approved links
- rejected CPE/CVE suggestions do not create links
- ambiguous CPE tokens stay unresolved until one suggestion is approved

## Implementation Defaults

- `companies.name` remains the registry/legal name for existing imports. `companies.display_name` is the consumer-facing label.
- GLEIF parent fields remain on `companies` as source fields, but resolved company-to-company ownership and parentage should be written to `company_relationships`.
- Parent-chain consumers should prefer `direct_parent` over `subsidiary_of`; `ultimate_parent` is a denormalized cache of the direct-parent chain.
- `company_sources` remains the source table for resolved company facts. Source-specific candidate tables do not replace it.
- Confidence lives on suggestion and link tables only, not on resolved root entities.
- A field-update review queue is deferred. Do not add it to the first migration plan.
- The first implementation adds CPE/CVE suggestion tables and compact approved link tables. Rich organization and project contact/forum tables can be phased in after resolver and mapping are stable.
- Automatic workflows can create CPE/CVE suggestions, but approved links should be created only by explicit review or a trusted approval path.
- The resolver does not write to a generic observation table. Source-specific ingestion endpoints own candidate creation for their source.
- External consumer integration and CPE lookup API contracts are phase 2 work.
