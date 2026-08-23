# Sweden company addresses — per-source artifacts and a merged final

Owner decision 2026-08-23: addresses follow the `info` datatype's structure. The one structural
difference: a company can have SEVERAL addresses. Geocodes augment the address (stored natively,
like the Swedish description augments the English one). No LLM anywhere in this datatype.

## 1. Decision

Reuse the `se_company` machinery end to end: per-source artifact tables with the standard envelope,
a merged final with provenance + a correction ledger, `publish_with_stage(new_versions_only=True)`,
evidence-based change detection, the `execute` gate, `resolve_all`/`resolve_all_before`, a ledger
sensor and the weekly schedule. New code lives in `defs/se_company/` (`address_rules.py`,
`address.py`, artifact SELECTs in the existing per-source modules `scb.py` — a second asset — and a
new `bolagsverket.py`).

## 2. Sources (today)

Exactly two, both already flowing with full provenance through `se_company_addresses_current`
(one row per company per source; rename-swap snapshot of the append-only `se_company_addresses`):

| source | address_type | fields |
|---|---|---|
| `bolagsverket` | `postal` | care_of, street address (raw + normalized), postal code, city, country (SE-asserted only) |
| `scb` | `visiting_or_postal` | same shape (SCB does not distinguish visiting vs postal) |

Provenance per row: `source_run_id`, `source_record_id`, `source_payload_hash`, derived
`source_record_uid` (matches `company_source_records`). Future sources (CommonCrawl imprint
addresses, Lantmäteriet if Geotorget approval lands) become new artifact tables, nothing else changes.

## 3. Artifacts

`se_company_address_bolagsverket` (group `se_company_bolagsverket`, new module `bolagsverket.py`) and
`se_company_address_scb` (group `se_company_scb`, second asset in `scb.py`). Standard envelope —
`company_id String`, `source_record_uid String`, `observed_at DateTime64(3,'UTC')` = append time
(the register stamp is a bulk-load constant; the info pilot proved it cannot order versions),
`source_run_id String`, `evidence_hash FixedString(64) MATERIALIZED` over the payload — then payload:

    address_type LowCardinality(String), care_of Nullable(String),
    street_address Nullable(String), normalized_address Nullable(String),
    postal_code Nullable(String), city Nullable(String), country_code Nullable(String)

`ENGINE = ReplacingMergeTree(observed_at) ORDER BY (company_id, source_record_uid)`,
`CONSTRAINT has_company CHECK match(company_id, '^([0-9]{10}|[0-9]{12})$')`. Reads
`se_company_addresses_current` (deps: `sweden_company_addresses_clickhouse`); appends only
changed evidence via the anti-join.

## 4. Final: `se_company_address`

Several rows per company. `address_key FixedString(64)` = sha256 of
`(address_type, normalized address fields)` — deterministic, stable across sources that agree.

    company_id, address_key,
    address_type LowCardinality(String),
    care_of Nullable(String), street_address Nullable(String),
    normalized_address Nullable(String), postal_code Nullable(String),
    city Nullable(String), country_code Nullable(String),
    -- geocode augmentation (from the existing chain, stored at resolve time)
    address_id Nullable(FixedString(64)),          -- shared identity (se_addresses_current)
    latitude Nullable(Float64), longitude Nullable(Float64),
    geocode_status LowCardinality(String) DEFAULT '',   -- matched_exact / approximate / … / ''
    geocoded_at Nullable(DateTime64(3,'UTC')),
    is_current Bool DEFAULT true,                  -- versioned tombstone (see below)
    -- provenance (identical to se_company_info)
    sources Array(String), source_record_uids Array(String), evidence_hashes Array(String),
    evidence_set_hash FixedString(64) MATERIALIZED lower(hex(SHA256(arrayStringConcat(
        arraySort(arrayMap(x -> toString(x), evidence_hashes)), '\n')))),
    correction_ids Array(UUID) DEFAULT [], source_run_id String, resolved_at DateTime64(3,'UTC')

`ENGINE = ReplacingMergeTree(resolved_at) ORDER BY (company_id, address_key)`; constraints
`has_company`, `has_evidence`. No `suggestion_id`, no model columns — nothing model-written exists here.

**Set replacement.** Re-resolving a company recomputes its full address set: current keys are
published `is_current = true`; keys present in the previous resolution but no longer produced are
republished `is_current = false` (versioned tombstone). Readers always filter
`FINAL … WHERE is_current`. This is the one mechanism `info` does not have.

**Merge rules (`address_rules.py`, pure):** normalize each artifact row; identical
`(address_type, normalized)` across sources → one row, `sources` in precedence order
`bolagsverket › scb` for field values (Bolagsverket is the registration authority for the postal
address), every contributing uid/hash recorded; differing addresses → separate rows. Geocode:
resolve `address_key → address_id` through `se_company_address_members_current` /
`se_company_address_links_current` and read `se_address_geocodes_current`; store the outcome.
A geocode change is evidence: the change scan's per-source `observed_at` terms are joined by a
geocode term (max geocode snapshot version vs `resolved_at`) so re-geocoding re-resolves only
affected companies.

## 5. Ledger

`se_company_address_correction` — identical shape to `se_company_info_correction`. Kinds:
`override_field` (payload: any subset of the address text fields, keyed by `address_key`),
`reject_address` (payload: `{"address_key": …}` — the row is published `is_current = false`),
`undo`. Staleness by `evidence_set_hash` as in info. Sensor `se_company_address_correction_sensor`
(STOPPED until switch-on), review job scoped by `company_ids`, weekly `se_company_address_weekly`
(offset from the info schedule), `execute` gate + preview identical to info.

## 6. Backoffice

The company area's Address tab switches from the raw chain to `se_company_address FINAL … WHERE
is_current` with the corrections UI (override / reject / undo — the info page's pattern), sources
badges and the "evidence changed" staleness marker; the geocode block stays as it renders today.
A `/admin/se/company-address/corrections` ledger page mirrors the info one.

## 7. Retirements (owner-gated, after the final serves)

`se_company_addresses_canonical_current` (no reader), `se_company_address_display_current` (+dbt
build; dead geocode stubs), and — once the parity AssetCheck is retired — the legacy per-company
geocoder pair (`se_company_address_geocodes`, `se_company_address_geocode_results`). The shared
identity chain (`se_addresses_current`, links, members, `se_address_geocodes_current`) STAYS — it
is the geocode augmentation source.

## 8. Out of scope

Other countries; the address_resolution/golden-corpus geocoder itself; Lantmäteriet; changing the
public company page (switch-over is a later, separate decision — same rule as descriptions).
