# State Secretary of State Business Registries Field Catalog (PLANNING-ONLY / GENERIC)

> **Planning-only, generic source.** This represents the 50 states + DC as a class. There is **no common schema** — each jurisdiction has its own field names, codes, formats, freshness, access model, and price. **Colorado is cataloged separately** (`colorado_business_entities`) as the concrete open exemplar with observed data. No bulk dataset was downloaded for the generic class; many state bulk feeds are paid or require per-state legal agreements.

## Source Summary

- Country: United States
- Source type: official_registry_state (51 jurisdictions)
- Organization: Individual state Secretary of State offices
- URL: https://www.nass.org/can-i-vote/business-services
- License: varies by state — **per-state legal review required** before bulk reuse
- Access: free online search everywhere; bulk frequently paid/authenticated (e.g. AZ $2,000+/yr, SC $12,000/yr UCC, NC $750 setup + $250/yr). A minority are free/open (Colorado, and reportedly Oregon, Connecticut, Iowa, Minnesota).
- Freshness: varies
- Record shape: varies — no common schema
- Primary keys: per-state entity id
- Join keys: `state_code + ':' + state_entity_id`

## Fields (common conceptual fields only)

| Path | Source field | Meaning | Type | Semantic type | Notes |
|---|---|---|---|---|---|
| `<state>.entity_name` | entity_name | Entity name | string | legal_name | Authoritative for private companies |
| `<state>.entity_type` | entity_type | Legal form | string | legal_form | Per-state code sets |
| `<state>.formation_date` | formation_date | Formation date | date | date | Format varies |
| `<state>.status` | status | Standing | string | status | Vocabulary varies |
| `<state>.registered_agent` | registered_agent | Service-of-process agent | string | person | Not an owner |

## Interpretation Notes

- **This is the only authoritative source for the bulk of US private companies** — registration is a state function and there is no federal/national private-company register. Comprehensive coverage requires aggregating 51 jurisdictions.
- **No universal field exists.** Treat each state as its own source with its own per-source catalog when actually ingested (as done for Colorado). The fields above are conceptual placeholders for cross-state mapping, not a real schema.
- **Access is the main obstacle, not data shape.** Free public *search* exists everywhere; *bulk* download is the bottleneck — paid in many states, free/open in a few. Read each state's bulk-data agreement individually; some prohibit marketing/solicitation use.
- **Ownership/officers** are rarely published openly; the registered agent (service of process) is the commonly available "person" field.
- No `sample_record.json` — generic class with no single downloaded dataset.
