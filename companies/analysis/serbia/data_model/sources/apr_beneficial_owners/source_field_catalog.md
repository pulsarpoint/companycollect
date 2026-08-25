# APR Central Register of Beneficial Owners Field Catalog

> **PLANNING-ONLY / RESTRICTED PERSONAL DATA.** No beneficial-owner record was
> copied. The CEV portal redirects to APR eID/SSO, and the automated service is a
> separate contracted source. This catalog is the target semantic contract
> derived from APR's current public documentation and the 2025 Act—not a claim
> about unpublished JSON/XML paths.

## Source Summary

- Register: Centralna evidencija stvarnih vlasnika (CEV)
- APR overview: https://www.apr.gov.rs/registri/centralna-evidencija-stvarnih-vlasnika/o-evidenciji.2399.html
- Current law used for the field contract: https://www.apr.gov.rs/upload/Portals/0/zakoni%20uredbe%20pravilnici/Zakoni/2025/Zakon_o_Centralnoj_evidenciji_stvarnih_vlasnika__19_2025_9__51_2025_28.pdf
- Access: eID/SSO for the portal; justified-interest restrictions apply to sensitive identifiers/documents; automated delivery requires a separate agreement
- Join key: `maticni_broj`
- Actual payload path, authentication protocol, pagination/change semantics and fees: not publicly documented

## Target Record Shape

| Group | Target fields | Notes |
|---|---|---|
| Identity regime | `person_kind` | domestic, foreign, refugee/displaced |
| Person | `name` | personal data |
| Sensitive identifier | `personal_identifier_kind`, `personal_identifier_hmac`, `personal_identifier_issuing_country_code` | never store JMBG/passport/card value raw; HMAC only if lawful and necessary |
| Birth | `birth_date`, `birth_place`, `birth_country_code` | field-level access control |
| Residence/status | `residence_country_code`, `stay_country_code`, `citizenship_country_codes[]` | country codes normalized after receipt |
| Legal basis | `basis_code`, `basis_label_raw` | preserve source and normalize APR OSV/TR codes |
| Control | `ownership_percentage`, `voting_rights_percentage` | relevant to OSV1 variants |
| Dates | `acquired_on`, `registered_on`, `documents_registered_on` | business-effective and register-recording dates are distinct |
| Documents | `has_supporting_documents`, `supporting_document_count` | document bodies do not belong in ClickHouse |
| Trust | `trust_*` | legal form/name/address/id/origin/relationship for OSV4/TR |
| Quality | `has_discrepancy`, `discrepancy_note` | visibility must be confirmed in the contract |
| History/provenance | `is_present`, run/record/payload hashes, `observed_at` | mapper envelope; supports tombstones and current-state resolution |

## Basis Codes

- `OSV1`, `OSV1A/N`, `OSV1A/P`, `OSV1B/N`, `OSV1B/P`: direct/indirect share or voting ownership at the statutory threshold.
- `OSV2`: dominant influence over business conduct and decisions.
- `OSV3`: decisive financing influence.
- `OSV4/1`–`OSV4/5`: trust roles/relationships.
- `OSV5/1`–`OSV5/3`: foundation/endowment relationships.
- `OSV6`, `OSV7`: representative/management fallback bases where another beneficial owner cannot be identified.
- `TR1`–`TR5`: trust-specific roles.

The raw code and label should always be retained. Ownership and voting
percentages are nullable because non-ownership control bases do not have a
meaningful percentage.

## Security Boundary

Raw JMBG, passport, foreign identity-card, foreigner-number and refugee-card
values must be removed before ClickHouse. When deterministic linking is an
approved purpose, load only `HMAC-SHA256(secret, normalized_identifier)`. Keep
the HMAC key outside Dagster, ClickHouse and source artifacts, rotate it under a
documented policy, and restrict birth/discrepancy fields to authorized roles.

Supporting documents need a separate encrypted object store, retention policy
and audit trail. The proposed ClickHouse table stores only availability/count
metadata, not the documents or unrestricted object keys.
