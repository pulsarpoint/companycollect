# Latvia Company Profile — Mapping Report

One authoritative open source (UR Register of Enterprises, **CC0-1.0 / public domain**), keyed on the **regcode**
(11-digit). Financials join via statement_id/file_id then `legal_entity_registration_number` → regcode.

| Profile path | Source | Source path | Join key | Freshness | Access/License | Precedence | Notes |
|---|---|---|---|---|---|---|---|
| registration.regcode | ur_register | regcode | self | daily | public / CC0 | authoritative | 11-digit |
| registration.register_type | ur_register | regtype_text | regcode | daily | public / CC0 | authoritative | Komercreģistrs |
| tax_identifiers.vat_id | vid_vat | LV + regcode | regcode | real-time | public / validation | derived | not in register |
| tax_identifiers.vat_valid | vid_vat | valid | vat_id | real-time | public / validation | enrichment | point-in-time |
| legal_identity.legal_name | ur_register | name | regcode | daily | public / CC0 | authoritative | |
| legal_identity.legal_form | ur_register | type_text (+ type) | regcode | daily | public / CC0 | authoritative | SIA/AS/IK |
| status.value | ur_register | terminated + closed | regcode | daily | public / CC0 | authoritative | derived |
| incorporation.registration_date | ur_register | registered | regcode | daily | public / CC0 | authoritative | |
| incorporation.termination_date | ur_register | terminated | regcode | daily | public / CC0 | authoritative | |
| registered_location.* | ur_register | address / index / atvk | regcode | daily | public / CC0 | authoritative | |
| share_capital | ur_officers_members | equity-capitals (pamatkapitāls) | regcode | regular | public / CC0 | authoritative | EUR |
| officers[] | ur_officers_members | amatpersonas | regcode | regular | public / CC0 | authoritative | **PII (GDPR)** |
| members[] | ur_officers_members | dalībnieki | regcode | regular | public / CC0 | authoritative | registered owners; **PII** |
| beneficial_owners[] | ur_beneficial_owners | beneficial_owners.csv | regcode | regular | public / CC0 | authoritative | open BO; **PII** |
| financial_statements[] | ur_financial_statements | financial_statements + balance_sheets + income_statements + cash_flow | statement_id/file_id → regcode | annual | public / CC0 | authoritative | **structured**, EUR, employees |

## Precedence Rules

1. **Single authoritative source.** Everything comes from the UR open data (CC0). No aggregator to reconcile;
   the only cadence difference is daily (register) vs annual (financials).
2. **Financial join is two-step.** Pivot `balance_sheets`/`income_statements`/`cash_flow_statements` by
   `statement_id` (= `financial_statements.id`) and `file_id` into a per-report statement, then join
   `legal_entity_registration_number` → register `regcode`.
3. **Three person/ownership layers, distinct:** officers (amatpersonas), registered members (dalībnieki),
   beneficial owners (patiesie labuma guvēji). Never conflate.
4. **VAT** = `LV` + regcode (derivable; VIES/VID validates).
5. **data.gov.lv** is the CKAN access/refresh layer.

## Missing-Data Notes

- **No NACE/activity code** in the register CSV (available via other UR/CSP datasets if needed).
- **No separate tax id** (VAT = LV + regcode).
- **Currency** EUR; pre-2014 financials may be LVL; apply `rounded_to_nearest`.
- **GDPR**: officers, members and beneficial owners are personal data — lawful basis + retention; no direct
  marketing. CC0 governs IP reuse only.
