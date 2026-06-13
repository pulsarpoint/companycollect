# Denmark Company Profile — Mapping & Precedence

This maps each combined-profile field to its source field, join key, freshness, and
license/access status, and documents precedence and missing-data rules.

## Join keys

- **`cvrNummer` (8-digit)** is the universal join key across every CVR layer: base register
  (`cvr_permanent`), filing metadata (`cvr_offentliggoerelser`), XBRL facts
  (`cvr_regnskab_xbrl` via `gsd:IdentificationNumberCvrOfReportingEntity`), and registration
  texts (`cvr_registreringstekster`). Format as a zero-padded 8-digit string.
- **`pNummer` (10-digit)** joins production units to their parent company via
  `produktionsenhed.virksomhedsrelation[].cvrNummer`.
- **`LEI` (ISO 17442)** optionally links listed/IFRS filers across XBRL and GLEIF.
- **`filing_id` (`_id` URN)** is the per-filing key linking `cvr_offentliggoerelser` to the
  documents parsed in `cvr_regnskab_xbrl`.

## Mapping table

| Profile path | Source | Source path | Join key | Freshness | License/Access | Precedence / Notes |
|---|---|---|---|---|---|---|
| registration.cvr_nummer | cvr_permanent | cvrNummer | cvrNummer | near real-time | free / credentials | Canonical id. Also present on every other source. |
| registration.vat_id | cvr_permanent | cvrNummer (derived) | cvrNummer | near real-time | free / credentials | Derived: `'DK'+cvrNummer`. |
| registration.lei | cvr_regnskab_xbrl | gsd:LegalEntityIdentifierOfReportingEntity | cvrNummer | per filing | free / open | Listed/IFRS only; null otherwise. |
| registration.incorporation_date | cvr_permanent | stiftelsesDato | cvrNummer | near real-time | free / credentials | Authoritative founding date. |
| legal_identity.navn | cvr_permanent | navne[] (gyldigTil==null) | cvrNummer | near real-time | free / credentials | **Precedence: CVR over XBRL.** Fallback to gsd:NameOfReportingEntity when base register unavailable. |
| legal_identity.previous_names | cvr_permanent | navne[] (closed periods) | cvrNummer | near real-time | free / credentials | History. |
| legal_identity.legal_form | cvr_permanent | virksomhedsform[].virksomhedsformkode | cvrNummer | near real-time | free / credentials | Current entry. |
| legal_identity.purpose | cvr_permanent | attributter[type=FORMÅL] | cvrNummer | near real-time | free / credentials | |
| legal_identity.share_capital | cvr_permanent | attributter[type=KAPITAL]+currency | cvrNummer | near real-time | free / credentials | Parse amount + currency. |
| status.current_status | cvr_permanent | virksomhedsstatus[].status (current) | cvrNummer | near real-time | free / credentials | Or virksomhedMetadata.sammensatStatus. |
| status.dissolution_date | cvr_permanent | livsforloeb[].periode.gyldigTil | cvrNummer | near real-time | free / credentials | Close of last lifecycle period when OPLØST. |
| status.advertising_protected | cvr_permanent | reklamebeskyttelse | cvrNummer | near real-time | free / credentials | **License gate** for marketing use. |
| activity.primary | cvr_permanent | hovedbranche[].branchekode/tekst | cvrNummer | near real-time | free / credentials | DB07/NACE. |
| activity.secondary | cvr_permanent | bibranche1/2/3[] | cvrNummer | near real-time | free / credentials | |
| registered_location.* | cvr_permanent | beliggenhedsadresse[] (current) | cvrNummer | near real-time | free / credentials | XBRL gsd:Address* is a fallback. |
| contact.* | cvr_permanent | elektroniskPost/telefonNummer/hjemmeside | cvrNummer | near real-time | free / credentials | Gate marketing on advertising_protected. |
| employment[] | cvr_permanent | aarsbeskaeftigelse[] / erstMaanedsbeskaeftigelse[] | cvrNummer | near real-time | free / credentials | Interval bands, not exact. |
| production_units[] | cvr_permanent | produktionsenhed (virksomhedsrelation.cvrNummer) | cvrNummer→pNummer | near real-time | free / credentials | Separate index. |
| participants[] | cvr_permanent | deltagerRelation[] + deltager index | cvrNummer | near real-time | free / credentials | **PERSONAL DATA — GDPR.** |
| financial_filings[] | cvr_offentliggoerelser | _source.* + dokumenter[] | cvrNummer / _id | near real-time | **free / open** | Discovery layer; no auth. |
| financial_statements[].period* | cvr_regnskab_xbrl | gsd:ReportingPeriod{Start,End}Date | cvrNummer / filing_id | per filing | free / open | Cross-check with regnskabsperiode. |
| financial_statements[].reporting_class | cvr_regnskab_xbrl | fsa:ClassOfReportingEntity | cvrNummer | per filing | free / open | A–D. |
| financial_statements[].currency | cvr_regnskab_xbrl | xbrli:unit (iso4217) | cvrNummer | per filing | free / open | Per fact's unitRef. |
| financial_statements[].consolidated | cvr_regnskab_xbrl | cmn:ConsolidatedSoloDimension | cvrNummer | per filing | free / open | Must honour before storing facts. |
| financial_statements[].line_items[] | cvr_regnskab_xbrl | fsa:* facts | cvrNummer | per filing | free / open | Not in interim identity sample; present in full reports. |
| financial_statements[].board[] | cvr_regnskab_xbrl | cmn:NameAndSurnameOfMemberOf{Exec,Sup}Board | cvrNummer | per filing | free / open | **PERSONAL DATA — GDPR.** |
| change_history[] | cvr_registreringstekster | provisional | cvrNummer | near real-time | free / credentials | **PLANNING-ONLY** — schema unconfirmed. |
| source_provenance[] | all | n/a | n/a | n/a | n/a | One entry per contributing source. |

## Source precedence

1. **Identity / status / structure** → `cvr_permanent` is authoritative (official base
   register). When it is unavailable (no credentials yet), `cvr_regnskab_xbrl` `gsd:` fields
   (name, address, LEI) are a partial fallback for filers that have published a report.
2. **Financial figures** → `cvr_regnskab_xbrl` (parsed XBRL) is authoritative for line items;
   `cvr_offentliggoerelser` only provides the filing metadata and document links.
3. **History** → `cvr_permanent` period-bearing arrays (`navne`, `virksomhedsstatus`,
   `livsforloeb`) first; `cvr_registreringstekster` adds narrative change events later.
4. Third-party wrappers (`cvr.dev`, `cvrapi.dk`, `apicvr.dk`) are **never** preferred over the
   official `distribution.virk.dk` sources for ingestion; use only for ad-hoc lookups.

## Missing-data rules

- If the base register is not yet accessible (credentials pending), build a **partial profile**
  from the open financial sources: `cvr_nummer`, `vat_id` (derived), `lei`, name/address from
  XBRL, and full `financial_filings` / `financial_statements`. Leave `legal_form`, `status`,
  `activity`, `employment`, `participants`, `production_units` null and note the gap.
- Old filings link only to `image/tiff` / `application/pdf` — no structured figures available;
  populate `financial_filings[].documents` but leave `financial_statements` empty for those.
- Honour `reklamebeskyttelse`: when true, suppress `contact` from any marketing-facing use.
