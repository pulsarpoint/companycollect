# Colorado Business Entities Field Catalog

## Source Summary

- Country: United States
- Source type: official_registry_state (Colorado) — exemplar of a free, open state registry
- Organization: Colorado Secretary of State / Colorado Information Marketplace (Socrata)
- URL: https://data.colorado.gov/resource/4ykn-tg5h.json (dataset 4ykn-tg5h)
- License: Open data (Socrata); verify Colorado open-data terms; attribution recommended
- Access: public (Socrata SODA; app token recommended for high volume, not required)
- Freshness: regularly updated; 1M+ entities back to the 1800s
- Record shape: JSON array, one object per entity; optional/mailing/agent fields are sparse
- Primary keys: `entityid` (unique within Colorado)
- Join keys: `entityid` → global key `CO:` + entityid

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| entityid | entityid | CO entity id | string | identifier | 20251665680 | Unique within CO; prefix with state |
| entityname | entityname | Entity name | string | legal_name | The Luxe Lab LLC | May carry appended delinquency note |
| principaladdress1 | principaladdress1 | Principal street | string | address | 660 Willow Wood Ln | |
| principalcity | principalcity | Principal city | string | address | Denver | |
| principalstate | principalstate | Principal state | string | geography | CO, IL | Can be out-of-state |
| principalzipcode | principalzipcode | Principal ZIP | string | address | 80249 | |
| principalcountry | principalcountry | Principal country | string | geography | US | |
| mailingaddress1 | mailingaddress1 | Mailing street | string | address | PO BOX 719 | Sparse |
| mailingcity/state/zip/country | mailing* | Mailing address parts | string | address/geography | CORTEZ, CO | Sparse |
| entitystatus | entitystatus | Standing | string | status | Good Standing, Delinquent | Other values exist |
| jurisdictonofformation | jurisdictonofformation | Formation jurisdiction | string | geography | CO, TX | **Misspelled key** — preserve exactly |
| entitytype | entitytype | Legal form code | string | legal_form | DLLC, FPC | CO SoS abbreviations |
| agentfirstname/middlename/lastname/organizationname | agent* | Registered agent (person or org) | string | person | C T CORPORATION SYSTEM | Service-of-process contact, not owner |
| agentprincipal* | agentprincipal* | Agent address | string | address | Centennial, CO | |
| entityformdate | entityformdate | Formation/registration date | datetime | date | 1978-02-28T00:00:00.000 | True incorporation date |

## Interpretation Notes

- **Why Colorado:** there is no national US private-company register; formation happens per state. Colorado is the **exemplar free/open state registry** (most states paywall bulk data). This catalog models Colorado specifically; the same *shape* (entity id, name, address, status, type, registered agent, formation date) recurs across other states' registries but with different field names — do not assume identical keys elsewhere.
- **Global key:** `entityid` is unique only within Colorado. Build the cross-state company_id as `state_code + ':' + entityid` (e.g. `CO:20251665680`).
- **`jurisdictonofformation` is misspelled in the source** (missing an 'i'). Preserve the exact key when parsing. When its value is not `CO`, the entity is *foreign* to Colorado (formed in another state, e.g. `TX`), and `entitytype` will use a Foreign prefix (e.g. `FPC` = Foreign Profit Corporation). The authoritative home register for such entities is the formation state, not Colorado.
- **Registered agent ≠ owner/officer.** Colorado does not publish beneficial owners or directors here. The agent is the legal contact for service of process and may be a commercial agent (e.g. C T Corporation System) serving thousands of companies. Either the person-name fields or `agentorganizationname` is populated.
- **Status can leak into the name.** Non-good-standing records sometimes append a status note to `entityname` (e.g. `"SOUTHWEST CONTRACTING, LLC, Delinquent May 1, 2016"`). Clean the name and rely on `entitystatus` for standing.
- **`entityformdate`** is the most reliable *incorporation/registration date* across all four analyzed US sources (SEC ticker file lacks it, IRS only has a ruling month). ISO8601 with `.000` millis; the time component is filler.
- **Sparse optional fields:** mailing-address and agent-mailing fields appear only when they differ from the principal/agent-principal addresses.
