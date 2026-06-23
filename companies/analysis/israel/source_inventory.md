# Israel - Source Inventory

| Source | Org | Type | Access | Formats | License | Status |
|---|---|---|---|---|---|---|
| `ica_companies` | Ministry of Justice / Companies Registrar | official registry | public datastore API | JSON, CSV | other-open | **recommended** |
| `ica_partnerships` | Ministry of Justice / Companies Registrar | partnerships registry | public | JSON, CSV | other-open | useful secondary |
| `ica-changes` | Ministry of Justice / Companies Registrar | changes/events | public | JSON, CSV | other-open | useful secondary |

## Roles

- `ica_companies` - company registry spine keyed on company number.
- `ica_partnerships` - partnership registry, separate entity type.
- `ica_changes` - change/event feed for companies and partnerships.

## Join keys

Use **company number** (`מספר חברה`) as the registration id.
