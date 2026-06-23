# Jordan - Source Inventory

| Source | Org | Type | Access | Formats | License | Status |
|---|---|---|---|---|---|---|
| Table of individual institutions 2 | Ministry of Industry Trade & Supply | sole-proprietor / individual institution register | public bulk | CSV, XLSX, JSON, XML | Open Jordanian License | useful secondary |
| Commercial registry | Ministry of Industry Trade & Supply | aggregate/statistical commercial registry | public bulk | CSV | Open Jordanian License | not company-level |
| Listed Companies at the ASE | Amman Stock Exchange | listed company list | public bulk | CSV | Open Jordanian License | sample_only |

## Roles

- `individual_institutions_2` - public register-like data for individual institutions; includes proprietor names.
- `commercial_registry_aggregate` - aggregate counts, not a usable company master.
- `ase_listed_companies` - listed-company name list only.

## Join keys

For individual institutions, preserve `رقم_المؤسسات_الفردية` and `الرقم_الوطني`.
These are not equivalent to a complete company registration key.
