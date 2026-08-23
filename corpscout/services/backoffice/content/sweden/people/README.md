# Sweden company-person role mappings

`role_mappings.sqlite` starts with mappings copied from the three source-owned
Dagster mapping modules. Exact original-role mappings can also be curated from
the Sweden people sources page in backoffice; do not edit SQLite rows manually.

Refresh the artifact after changing a Dagster role mapping:

```bash
pnpm sync:sweden-role-mappings
```

The sync preserves rows created by backoffice and lets a Dagster-owned mapping
replace an identical curated key when that mapping is promoted into source
code. Backoffice combines the mappings with distinct original role values read
from ClickHouse at request time.
